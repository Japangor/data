# Force threading mode for Flask-SocketIO
# async_mode = 'threading'
async_mode = 'eventlet'  # Use eventlet for proper WebSocket support

import os
import json
import logging
from confluent_kafka import Consumer, KafkaException
from flask import Flask, jsonify, request
from threading import Thread, Lock
from flask_cors import CORS
import time
from flask_socketio import SocketIO, emit
import signal
import sys
import socket
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = 'cricket'

consumer_conf = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'cricket-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = None  # Will be initialized in consume_loop

all_events_by_match = {}
latest_event_by_match = {}
lock = Lock()
kafka_connected = False
num_events_processed = 0

app = Flask(__name__)
CORS(app)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode=async_mode,
    logger=True,  # Enable for debugging
    engineio_logger=True,  # Enable for debugging
    ping_interval=25,  # Lower ping interval for mobile clients
    ping_timeout=60
)

shutdown_flag = False

teams_by_id = {}
players_by_team = {}
players_by_id = {}

def signal_handler(sig, frame):
    global shutdown_flag
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    shutdown_flag = True
    # Give threads a moment to exit
    time.sleep(1)
    sys.exit(0)

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def enrich_with_non_striker_stats(event):
    non_striker_stats = event.get('non_striker_stats')
    return {
        'match_id': event.get('match_id'),
        'series': event.get('series'),
        'venue': event.get('venue'),
        'teams': [team.get('name') for team in event.get('teams', [])],
        'score': event.get('Score'),
        'toss_winner': event.get('toss_winner'),
        'toss_decision': event.get('toss_decision'),
        'current_over': event.get('over'),
        'inning_no': event.get('InningNo'),
        'striker': {
            'name': event.get('batsman_name'),
            'runs': (event.get('batsman_stats') or {}).get('runs'),
        },
        'non_striker': {
            'name': event.get('non_striker_name'),
            'runs': (non_striker_stats or {}).get('runs'),
            'balls': (non_striker_stats or {}).get('balls'),
            'fours': (non_striker_stats or {}).get('fours'),
            'sixes': (non_striker_stats or {}).get('sixes'),
        },
        'bowler': {
            'name': event.get('bowler_name'),
            'stats': event.get('bowler_stats'),
        },
        'timestamp': event.get('timestamp'),
        'commentary': event.get('commentary'),
    }

@app.route('/health', methods=['GET'])
def health():
    with lock:
        return jsonify({
            "status": "ok",
            "kafka_connected": kafka_connected,
            "num_events_processed": num_events_processed,
            "num_matches": len(all_events_by_match),
            "num_latest_events": len(latest_event_by_match)
        })

@app.route('/live-scores', methods=['GET'])
def live_scores():
    match_id = request.args.get('match_id')
    with lock:
        logger.debug(f"[API] latest_event_by_match: {latest_event_by_match}")
        if match_id:
            latest = latest_event_by_match.get(match_id)
            if not latest:
                logger.info(f"/live-scores requested for match_id {match_id} but no events consumed yet.")
                return jsonify([])
            return jsonify(enrich_with_non_striker_stats(latest))
        else:
            return jsonify({mid: enrich_with_non_striker_stats(ev) for mid, ev in latest_event_by_match.items()})

@app.route('/match-history', methods=['GET'])
def match_history():
    match_id = request.args.get('match_id')
    with lock:
        logger.debug(f"[API] all_events_by_match: {all_events_by_match}")
        if match_id:
            events = all_events_by_match.get(match_id, [])
            if not events:
                logger.info(f"/match-history requested for match_id {match_id} but no events consumed yet.")
                return jsonify([])
            return jsonify(events)
        else:
            return jsonify(all_events_by_match)

@app.route('/live-matches', methods=['GET'])
def live_matches():
    with lock:
        logger.debug(f"[API] live-matches: {latest_event_by_match}")
        matches = []
        for match_id, latest in latest_event_by_match.items():
            # Get team names
            team_names = []
            team_scores = []
            teams = latest.get('teams', [])
            for team in teams:
                team_names.append(team.get('name'))
                team_scores.append(latest.get('Score'))
            non_striker_stats = latest.get('non_striker_stats')
            matches.append({
                'match_id': match_id,
                'series': latest.get('series'),
                'venue': latest.get('venue'),
                'teams': team_names,
                'scores': team_scores,
                'toss_winner': latest.get('toss_winner'),
                'toss_decision': latest.get('toss_decision'),
                'current_over': latest.get('over'),
                'inning_no': latest.get('InningNo'),
                'striker': {
                    'name': latest.get('batsman_name'),
                    'runs': (latest.get('batsman_stats') or {}).get('runs'),
                },
                'non_striker': {
                    'name': latest.get('non_striker_name'),
                    'runs': (non_striker_stats or {}).get('runs'),
                    'balls': (non_striker_stats or {}).get('balls'),
                    'fours': (non_striker_stats or {}).get('fours'),
                    'sixes': (non_striker_stats or {}).get('sixes'),
                },
                'bowler': {
                    'name': latest.get('bowler_name'),
                    'stats': latest.get('bowler_stats'),
                },
            })
            # Try to get non-striker runs if available
            non_striker_id = latest.get('non_striker_id')
            if non_striker_id and 'players_by_id' in globals():
                player = players_by_id.get(non_striker_id)
                if player and 'runs' in player:
                    matches[-1]['non_striker']['runs'] = player['runs']
            # If not, try to get from event (if available)
            if matches[-1]['non_striker']['runs'] is None:
                # Sometimes non-striker stats may be in event
                non_striker_stats = latest.get('non_striker_stats')
                if non_striker_stats and 'runs' in non_striker_stats:
                    matches[-1]['non_striker']['runs'] = non_striker_stats['runs']
        return jsonify(matches)

def get_live_matches_data():
    matches = []
    for match_id, latest in latest_event_by_match.items():
        team_names = []
        team_scores = []
        teams = latest.get('teams', [])
        for team in teams:
            team_names.append(team.get('name'))
            team_scores.append(latest.get('Score'))
        non_striker_stats = latest.get('non_striker_stats')
        match = {
            'match_id': match_id,
            'series': latest.get('series'),
            'venue': latest.get('venue'),
            'teams': team_names,
            'scores': team_scores,
            'toss_winner': latest.get('toss_winner'),
            'toss_decision': latest.get('toss_decision'),
            'current_over': latest.get('over'),
            'inning_no': latest.get('InningNo'),
            'striker': {
                'name': latest.get('batsman_name'),
                'runs': (latest.get('batsman_stats') or {}).get('runs'),
            },
            'non_striker': {
                'name': latest.get('non_striker_name'),
                'runs': (non_striker_stats or {}).get('runs'),
                'balls': (non_striker_stats or {}).get('balls'),
                'fours': (non_striker_stats or {}).get('fours'),
                'sixes': (non_striker_stats or {}).get('sixes'),
            },
            'bowler': {
                'name': latest.get('bowler_name'),
                'stats': latest.get('bowler_stats'),
            },
        }
        non_striker_id = latest.get('non_striker_id')
        if non_striker_id and 'players_by_id' in globals():
            player = players_by_id.get(non_striker_id)
            if player and 'runs' in player:
                match['non_striker']['runs'] = player['runs']
        if match['non_striker']['runs'] is None:
            non_striker_stats = latest.get('non_striker_stats')
            if non_striker_stats and 'runs' in non_striker_stats:
                match['non_striker']['runs'] = non_striker_stats['runs']
        matches.append(match)
    return matches

# Periodically emit live-matches to all clients

def emit_live_matches_periodically():
    while not shutdown_flag:
        with lock:
            matches = get_live_matches_data()
        socketio.emit('live-matches', matches)
        time.sleep(5)  # Emit every 5 seconds

@socketio.on('connect')
def handle_connect():
    sid = request.sid
    logger.info(f"WebSocket client connected: {sid}")
    emit('welcome', {'message': 'Connected to cricket live server'})  # Flutter handshake
    with lock:
        matches = get_live_matches_data()
        emit('live-matches', matches)
        if latest_event_by_match:
            emit('new_event', latest_event_by_match)

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    logger.info(f"WebSocket client disconnected: {sid}")

@socketio.on_error_default  # handles all namespaces without an explicit error handler
def default_error_handler(e):
    logger.error(f"SocketIO error: {e}")
    emit('error', {'error': str(e)})

def reconnect_consumer():
    global consumer, kafka_connected
    while not shutdown_flag:
        try:
            consumer = Consumer(consumer_conf)
            consumer.subscribe([topic])
            kafka_connected = True
            logger.info(f"Connected and subscribed to topic: {topic}")
            return
        except Exception as e:
            kafka_connected = False
            logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def consume_loop():
    global latest_event_by_match, kafka_connected, num_events_processed
    logger.info("Starting consume_loop thread...")
    reconnect_consumer()
    while not shutdown_flag:
        try:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                if msg.error().code() == KafkaException._ALL_BROKERS_DOWN:
                    kafka_connected = False
                    logger.error("All brokers down. Attempting to reconnect...")
                    reconnect_consumer()
                continue
            try:
                event = json.loads(msg.value().decode('utf-8'))
                logger.info(f"[consume_loop] Received event: {event}")
                # Validate over and ball
                try:
                    over_val = float(event.get('over', 0) or 0)
                    ball_val = float(event.get('ball', 0) or 0)
                except Exception:
                    logger.warning(f"Skipping event with invalid over/ball: {event}")
                    continue
                with lock:
                    match_id = event.get("match_id")
                    if not match_id:
                        logger.warning("Event missing match_id, skipping.")
                        continue
                    if match_id not in all_events_by_match:
                        all_events_by_match[match_id] = []
                    all_events_by_match[match_id].append(event)
                    logger.info(f"[consume_loop] Appended event to all_events_by_match[{match_id}]. Total events: {len(all_events_by_match[match_id])}")
                    all_events_by_match[match_id].sort(key=lambda e: (
                        float(e.get('over', 0) or 0),
                        float(e.get('ball', 0) or 0)
                    ))
                    latest_event_by_match[match_id] = all_events_by_match[match_id][-1]
                    logger.info(f"[consume_loop] Updated latest_event_by_match[{match_id}]: {latest_event_by_match[match_id]}")
                    num_events_processed += 1
                    # Extract teams and players from event
                    event_teams = event.get('teams', [])
                    for team in event_teams:
                        team_id = team.get('id')
                        if team_id:
                            teams_by_id[team_id] = team
                            players = team.get('players', [])
                            if team_id not in players_by_team:
                                players_by_team[team_id] = set()
                            for player in players:
                                player_id = player.get('id')
                                if player_id:
                                    players_by_team[team_id].add(player_id)
                                    players_by_id[player_id] = player
                logger.info(f"Consumed event: Match {match_id} | Over {event.get('over', '?')} | {event.get('batsman_name', event.get('batsman', '?'))} vs {event.get('bowler_name', event.get('bowler', '?'))} | Score: {event.get('Score', '?')} ")
                socketio.emit('new_event', event)
            except Exception as e:
                logger.error(f"Failed to parse/process event: {e}")
        except Exception as e:
            kafka_connected = False
            logger.error(f"Kafka poll error: {e}. Attempting to reconnect...")
            reconnect_consumer()
    try:
        consumer.close()
        logger.info("Consumer closed.")
    except Exception:
        pass

def run_api():
    try:
        # Try to get the local IP address for Docker/host info
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = 'localhost'
    logger.info(f"Starting Flask API with WebSocket support on port 5000... (async_mode={async_mode})")
    logger.info(f"API should be accessible at: http://localhost:5000/ (if running locally)")
    logger.info(f"If running in Docker, try: http://127.0.0.1:5000/ or http://{local_ip}:5000/")
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except OSError as e:
        logger.error(f"Failed to bind to port 5000: {e}. Is another process using this port?")
        raise

@app.route('/teams', methods=['GET'])
def get_teams():
    team_id = request.args.get('team_id')
    with lock:
        if team_id:
            team = teams_by_id.get(team_id)
            if not team:
                return jsonify({'error': 'Team not found'}), 404
            return jsonify(team)
        else:
            return jsonify(list(teams_by_id.values()))

@app.route('/players', methods=['GET'])
def get_players():
    team_id = request.args.get('team_id')
    with lock:
        if not team_id:
            return jsonify({'error': 'team_id is required'}), 400
        player_ids = players_by_team.get(team_id)
        if not player_ids:
            return jsonify({'error': 'No players found for this team'}), 404
        # Return player details
        return jsonify([players_by_id[pid] for pid in player_ids])

if __name__ == "__main__":
    import eventlet
    import eventlet.wsgi
    logger.info("Starting consumer main...")
    consumer_thread = Thread(target=consume_loop, daemon=True)
    consumer_thread.start()
    # Start periodic live-matches emitter thread
    live_matches_thread = threading.Thread(target=emit_live_matches_periodically, daemon=True)
    live_matches_thread.start()
    time.sleep(2)
    run_api()