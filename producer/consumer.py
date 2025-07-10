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
import random
import string
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import defaultdict
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

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
# Change latest_event_by_match to store per-inning events
# latest_event_by_match = {}
latest_event_by_match = {}  # {match_id: {inning_no: latest_event}}
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

restart_lock = Lock()

# Track the current Kafka consumer group id
global_group_id = 'cricket-consumer-group'

# --- PostgreSQL connection setup (adjust as needed) ---
# Example: db_conn = psycopg2.connect(dbname='yourdb', user='youruser', password='yourpass', host='localhost')
db_conn = psycopg2.connect(dbname='cricket_alarm', user='postgres', password='admin',     host='host.docker.internal')

# --- API: Register/Update User with FCM Token ---
@app.route('/register', methods=['POST'])
def register_user():
    data = request.get_json()
    user_id = data.get('user_id')
    name = data.get('name')
    fcm_token = data.get('fcm_token')
    if not user_id or not name or not fcm_token:
        return jsonify({'error': 'user_id, name, and fcm_token are required'}), 400
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, name, fcm_token)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name, fcm_token = EXCLUDED.fcm_token
                """, (user_id, name, fcm_token)
            )
            db_conn.commit()
        return jsonify({'status': 'registered', 'user_id': user_id, 'name': name}), 200
    except Exception as e:
        db_conn.rollback()
        return jsonify({'error': str(e)}), 500

# --- API: Subscribe to Player Milestone Event ---
@app.route('/subscribe', methods=['POST'])
def subscribe_player():
    data = request.get_json()
    user_id = data.get('user_id')
    player_id = data.get('player_id')
    event_type = data.get('event_type')
    if not user_id or not player_id or not event_type:
        return jsonify({'error': 'user_id, player_id, and event_type are required'}), 400
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subscriptions (user_id, player_id, event_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, player_id, event_type) DO NOTHING
                """, (user_id, player_id, event_type)
            )
            db_conn.commit()
        return jsonify({'status': 'subscribed', 'user_id': user_id, 'player_id': player_id, 'event_type': event_type}), 200
    except Exception as e:
        db_conn.rollback()
        return jsonify({'error': str(e)}), 500

# --- API: Unsubscribe from Player Milestone Event ---
@app.route('/unsubscribe', methods=['POST'])
def unsubscribe_player():
    data = request.get_json()
    user_id = data.get('user_id')
    player_id = data.get('player_id')
    event_type = data.get('event_type')
    if not user_id or not player_id or not event_type:
        return jsonify({'error': 'user_id, player_id, and event_type are required'}), 400
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM subscriptions WHERE user_id=%s AND player_id=%s AND event_type=%s
                """, (user_id, player_id, event_type)
            )
            db_conn.commit()
        return jsonify({'status': 'unsubscribed', 'user_id': user_id, 'player_id': player_id, 'event_type': event_type}), 200
    except Exception as e:
        db_conn.rollback()
        return jsonify({'error': str(e)}), 500

# --- API: List All Subscriptions for a User ---
@app.route('/subscriptions', methods=['GET'])
def list_subscriptions():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400
    try:
        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT player_id, event_type FROM subscriptions WHERE user_id=%s
                """, (user_id,)
            )
            rows = cur.fetchall()
        return jsonify({'user_id': user_id, 'subscriptions': rows}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# --- API: List All Possible Milestone Event Types ---
@app.route('/event-types', methods=['GET'])
def list_event_types():
    # This could be dynamic, but for now, return a static list
    event_types = [
        '50_runs',
        '100_runs',
        'wicket',
        '5_wickets',
        'hat_trick',
        '200_runs',
        # Add more as needed
    ]
    return jsonify({'event_types': event_types}), 200

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
        'batting_team': event.get('batting_team_name') or event.get('batting_team'),
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
            innings = latest_event_by_match.get(match_id, {})
            if not innings:
                logger.info(f"/live-scores requested for match_id {match_id} but no events consumed yet.")
                return jsonify([])
            return jsonify({str(inning): enrich_with_non_striker_stats(ev) for inning, ev in innings.items()})
        else:
            return jsonify({mid: {str(inning): enrich_with_non_striker_stats(ev) for inning, ev in innings.items()} for mid, innings in latest_event_by_match.items()})

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
        for match_id, innings in latest_event_by_match.items():
            for inning_no, latest in innings.items():
                team_names = []
                team_scores = []
                teams = latest.get('teams', [])
                for team in teams:
                    team_names.append(team.get('name'))
                    team_scores.append(latest.get('Score'))
                non_striker_stats = latest.get('non_striker_stats')
                matches.append({
                    'match_id': match_id,
                    'inning_no': inning_no,
                    'series': latest.get('series'),
                    'venue': latest.get('venue'),
                    'teams': team_names,
                    'scores': team_scores,
                    'toss_winner': latest.get('toss_winner'),
                    'toss_decision': latest.get('toss_decision'),
                    'current_over': latest.get('over'),
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
                if matches[-1]['non_striker']['runs'] is None:
                    non_striker_stats = latest.get('non_striker_stats')
                    if non_striker_stats and 'runs' in non_striker_stats:
                        matches[-1]['non_striker']['runs'] = non_striker_stats['runs']
        return jsonify(matches)

def get_live_matches_data():
    matches = []
    for match_id, innings in latest_event_by_match.items():
        for inning_no, latest in innings.items():
            team_names = []
            team_scores = []
            teams = latest.get('teams', [])
            for team in teams:
                team_names.append(team.get('name'))
                team_scores.append(latest.get('Score'))
            non_striker_stats = latest.get('non_striker_stats')
            match = {
                'match_id': match_id,
                'inning_no': inning_no,
                'series': latest.get('series'),
                'venue': latest.get('venue'),
                'teams': team_names,
                'scores': team_scores,
                'toss_winner': latest.get('toss_winner'),
                'toss_decision': latest.get('toss_decision'),
                'current_over': latest.get('over'),
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

# --- FCM HTTP v1 Setup ---
# Place your service account JSON in the project and set these environment variables:
#   FCM_SERVICE_ACCOUNT_FILE=service-account.json
#   FCM_PROJECT_ID=your-firebase-project-id
FCM_SERVICE_ACCOUNT_FILE = os.getenv("FCM_SERVICE_ACCOUNT_FILE", "cricket.json")
FCM_PROJECT_ID = os.getenv("FCM_PROJECT_ID", "cricket-c7b8f")

# --- FCM HTTP v1 Notification Function ---
def send_fcm_notification(fcm_token, title, body, data=None):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            FCM_SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
        credentials.refresh(GoogleAuthRequest())
        access_token = credentials.token
        url = f"https://fcm.googleapis.com/v1/projects/{FCM_PROJECT_ID}/messages:send"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; UTF-8",
        }
        # Convert all data values to strings
        data = {k: str(v) for k, v in (data or {}).items()}
        message = {
            "message": {
                "token": fcm_token,
                "notification": {
                    "title": title,
                    "body": body
                },
                "data": data
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(message))
        if response.status_code == 200:
            logger.info(f"[FCM v1] Sent to {fcm_token}: {title} - {body} | {data}")
        else:
            logger.error(f"[FCM v1] Failed: {response.status_code} {response.text}")
        return response.json()
    except Exception as e:
        logger.error(f"[FCM v1] Exception: {e}")
        return None

# --- Milestone notification state (to avoid duplicate notifications per match/player/milestone) ---
notified_milestones = defaultdict(set)  # {(match_id, player_id, event_type): set([milestone_value])}

# --- Milestone check and notification logic ---
def check_and_notify_milestones(event):
    match_id = event.get('match_id')
    # Batsman milestones
    batsman_id = event.get('batsman_id')
    batsman_name = event.get('batsman_name')
    runs = (event.get('batsman_stats') or {}).get('runs', 0)
    for milestone in [50, 100, 200]:
        key = (match_id, batsman_id, f"{milestone}_runs")
        if runs >= milestone and milestone not in notified_milestones[key]:
            notify_subscribers(batsman_id, f"{milestone}_runs", batsman_name, runs, match_id)
            notified_milestones[key].add(milestone)
    # Bowler milestones
    bowler_id = event.get('bowler_id')
    bowler_name = event.get('bowler_name')
    wickets = (event.get('bowler_stats') or {}).get('wickets', 0)
    for milestone in [1, 3, 5]:
        key = (match_id, bowler_id, f"{milestone}_wickets")
        if wickets >= milestone and milestone not in notified_milestones[key]:
            notify_subscribers(bowler_id, f"{milestone}_wickets", bowler_name, wickets, match_id)
            notified_milestones[key].add(milestone)

# --- Notify all subscribers for a player/event_type ---
def notify_subscribers(player_id, event_type, player_name, value, match_id):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM subscriptions WHERE player_id=%s AND event_type=%s",
            (player_id, event_type)
        )
        user_ids = [row[0] for row in cur.fetchall()]
        for user_id in user_ids:
            cur.execute("SELECT fcm_token FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            if row and row[0]:
                send_fcm_notification(
                    row[0],
                    title="Milestone reached!",
                    body=f"Player {player_name} reached {event_type.replace('_', ' ')} ({value}) in match {match_id}",
                    data={"player_id": player_id, "event_type": event_type, "match_id": match_id, "value": value}
                )

def consume_loop():
    global latest_event_by_match, kafka_connected, num_events_processed, global_group_id
    logger.info(f"Starting consume_loop thread... (group.id={consumer_conf['group.id']})")
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
                    inning_no = event.get("InningNo", 1)
                    if match_id not in latest_event_by_match:
                        latest_event_by_match[match_id] = {}
                    latest_event_by_match[match_id][inning_no] = event
                    logger.info(f"[consume_loop] Updated latest_event_by_match[{match_id}][{inning_no}]: {latest_event_by_match[match_id][inning_no]}")
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
                # --- Milestone notification logic ---
                check_and_notify_milestones(event)
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

@app.route('/restart-stream', methods=['POST'])
def restart_stream():
    global consumer, all_events_by_match, latest_event_by_match, kafka_connected, num_events_processed, global_group_id
    with restart_lock:
        logger.info("Received request to restart stream and replay all events from the beginning.")
        # Stop the current consumer
        globals()['shutdown_flag'] = True
        time.sleep(1)  # Give the consumer thread a moment to exit
        # Clear in-memory state
        with lock:
            all_events_by_match.clear()
            latest_event_by_match.clear()
            teams_by_id.clear()
            players_by_team.clear()
            players_by_id.clear()
            num_events_processed = 0
        # Generate a new group id (forces Kafka to replay from the beginning)
        new_group_id = f"cricket-consumer-group-replay-{int(time.time())}-{''.join(random.choices(string.ascii_lowercase+string.digits, k=4))}"
        logger.info(f"Restarting consumer with new group id: {new_group_id}")
        consumer_conf['group.id'] = new_group_id
        global_group_id = new_group_id
        # Reset shutdown flag and start a new consumer thread
        globals()['shutdown_flag'] = False
        consumer_thread = Thread(target=consume_loop, daemon=True)
        consumer_thread.start()
        return jsonify({"status": "restarted", "new_group_id": new_group_id}), 200

@app.route('/current-group-id', methods=['GET'])
def get_current_group_id():
    return jsonify({"current_group_id": global_group_id}), 200

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