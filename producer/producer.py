import os
import time
import json
import logging
from confluent_kafka import Producer, Consumer
from threading import Thread, Event

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = 'cricket'
kafka_config = {'bootstrap.servers': kafka_broker}
producer = Producer(kafka_config)

# Configurable delay between events
DELAY_BETWEEN_EVENTS = int(os.getenv("DELAY_BETWEEN_EVENTS", 5))

# Global restart flag
restart_flag = Event()

# Helper functions

def safe_get(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d

def get_player_name(player_id, teams):
    for team in teams.values():
        player = team.get("Players", {}).get(str(player_id))
        if player:
            return player.get("Name_Full")
    return None

def load_all_matches(base_dir):
    match_folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f))]
    matches = []
    for folder in match_folders:
        match_path = os.path.join(base_dir, folder, 'match.json')
        comm1 = os.path.join(base_dir, folder, 'commentary_all_1.json')
        comm2 = os.path.join(base_dir, folder, 'commentary_all_2.json')
        if os.path.exists(match_path) and os.path.exists(comm1) and os.path.exists(comm2):
            try:
                with open(match_path, encoding='utf-8') as f:
                    match_data = json.load(f)
                # Helper to load commentary and summary
                def load_comm_with_summary(path):
                    with open(path, encoding='utf-8') as f:
                        data = json.load(f)
                        commentary = data.get("Commentary", [])
                        # Don't reverse here - we'll sort properly later
                        # Extract summary fields (everything except Commentary)
                        summary = {k: v for k, v in data.items() if k != "Commentary"}
                        return commentary, summary
                events_1, summary_1 = load_comm_with_summary(comm1)
                events_2, summary_2 = load_comm_with_summary(comm2)
                # Attach summary to each event
                all_events = []
                for ev in events_1:
                    ev["_summary"] = summary_1
                    all_events.append(ev)
                for ev in events_2:
                    ev["_summary"] = summary_2
                    all_events.append(ev)
                matches.append({
                    'folder': folder,
                    'match_data': match_data,
                    'commentary': all_events
                })
            except Exception as e:
                logger.error(f"Failed to load match in {folder}: {e}")
    return matches

def check_restart_signal_via_kafka():
    """Check for restart signals via Kafka restart topic"""
    try:
        # Create a separate consumer for restart signals
        restart_consumer_conf = {
            'bootstrap.servers': kafka_broker,
            'group.id': f'producer-restart-listener-{int(time.time())}-{os.getpid()}',
            'auto.offset.reset': 'earliest',  # Check all messages to catch any restart signals
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'heartbeat.interval.ms': 2000
        }
        restart_consumer = Consumer(restart_consumer_conf)
        restart_consumer.subscribe(['cricket-restart'])
        
        # Poll for messages with short timeout, check multiple messages
        messages_checked = 0
        while messages_checked < 10:  # Check up to 10 messages
            msg = restart_consumer.poll(0.05)
            if msg is None:
                break
            messages_checked += 1
            
            if not msg.error():
                try:
                    restart_signal = json.loads(msg.value().decode('utf-8'))
                    if restart_signal.get('action') == 'restart':
                        logger.info(f"[RESTART SIGNAL] Received restart signal via Kafka: {restart_signal}")
                        restart_consumer.close()
                        return restart_signal.get('timestamp')
                except Exception as e:
                    logger.warning(f"Failed to parse restart signal: {e}")
        
        restart_consumer.close()
        return None
    except Exception as e:
        # Log errors for debugging but don't spam
        if hasattr(check_restart_signal_via_kafka, 'error_count'):
            check_restart_signal_via_kafka.error_count += 1
        else:
            check_restart_signal_via_kafka.error_count = 1
        
        if check_restart_signal_via_kafka.error_count <= 3:
            logger.warning(f"Error checking restart signal: {e}")
        return None

def check_restart_flag_file():
    """Check for restart flag file"""
    try:
        restart_flag_path = "/tmp/cricket_restart_flag"
        if os.path.exists(restart_flag_path):
            with open(restart_flag_path, 'r') as f:
                content = f.read().strip()
                if "FORCE_RESTART_FROM_BEGINNING" in content:
                    logger.info(f"[RESTART FLAG] Found restart flag file: {content}")
                    # Remove the flag file after reading
                    os.remove(restart_flag_path)
                    return True
        return False
    except Exception as e:
        return False

def stream_match_events_with_restart_check(match, delay):
    """Modified stream function that checks for restart signals and calculates fresh stats"""
    commentary = match['commentary']
    match_data = match['match_data']
    teams = match_data.get("Teams", {})
    team_home_id = safe_get(match_data, ["Matchdetail", "Team_Home"])
    team_away_id = safe_get(match_data, ["Matchdetail", "Team_Away"])
    team_home = safe_get(teams, [str(team_home_id), "Name_Full"], str(team_home_id))
    team_away = safe_get(teams, [str(team_away_id), "Name_Full"], str(team_away_id))
    raw_toss_winner = safe_get(match_data, ["Matchdetail", "Tosswonby"])
    toss_winner = safe_get(teams, [str(raw_toss_winner), "Name_Full"], str(raw_toss_winner))
    toss_decision = safe_get(match_data, ["Matchdetail", "Toss_elected_to"])
    match_id = safe_get(match_data, ["Matchdetail", "Match", "Id"], match['folder'])
    series = safe_get(match_data, ["Matchdetail", "Series", "Name"], "")
    venue = safe_get(match_data, ["Matchdetail", "Venue", "Name"], "")
    playing_elevens = {
        team_home: [safe_get(player, ["Name_Full"]) for player in safe_get(teams, [str(team_home_id), "Players"], {}).values()],
        team_away: [safe_get(player, ["Name_Full"]) for player in safe_get(teams, [str(team_away_id), "Players"], {}).values()]
    }
    innings = match_data.get("Innings", [])
    inning_team_map = []
    for inn in innings:
        inning_team_map.append({
            "batting_team_id": str(inn["Battingteam"]),
            "bowling_team_id": str(inn["Bowlingteam"]),
            "batting_team": teams[str(inn["Battingteam"])]["Name_Full"],
            "bowling_team": teams[str(inn["Bowlingteam"])]["Name_Full"]
        })
    
    # Initialize fresh stats dictionaries for this match
    batsman_stats = {}
    bowler_stats = {}
    
    def update_batsman_stats(batsman_id, runs, is_four, is_six):
        stats = batsman_stats.setdefault(batsman_id, {"runs": 0, "balls": 0, "fours": 0, "sixes": 0})
        stats["balls"] += 1
        stats["runs"] += int(runs) if runs else 0
        if is_four:
            stats["fours"] += 1
        if is_six:
            stats["sixes"] += 1
        return stats
    
    def update_bowler_stats(bowler_id, runs, wicket):
        stats = bowler_stats.setdefault(bowler_id, {"balls": 0, "runs": 0, "wickets": 0})
        stats["balls"] += 1
        stats["runs"] += int(runs) if runs else 0
        if wicket:
            stats["wickets"] += 1
        return stats

    # Group commentary events by innings using summary fields
    innings_events = {}
    for event in commentary:
        if not event.get("Isball", False):
            continue
        if not event.get("Over") or str(event.get("Over")).strip() == "":
            continue
        summary = event.get("_summary", {})
        inning_no = int(summary.get("InningNo", 1))
        batting_team_id = str(summary.get("BattingTeam_Id", ""))
        bowling_team_id = str(summary.get("BowlingTeam_Id", ""))
        key = (inning_no, batting_team_id, bowling_team_id)
        if key not in innings_events:
            innings_events[key] = []
        innings_events[key].append(event)

    # Build teams list with id, name, and players 
    teams_list = []
    for team_id in [team_home_id, team_away_id]:
        team_obj = teams.get(str(team_id), {})
        team_name = team_obj.get("Name_Full", str(team_id))
        players_dict = team_obj.get("Players", {})
        players_list = []
        for pid, pinfo in players_dict.items():
            players_list.append({
                "id": pid,
                "name": pinfo.get("Name_Full", pid)
            })
        teams_list.append({
            "id": str(team_id),
            "name": team_name,
            "players": players_list
        })
    
    # Stream events for each inning in order
    restart_signal_timestamp = None
    event_count = 0
    
    logger.info(f"[FRESH START] Starting match {match_id} from beginning with fresh stats (0/0)")
    
    for idx, ((inning_no, batting_team_id, bowling_team_id), events) in enumerate(sorted(innings_events.items())):
        inning_info = None
        for itm in inning_team_map:
            if itm["batting_team_id"] == batting_team_id and itm["bowling_team_id"] == bowling_team_id:
                inning_info = itm
                break
        if not inning_info:
            inning_info = {"batting_team": batting_team_id, "bowling_team": bowling_team_id}
        
        logger.info(f"[INNING {inning_no}] Starting inning {inning_no}: {inning_info['batting_team']} batting vs {inning_info['bowling_team']}")
        
        for event in events:
            # Check for restart signal every 5 events to reduce overhead
            if event_count % 5 == 0:
                current_restart_signal = check_restart_signal_via_kafka()
                file_restart_flag = check_restart_flag_file()
                
                if current_restart_signal and current_restart_signal != restart_signal_timestamp:
                    logger.info(f"[RESTART SIGNAL] Detected restart signal for match {match_id}, stopping current stream...")
                    restart_flag.set()  # Set global restart flag
                    return  # Exit this match's streaming
                
                if file_restart_flag:
                    logger.info(f"[RESTART FILE] Detected restart flag file for match {match_id}, stopping current stream...")
                    restart_flag.set()  # Set global restart flag
                    return  # Exit this match's streaming
            
            # Also check global restart flag
            if restart_flag.is_set():
                logger.info(f"[RESTART FLAG] Global restart flag set for match {match_id}, stopping current stream...")
                return  # Exit this match's streaming
            
            event_count += 1
            
            batsman_id = event.get("Batsman", "")
            bowler_id = event.get("Bowler", "")
            non_striker_id = event.get("Non_Striker", "")
            batsman_name = event.get("Batsman_Name") or get_player_name(batsman_id, teams)
            non_striker_name = event.get("Non_Striker_Name") or get_player_name(non_striker_id, teams)
            bowler_name = event.get("Bowler_Name") or get_player_name(bowler_id, teams)
            runs = event.get("Runs")
            wicket = 1 if event.get("Wicket") else 0
            is_four = False
            is_six = False
            try:
                is_four = int(runs) == 4 if runs else False
                is_six = int(runs) == 6 if runs else False
            except Exception:
                pass
            
            # ALWAYS calculate fresh stats from scratch (ignore pre-calculated details)
            # This ensures we start from 0/0 on restart and build stats progressively
            batsman_current_stats = update_batsman_stats(batsman_id, runs, is_four, is_six)
            
            # Add non-striker stats - also calculate fresh
            if non_striker_id:
                stats = batsman_stats.get(non_striker_id)
                if stats:
                    non_striker_stats = {
                        "runs": stats.get("runs", 0),
                        "balls": stats.get("balls", 0),
                        "fours": stats.get("fours", 0),
                        "sixes": stats.get("sixes", 0),
                    }
                else:
                    non_striker_stats = None
            else:
                non_striker_stats = None
                
            # Calculate fresh bowler stats from scratch
            bowler_current_stats = update_bowler_stats(bowler_id, runs, wicket)
            
            match_event = {
                "match_id": match_id,
                "series": series,
                "venue": venue,
                "teams": teams_list,
                "playing_elevens": playing_elevens,
                "toss_winner": toss_winner,
                "toss_decision": toss_decision,
                "over": safe_get(event, ["Over"]),
                "ball": safe_get(event, ["Ball"]),
                # Player info
                "batsman_id": batsman_id,
                "batsman_name": batsman_name,
                "non_striker_id": non_striker_id,
                "non_striker_name": non_striker_name,
                "bowler_id": bowler_id,
                "bowler_name": bowler_name,
                "runs": safe_get(event, ["Runs"]),
                "wicket": safe_get(event, ["Wicket"]),
                "extras": safe_get(event, ["Extras"]),
                "commentary": safe_get(event, ["Commentary"], ""),
                "Score": event.get("Score", ""),
                "timestamp": safe_get(event, ["Timestamp"]),
                "batsman_stats": batsman_current_stats,
                "non_striker_stats": non_striker_stats,
                "bowler_stats": bowler_current_stats,
                "batting_team": inning_info["batting_team"],
                "bowling_team": inning_info["bowling_team"],
                "batting_team_name": inning_info["batting_team"],
                "bowling_team_name": inning_info["bowling_team"],
                "InningNo": inning_no,
            }
            try:
                producer.produce(topic, key=str(match_event["match_id"]), value=json.dumps(match_event))
                logger.info(f"Produced event: Match {match_event['match_id']} | Over {match_event['over']} | Inning {inning_no} | {match_event['batting_team']} batting | {match_event['batsman_name']} ({match_event['batsman_stats']['runs']}/{match_event['batsman_stats']['balls']}) vs {match_event['bowler_name']} | Score: {match_event['Score']}")
            except Exception as e:
                logger.error(f"Failed to produce event: {e}")
            producer.poll(0)
            time.sleep(delay)

def main():
    while True:  # Continuous loop to handle restarts
        logger.info("Starting cricket event producer...")
        restart_flag.clear()  # Clear restart flag at the beginning
        
        # Force reload matches from disk to ensure fresh data
        base_dir = os.path.dirname(__file__)
        matches = load_all_matches(base_dir)
        if not matches:
            logger.error("No matches found to produce events.")
            time.sleep(10)  # Wait before retrying
            continue
            
        logger.info(f"Loaded {len(matches)} matches, starting to stream events from the VERY BEGINNING (0/0 scores)...")
        
        # Sort matches and their events to ensure we start from the very beginning
        for match in matches:
            # Use all commentary events
            all_events = match['commentary']
            
            # Filter to only include ball events (Isball=true) and sort chronologically
            ball_events = []
            for event in all_events:
                if event.get('Isball') and event.get('Over'):
                    try:
                        # Parse over as float to ensure proper sorting
                        over_val = float(event.get('Over'))
                        if over_val >= 0.1:  # Include from 0.1 overs (first ball)
                            ball_events.append(event)
                    except (ValueError, TypeError):
                        continue
            
            # Sort by inning number first, then over and ball to get chronological order from start
            try:
                ball_events.sort(key=lambda e: (
                    int(e.get('_summary', {}).get('InningNo', 1)),  # Sort by inning first
                    float(e.get('Over', 0)),
                    float(e.get('Ball', 0) or 0)
                ))
                match['commentary'] = ball_events
                first_over = ball_events[0].get('Over', 'N/A') if ball_events else 'N/A'
                first_inning = ball_events[0].get('_summary', {}).get('InningNo', 'N/A') if ball_events else 'N/A'
                last_over = ball_events[-1].get('Over', 'N/A') if ball_events else 'N/A'
                last_inning = ball_events[-1].get('_summary', {}).get('InningNo', 'N/A') if ball_events else 'N/A'
                logger.info(f"Match {match.get('folder', 'unknown')}: Loaded {len(ball_events)} ball events")
                logger.info(f"  First event: Inning {first_inning}, Over {first_over}")
                logger.info(f"  Last event: Inning {last_inning}, Over {last_over}")
            except Exception as e:
                logger.warning(f"Could not sort events for match {match.get('folder', 'unknown')}: {e}")
                match['commentary'] = ball_events
                logger.info(f"Match {match.get('folder', 'unknown')}: Using unsorted order, {len(ball_events)} events")
        
        threads = []
        for match in matches:
            t = Thread(target=stream_match_events_with_restart_check, args=(match, DELAY_BETWEEN_EVENTS), daemon=True)
            t.start()
            threads.append(t)
        
        # Monitor for restart signals while threads are running
        all_threads_finished = False
        while not all_threads_finished:
            # Check if any thread is still alive
            alive_threads = [t for t in threads if t.is_alive()]
            if not alive_threads:
                all_threads_finished = True
                break
            
            # Check for restart signal every 2 seconds
            restart_signal = check_restart_signal_via_kafka()
            if restart_signal or restart_flag.is_set():
                logger.info("Restart signal detected during streaming, stopping all threads...")
                restart_flag.set()  # Signal all threads to stop
                
                # Wait a bit for threads to stop gracefully
                time.sleep(2)
                
                # Force stop any remaining threads
                for t in threads:
                    if t.is_alive():
                        logger.info(f"Thread still alive, waiting for graceful exit...")
                
                break
            
            time.sleep(2)  # Check every 2 seconds
        
        producer.flush()
        
        # Check if we should restart
        restart_signal = check_restart_signal_via_kafka()
        file_restart = check_restart_flag_file()
        
        if restart_flag.is_set() or restart_signal or file_restart:
            logger.info("Restart signal confirmed, COMPLETELY restarting producer process from beginning...")
            restart_flag.clear()  # Clear the restart flag for next iteration
            
            # Instead of exiting, just continue the loop to restart from beginning
            logger.info("Restarting producer from beginning...")
            continue  # Restart the main loop
        else:
            logger.info("All events sent, producer finished.")
            break  # Exit if no restart signal

if __name__ == "__main__":
    main()