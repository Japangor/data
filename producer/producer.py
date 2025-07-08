import os
import time
import json
import logging
from confluent_kafka import Producer
from threading import Thread

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
                with open(comm1, encoding='utf-8') as f:
                    comm_data_1 = json.load(f)
                with open(comm2, encoding='utf-8') as f:
                    comm_data_2 = json.load(f)
                events_1 = comm_data_1.get("Commentary", [])[::-1]
                events_2 = comm_data_2.get("Commentary", [])[::-1]
                all_events = events_1 + events_2
                matches.append({
                    'folder': folder,
                    'match_data': match_data,
                    'commentary': all_events
                })
            except Exception as e:
                logger.error(f"Failed to load match in {folder}: {e}")
    return matches

def stream_match_events(match, delay):
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
            "batting_team": teams[str(inn["Battingteam"])] ["Name_Full"],
            "bowling_team": teams[str(inn["Bowlingteam"])] ["Name_Full"]
        })
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

    # Group commentary events by innings
    innings_events = [[] for _ in range(len(inning_team_map))]
    for event in commentary:
        if not event.get("Isball", False):
            continue
        if not event.get("Over") or str(event.get("Over")).strip() == "":
            continue
        # Use InningNo from event to assign to correct inning
        event_inning_no = int(event.get("InningNo", 1))
        if 1 <= event_inning_no <= len(innings_events):
            innings_events[event_inning_no - 1].append(event)

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
    for inning_idx, inning_info in enumerate(inning_team_map):
        inning_no = inning_idx + 1  # 1-based inning number
        for event in innings_events[inning_idx]:
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
            batsman_details = event.get("Batsman_Details")
            bowler_details = event.get("Bowler_Details")
            if batsman_details:
                batsman_current_stats = {
                    "runs": int(batsman_details.get("Runs", 0)),
                    "balls": int(batsman_details.get("Balls", 0)),
                    "fours": int(batsman_details.get("Fours", 0)),
                    "sixes": int(batsman_details.get("Sixes", 0)),
                }
            else:
                batsman_current_stats = update_batsman_stats(batsman_id, runs, is_four, is_six)
            # Add non-striker stats
            if non_striker_id:
                non_striker_details = event.get("Non_Striker_Details")
                if non_striker_details:
                    non_striker_stats = {
                        "runs": int(non_striker_details.get("Runs", 0)),
                        "balls": int(non_striker_details.get("Balls", 0)),
                        "fours": int(non_striker_details.get("Fours", 0)),
                        "sixes": int(non_striker_details.get("Sixes", 0)),
                    }
                else:
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
            if bowler_details:
                balls = int(bowler_details.get("Overs", "0").split(".")[0]) * 6 + int(bowler_details.get("Overs", "0").split(".")[1]) if "." in str(bowler_details.get("Overs", "0")) else int(bowler_details.get("Overs", "0")) * 6
                bowler_current_stats = {
                    "balls": balls,
                    "runs": int(bowler_details.get("Runs", 0)),
                    "wickets": int(bowler_details.get("Wickets", 0)),
                }
            else:
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
                "batting_team": event.get("BattingTeam") or inning_info["batting_team"],
                "bowling_team": event.get("BowlingTeam") or inning_info["bowling_team"],
                "batting_team_name": event.get("BattingTeam") or inning_info["batting_team"],
                "bowling_team_name": event.get("BowlingTeam") or inning_info["bowling_team"],
                "InningNo": inning_no,  
            }
            try:
                producer.produce(topic, key=str(match_event["match_id"]), value=json.dumps(match_event))
                logger.info(f"Produced event: Match {match_event['match_id']} | {match_event['over']} {match_event['batting_team']} batting, {match_event['bowling_team']} bowling | {match_event['batsman_name']} (on strike) / {match_event['non_striker_name']} (non-striker) vs {match_event['bowler_name']} | Score: {match_event['Score']} | Batsman stats: {match_event['batsman_stats']} | Bowler stats: {match_event['bowler_stats']}")
            except Exception as e:
                logger.error(f"Failed to produce event: {e}")
            producer.poll(0)
            time.sleep(delay)

def main():
    base_dir = os.path.dirname(__file__)
    matches = load_all_matches(base_dir)
    if not matches:
        logger.error("No matches found to produce events.")
        return
    threads = []
    for match in matches:
        t = Thread(target=stream_match_events, args=(match, DELAY_BETWEEN_EVENTS), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    producer.flush()
    logger.info("All events sent.")

if __name__ == "__main__":
    main()