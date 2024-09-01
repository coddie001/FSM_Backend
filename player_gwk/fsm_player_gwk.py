"""/
    - create player game weeks
    - store game week info for scoring
    - gameweek counter

define function(current gameweek)
select all players fixtures where gameweek is the current gameweek
create gameweek
Store player game details
Update team rating and form counter
update gameweek status


/"""

from flask import Flask, request, jsonify
import requests
import sqlite3
import json
# import apschedule
import time
import os
import pandas as pd
from datetime import datetime, date


app = Flask(__name__)

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

##############################################################################
# FETCH PLAYER FIXTURES FROM DB
##############################################################################
def fetch_player_fixtures():
    total_gwk = 39
    all_gwks = list(range(1, total_gwk + 1))  # Create a list from 1 to total_gwk
    print("All Gameweeks:", all_gwks)

    # Convert the list to a tuple for SQL query
    gwk_tuple = tuple(all_gwks)
    print("Gameweek Tuple:", gwk_tuple)

    # Fetch player fixtures from the database
    query = "SELECT event_time, element_id, event_id, fixture_id, player_id, finished FROM PlayerFixtures"
    player_fix = pd.read_sql(query, conn)
    return pd.DataFrame(player_fix)

player_fix = fetch_player_fixtures()
print("Fetched Player Fixtures:")
print(player_fix)

##############################################################################
# PROCESS PLAYER FIXTURES AND STORE IN DATAFRAME
##############################################################################
def process_player_fixtures(event_id, event_date, element_id):
    # List to store player data
    player_data_list = []

    # Check if the event date is today
    if event_date == date.today():
        url = f'https://fantasy.premierleague.com/api/event/{event_id}/live/'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            print("Fetched Data:", data)

            # Filter the data for the specific element_id
            player_data = next(
                (player['stats'] for player in data['elements'] if player['id'] == element_id), None
            )

            if player_data:
                filtered_player = {
                    "id": element_id,
                    "starts": player_data.get("starts", 0),
                    "minutes": player_data.get("minutes", 0),
                    "goals_scored": player_data.get("goals_scored", 0),
                    "goals_conceded": player_data.get("goals_conceded", 0),
                    "saves": player_data.get("saves", 0),
                    "assists": player_data.get("assists", 0),
                    "clean_sheets": player_data.get("clean_sheets", 0),
                    "yellow_cards": player_data.get("yellow_cards", 0),
                    "red_cards": player_data.get("red_cards", 0),
                    "total_points": player_data.get("total_points", 0),
                    "bonus": player_data.get("bonus", 0),
                    "bps": player_data.get("bps", 0),
                }
                print(f"Processed Player Data: {filtered_player}")
                player_data_list.append(filtered_player)
            else:
                print(f"No data found for element_id {element_id} in event {event_id}")

        else:
            print(f'Failed to fetch data for event {event_id}: HTTP {response.status_code}')
    else:
        print(f'Event {event_id} is not for today')

    # Convert the list of dictionaries to a DataFrame
    if player_data_list:
        df_filtered_player = pd.DataFrame(player_data_list)
        print("Filtered Player DataFrame:")
        print(df_filtered_player)
        return df_filtered_player
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data is available

# Iterate over each player fixture and process it
filtered_player_dfs = []
for index, row in player_fix.iterrows():
    event_id = row['event_id']
    print(f"Event ID: {event_id}")

    element_id = row['element_id']
    print(f"Element ID: {element_id}")

    # Parse the event_time to a datetime object, including timezone info
    event_time = pd.to_datetime(row['event_time'], utc=True)
    print("Event Time:", event_time)

    # Extract the date from the event_time
    event_date = event_time.date()
    print("Event Date:", event_date)

    # Process the fixture and get the filtered DataFrame
    df_filtered_player = process_player_fixtures(event_id, event_date, element_id)
    if not df_filtered_player.empty:
        filtered_player_dfs.append(df_filtered_player)

# Combine all individual DataFrames into a single DataFrame
if filtered_player_dfs:
    final_df = pd.concat(filtered_player_dfs, ignore_index=True)
    print("Final DataFrame with Filtered Player Data:")
    print(final_df)
else:
    print("No data to display.")
##############################################################################
# CHECK PLAYER GAME STATUS
##############################################################################
final_df['minutes'] = minutes
def game_status(event_time, minutes):
    current_time = datetime.now()
    
    if current_time >= event_time and minutes < 90:
        return 'started'
    elif current_time > event_time and minutes == 90:
        return 'finished'
    else:
        return 'pending'

##############################################################################
# UPDATE PLAYER GWK DB BEFORE AND AFTER GAME
##############################################################################
def update_gwkDB_before_game(final_df):
    # Update the player GWK data in the DB before the game starts
    for index, row in final_df.iterrows():
        if row['game_status'] == 'pending':
            cursor.execute("""
                UPDATE PlayerGWK
                SET starts = ?, minutes = ?, goals_scored = ?, assists = ?, clean_sheets = ?,
                    yellow_cards = ?, red_cards = ?, total_points = ?, bonus = ?, bps = ?
                WHERE element_id = ? AND event_id = ?
            """, (
                row['starts'], row['minutes'], row['goals_scored'], row['assists'], row['clean_sheets'],
                row['yellow_cards'], row['red_cards'], row['total_points'], row['bonus'], row['bps'],
                row['id'], row['event_id']
            ))
    conn.commit()

def update_gwkDB_after_game(final_df):
    # Update the player GWK data in the DB after the game ends
    for index, row in final_df.iterrows():
        if row['game_status'] == 'finished':
            cursor.execute("""
                UPDATE PlayerGWK
                SET minutes = ?, goals_scored = ?, assists = ?, clean_sheets = ?,
                    yellow_cards = ?, red_cards = ?, total_points = ?, bonus = ?, bps = ?
                WHERE element_id = ? AND event_id = ?
            """, (
                row['minutes'], row['goals_scored'], row['assists'], row['clean_sheets'],
                row['yellow_cards'], row['red_cards'], row['total_points'], row['bonus'], row['bps'],
                row['id'], row['event_id']
            ))
    conn.commit()

# Call these functions at appropriate times (e.g., before and after the games)
update_gwkDB_before_game(final_df)
update_gwkDB_after_game(final_df)
    


"""##########################################################################
player_fix = pd.read_sql("SELECT event_time, event_id, fixture_id, player_id, finished FROM PlayerFixtures", conn)
df_player_fix = pd.DataFrame(player_fix)
print (df_player_fix)

for event_id in player_fix(iterrows):
    if event_id == 1 and event_time == date.today():
        URL ='https://fantasy.premierleague.com/api/event/{event_id}/live/'
        response = requests.get(URL)
        data = response.json
    elif event_id > 1 and event_time == date.today():
        URL ='https://fantasy.premierleague.com/api/event/{event_id}/live/'
        response = requests.get(URL)
        data = response.json
    else:
        print ('event is not for today')

#########################################################################

# Fetch player fixtures for all gameweeks
player_fix = pd.DataFrame()  # Initialize an empty DataFrame
for i in gwk_tuple:
    query = f"SELECT event_time, event_id, fixture_id, player_id, finished FROM PlayerFixtures WHERE event_name = 'Gameweek {i}'"
    df = pd.read_sql(query, conn)
    player_fix = pd.concat([player_fix, df])

##############################################################################
# CREATE PLAYER GWK
##############################################################################
# Convert the event_time to datetime and format it
player_fix['kickoff_date'] = pd.to_datetime(player_fix['event_time']).dt.strftime('%Y-%m-%d')
print(player_fix['kickoff_date'])

# Function to determine game status
def game_status(event_time, finished):
    current_time = datetime.now()
    
    if current_time >= event_time and finished == 0:
        return 'started'
    elif current_time > event_time and finished == 1:
        return 'finished'
    else:
        return 'pending'


###############################################################################

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

# Function to create a game week entry in the database
def create_gwk(player_fix):
    for index, row in player_fix.iterrows():
        status = game_status(row['event_time'], row['is_finished'])
        cursor.execute('''
            INSERT INTO PlayerGWK (player_id, event_id, gp, gp_multi, goals, assist, saves, clean_sheet, fpl_bp, fsm_bp, game_status, event_time, kickoff_date, snapshot_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['player_id'], row['event_id'], 0, 0, 0, 0, 0, 0, 0, status, row['event_time'], row['kickoff_date'], snapshot_id))
    conn.commit()

##############################################################################
# UPDATE PLAYER GWK
##############################################################################
def update_gwk():
    # Corrected SQL query syntax and added missing commas
    player_gwk = pd.read_sql(f"SELECT player_id, event_id, game_status, kickoff_date FROM PlayerGWK WHERE kickoff_date = '{datetime.today().strftime('%Y-%m-%d')}'", conn)
    print(player_gwk)

    for index, row in player_gwk.iterrows():
        status = 'started'  # Assuming you want to update to 'started'
        cursor.execute('''
            UPDATE PlayerGWK 
            SET game_status = ? 
            WHERE player_id = ? AND event_id = ?
        ''', (status, row['player_id'], row['event_id'], snapshot_id))
        conn.commit()
        try:
            URL = f'https://fantasy.premierleague.com/api/event/{row["event_id"]}/live/'
            response = requests.get(URL)
            data = response.json()

            player_performance = data['elements']
            # You'll need to parse `player_performance` to extract gamepoint, goals, etc.

        except Exception as e:
            print(f"Error fetching data: {e}")
            status = 'finished' if row['game_status'] == 'started' else 'pending'

        finally:
            multiplier ==10
            if gp == 1 | gp > 1:
                gp_multi = gp * multiplier

            cursor.execute('''
                UPDATE PlayerGWK 
                SET gp = ?, gp_multi = ?, goals = ?, assist = ?, cleansheet = ?, fplbp = ?
                WHERE player_id = ? AND event_id = ?
            ''', (0, 0, 0, 0, 0, row['player_id'], row['event_id'], snapshot_id))  # Replace zeros with actual values
            conn.commit()

# Create game week entries based on player fixtures
create_gwk(player_fix)
conn.close()
############################################################################## FLASK
@app.route('/gwk_info', methods=['POST'])
def gwk_info():
    pass"""
