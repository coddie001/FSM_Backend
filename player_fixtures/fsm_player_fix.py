"""/- Fetch & Update player fixtures
	- create player game weeks
	- create player fixtures
/"""

from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import string
import json
import os

app = Flask(__name__)

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

############################################################################## GET PLAYER FIXTURES

# Query DB for teams
teams = pd.read_sql("SELECT team_id, team_name FROM Teams", conn)
print(teams)

# Convert to dictionaries for easy mapping
team_dict = dict(zip(teams['team_id'], teams['team_name']))

# Select player information from the players table
players = pd.read_sql("SELECT element_id, player_id, status, web_name FROM players", conn)
print(players)

# Define dictionary to store player fixtures DataFrames
player_fixtures_dict = {}
df_fix_col = ['id', 'code', 'team_h', 'team_h_score', 'team_a', 'team_a_score', 'event', 'finished', 'minutes', 'provisional_start_time', 'kickoff_time', 'event_name', 'is_home', 'difficulty']

# Create a dictionary to map element_id to player_id
element_to_player_id = dict(zip(players['element_id'], players['player_id']))

for player in players['element_id']:
    url = f"https://fantasy.premierleague.com/api/element-summary/{player}/"
    response = requests.get(url)
    # Check for successful response (status code 200)
    if response.status_code == 200:
        data = response.json()
        print(data)

        # Extract and filter fixtures data
        fixtures = data['fixtures']

        # Create DataFrame for current player's fixtures
        df_fixtures = pd.DataFrame(fixtures, columns=df_fix_col)

        # Replace team IDs with team names
        def replace_team_ids(df_fixtures, team_dict):
            df_fixtures['team_h'] = df_fixtures['team_h'].map(team_dict)
            df_fixtures['team_a'] = df_fixtures['team_a'].map(team_dict)
            return df_fixtures

        df_fixtures = replace_team_ids(df_fixtures.copy(), team_dict.copy())

        # Map the player_id to the fixtures DataFrame
        player_id = element_to_player_id[player]

        # Add player's fixture DataFrame to the dictionary with player_id
        player_fixtures_dict[player_id] = df_fixtures

        # Print the transformed DataFrame (optional)
        print(df_fixtures)

# Access DataFrame for a specific player (optional)
player_42_fixtures = player_fixtures_dict.get(11, None)
if player_42_fixtures is not None:
    print(f"shape 2 {player_42_fixtures.shape}")

############################################################################## INSERT PLAYER FIXTURES INTO DB
# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

column_mapping = {
    'id': 'fixture_id',
    'event': 'event_id',
    'kickoff_time': 'event_time',
    'team_h': 'team_h',
    'team_h_score': 'team_h_score',
    'team_a': 'team_a',
    'team_a_score': 'team_a_score',
    'is_home': 'is_home',
    'finished': 'finished',
    'difficulty': 'difficulty',
    'event_name': 'event_name',
}

# Function to insert a player's fixture DataFrame into the database
def insert_player_fixtures(player_id, df_fixtures):
    # Rename DataFrame columns to match the database schema
    df_fixtures = df_fixtures.rename(columns=column_mapping)

    # Set 'event_name' as the index but keep it in the DataFrame
    df_fixtures.set_index('event_name', inplace=True, drop=False)

    # Add missing columns with default values
    df_fixtures['player_id'] = player_id  # Add player_id
    df_fixtures['snapshot_id'] = snapshot_id  # Assuming snapshot_id will be filled later
    df_fixtures['pu_fix'] = ''  # Assuming pu_fix is a string, default empty
    df_fixtures['u_fix'] = ''  # Assuming u_fix is a string, default empty

    # Reorder columns to match the database table schema
    db_columns = ['fixture_id', 'player_id', 'snapshot_id', 'event_id', 'event_time', 'event_name',
                  'team_h', 'team_h_score', 'team_a', 'team_a_score', 'is_home', 'finished',
                  'difficulty', 'pu_fix', 'u_fix']

    df_fixtures = df_fixtures[db_columns]

"""# Insert DataFrame into the database table
    df_fixtures.to_sql('PlayerFixtures', conn, if_exists='append', index=False)

# Example: Iterate over your player_fixtures_dict and insert each DataFrame into the database
for player_id, df_fixtures in player_fixtures_dict.items():
    print(f"Inserting fixtures for player_id: {player_id}")
    insert_player_fixtures(player_id, df_fixtures)

######################################

# Export to CSV (optional, modify path as needed)
# player_42_fixtures.to_csv("fsm_fixtures.csv", index=False)
# print("DF exported to csv")"""


####################################### Flask route to retrieve player scores
# Reconnect to the SQLite database in the route
def get_db_connection():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    db_path = os.path.join(parent_dir, 'players.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

@app.route('/player_fix', methods=['POST'])
def get_player_fix():
    data = request.get_json()
    player_id = data.get('player_id', 'FSM01')
    event_name = data.get('event_name', 'Gameweek1')
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()

    # Query to get fixture details based on player_id and event_name
    cursor.execute("""
        SELECT fixture_id, player_id, event_id, event_time, team_h, team_h_score, 
               team_a, team_a_score, is_home, finished, difficulty, event_name
        FROM PlayerFixtures
        WHERE player_id = ? AND event_name = ?
    """, (player_id, event_name))
    
    # Fetch the result
    row = cursor.fetchone()
    
    # Close the database connection
    conn.close()

    if row:
        # Convert the row to a dictionary
        result = {
            "fixture_id": row['fixture_id'],
            "player_id": row['player_id'],
            "event_id": row['event_id'],
            "event_time": row['event_time'],
            "team_h": row['team_h'],
            "team_h_score": row['team_h_score'],
            "team_a": row['team_a'],
            "team_a_score": row['team_a_score'],
            "is_home": row['is_home'],
            "finished": row['finished'],
            "difficulty": row['difficulty'],
            "event_name": row['event_name']
        }
    else:
        # Handle case where no data is found
        result = {"error": "No fixture found for the given player_id and event_name"}
    
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)