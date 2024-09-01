"""/

1. Load 23_24 players into pandas df - done

2. Fetch all players for 23_24 season sorted by atleast players with 1 point both from API and pandas - done

3. sort player for 24_25 season - done

4. Define function - done

3. Create tables for all players and season - done

4. Load PD 23_24 players info DB (Assign ID and status) - done

5. Load PD 24_25 players into DB (Assign ID and status) - pending

/"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string
import sqlite3
from datetime import datetime
import re
import os

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Connect to the database
# conn = sqlite3.connect('players.db')

def gen_ns_players():
    # Fetch the 24&25 season player data from the URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    response = requests.get(url)
    # Parse the JSON response
    data = response.json()
    # Extract and filter player data
    players = data['elements']
    
    ns_players = []
    for player in players:
        player_data = {
            "id": player["id"],
            "first_name": player["first_name"],
            "second_name": player["second_name"],
            "web_name": player["web_name"],
            "team": player["team"],
            "photo": player["photo"],
            "points_per_game": player["points_per_game"],
            "total_points": player["total_points"],
            "goals_scored": player["goals_scored"],
            "goals_conceded": player["goals_conceded"],
            "own_goals": player["own_goals"],
            "penalties_saved": player["penalties_saved"],
            "penalties_missed": player["penalties_missed"],
            "saves": player["saves"],
            "bonus": player["bonus"],
            "bps": player["bps"],
            "starts": player["starts"],
            "assists": player["assists"],
            "clean_sheets": player["clean_sheets"],
            "yellow_cards": player["yellow_cards"],
            "red_cards": player["red_cards"],
        }
        ns_players.append(player_data)
    
    print("Filtered players:", len(ns_players))
    return ns_players

# Call the function to test it
filtered_players = gen_ns_players()
print(filtered_players)
################################################################## MAIN Execution#######################################################

def main():
    # Load 23_24 players into pandas df
    ls_players = pd.read_csv('FPL_season_23_24.csv')
    df_ls_players = pd.DataFrame(ls_players)
    print(df_ls_players)

    num_of_scores = []
    df_scores = df_ls_players.iloc[:, 3]
    for scores in df_scores:
        scores = df_scores[df_scores > 0]
        num_of_scores = len(scores)
    print(num_of_scores)
    # Fetch the 24&25 season player data from the URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    response = requests.get(url)

    # Parse the JSON response
    data = response.json()

    # Extract and filter player data
    players = data['elements']
    filtered_players = [
        {
            "id": player["id"],
            "first_name": player["first_name"],
            "second_name": player["second_name"],
            "web_name": player["web_name"],
            "team": player["team"],
            "photo": player["photo"],
            "points_per_game": player["points_per_game"],
            "total_points": player["total_points"],
            "goals_scored": player["goals_scored"],
            "goals_conceded": player["goals_conceded"],
            "own_goals": player["own_goals"],
            "penalties_saved": player["penalties_saved"],
            "penalties_missed": player["penalties_missed"],
            "saves": player["saves"],
            "bonus": player["bonus"],
            "bps": player["bps"],
            "starts": player["starts"],
            "assists": player["assists"],
            "clean_sheets": player["clean_sheets"],
            "yellow_cards": player["yellow_cards"],
            "red_cards": player["red_cards"],
        }
        for player in players if player['total_points'] != 0 
    ]

    df_player_ls_col = [
        'id', 'first_name', 'team', 'second_name', 'web_name', 'photo',
        'points_per_game'
    ]

    df_filtered = pd.DataFrame(filtered_players, columns=df_player_ls_col)
    df = df_filtered.reset_index(drop=True)

    count = len(df_filtered)
    print(f"Count of players in DataFrame:{count}")

    ##################################################### Analyze past season performance

    season_count = 0
    all_filtered_season = []

    df_season_ls_col = ['element_id', 'season_name', 'total_points', 'start_cost', 'end_cost', 'goals_scored', 'goals_conceded', 'own_goals',
                        'penalties_saved', 'penalties_missed', 'saves', 'bonus', 'bps', 'starts', 'assists', 'clean_sheets', 'yellow_cards', 'red_cards']

    # Define the minimum season to include (2023/24 and onwards)
    min_season = "2023/24"

    # Define a regex pattern for valid season format 'YYYY/YY'
    season_pattern = re.compile(r'^\d{4}/\d{2}$')

    # Iterate through each element_id
    for element_id in df_filtered['id']:
        url = f"https://fantasy.premierleague.com/api/element-summary/{element_id}/"
        response = requests.get(url)
        data = response.json()

        # Extract history past seasons
        history_past = data.get('history_past', [])

        # Check for empty history_past directly
        if not history_past:
            print(f"No valid seasons found for element ID: {element_id}")
            continue

        # Step 1: Filter out entries with invalid or null season names
        valid_seasons = [season for season in history_past if season.get('season_name') and season_pattern.match(season['season_name'])]

        # Step 2: Filter seasons that are 2023/24 or greater
        valid_seasons = [season for season in valid_seasons if season['season_name'] >= min_season]

        # Proceed only if there are valid seasons that match the minimum season criteria
        if valid_seasons:
            # Find the most recent season
            max_season = max(valid_seasons, key=lambda x: x['season_name'])

            print(f"Max season identified for element ID {element_id}: {max_season['season_name']}")

            # Construct the filtered season data dictionary
            filtered_season = {
                "element_id": element_id,
                "season_name": max_season.get("season_name"),
                "total_points": max_season.get("total_points"),
                "start_cost": max_season.get("start_cost"),
                "end_cost": max_season.get("end_cost"),
                "goals_scored": max_season.get("goals_scored"),
                "goals_conceded": max_season.get("goals_conceded"),
                "own_goals": max_season.get("own_goals"),
                "penalties_saved": max_season.get("penalties_saved"),
                "penalties_missed": max_season.get("penalties_missed"),
                "saves": max_season.get("saves"),
                "bonus": max_season.get("bonus"),
                "bps": max_season.get("bps"),
                "starts": max_season.get("starts"),
                "assists": max_season.get("assists"),
                "clean_sheets": max_season.get("clean_sheets"),
                "yellow_cards": max_season.get("yellow_cards"),
                "red_cards": max_season.get("red_cards"),
            }

            # Only append if all values in the filtered season are valid (not None or NaN)
            if all(value is not None for value in filtered_season.values()):
                season_count += 1
                all_filtered_season.append(filtered_season)

        else:
            print(f"No valid seasons found for element ID: {element_id}")

    # Print the count of players with data for the target season
    print(f"Number of element IDs that had data for the target season: {season_count}")

    # Print the length of all_filtered_season to check the size
    print(f"Total records in all_filtered_season: {len(all_filtered_season)}")

    # Optional: Print some entries from all_filtered_season for verification
    if all_filtered_season:
        print("Sample filtered seasons:")
        for entry in all_filtered_season[:5]:  # Print first 5 entries
            print(entry)

    # Create a DataFrame from the filtered season data
    df_season = pd.DataFrame(all_filtered_season, columns=df_season_ls_col)

    # Function to merge the filtered DataFrame with the season data
    def gen_ls_players(df_filtered, df_season):
        df_players_ls = pd.merge(
            df_filtered, df_season, left_on='id', right_on='element_id', how='left'
        )
        return df_players_ls

    # Example of how to use the function
    df_players_ls = gen_ls_players(df_filtered, df_season)
    print(df_players_ls.head())
    # Update 'season_name' column to replace NaN with '2023/24'
    df_players_ls['season_name'] = df_players_ls['season_name'].fillna('2023/24')

    # Print the updated 'season_name' column to verify
    print(df_players_ls['season_name'])
    unique_season_names = df_players_ls['season_name'].unique()
    print("Unique values in 'season_name':", unique_season_names)

    ns_players = gen_ns_players()

    df_ns_players = pd.DataFrame(ns_players)

    count = len(ns_players)
    print(f"Count of ns_players: {count}")
    print(df_ns_players)

    # Define the function to get the current season
    def get_current_season():
        current_year = datetime.now().year
        current_season = f"{current_year}/{str(current_year + 1)[-2:]}"
        return current_season
    # Get the current season
    current_season = get_current_season()
    ############################################################ Map df_ls_players to team and season foreign keys to assign their corresponding values

    # Connect to the SQLite database
    # conn = sqlite3.connect('players.db')
    # cursor = conn.cursor()

    # Ensure foreign keys are enabled
    conn.execute("PRAGMA foreign_keys = ON")

    # Fetch or create a snapshot entry
    cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
    snapshot_id = cursor.lastrowid

    # Fetch the team and season mappings
    team_mapping = pd.read_sql_query("SELECT team_name, team_id FROM Teams", conn)
    season_mapping = pd.read_sql_query("SELECT season_name, season_id FROM Seasons", conn)

    # Convert to dictionaries for easy mapping
    # team_dict = dict(zip(team_mapping['team_id'], team_mapping['team_name']))
    season_dict = dict(zip(season_mapping['season_name'], season_mapping['season_id']))

    # Map the current season's ID from the season mapping DataFrame
    current_season_id = season_mapping.loc[season_mapping['season_name'] == current_season, 'season_id'].values[0]

    # Map 'team' and 'season_name' to 'team_id' and 'season_id'
    df_players_ls = df_ls_players.rename(columns={'team': 'team_id'})
    #df_ls_players.rename(columns = {'team':'team_id'})
    # df_ls_players['team_id'] = df_ls_players['team'].map(team_dict)
    # df_players_ls['season_id'] = df_players_ls['season_name'].map(season_dict)
    df_ns_players['season_id'] = current_season_id

    #print(f"list of teams and seasons:{df_players_ls['team_id']}")

    # Define the columns for All_Players
    all_players_columns = [
        'id', 'element_id', 'team_id', 'season_id', 'first_name', 'second_name',
        'web_name', 'photo', 'points_per_game', 'total_points', 'start_cost', 'end_cost', 'goals_scored',
        'goals_conceded', 'own_goals', 'penalties_saved', 'penalties_missed', 'saves', 'bonus', 'bps',
        'starts', 'assists', 'clean_sheets', 'yellow_cards', 'red_cards'
    ]

    # Rename 'id' to 'fpl_player_id' for the insertion
    # df_ls_players.rename(columns={'id': 'fpl_player_id'}, inplace=True)

    # Insert data into All_Players

    # Add the snapshot_id to the dataframe
    df_ls_players['snapshot_id'] = snapshot_id

    # Insert data into All_Players
    """df_ls_players[all_players_columns + ['snapshot_id']].to_sql('All_Players', conn, if_exists='append', index=False)

    # Commit the transaction
    conn.commit()"""

    # Insert data for ns_players
    for _, player in df_ns_players.iterrows():
        cursor.execute('''
            INSERT INTO All_Players (id, element_id, team_id, season_id, first_name, second_name, web_name, photo, 
            points_per_game, total_points, goals_scored, goals_conceded, own_goals, penalties_saved, 
            penalties_missed, saves, bonus, bps, starts, assists, clean_sheets, yellow_cards, red_cards, snapshot_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            player['id'], player['id'], player['team'],player['season_id'], player['first_name'], player['second_name'], 
            player['web_name'], player['photo'], player['points_per_game'], player['total_points'], 
            player['goals_scored'], player['goals_conceded'], player['own_goals'], player['penalties_saved'], 
            player['penalties_missed'], player['saves'], player['bonus'], player['bps'], player['starts'], 
            player['assists'], player['clean_sheets'], player['yellow_cards'], player['red_cards'], snapshot_id
        ))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()

    print("Data inserted into All_Players successfully.")


if __name__ == "__main__":
    main()
