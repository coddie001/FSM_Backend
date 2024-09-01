from flask import Flask, request, jsonify
import requests
import sqlite3
import json
import time
import os
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

# Define the specific IDs you want to store data for
specific_ids = [247, 224, 506, 235, 342, 18, 433, 550, 497, 16, 17, 398, 328, 182, 13]
player_id = ['FSM01', 'FSM02' , 'FSM03', 'FSM04', 'FSM05', 'FSM06', 'FSM07', 'FSM08', 'FSM09', 'FSM10', 'FSM11', 'FSM12', 'FSM13', 'FSM14', 'FSM15']

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Set the timezone to West African Time
wat_timezone = timezone('Africa/Lagos')

# Function to check if a gameweek has been processed
def is_gameweek_processed(conn, gwk):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM PlayerGWK WHERE gameweek = ?", (gwk,))
    result = cursor.fetchone()
    return result[0] > 0

# Function to fetch data and process it for a specific gameweek
def fetch_and_process_data(gwk):
    # Create a new connection for this thread
    conn = sqlite3.connect(db_path)
    
    # Check if this gameweek has already been processed
    if is_gameweek_processed(conn, gwk):
        print(f"Gameweek {gwk} already processed. Skipping.")
        conn.close()
        return
    
    # Fetch data from the API
    URL = f'https://fantasy.premierleague.com/api/event/{gwk}/live/'
    response = requests.get(URL)
    data = response.json()

    # Extract the 'elements' data from the response
    elements = data['elements']

    # Filter the data to include only the specific player IDs
    filtered_data = [
        {
            "element_id": element["id"],
            "player_id": player_id[specific_ids.index(element["id"])],
            "gameweek": gwk,
            "minutes": element["stats"]["minutes"],
            "total_points": element["stats"]["total_points"],
            "goals_scored": element["stats"]["goals_scored"],
            "goals_conceded": element["stats"]["goals_conceded"],
            "own_goals": element["stats"]["own_goals"],
            "saves": element["stats"]["saves"],
            "bonus": element["stats"]["bonus"],
            "bps": element["stats"]["bps"],
            "starts": element["stats"]["starts"],
            "assists": element["stats"]["assists"],
            "clean_sheets": element["stats"]["clean_sheets"],
            "yellow_cards": element["stats"]["yellow_cards"],
            "red_cards": element["stats"]["red_cards"]
        }
        for element in elements if element['id'] in specific_ids
    ]

    # Normalize the filtered data into a pandas DataFrame
    df_gwk = pd.json_normalize(filtered_data)

    if not df_gwk.empty:
        # Fetch or create a snapshot entry
        cursor = conn.cursor()
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid

        # Iterate over each row in the DataFrame and insert it into the database
        for _, data in df_gwk.iterrows():
            cursor.execute('''
                INSERT INTO PlayerGWK (
                    player_id, snapshot_id, element_id, gameweek, total_points, gp_multi, gwk_avg_contr, mean_dev1, 
                    mean_dev2, form_count, minutes, goals_scored, goals_conceded, assists, saves, clean_sheets, 
                    red_cards, yellow_cards, own_goals, bonus, bps, starts, fsm_bp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["player_id"], snapshot_id, data["element_id"], data["gameweek"], data["total_points"],
                0, 0, 0.0, 0.0, 0, data["minutes"], data["goals_scored"], data["goals_conceded"], data["assists"], 
                data["saves"], data["clean_sheets"], data["red_cards"], data["yellow_cards"], data["own_goals"], 
                data["bonus"], data["bps"], data["starts"], 0
            ))

        # Commit the transaction
        conn.commit()
        print(f"Data for Gameweek {gwk} inserted into the database.")
    else:
        print(f"No valid data for Gameweek {gwk}. Will retry.")

    # Close the connection for this thread
    conn.close()

# Scheduler to run the function every 3 days for each gameweek
scheduler = BackgroundScheduler(timezone=wat_timezone)

# Schedule each gameweek separately to run every 3 days
for i, gwk in enumerate(range(1, 39)):
    # Calculate the start time for each gameweek, adjusting for the correct timezone
    start_time = wat_timezone.localize(datetime(2024, 8, 25, 23, 28)) + timedelta(minutes=3*i)
    scheduler.add_job(fetch_and_process_data, 'interval', minutes=3, start_date=start_time, args=[gwk])

# Start the scheduler
scheduler.start()

# Keep the script running to allow the scheduler to execute jobs
try:
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    print("Shutting down scheduler...")
    scheduler.shutdown()

# List scheduled jobs
jobs = scheduler.get_jobs()
for job in jobs:
    print(f"Job ID: {job.id}, next run time: {job.next_run_time}")