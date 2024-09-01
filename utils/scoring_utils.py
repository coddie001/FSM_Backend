import requests
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import timezone
import numpy as np
from flask import Flask, jsonify

app = Flask(__name__)

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

#########################################################################################################
# Calculate team_rating score
#########################################################################################################

def team_rating():
	conn = sqlite3.connect(db_path)
	tms = pd.read_sql("SELECT team_id, pl_team_id, team_name, wins, draws,losses, matches_played FROM teams", conn)
	player_tms = pd.read_sql("SELECT player_id, pl_team_id FROM players", conn)

	conn.close()
	teams = pd.DataFrame(tms)
	player_teams = pd.DataFrame(player_tms)
	teams['win_weight'] = teams ['wins'] * 1.00
	teams['draw_weight'] = teams ['draws'] * 0.50
	teams['loss_weight'] = teams ['losses'] * 0.00
	teams['total_weight'] = teams ['win_weight'] + teams ['draw_weight'] + teams ['loss_weight']
	total_games = 39
	print(teams)

	tr_avg_list = []

	for index, row in teams.iterrows():
		if 1 <= row['matches_played'] <= 2 and row['matches_played'] <= total_games:
			tr_avg = row['total_weight'] / row['matches_played']
		elif row['matches_played'] >= 3 and row['matches_played'] <= total_games:
			tr_avg = row['total_weight'] / 3
		else:
			tr_avg = None 

		tr_avg_list.append(tr_avg)

	teams['tr_avg'] = tr_avg_list

	return teams, teams['tr_avg'], player_teams

teams, tr_avg, player_teams = team_rating()
print(teams)

teams_id_dict = dict(zip(teams['pl_team_id'], teams['tr_avg']))
teams_name_dict = dict(zip(teams['pl_team_id'], teams['team_name']))
player_teams['team_name'] = player_teams['pl_team_id'].map(teams_name_dict)
player_teams['tr_avg'] = player_teams['pl_team_id'].map(teams_id_dict)
print (player_teams)

#########################################################################################################
# Calculate gwk_scoring
#########################################################################################################
def gwk_scoring():
    conn = sqlite3.connect(db_path)
    gwk = pd.read_sql("SELECT player_id, element_id, gameweek, total_points FROM playergwk", conn)

    p_gwk = gwk['gameweek'].max() - 1
    print(p_gwk)

    p_p_gwk = gwk['gameweek'].max() - 2
    print(p_gwk)

    player_v = pd.read_sql(f"SELECT player_id, listing_price, listing_ptv, gwk_{p_gwk}_price, gwk_{p_gwk}_ptv, gwk_{p_p_gwk}_price, gwk_{p_p_gwk}_ptv  FROM playerprices", conn)
    conn.close()

    player_gwk = pd.merge(gwk, player_v, how = 'left', on = 'player_id')
        
    # Rename the 'total_points' column to 'gp'
    player_gwk.rename(columns={'total_points': 'gp'}, inplace=True)

    # Update the 'total_points' column based on 'gp'
    player_gwk['total_points'] = player_gwk['gp']
    player_gwk['gp_multi'] = player_gwk['gp'] * 10 
    print (player_gwk)

    current_gwk = player_gwk['gameweek'].max()
    print(current_gwk)

    # Assuming previous_gwk is correctly defined as the previous gameweek
    previous_gwk = player_gwk['gameweek'].max() - 1
    print(previous_gwk)

    past_previous_gwk = player_gwk['gameweek'].max() - 2
    print(previous_gwk)


    total_gwk_points_0 = player_gwk.loc[player_gwk['gameweek'] == current_gwk - 2, 'gp_multi'].sum()

    
    total_gwk_points_1 = player_gwk.loc[player_gwk['gameweek'] == current_gwk - 1, 'gp_multi'].sum()
    print(total_gwk_points_1)

    # Calculate the total gameweek points for the current gameweek
    total_gwk_points_2 = player_gwk.loc[player_gwk['gameweek'] == current_gwk, 'gp_multi'].sum()
    print(total_gwk_points_2)

    wac_wk0 = []
    wac_wk1 = []
    wac_wk2 = []

    player_ids = []
    
    grouped = player_gwk.groupby('gameweek')

    # Iterate over each group
    for gameweek, group in grouped:
        w_wk0, w_wk1, w_wk2 = [], [], []

        for _, row in group.iterrows():
            #point_value = row['gp_multi'] / row['listing_price'] if gameweek == 1 else row['gp_multi'] / row[f'gwk_{previous_gwk}_price']
            point_value = (row['gp_multi'] / row['listing_price'] if gameweek == 1 else row['gp_multi'] / row[f'gwk_{past_previous_gwk}_price'] if gameweek == previous_gwk 
            	else row['gp_multi'] / row[f'gwk_{previous_gwk}_price'])

            # Calculate weighted contributions for each week
            if gameweek < current_gwk and gameweek < previous_gwk:
                player_ids.append(row['player_id'])
                w_wk0_value = point_value * total_gwk_points_0
                w_wk0.append(w_wk0_value)
            elif gameweek < current_gwk and gameweek == previous_gwk:
            	w_wk1_value = point_value * total_gwk_points_1
            	w_wk1.append(w_wk1_value)
            elif gameweek == current_gwk:
                #player_ids.append(row['player_id'])
                w_wk2_value = point_value * total_gwk_points_2
                w_wk2.append(w_wk2_value)
            else:
                print("gameweek is upcoming")

        # Convert lists to numpy arrays for numerical operations
        wac_wk0.extend(w_wk0)  # Extend the main list with this game's WAC values
        wac_wk1.extend(w_wk1)
        wac_wk2.extend(w_wk2)

    # Convert to numpy arrays after processing all groups
    wac_wk0 = np.array(wac_wk0)
    wac_wk1 = np.array(wac_wk1)
    wac_wk2 = np.array(wac_wk2)

    # Calculate the mean of each week's WAC
    mean_wac_wk0 = np.mean(wac_wk0)
    mean_wac_wk1 = np.mean(wac_wk1)
    mean_wac_wk2 = np.mean(wac_wk2)

    # Calculate deviations from the mean for each week
    dev_wk0 = wac_wk0 - mean_wac_wk0
    dev_wk1 = wac_wk1 - mean_wac_wk1
    dev_wk2 = wac_wk2 - mean_wac_wk2

    # Print deviations for verification
    print("Deviations for Week 0:", dev_wk0)
    print("Deviations for Week 1:", dev_wk1)
    print("Deviations for Week 2:", dev_wk2)

    # Create a new DataFrame with player IDs and calculated values
    current_gwk_df = pd.DataFrame({
        'player_id': player_ids,
        'wac_wk0': wac_wk0,
        'wac_wk1': wac_wk1,
        'wac_wk2': wac_wk2,
        'dev_wk0': dev_wk0,
        'dev_wk1': dev_wk1,
        'dev_wk2': dev_wk2
    })

    # Display or further process the new DataFrame
    print(current_gwk_df)
    return player_gwk, current_gwk_df

# Execute the gwk_scoring function and print the result
player_gwk, current_gwk_df = gwk_scoring()
print(player_gwk)
print(current_gwk_df)
#########################################################################################################
# Calculate form_rating score
#########################################################################################################

def form_rating(current_gwk_df):
    form_ratings = []  # Initialize a list to store form ratings

    for idx, row in current_gwk_df.iterrows():
        dev_0 = row['dev_wk0']
        dev_1 = row['dev_wk1']
        dev_2 = row['dev_wk2']

        if dev_0 >= 0 and dev_1 >= 0 and dev_2 >= 0:
            form_rating = 3
        elif (dev_0 >= 0 and dev_2 >= 0 and dev_1 < 0) or \
             (dev_0 >= 0 and dev_1 >= 0 and dev_2 < 0) or \
             (dev_1 >= 0 and dev_2 >= 0 and dev_0 < 0):
            form_rating = 2
        elif (dev_0 >= 0 and dev_1 < 0 and dev_2 < 0) or \
             (dev_1 >= 0 and dev_0 < 0 and dev_2 < 0) or \
             (dev_2 >= 0 and dev_0 < 0 and dev_1 < 0):
            form_rating = 1
        else:
            form_rating = 0

        form_ratings.append(form_rating)

    current_gwk_df['form_rating'] = form_ratings  # Assign the list to the DataFrame column
    return current_gwk_df

player_gwk2 = form_rating(current_gwk_df)
print(player_gwk2)

#########################################################################################################
# UPDATE PLAYER GWK, Teams DB
#########################################################################################################
def update_DBs(player_gwk2, player_gwk, teams, conn):
    cursor = conn.cursor()

    # Updating PlayerGWK with player_gwk2 data
    for _, player in player_gwk2.iterrows():
        # Fetch or create a snapshot entry
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid

        # Perform the update on PlayerGWK
        cursor.execute('''
            UPDATE PlayerGWK
            SET snapshot_id = ?, 
                gwk_avg_contr = ?, 
                mean_dev1 = ?, 
                mean_dev2 = ?, 
                form_count = ?
            WHERE player_id = ? AND gameweek = (SELECT MAX(gameweek) FROM PlayerGWK WHERE player_id = ?)
        ''', (
            snapshot_id,
            player.get('wac_wk2'),
            player.get('dev_wk1'),
            player.get('dev_wk2'),
            player.get('form_rating'),
            player.get('player_id'),
            player.get('player_id')
        ))

    # Updating PlayerGWK with player_gwk data
    for _, player in player_gwk.iterrows():
        # Fetch or create a snapshot entry
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid
        
        # Perform the update on PlayerGWK
        cursor.execute('''
            UPDATE PlayerGWK
            SET snapshot_id = ?, 
                gp_multi = ?
            WHERE player_id = ? AND gameweek = (SELECT MAX(gameweek) FROM PlayerGWK WHERE player_id = ?)
        ''', (
            snapshot_id,
            player.get('gp_multi'),
            player.get('player_id'),
            player.get('player_id')
        ))

    # Updating Teams table with teams data
    for _, team in teams.iterrows():
        # Fetch or create a snapshot entry
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid

        # Perform the update on Teams
        cursor.execute('''
            UPDATE Teams
            SET snapshot_id = ?, 
                team_rating = ?
            WHERE pl_team_id = ?
        ''', (
            snapshot_id,
            team.get('tr_avg'),
            team.get('pl_team_id')
        ))

    # Commit the changes to the database
    conn.commit()

conn = sqlite3.connect(db_path)
try:
    update_DBs(player_gwk2, player_gwk, teams, conn)
finally:
    conn.close()
#########################################################################################################
# flask app
#########################################################################################################
@app.route('/player_rating', methods=['GET'])
def player_rating():
    pr = [
        'player_id', 'form_rating','dev_wk0','dev_wk1', 'dev_wk2','wac_wk0','wac_wk1','wac_wk2'
    ]
    
    player_gwk_subset = player_gwk2[pr]
    data = player_gwk_subset.to_dict(orient='records')
    
    return jsonify(data)

@app.route('/gwk_info', methods=['GET'])
def gwk_info():
    gwk = [
        'player_id', 'element_id', 'gameweek','total_points', 
        'gp_multi', 'listing_price', 'listing_ptv', 'gwk_1_price', 'gwk_1_ptv'
    ]
    
    player_gwk_subset = player_gwk[gwk]
    data = player_gwk_subset.to_dict(orient='records')
    
    return jsonify(data)

@app.route('/team_rating', methods=['GET'])
def team_rating():
    tr = [
        'player_id', 'team_name', 'tr_avg'
    ]
    
    tr_gwk_subset = player_teams[tr]
    data2 = tr_gwk_subset.to_dict(orient='records')
    
    return jsonify(data2)


#########################################################################################################
# main app
#########################################################################################################

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5030)
    
    # Define timezone (assuming 'Africa/Lagos')
    wat_timezone = timezone('Africa/Lagos')

    scheduler = BackgroundScheduler(timezone=wat_timezone)
    
    # Schedule the update functions to run every 3 days
    scheduler.add_job(team_rating, 'interval', days=3, next_run_time=wat_timezone.localize(datetime.now()))
    scheduler.add_job(gwk_scoring, 'interval', days=3, next_run_time=wat_timezone.localize(datetime.now()))
    scheduler.add_job(form_rating, 'interval', days=3, args=[current_gwk_df], next_run_time=wat_timezone.localize(datetime.now()))
    
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down scheduler...")
        scheduler.shutdown()