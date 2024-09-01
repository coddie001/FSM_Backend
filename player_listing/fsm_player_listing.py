"""/
- Player listing service
	- connect to DB all players and fetch all - done
	- calculate z_scores - done
	- Evaluate All_players from last season - in progress
	- Evaluate All_players for this season - in progress
	- Provide a list of suggested players(pre-listing)
	- De-listing players


/"""
from flask import Flask, request, jsonify
from utils.player_utils import process_players
from utils.db_utils import connect_to_db, create_snapshot
import sqlite3
import pandas as pd
import numpy as np
import random

app = Flask(__name__)

"""########################################################################### Generate Z_scores (code_snippet)

# Ensure foreign keys are enabled
conn.execute("PRAGMA foreign_keys = ON")

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

# Select web_name, total_points from DB

all_players = pd.read_sql_query("SELECT all_players_id, web_name, total_points FROM All_Players", conn)

df_all_players = pd.DataFrame(all_players)

# Dataframe total points

total_points = df_all_players['total_points']

# function to generate mean and standard deviation from player points

def generate_avgs(total_points):
	mean_p = sum(total_points)/len(total_points)
	std = np.std(total_points)
	return mean_p, std

mean_point, standard_dev = generate_avgs(total_points)
print(mean_point,standard_dev)


# Generate Z_scores
z_scores =(total_points-mean_point)/standard_dev

for i, z in enumerate(z_scores):
	print(f"Player: {df_all_players.iloc[i]['web_name']}, Z-score: {z}")

# Update the dataframe with the Z-scores and snapshot_id
df_all_players['ls_z_scores'] = z_scores
print(z_scores)
df_all_players['snapshot_id'] = snapshot_id

# Here you should ideally update the existing rows, not append as new rows
for index, row in df_all_players.iterrows():
    cursor.execute('''
        UPDATE All_Players
        SET ls_z_scores = ?, snapshot_id = ?
        WHERE all_players_id = ?
    ''', (row['ls_z_scores'], row['snapshot_id'], row['all_players_id']))

"""########################################################################### Evaluate players for suggestions
@app.route('/generate_prelist', methods=['GET'])
def generate_prelist():
    # Connect to the database
    conn = connect_to_db('players.db')
    cursor = conn.cursor()
    
    # Query the database
    ls_players = pd.read_sql_query(
        "SELECT element_id, web_name, total_points, ls_z_scores FROM All_Players", conn
    )
    lsp_list = ls_players.values.tolist()

    def generate_prelist(lsp_list):
        # Initialize lists to hold players based on their Z-scores
        group_0_1 = []
        group_1_2 = []
        group_2_up = []

        # Sort players into the appropriate groups based on Z-scores
        for player in lsp_list:
            z_score = player[3]  # Assuming Z-score is the 4th column
            if 0 < z_score < 1:
                group_0_1.append(player)
            elif 1 <= z_score < 2:
                group_1_2.append(player)
            elif z_score >= 2:
                group_2_up.append(player)

        # Select random players from each group
        selected_0_1 = random.sample(group_0_1, min(3, len(group_0_1)))
        selected_1_2 = random.sample(group_1_2, min(6, len(group_1_2)))
        selected_2_up = random.sample(group_2_up, min(6, len(group_2_up)))

        # Combine the selected players into a final list
        fsm_prelist = selected_0_1 + selected_1_2 + selected_2_up
        return fsm_prelist

    # Generate the final list of players
    fsm_prelist = generate_prelist(lsp_list)

    # Close the database connection
    conn.close()

    return jsonify(fsm_prelist), 200
    conn.commit()
    conn.close()
###########################################################################
	
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)