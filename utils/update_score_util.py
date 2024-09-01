import pandas as pd
import sqlite3
import requests
import os
from flask import Flask, request, jsonify
import numpy as np

app = Flask(__name__)

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

player_tms = pd.read_sql("SELECT player_id, pl_team_id FROM players", conn)

conn.close()
player_teams = pd.DataFrame(player_tms)

# Define the dictionary for team results
"""wk1_team_results = {
    'Manchester City': 'win',
    'Brighton and Hove Albion': 'win',
    'Arsenal': 'win',
    'Liverpool': 'win',
    'Tottenham Hotspur': 'draw',
    'Newcastle United': 'win',
    'Nottingham Forest': 'draw',
    'Chelsea': 'loss',
    'West Ham United': 'loss',
    'Fulham': 'loss',
    'Manchester United': 'win',
    'Aston Villa': 'win',
    'Brentford': 'win',
    'Bournemouth': 'draw',
    'Leicester City': 'draw',
    'Southampton': 'loss',
    'Crystal Palace': 'loss',
    'Ipswich Town': 'loss',
    'Wolverhampton Wanderers': 'loss',
    'Everton': 'loss'
}

# Define the dictionary for team results
wk1_team_id = {
    'Manchester City': 13,
    'Brighton and Hove Albion': 5,
    'Arsenal': 1,
    'Liverpool': 12,
    'Tottenham Hotspur': 18,
    'Newcastle United': 15,
    'Nottingham Forest': 16,
    'Chelsea': 6,
    'West Ham United': 19,
    'Fulham': 9,
    'Manchester United': 14,
    'Aston Villa': 2,
    'Brentford': 4,
    'Bournemouth': 3,
    'Leicester City': 11,
    'Southampton': 17,
    'Crystal Palace': 7,
    'Ipswich Town': 10,
    'Wolverhampton Wanderers': 20,
    'Everton': 8
}
# Initialize lists to hold team data
team_names = []
wins = []
draws = []
losses = []

# Populate lists based on the results dictionary
for team, result in wk1_team_results.items():
    team_names.append(team)
    # Fill in win, draw, loss as 1 or 0 based on the team's result
    wins.append(1 if result == 'win' else 0)
    draws.append(1 if result == 'draw' else 0)
    losses.append(1 if result == 'loss' else 0)

# Create the DataFrame with the specified columns
team_rankings = pd.DataFrame({
    'team_name': team_names,
    'win': wins,
    'draw': draws,
    'loss': losses
})

team_rankings['win_weight'] = team_rankings['win'] * 1.00
team_rankings['draw_weight'] = team_rankings['draw'] * 0.50
team_rankings['loss_weight'] = team_rankings['loss'] * 0.00
team_rankings['team_ratings'] = team_rankings['win_weight'] + team_rankings['draw_weight'] + team_rankings['loss_weight']

# Map the team IDs to the DataFrame
team_rankings['team_id'] = team_rankings['team_name'].map(wk1_team_id)

# Display the DataFrame
print(team_rankings)

teams_ratings_dict = dict(zip(team_rankings['team_id'], team_rankings['team_ratings']))
teams_name_dict = dict(zip(team_rankings['team_id'], team_rankings['team_name']))
player_teams['team_name'] = player_teams['pl_team_id'].map(teams_name_dict)
player_teams['tr_avg'] = player_teams['pl_team_id'].map(teams_ratings_dict)
print (player_teams)"""

############################################################ WK2 Team Rating #########################


############################################################WK1 & WK2 SCORING##########################
def gwk_scoring():
    conn = sqlite3.connect(db_path)
    gwk = pd.read_sql("SELECT player_id, element_id, gameweek, total_points FROM playergwk", conn)
    player_v = pd.read_sql("SELECT player_id, listing_price, listing_ptv, gwk_1_price, gwk_1_ptv FROM playerprices", conn)
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
            point_value = row['gp_multi'] / row['listing_price'] if gameweek == 1 else row['gp_multi'] / row[f'gwk_{previous_gwk}_price']

            # Calculate weighted contributions for each week
            if gameweek < current_gwk:
                player_ids.append(row['player_id'])
                w_wk0_value = point_value * total_gwk_points_0
                w_wk1_value = point_value * total_gwk_points_1
                w_wk0.append(w_wk0_value)
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

def form_rating(current_gwk_df):
    form_ratings = []  # Initialize a list to store form ratings

    for idx, row in current_gwk_df.iterrows():
        dev_1 = row['dev_wk1']
        dev_2 = row['dev_wk2']

        if dev_1 >= 0 and dev_2 >= 0:
            form_rating = 3
        elif (dev_2 >= 0 and dev_1 < 0) or \
             (dev_1 >= 0 and dev_2 < 0):
            form_rating = 2
        else:
            form_rating = 0

        form_ratings.append(form_rating)

    """for idx, row in current_gwk_df.iterrows():
        dev_1 = row['dev_wk1']

        if dev_1 >= 0:
            form_rating = 3
        else:
            form_rating = 0

        form_ratings.append(form_rating)"""

    current_gwk_df['form_rating'] = form_ratings  # Assign the list to the DataFrame column
    return current_gwk_df

player_gwk2 = form_rating(current_gwk_df)
print(player_gwk2)

###############################scoring function (week 1)####################################

def update_player_scores(player_gwk2, player_gwk):
    consolidated_player_information2 = []  # List to store consolidated player updates

    player_id = player_gwk['player_id'][-15:]
    gameweek = player_gwk['gameweek'][-15:]

    # for week 1
    form_ratings = player_gwk2['form_rating']
    fr_player_id = player_gwk2['player_id']
    form_ratings_dict = dict(zip(fr_player_id, form_ratings))

    """player_id = player_gwk['player_id'][0:15]
    gameweek = player_gwk['gameweek'][0:15]"""

    # Fetch team rating data - week 2 and beyond
    URL = 'http://127.0.0.1:5030/team_rating'
    response5 = requests.get(URL)
    data5 = response5.json()

    team_name = [item.get('team_name', 0) for item in data5]
    team_ratings = [item.get('tr_avg', 0) for item in data5]
    team_player_id = [item.get('player_id', 0) for item in data5]

    team_name_dict = dict(zip(team_player_id, team_name))
    team_ratings_dict = dict(zip(team_player_id, team_ratings))

    #for week 1
    """team_name = player_teams['team_name']
    team_ratings = player_teams['tr_avg']
    team_player_id = player_teams['player_id']

    team_name_dict = dict(zip(team_player_id, team_name))
    team_ratings_dict = dict(zip(team_player_id, team_ratings))"""

    # Fetch player information
    URL = 'http://127.0.0.1:5005/player_information'
    response2 = requests.get(URL)
    age = []
    ls_z_scores = []
    web_name = []
    player_id2 = []
    if response2.status_code == 200:
        data2 = response2.json()  # Parse the JSON response
        if isinstance(data2, list):  # Ensure the response is a list of dictionaries
            for player_info in data2:
                age.append(player_info.get('age', 'N/A'))
                ls_z_scores.append(player_info.get('ls_z_scores', 'N/A'))
                web_name.append(player_info.get('web_name', 'N/A'))
                player_id2.append(player_info.get('player_id', 'N/A'))

    age_dict = dict(zip(player_id2, age))
    web_name_dict = dict(zip(player_id2, web_name))
    ls_z_scores_dict = dict(zip(player_id2, ls_z_scores))

    # Creating DataFrame
    ply_gwk_scoring_df = pd.DataFrame({
        'player_id': player_id,
        'gameweek': gameweek,
    })
    
    ply_gwk_scoring_df['web_name'] = ply_gwk_scoring_df['player_id'].map(web_name_dict)
    ply_gwk_scoring_df['age'] = ply_gwk_scoring_df['player_id'].map(age_dict)
    ply_gwk_scoring_df['ls_z_scores'] = ply_gwk_scoring_df['player_id'].map(ls_z_scores_dict)
    ply_gwk_scoring_df['team_name'] = ply_gwk_scoring_df['player_id'].map(team_name_dict)
    ply_gwk_scoring_df['team_ratings'] = ply_gwk_scoring_df['player_id'].map(team_ratings_dict)
    ply_gwk_scoring_df['form_ratings'] = ply_gwk_scoring_df['player_id'].map(form_ratings_dict)

    print(ply_gwk_scoring_df)

    # Process DataFrame and make POST requests
    for index, rows in ply_gwk_scoring_df.iterrows():
        json_data2 = {
            'age': rows['age'],
            'ls_performance': rows['ls_z_scores'],
            'form': rows['form_ratings'],
            'team_rating': rows['team_ratings'],
        }

        # Send POST request
        response = requests.post('http://127.0.0.1:5020/player_configs', json=json_data2)
        
        if response.status_code == 200:
            data6 = response.json()

            # Key mapping for modifying response keys
            key_mapping = {
                'pl_age': 'new_age_weight',
                'pl_age_price': 'new_age_price',
                'pl_form': 'new_form_weight',
                'pl_form_price': 'new_form_price',
                'pl_l': 'lsp_weight',
                'pl_l_price': 'lsp_price',
                'pl_team_rating': 'new_tr_weight',
                'pl_team_rating_price': 'new_tr_price'
            }
            new_data6 = {key_mapping.get(k, k): v for k, v in data6.items()}

            consolidated_info2 = {
                'player_id': rows['player_id'],
                'gameweek': rows['gameweek'],
                'web_name': rows['web_name'],
                'age': rows['age'],
                'team_name': rows['team_name'],
                'ls_z_scores': rows['ls_z_scores'],
                'form_ratings': rows['form_ratings'],
                'team_ratings': rows['team_ratings'],
                **new_data6
            }

            if consolidated_info2:
                consolidated_player_information2.append(consolidated_info2)
        else:
            print(f"Error: Failed to retrieve data for player at index {index}. Status code: {response.status_code}")

    return consolidated_player_information2

consolidated_player_information2 = update_player_scores(player_gwk2, player_gwk)
print(consolidated_player_information2)

##################################################################################
# Create and Update the player score endpoints
##################################################################################

# Flask route to retrieve player scores
@app.route('/scores_update', methods=['GET'])
def get_scores_update():
    scores_update = consolidated_player_information2
    return jsonify(scores_update)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5026)

conn.commit()
conn.close()