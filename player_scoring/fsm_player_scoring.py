"""/Scoring service 

Implementpost api from player config

Call the players worker util  to get listed player information

Call the players worker util service to post player score and listing price/ptv
-----------

call the gwk worker util service to get gameweek status, gameweek information (team rating counters, form counter, player points)

Calculate player scores (WAC, Deviation, new price and ptv) weekly and update DB (notify player util service after insertion)



/"""

from flask import Flask, request, jsonify
import requests
import sqlite3
import pandas as pd
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

"""/Scoring service 

Implementpost api from player config

Call the players worker util  to get listed player information

Call the players worker util service to post player score and listing price/ptv
-----------

call the gwk worker util service to get gameweek status, gameweek information (team rating counters, form counter, player points)

Calculate player scores (WAC, Deviation, new price and ptv) weekly and update DB (notify player util service after insertion)



/"""

from flask import Flask, request, jsonify
import requests
import sqlite3
import pandas as pd
import json

app = Flask(__name__)

conn = sqlite3.connect('players.db')
cursor = conn.cursor()

################################################################# Consume GET & Post endpoint from player config service,
################################################################# Consume Get endpoint from player service (fetch player information) and fetch unique player scores and weights
################################################################# Merge Dictionaties and Update Player score DB and Player service
#*********** Get score weights and default prices

URL = 'http://127.0.0.1:5020/default_prices'

response = requests.get(URL)

if response.status_code == 200:
    data = response.json()  # This line parses the JSON response into a Python dictionary

    # Now you can use data directly without needing to use json.loads again
    activation_price = data['activation_price']
    persona_weight = data['persona_weight']
    ls_weight = data ['ls_performance_weight']
    persona_form_weight = data['persona_sub_attributes']['persona_form_weight']
    persona_age_weight = data['persona_sub_attributes']['persona_age_weight']
    persona_team_rating_weight = data['persona_sub_attributes']['persona_team_rating_weight']

    # ... other operations using the dictionary

else:
    print(f"Error: Failed to retrieve data. Status code: {response.status_code}")


print({activation_price},{persona_weight},{persona_age_weight},{persona_form_weight},{persona_team_rating_weight},{ls_weight})

#player_configs = pd.Series(activation_price,persona_weight,persona_age_weight,persona_form_weight,persona_team_rating_weight, index=['activation_price', 'persona_weight', 'persona_age_weight', 'persona_form_weight', 'persona_team_rating_weight'])

#print(player_configs)

#*********** Get Player information & Query Config Service for scores

# URL to get player information
URL = 'http://127.0.0.1:5005/player_information'

# Function to handle the POST request and consolidate information
def process_player_info(player_info):
    web_name = player_info.get('web_name', 'N/A')
    player_id = player_info.get('player_id', 'N/A')
    age = player_info.get('age', 'N/A')
    ls_z_scores = player_info.get('ls_z_scores', 'N/A')

    # Create the JSON data for the POST request
    json_data = {
        'age': age,
        'ls_performance': ls_z_scores,
        'form': 3,
        'team_rating': 1.00
    }

    # Send POST request to another endpoint
    response = requests.post('http://127.0.0.1:5020/player_configs', json=json_data)

    if response.status_code == 200:
        data3 = response.json()  # Parse the JSON response
        
        # Key mapping for modifying data3 keys
        key_mapping = {
            'pl_age': 'age_weight',
            'pl_age_price': 'age_price',
            'pl_form': 'form_weight',
            'pl_form_price': 'form_price',
            'pl_l': 'lsp_weight',
            'pl_l_price': 'lsp_price',
            'pl_team_rating': 'tr_weight',
            'pl_team_rating_price': 'tr_price'
        }

        # Modify data3 keys using key_mapping
        new_data3 = {key_mapping.get(k, k): v for k, v in data3.items()}

        # Consolidate original player information with modified data3
        consolidated_info = {
            'web_name': web_name,
            'player_id': player_id,
            'age': age,
            'ls_z_scores': ls_z_scores,
            **new_data3  # Unpack the modified POST response data
        }

        return consolidated_info
    else:
        print(f"Error in POST request: {response.status_code}")
        return None

# Send GET request to retrieve player information
response2 = requests.get(URL)
consolidated_player_information = []  # List to store consolidated player information

if response2.status_code == 200:
    data2 = response2.json()  # Parse the JSON response

    if isinstance(data2, list):  # Ensure the response is a list of dictionaries
        for player_info in data2:
            consolidated_info = process_player_info(player_info)
            if consolidated_info:
                consolidated_player_information.append(consolidated_info)
    else:
        print("Unexpected data format, expected a list of dictionaries:", data2)
else:
    print(f"Error: Failed to retrieve data. Status code: {response2.status_code}")

#print(consolidated_player_information)

##################################################################################
# Insert into player score DB with default prices
##################################################################################

for player in consolidated_player_information:
    print(player)
    # Fetch or create a snapshot entry
    """cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
    snapshot_id = cursor.lastrowid

    # Insert a new row into the PlayerScores table
    cursor.execute('''
        INSERT INTO PlayerScores (player_id, snapshot_id, age_weight, age_price, form_weight, form_price, tr_weight, tr_price, lsp_weight, lsp_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            player['player_id'],
            snapshot_id,
            player.get('age_weight', 0),  # Defaulting to 0 if key doesn't exist
            player.get('age_price', 0),
            player.get('form_weight', 0),
            player.get('form_price', 0),
            player.get('tr_weight', 0),
            player.get('tr_price', 0),
            player.get('lsp_weight', 0),
            player.get('lsp_price', 0)
        ))"""

##################################################################################
# Update the player value
##################################################################################
def update_player_scores():
    consolidated_player_information2 = []  # List to store consolidated player updates

    # Fetch player rating data
    URL = 'http://127.0.0.1:5030/gwk_info'
    response4 = requests.get(URL)
    data4 = response4.json()

    # Extracting relevant fields from gwk info data
    ply_id = [item.get('player_id', 0) for item in data4]
    player_id = ply_id[-15:]
    gweek = [item.get('gameweek', 0) for item in data4]
    gameweek = gweek[-15:]

    # Fetch team rating data
    URL = 'http://127.0.0.1:5030/team_rating'
    response5 = requests.get(URL)
    data5 = response5.json()

    team_name = [item.get('team_name', 0) for item in data5]
    team_ratings = [item.get('tr_avg', 0) for item in data5]
    team_player_id = [item.get('player_id', 0) for item in data5]

    team_name_dict = dict(zip(team_player_id, team_name))
    team_ratings_dict = dict(zip(team_player_id, team_ratings))

    # Fetch form rating data
    URL = 'http://127.0.0.1:5030/player_rating'
    response5 = requests.get(URL)
    data5 = response5.json()

    form_ratings = [item.get('form_rating', 0) for item in data5]
    fr_player_id = [item.get('player_id', 0) for item in data5]
    form_ratings_dict = dict(zip(fr_player_id, form_ratings))

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

consolidated_player_information2 = update_player_scores()
print(consolidated_player_information2)

##################################################################################
# Update player score DB with default prices
##################################################################################

def update_database(consolidated_player_information2):
    connection = sqlite3.connect('players.db')  # Adjust with your database path
    cursor = connection.cursor()

    for player in consolidated_player_information2:
        # Fetch or create a snapshot entry
        cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
        snapshot_id = cursor.lastrowid

        # Update the PlayerScores table
        cursor.execute('''
            UPDATE PlayerScores
            SET snapshot_id = ?, 
                new_age_weight = ?, 
                new_age_price = ?, 
                new_form_weight = ?, 
                new_form_price = ?, 
                new_tr_weight = ?, 
                new_tr_price = ?, 
                lsp_weight = ?, 
                lsp_price = ?
            WHERE player_id = ?
        ''', (
            snapshot_id,
            player.get('new_age_weight', 0),  
            player.get('new_age_price', 0),
            player.get('new_form_weight', 0),
            player.get('new_form_price', 0),
            player.get('new_tr_weight', 0),
            player.get('new_tr_price', 0),
            player.get('lsp_weight', 0),
            player.get('lsp_price', 0),
            player['player_id']
        ))

    connection.commit()
    connection.close()
update_database(consolidated_player_information2)
##################################################################################
# Create and Update the player score endpoints
##################################################################################
# Flask route to retrieve player scores
@app.route('/player_scores', methods=['GET'])
def get_player_scores():
    player_scores = consolidated_player_information
    return jsonify(player_scores)

# Flask route to retrieve player scores
@app.route('/scores_update', methods=['GET'])
def get_scores_update():
    scores_update = consolidated_player_information2
    return jsonify(scores_update)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5022)

conn.commit()
conn.close()
