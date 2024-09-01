"""- import suggested players from listing service
	- write logic for listing players (player activation balance and the activation price)
		- activation balance can not go below 0, initial activation balance of $1,000,000
		- activation price is $50000 by default
		- define function default season signing (15 players) - $750,0000
		- define function generate player ID (random)
		- define function create player (Queue - FIFO player for activation and do the following, debit value for player signing and update new balance on table, generate player ID, assign player status)
		- Update all tables with new players
		- create get method to fetch player information"""


import requests
import pandas as pd
import csv
import sqlite3
from bs4 import BeautifulSoup
from flask import Flask, jsonify
import re
from datetime import datetime
import time
import os

app = Flask(__name__)

"""####################################################### DATAFRAME Suggested players

URL = 'http://127.0.0.1:5001/generate_prelist'

response = requests.get(URL)

data = response.json()

df_suggested_players = pd.DataFrame(data)

print (df_suggested_players)

df_suggested_players.to_csv('df_suggested_players.csv')

"""####################################################### Update Activations DB, deduct player signing fee from activation credit


suggested_players = pd.read_csv('df_suggested_players.csv')
# print(suggested_players)

# Connect to the database
conn = sqlite3.connect('players.db')
cursor = conn.cursor()

"""# Values to insert
activation_credit = 1000000
last_debit_value = 0

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

# Insert a new row into the Activations table
cursor.execute('''
    INSERT INTO Activations (snapshot_id, activation_credit, last_debit_value)
    VALUES (?, ?, ?)
''', (snapshot_id, activation_credit, last_debit_value))

# Commit the changes
conn.commit()

# Verify the insertion
cursor.execute('SELECT * FROM Activations WHERE snapshot_id = ?', (snapshot_id,))
new_row = cursor.fetchone()
print(f"Inserted row: {new_row}")"""

# Query the database
activation_cr = pd.read_sql_query("SELECT activation_credit_id, activation_credit, last_debit_value FROM Activations", conn)

signing_fee = 50000
total_debit = 0

activation_credit_id = activation_cr['activation_credit_id'].iloc[0]
credit = activation_cr['activation_credit'].iloc[0]
debit = activation_cr['last_debit_value'].iloc[0]
print(f"credit and debit value: {credit} {debit} {activation_credit_id}")

def player_listing(credit, suggested_players):
    global signing_fee, total_debit
    new_activation_cr = 0
    count_players = 0
    for index, player in suggested_players.iterrows():
        count_players += 1
        new_signing_fee = signing_fee * count_players
        print(new_signing_fee)
    total_debit += new_signing_fee
    print(total_debit)
    new_activation_cr = credit - total_debit
    return new_activation_cr, total_debit

new_activation_cr, total_debit = player_listing(credit, suggested_players)

print(f"New activation credit: {new_activation_cr}")
print(f"Total debit: {total_debit}")

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

conn.commit()

# Create a DataFrame with the values for easier control of insertion
df_activations = pd.DataFrame({
	'activation_credit_id':[activation_credit_id],
    'activation_credit': [new_activation_cr],
    'last_debit_value': [total_debit],
    'snapshot_id': [snapshot_id]
})

"""# Convert DataFrame to a list of tuples
data_to_update = df_activations.to_records(index=False).tolist()
print(data_to_update)

# Update the data in the Activations table
cursor.executemany('''
    UPDATE Activations
    SET snapshot_id = ?, activation_credit = ?, last_debit_value = ?, activation_credit_id = ?
''', [(row[3], row[1], row[2], row[0]) for row in data_to_update])

# Commit the changes
conn.commit()

# Verify the update
for row in data_to_update:
    activation_credit_id = row[0]
    cursor.execute('SELECT * FROM Activations WHERE activation_credit_id = ?', (activation_credit_id,))
    updated_row = cursor.fetchone()
    print(f"Updated row: {updated_row}")


# Insert data into All_Players
# df_activations.to_sql('Activations', conn, if_exists='append', index=activation_credit_id)

# Commit the transaction to save changes
conn.commit()

# Close the database connection
conn.close()

"""############################################################################################################# Generate Player IDs & Statuses
def generate_player_ID(suggested_players):
    # Initialize the ID counter
    ID_counter = 1  # Start from 1 or 0 depending on your preference
    player_ids = []  # List to store generated player IDs
    
    # Iterate through each player
    for index, player in suggested_players.iterrows():
        # Format the ID with leading zeros (e.g., 001, 002, etc.)
        player_id = f"FSM{ID_counter:02d}"  # 'P' is a prefix, and 03d formats ID to 3 digits with leading zeros
        player_ids.append(player_id)
        
        # Print the generated ID for debugging
        print(f"Player {index + 1}: ID = {player_id}")
        
        # Increment the counter
        ID_counter += 1
    
    return player_ids

# Generate player IDs
player_ids = generate_player_ID(suggested_players)
print(player_ids)

def generate_player_status(suggested_players, player_ids):
    Player_statuses = []
    for player_id in player_ids:
        if player_id !=None:
            status = "listed"
        else:
            status = "de-listed"
        
        Player_statuses.append(status)
    
    return Player_statuses

# Example usage:
Player_statuses = generate_player_status(suggested_players, player_ids)

#print(Player_statuses)

df_listed_cols = ['ID', 'Element ID', 'fsm_name', 'points', 'z_scores']
df_listed = pd.DataFrame(suggested_players)
print(df_listed)
# Renaming specific columns
df_listed_info = df_listed.rename(columns={
    '0': 'ID',
    '0': 'element_id',
    '1': 'web_name',
    '2': 'ls_total_point',
    '3': 'ls_z_scores'
})


df_listed_info ['fsm_player_ID'] = player_ids
df_listed_info ['fsm_status'] = Player_statuses

print(df_listed_info)


###################################################################################################### Prepare listed player for final action by merging statuses & IDs and Update DBs (Player, All Players)


# Query the database
player_info = pd.read_sql_query("""
    SELECT all_players_id, first_name, second_name,web_name,season_ID
    FROM All_Players 
    WHERE web_name IN ('Iwobi','Harrison','Van de Ven','Pickford','Bernardo',
                       'Saliba','Gibbs-White','Hee Chan','Richarlison','Rice',
                       'Saka','Gordon','M.Salah','Palmer','Ødegaard')
""", conn)

#print (player_info)

new_player_info = pd.merge(df_listed_info, player_info,how='left', on='web_name')


# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

new_player_info['snapshot_id'] = snapshot_id


print (new_player_info)

# Insert new rows into the Players table
for index, row in new_player_info.iterrows():
	cursor.execute('''
		UPDATE Players
		SET element_id = ?,snapshot_id = ?
		WHERE web_name = ?
		''', (row['element_id'], snapshot_id, row['web_name']))
"""cursor.execute('''
        INSERT INTO Players (player_id, snapshot_id, season_id, web_name, first_name, second_name, ls_z_scores, ls_total_point, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (row['fsm_player_ID'], row['snapshot_id'], row['season_id'], row['web_name'], row['first_name'], row['second_name'], row['ls_z_scores'], row['ls_total_point'], row['fsm_status']))

    cursor.execute('''
    UPDATE Players
    SET season_id = ?,snapshot_id = ?
    ''', (2, snapshot_id))
    cursor.execute('''
	    UPDATE All_Players
	    SET season_id = ?, snapshot_id = ?
	    WHERE all_players_id = ? AND web_name = ?
	''', (2, row['snapshot_id'], row['all_players_id'], row['web_name']))"""



# Commit the changes
conn.commit()


###################################################################################################### Get Player Age & DOB

conn = sqlite3.connect('players.db')
cursor = conn.cursor()

# Select first name and second name from the players table
cursor.execute("SELECT first_name, second_name FROM players")
players = cursor.fetchall()
player_data = []

for player in players:
    first_name = player[0]
    second_name = player[1]
    full_name = f"{first_name} {second_name}"
    print(full_name)
    
    # Use the full name in the search URL
    url = f'https://www.google.com//search?q=google+{full_name}+dob&amp;sca_esv=4a9febc07b5fa96a&amp;ie=UTF-8&amp;source=lnt&amp;tbs=qdr:h&amp;sa=X&amp;ved=0ahUKEwjN48G5g-2HAxWHD7kGHWGbAfQQpwUIDA'
    
    # Send a GET request to the URL
    response = requests.get(url)

    # Check for successful response (status code 200)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Inspect the HTML and use the correct class name
        divs_with_header = soup.find_all('div', class_="BNeawe s3v9rd AP7Wnd")
        
        # Extract and print the text content from the found divs
        for div in divs_with_header:
            info_list = re.split(r"; Born:", div.text)
            dob = next((item for item in info_list if "Born:" in item), None)
            if dob:  # If dob is not None, print or append it
                print(dob)
                if dob:  # If dob is not None, clean and append it
                # Apply regex to extract the formatted date of birth
                    pattern = r"(\d{1,2})\s+([a-zA-Z]{3,})\s+(\d{4})"
                    dob_match = re.search(pattern, dob)
                    if dob_match:
                        dob = dob_match.group(0)  # Get the matched date string
                    else:
                        dob = None  # If no match is found, set dob to None

                    # Extract age using regex
                    age_match = re.search(r"age\s(\d{2})\s?[Āá†]?years", div.text)
                    age = age_match.group(1) if age_match else 'Unknown'

                    # Append the player's data
                    player_data.append((first_name, second_name, dob, age))

        # Pause for 5 seconds before processing the next player
        time.sleep(5)

# Creating DataFrame from the collected data
df = pd.DataFrame(player_data, columns=['first_name', 'second_name', 'dob', 'age'])

# Convert the 'dob' column to datetime, if possible
df['dob'] = pd.to_datetime(df['dob'], errors='coerce')

# Display the DataFrame
print(df)

# Convert the 'dob' column to string format (YYYY-MM-DD) before updating the database
df['dob'] = df['dob'].dt.strftime('%Y-%m-%d')	

"""for index, row in df.iterrows():
    cursor.execute('''
    UPDATE Players
    SET dob = ?, age = ?
    WHERE second_name = ?
    ''', (row['dob'], row['age'], row['second_name']))"""


conn.commit()
conn.close()

# Save the DataFrame to a CSV file
#df.to_csv('player_dob.csv', index=False)

###################################################################################################### Flask app - Get (fetch player information from DB, convert to json and jsonify)
@app.route('/player_information', methods=['GET'])
def player_information():
    conn = sqlite3.connect('players.db')  # Open a connection to the database
    try:
        # Query the database
        fsm_players_activated = pd.read_sql_query("SELECT * FROM Players", conn)
        
        # Convert DataFrame to JSON
        fsm_players_activated_json = fsm_players_activated.to_dict(orient='records')
        
        # Return JSON response
        return jsonify(fsm_players_activated_json)
    finally:
        

        conn.close()  # Ensure the connection is closed

if __name__ == '__main__':
    app.run(host=os.getenv('IP', '0.0.0.0'), 
            port=int(os.getenv('PORT', 5005)))
###################################################################################################### Flask app - Post (update player information, service) - Player Utils



###################################################################################################### END
