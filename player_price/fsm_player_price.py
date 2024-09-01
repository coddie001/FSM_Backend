
"""/- Player price service
	- calculate listing price and ptv
	- calculate weekly new price and ptv and update DB
	- calculate rolling price and ptv and update DB

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
# GENERATE PLAYER PRICES & PTV
##############################################################################

URL = 'http://127.0.0.1:5022/player_scores'
response = requests.get(URL)
data = response.json()
print (data)

df_scores = pd.json_normalize(data)
print (df_scores)

URL2 = 'http://127.0.0.1:5022/scores_update'
response2 = requests.get(URL2)
data2 = response2.json()
print (data2)

df_scores2 = pd.json_normalize(data2)


# Remove the dollar sign and convert the price columns to numeric types
df_scores['age_price'] = df_scores['age_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores['tr_price'] = df_scores['tr_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores['lsp_price'] = df_scores['lsp_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores['form_price'] = df_scores['form_price'].replace('[\$,]', '', regex=True).astype(float)

df_scores2['new_age_price'] = df_scores2['new_age_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores2['new_tr_price'] = df_scores2['new_tr_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores2['lsp_price'] = df_scores2['lsp_price'].replace('[\$,]', '', regex=True).astype(float)
df_scores2['new_form_price'] = df_scores2['new_form_price'].replace('[\$,]', '', regex=True).astype(float)

print (df_scores2)

num_gwks = 38

def create_listing_value(df_scores):
    global num_gwks
    # Sum the columns to calculate the listing price
    df_scores['listing_price'] = df_scores[['age_price', 'tr_price', 'lsp_price', 'form_price']].sum(axis=1)
    
    # Calculate the PTV (price-to-value) by dividing the listing price by the number of gameweeks
    df_scores['listing_ptv'] = df_scores['listing_price'] / num_gwks
    
    return df_scores[['web_name', 'player_id','listing_price', 'listing_ptv']]

# Get the listing prices and PTVs
df_listings = create_listing_value(df_scores)

print(df_listings)

conn.execute("PRAGMA foreign_keys = ON")

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

"""for index, rows in df_scores.iterrows():
    cursor.execute('''
        INSERT INTO PlayerPrices (player_id, snapshot_id, listing_price, listing_ptv)
        VALUES (?, ?, ?, ?)
        ''', (rows['player_id'],snapshot_id, rows['listing_price'], rows['listing_ptv']))"""

conn.commit()


def update_player_value(df_scores2):
    global num_gwks
    # Sum the columns to calculate the listing price
    df_scores2['gwk_price'] = df_scores2[['new_age_price', 'new_tr_price', 'lsp_price', 'new_form_price']].sum(axis=1)
    
    # Calculate the PTV (price-to-value) by dividing the listing price by the number of gameweeks
    df_scores2['gwk_ptv'] = df_scores2['gwk_price'] / num_gwks
    
    return df_scores2[['web_name', 'player_id','gwk_price', 'gwk_ptv']]

# Get the listing prices and PTVs
df_gwk_values = update_player_value(df_scores2)

print(df_gwk_values)

#############################################################Create DB Coloumn for each new gameweek
"""current_gwk = tuple(df_scores2['gameweek'].values[:1])[0]

print (current_gwk)

# First ALTER TABLE statement for gwk_{current_gwk}_price
cursor.execute(f'''
    ALTER TABLE PlayerPrices
    ADD COLUMN gwk_{current_gwk}_price REAL;
''')

# Second ALTER TABLE statement for gwk_{current_gwk}_ptv
cursor.execute(f'''
    ALTER TABLE PlayerPrices
    ADD COLUMN gwk_{current_gwk}_ptv REAL;
''')

conn.commit()

##################################### INSERT INTO DB #################################################

conn.execute("PRAGMA foreign_keys = ON")

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

for index, rows in df_scores2.iterrows():
    cursor.execute(f'''
        UPDATE PlayerPrices
        SET snapshot_id = ?,
            gwk_{current_gwk}_price = ?,
            gwk_{current_gwk}_ptv = ?
        WHERE player_id = ?
        ''', (snapshot_id, rows['gwk_price'], rows['gwk_ptv'], rows['player_id']))

    conn.commit()"""

conn.close()
