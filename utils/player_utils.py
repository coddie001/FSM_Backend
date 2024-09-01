import pandas as pd
from all_players.all_players import gen_players_24_25  # Import function from all_players
import sqlite3

def process_players():
    # Call the imported function to get the data and convert it to a DataFrame
    players_24_25 = pd.DataFrame(gen_players_24_25())
    return players_24_25

# Expose only the processed DataFrame to other modules
players_24_25 = process_players()

print(players_24_25)


conn = sqlite3.connect('players.db')
cursor = conn.cursor()

all_players = pd.read_sql_query("SELECT all_players_id, web_name, total_points, ls_z_scores FROM All_Players", conn)