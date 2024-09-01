"""/- Player config service
    - persona config 
    - performance config 
    - Add new config

/"""
import sqlite3
import pandas as pd
from flask import Flask, jsonify, request
import os


app = Flask(__name__)

# Get the path to the directory one level higher than the current script
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Construct the full path to the players.db file
db_path = os.path.join(parent_dir, 'players.db')

# Connect to the SQLite database using the constructed path
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Connect to the database
# conn = sqlite3.connect('players.db')


# Values to insert
activation_price = 50000
persona_w = 0.70
age_w = 0.10 * persona_w
form_w = 0.70 * persona_w
team_rating_w = 0.20 * persona_w
ls_performance_w = 0.30
age = 0.00
form=0.00
team_rating=0.00
ls_performance=0.0000000000000


################################################################################ Insert Pre-defined Values into Player_Configs DB

"""# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

# Insert a new row into the Activations table
cursor.execute('''
    INSERT INTO Player_Configs (snapshot_id, persona, age, form, team_rating, ls_performance, activation_price)
    VALUES (?, ?, ?, ?, ?, ?, ?)
''', (snapshot_id, persona, age, form, team_rating, ls_performance, activation_price))

################################################################################ Expose Player config to flask web 
# Query the database
player_configs = pd.read_sql_query("SELECT player_config_id, persona, age, form, team_rating, ls_performance, activation_price FROM Player_Configs", conn)
print(player_configs)

# Commit the changes
conn.commit()"""

def default_prices():
    global activation_price, persona_w, age_w, form_w, ls_performance_w, team_rating_w
    p_price= 0.70 * activation_price
    a_price = age_w * activation_price
    f_price = form_w* activation_price
    tr_price = team_rating_w* activation_price
    l_price = ls_performance_w * activation_price
    return f_price, a_price, l_price, tr_price, p_price, activation_price, persona_w, age_w, form_w, ls_performance_w, team_rating_w

f_price, a_price, l_price, tr_price, p_price, activation_price, persona_w, age_w, form_w, ls_performance_w, team_rating_w= default_prices()

form_price='${:.2f}'.format(f_price)
age_price='${:.2f}'.format(a_price)
ls_performance_price='${:.2f}'.format(l_price)
team_rating_price='${:.2f}'.format(tr_price)
persona_price ='${:.2f}'.format(p_price)

print (form_price, age_price, ls_performance_price, team_rating_price, persona_price)


def player_configs(age_w, form_w, team_rating_w, ls_performance_w, f_price, a_price, l_price, tr_price, age, form, team_rating, ls_performance):
    # Initialize variables as float with 2 decimal places
    pl_age = 0.00
    pl_age_price = 0.00
    pl_form = 0.00
    pl_form_price = 0.00
    pl_team_rating = 0.00
    pl_team_rating_price= 0.00
    pl_l = 0.00
    pl_l_price = 0.00

    # Age calculation
    if age < 20:
        pl_age = round(1.00 * age_w, 4)
    elif 20 <= age < 30:
        pl_age = round(0.70 * age_w, 4)
    elif 30 <= age < 36:
        pl_age = round(0.35 * age_w, 4)
    else:
        pl_age = round(0.15 * age_w, 4)
    pl_age_price = round(pl_age * activation_price, 2)
    
    # Form calculation
    if form == 3:
        pl_form = round(1.00 * form_w, 4)
    elif form == 2:
        pl_form = round(0.75 * form_w, 4)
    elif form == 1:
        pl_form = round(0.50 * form_w, 4)
    else:
        pl_form = round(0.20 * form_w, 4)
    pl_form_price = round(pl_form * activation_price, 2)
    
    # Team rating calculation
    if team_rating <= 0.35:
        pl_team_rating = round(0.30 * team_rating_w, 4)
    elif team_rating < 0.50:
        pl_team_rating = round(0.60 * team_rating_w, 4)
    elif team_rating < 0.65:
        pl_team_rating = round(1.00 * team_rating_w, 4)
    else:
        pl_team_rating = round(1.00 * team_rating_w, 4)
    pl_team_rating_price = round(pl_team_rating * activation_price, 2)
    
    # Last season performance calculation
    if -1 < ls_performance <= 0:
        pl_l = round(0.10 * ls_performance_w, 4)
    elif 0 < ls_performance <= 1:
        pl_l = round(0.30 * ls_performance_w, 4)
    elif 1 < ls_performance <= 2:
        pl_l = round(0.50 * ls_performance_w, 4)
    else:
        pl_l = round(1.00 * ls_performance_w, 4)
    pl_l_price = round(pl_l * activation_price, 2)
    
    return pl_age, pl_age_price, pl_form, pl_form_price, pl_team_rating, pl_team_rating_price, pl_l, pl_l_price

pl_age, pl_age_price, pl_form, pl_form_price, pl_team_rating, pl_team_rating_price, pl_l, pl_l_price = player_configs(age_w, form_w, team_rating_w, ls_performance_w, f_price, a_price, l_price, tr_price, age, form, team_rating, ls_performance)

print (pl_age, pl_age_price, pl_form, pl_form_price, pl_team_rating, pl_team_rating_price, pl_l, pl_l_price)

@app.route('/player_configs', methods=['POST'])
def get_player_configs():
    data = request.get_json()
    
    # Extract values from the request
    age = data.get('age', 25)
    form = data.get('form', 1)
    team_rating = data.get('team_rating', 0.5)
    ls_performance = data.get('ls_performance', 1.0000000000000)
    f_price = data.get('f_price', 100)
    a_price = data.get('a_price', 100)
    l_price = data.get('l_price', 100)
    tr_price = data.get('tr_price', 100)
    
    # Call the player_configs function with the provided values
    pl_age, pl_age_price, pl_form, pl_form_price, pl_team_rating, pl_team_rating_price, pl_l, pl_l_price = player_configs(age_w, form_w, team_rating_w, ls_performance_w, f_price, a_price, l_price, tr_price, age, form, team_rating, ls_performance)
    # Validate the input ranges
    if not (15 <= age <= 60):
        return jsonify({"error": "Age must be between 15 and 60"}), 400
    if not (0 <= form <= 3):
        return jsonify({"error": "Form must be between 0 and 3"}), 400
    if not (0 <= team_rating <= 1):
        return jsonify({"error": "Team rating must be between 0 and 1"}), 400
    if not (-1 <= ls_performance <= 5):
        return jsonify({"error": "LS Performance must be between -1 and 5"}), 400
    # Add more validation as needed for the other parameters
    
    result = {
        "pl_age": pl_age,
        "pl_age_price": f"${pl_age_price:.2f}",
        "pl_form": pl_form,
        "pl_form_price": f"${pl_form_price:.2f}",
        "pl_team_rating": pl_team_rating,
        "pl_team_rating_price": f"${pl_team_rating_price:.2f}",
        "pl_l": pl_l,
        "pl_l_price": f"${pl_l_price:.2f}"
    }
    
    return jsonify(result)


@app.route('/default_prices', methods=['GET'])
def get_default_prices():
    f_price, a_price, l_price, tr_price, p_price, activation_price, persona_w, age_w, form_w, ls_performance_w, team_rating_w= default_prices()
    
    result = {
        "default_age_price": f"${a_price:.2f}",
        "default_form_price": f"${f_price:.2f}",
        "default_team_rating_price": f"${tr_price:.2f}",
        "default_ls_price": f"${l_price:.2f}",
        "default_persona_price": f"${p_price:.2f}",
        "ls_performance_weight": f"{ls_performance_w:.2f}",
        "persona_weight": f"{persona_w:.2f}",
        "persona_sub_attributes": {
            "persona_team_rating_weight": f"{team_rating_w:.2f}",
            "persona_form_weight": f"{form_w:.2f}",
            "persona_age_weight": f"{age_w:.2f}",
        },
        "activation_price": f"${activation_price:.2f}"
    }
    
    return jsonify(result)

if __name__ == '__main__':
    app.run(host=os.getenv('IP', '0.0.0.0'), 
            port=int(os.getenv('PORT', 5020)))