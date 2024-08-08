"""/
- Players service
	- create players and assign player ID and status
	- get player info
	- update player info()
	- get player fixtures ()
	- update playerfixtures
	- get player configs
	= update player configs
	- get player scores
	- update player scores
	- create player game week
	- get player game week info
	- update player game week

	
/"""

from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

PLAYER_LISTING_SERVICE_URL = 'http://player-listing-service:5001'
PLAYER_CONFIG_SERVICE_URL = 'http://player-config-service:5002'
PLAYER_PRICING_SERVICE_URL = 'http://player-pricing-service:5003'
PLAYER_GWK_SERVICE_URL = 'http://player-gwk-service:5004'
PLAYER_SCORING_SERVICE_URL = 'http://player-scoring-service:5005'

@app.route('/activate_players', methods=['POST'])
def activate_players():
    # Step 1: Query the player listing service for a list of suggested activations
    suggested_players = requests.get(f'{PLAYER_LISTING_SERVICE_URL}/suggested_activations').json()

    # Step 2: Check if there is enough credit for player activation
    for player in suggested_players:
        credit = requests.get(f'{PLAYER_PRICING_SERVICE_URL}/activation_price/{player["player_id"]}').json()
        if credit < player['activation_price']:
            return jsonify({"error": "Not enough credit"}), 400

    # Step 3: Count number of potential player activations
    potential_activations = len(suggested_players)

    # Step 4: Route to scoring service and config service for player weight assignment
    for player in suggested_players:
        player_config = requests.get(f'{PLAYER_CONFIG_SERVICE_URL}/config/{player["player_id"]}').json()
        activation_score = requests.post(f'{PLAYER_SCORING_SERVICE_URL}/activation_score', json=player_config).json()

    # Step 5: Route to pricing service to generate player listing price
    for player in suggested_players:
        listing_price = requests.post(f'{PLAYER_PRICING_SERVICE_URL}/listing_price', json=player).json()

    # Step 6: Create Player ID and update player information in DB and assign status new
    for player in suggested_players:
        player['status'] = 'new'
        response = requests.post(f'{PLAYER_LISTING_SERVICE_URL}/create_player', json=player)
        if response.status_code != 201:
            return jsonify({"error": "Failed to create player"}), 500

    # Step 7: Route to gameweek service to create player gameweek fixtures
    for player in suggested_players:
        response = requests.post(f'{PLAYER_GWK_SERVICE_URL}/create_fixtures', json={"player_id": player["player_id"]})
        if response.status_code != 201:
            return jsonify({"error": "Failed to create player fixtures"}), 500

    # Step 8: Update all tables and update player status to listed
    for player in suggested_players:
        player['status'] = 'listed'
        response = requests.put(f'{PLAYER_LISTING_SERVICE_URL}/update_player/{player["player_id"]}', json=player)
        if response.status_code != 200:
            return jsonify({"error": "Failed to update player status"}), 500

    return jsonify({"message": "Players activated successfully"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5006)
