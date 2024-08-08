"""/- Player gwk service
	- create player game weeks
	- create player fixtures
/"""

from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/activate_players', methods=['POST'])
def activate_players():
    # Step 1: Query the player listing service for a list of suggested activations
    suggested_players = requests.get(f'{PLAYER_LISTING_SERVICE_URL}/suggested_activations').json()


@app.route('/suggested_activations', methods=['GET'])
def get_suggested_activations():
    return jsonify(suggested_activations), 200











if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)