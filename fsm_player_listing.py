"""/
- Player listing service
	- Evaluate All_players from last season
	- Evaluate All_players for this season
	- Provide a list of suggested players(pre-listing)
	- De-listing players


/"""
from flask import Flask, request, jsonify

app = Flask(__name__)

players = []
suggested_activations = []

@app.route('/suggested_activations', methods=['GET'])
def get_suggested_activations():
    return jsonify(suggested_activations), 200

@app.route('/create_player', methods=['POST'])
def create_player():
    player = request.json
    players.append(player)
    return jsonify({"message": "Player created successfully"}), 201

@app.route('/update_player/<int:player_id>', methods=['PUT'])
def update_player(player_id):
    updated_player = request.json
    for player in players:
        if player['player_id'] == player_id:
            player.update(updated_player)
            return jsonify({"message": "Player updated successfully"}), 200
    return jsonify({"error": "Player not found"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)