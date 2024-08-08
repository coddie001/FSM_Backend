"""/- Player config service
    - persona config 
    - performance config 
    - Add new config

/"""


from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)