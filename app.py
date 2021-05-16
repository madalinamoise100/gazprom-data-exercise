import flask
import json
import core

app = flask.Flask(__name__)

@app.route("/app/meters")
def get_meter_data():
    data = core.data.get_meters()
    return flask.jsonify(data)

@app.route("/app/meter/<meter_id>")
def get_meters(meter_id):
    data = core.data.get_meter_data(meter_id)
    return flask.jsonify(data)

@app.route("/app/files")
def get_files():
    data = core.data.get_files()
    return flask.jsonify(data)

@app.route("/app/last_file")
def get_last_file():
    data = core.data.get_last_file()
    return flask.jsonify(data)

if __name__ == "__main__":
    import os

    if os.name == 'nt':
        host = os.environ["COMPUTERNAME"]
    else:
        host = 'localhost'

    app.run(host, 1337, debug=True)
