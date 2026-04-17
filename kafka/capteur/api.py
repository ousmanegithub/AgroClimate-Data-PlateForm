from flask import Flask, jsonify, request

from sensor import sensor_stream

app = Flask(__name__)


@app.before_request
def ensure_stream_started():
    sensor_stream.start()


@app.route("/sensor", methods=["GET"])
def get_sensor_data():
    data = sensor_stream.latest()
    if data is None:
        return jsonify({"message": "Sensor stream warming up"}), 503
    return jsonify(data)


@app.route("/sensor/batch", methods=["GET"])
def get_multiple_data():
    n = int(request.args.get("n", 10))
    data = sensor_stream.history(n=n)
    return jsonify(data)


@app.route("/sensor/<region>", methods=["GET"])
def get_sensor_by_region(region):
    n = int(request.args.get("n", 10))
    data = sensor_stream.history_by_region(region=region, n=n)
    return jsonify(data)


@app.route("/sensor/stats", methods=["GET"])
def sensor_stats():
    return jsonify(sensor_stream.stats())


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "sensor_stream": sensor_stream.stats()}


if __name__ == "__main__":
    sensor_stream.start()
    app.run(host="0.0.0.0", port=5000, debug=False)

