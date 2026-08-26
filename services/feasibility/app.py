from flask import Flask, request, jsonify
from scoring import score_sequence

app = Flask(__name__)


@app.route("/feasibility", methods=["POST"])
def feasibility():
    payload = request.get_json(silent=True) or {}
    sequence = payload.get("sequence", "")
    if not sequence:
        return jsonify({"error": "sequence is required"}), 400
    try:
        result = score_sequence(sequence)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
