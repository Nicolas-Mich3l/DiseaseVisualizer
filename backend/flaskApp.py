from flask import Flask, jsonify, request
from flask_cors import CORS
from db import dbWrapper
import numpy as np

app = Flask(__name__)
CORS(app)

db = None


# lazy load DB to not have flask crash during bootup
def get_db():
    global db
    if db is None:
        db = dbWrapper("./omop.db")
    return db


def calculate_stats(values):
    """
    Calculate summary statistics for a list of values.
    Returns n, median, p25, p75, mean.
    """
    if not values or len(values) == 0:
        return {"n": 0, "median": None, "p25": None, "p75": None, "mean": None}

    # Remove None/NaN values
    clean_values = [float(v) for v in values if type(v) is str or type(v) is float]
    if len(clean_values) == 0:
        return {"n": 0, "median": None, "p25": None, "p75": None, "mean": None}

    sorted_values = sorted(clean_values)
    n = len(sorted_values)

    return {
        "n": n,
        "median": float(np.median(sorted_values)),
        "p25": float(np.percentile(sorted_values, 25)),
        "p75": float(np.percentile(sorted_values, 75)),
        "mean": float(np.mean(sorted_values)),
    }


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """
    Endpoint: /api/stats?disease_id=<id>&measurement_id=<id>
    Returns summary statistics for disease vs non-disease cohorts.
    """
    disease_id = request.args.get("disease_id")
    measurement_id = request.args.get("measurement_id")

    if not disease_id or not measurement_id:
        return jsonify({"error": "Missing disease_id or measurement_id parameter"}), 400

    try:
        db_instance = get_db()
        disease_id = int(disease_id)
        measurement_id = int(measurement_id)

        # Get cohorts
        disease_cohort, healthy_cohort = db_instance.get_cohorts(disease_id)

        # Get measurements for each cohort
        disease_measurements = db_instance.get_measurements(
            disease_cohort["person_id"].tolist(), measurement_id
        )

        healthy_measurements = db_instance.get_measurements(
            healthy_cohort["person_id"].tolist(), measurement_id
        )

        # Extract values
        disease_values = disease_measurements["value_as_number"].tolist()
        healthy_values = healthy_measurements["value_as_number"].tolist()

        # Calculate statistics
        disease_stats = calculate_stats(disease_values)
        healthy_stats = calculate_stats(healthy_values)

        return jsonify({"diseaseStats": disease_stats, "healthyStats": healthy_stats})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cohort", methods=["GET"])
def get_cohort():
    """
    Endpoint: /api/cohort?disease_id=
    Returns both disease and non-disease cohorts with demographics.
    """
    disease_id = request.args.get("disease_id")
    if not disease_id:
        return jsonify({"error": "Missing disease_id parameter"}), 400

    try:
        db_instance = get_db()
        disease_id = int(disease_id)
        disease_cohort, healthy_cohort = db_instance.get_cohorts(disease_id)
        out = jsonify(
            {
                "disease": disease_cohort.to_dict(orient="records"),
                "healthy": healthy_cohort.to_dict(orient="records"),
            }
        )
        return out

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cohort-full", methods=["GET"])
def get_cohort_full():
    """
    Endpoint: /api/cohort-full?disease_id=<id>&measurement_id=<id>
    Returns cohorts, age/sex distribution, and summary statistics in one call.
    """
    disease_id = request.args.get("disease_id")
    measurement_id = request.args.get("measurement_id")

    if not disease_id:
        return jsonify({"error": "Missing disease_id parameter"}), 400

    try:
        db_instance = get_db()
        disease_id = int(disease_id)

        # Get cohorts
        disease_cohort, healthy_cohort = db_instance.get_cohorts(disease_id)
        response = {
            "disease": disease_cohort.to_dict(orient="records"),
            "healthy": healthy_cohort.to_dict(orient="records"),
        }

        if measurement_id:
            measurement_id = int(measurement_id)
            # Get measurementsds
            disease_measurements = db_instance.get_measurements(
                disease_cohort["person_id"].tolist(), measurement_id
            )
            healthy_measurements = db_instance.get_measurements(
                healthy_cohort["person_id"].tolist(), measurement_id
            )

            # Calculate statistics
            disease_values = disease_measurements["value_as_number"].tolist()
            healthy_values = healthy_measurements["value_as_number"].tolist()
            response["diseaseStats"] = calculate_stats(disease_values)
            response["healthyStats"] = calculate_stats(healthy_values)
            response["diseaseMeasurments"] = disease_values
            response["healthyMeasurements"] = healthy_values

        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
