import os
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import pandas as pd
import logging
# Ensure ServerApi is imported if needed for your MongoDB connection
# from pymongo.server_api import ServerApi

# ... (connection setup and fetch_data function remain the same) ...

load_dotenv()
app = Flask(__name__)

# Replace with your MongoDB URI
MONGODB_URI = os.getenv('MONGODB_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

# Simplified connection structure (adapt as needed for your specific URI)
try:
    client = MongoClient(MONGODB_URI)
    collection = client[DATABASE_NAME][COLLECTION_NAME]
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection error: {e}")

# Helper function (assuming it converts the 'ts' column to datetime objects)
def fetch_data():
    data = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(data)
    # This line is crucial for conversion from epoch (unit="s") to datetime object
    df["ts"] = pd.to_datetime(df["ts"], unit="s")
    return df

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/debug")
def debug():
    df = fetch_data()
    return df.head().to_json(orient="records")


@app.route("/api/data")
def api_data():
    df = fetch_data()
    logging.warning(df.head())

    result = {}
    for device in df["device"].unique():
        device_df = df[df["device"] == device].sort_values("ts")
        result[device] = {
            # FIX: Use .isoformat() to generate reliable ISO 8601 timestamps
            #"timestamps": device_df["ts"].apply(lambda x: x.isoformat()).tolist(),
            "timestamps": device_df["ts"].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ').tolist(),
            "co": device_df["co"].tolist(),
            "humidity": device_df["humidity"].tolist(),
            "temp": device_df["temp"].tolist()
        }

    return jsonify(result)
'''


@app.route("/api/data")
def api_data():
    try:
        df = fetch_data()

        # Check if data is empty
        if df.empty:
            return jsonify({"error": "No data available"})

        result = {}
        for device in df["device"].unique():
            device_df = df[df["device"] == device].sort_values("ts")

            # Ensure we have data for this device
            if not device_df.empty:
                # Convert to ISO format with timezone
                result[device] = {
                    "timestamps": device_df["ts"].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ').tolist(),
                    "co": device_df["co"].fillna(0).astype(float).round(6).tolist(),
                    "humidity": device_df["humidity"].fillna(0).astype(float).round(2).tolist(),
                    "temp": device_df["temp"].fillna(0).astype(float).round(2).tolist()
                }

        app.logger.info(f"Returning data for {len(result)} devices")
        return jsonify(result)

    except Exception as e:
        app.logger.error(f"Error in api_data: {str(e)}")
        return jsonify({"error": str(e)})

'''

@app.route("/api/debug-data")
def debug_data():
    df = fetch_data()

    # Check if dataframe is empty
    if df.empty:
        return jsonify({"error": "No data found", "columns": list(df.columns)})

    # Get basic info about the data
    debug_info = {
        "total_records": len(df),
        "devices": df["device"].unique().tolist(),
        "columns": df.columns.tolist(),
        "date_range": {
            "min": str(df["ts"].min()) if not df.empty else "No data",
            "max": str(df["ts"].max()) if not df.empty else "No data"
        },
        "sample_data": df.head(3).to_dict('records') if not df.empty else []
    }

    return jsonify(debug_info)


if __name__ == "__main__":
    app.run(debug=True)
