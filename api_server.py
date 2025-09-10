# -----------------------------------------------------------------------------
# File: api_server.py
# Description: This is the Backend API Server built with FastAPI.
# Its only job is to connect securely to MongoDB and provide data to anyone who asks.
# -----------------------------------------------------------------------------
import os
from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

# Load environment variables from a .env file for security
load_dotenv()

# --- Database Connection ---
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")

# Check if environment variables are loaded
if not all([MONGO_USER, MONGO_PASSWORD, MONGO_HOST]):
    raise ValueError("Missing MongoDB credentials in .env file. Please create one.")

# Create the secure connection string
MONGO_URI = f"mongodb://{MONGO_USER}:{quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:27019/?authSource=admin"
MONGO_DB_NAME = "Smart_Framing_Db"

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB_NAME]

# Initialize the FastAPI app
app = FastAPI(
    title="SmartFarm IoT API",
    description="API for providing telemetry data from SmartFarm sensors and Raspberry Pi.",
    version="1.0.0"
)

# --- Helper Function ---
def mongo_to_json(cursor):
    """Converts MongoDB cursor to a JSON serializable list of dictionaries."""
    results = list(cursor)
    # Convert ObjectId to string for JSON compatibility
    for doc in results:
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])
    return results

# --- API Endpoints ---

@app.get("/", tags=["Status"])
def read_root():
    """Root endpoint to check if the API is running."""
    return {"status": "SmartFarm API is running!"}

@app.get("/data/{source}", tags=["Telemetry Data"])
def get_telemetry_data(
    source: str,
    start_date: datetime = Query(None, description="Start date for the data range (ISO format, e.g., 2025-09-10T00:00:00)"),
    end_date: datetime = Query(None, description="End date for the data range (ISO format, e.g., 2025-09-10T23:59:59)")
):
    """
    Fetches telemetry data from a specified source (SmartFarm or RaspberryPi)
    within a given time range.
    """
    if source.lower() == "smartfarm":
        collection = db["telemetry_data_clean"]
        device_name = "SmartFarm"
    elif source.lower() == "raspberrypi":
        collection = db["raspberry_pi_telemetry_clean"]
        device_name = "raspberry_pi_status"
    else:
        raise HTTPException(status_code=404, detail="Source not found. Use 'smartfarm' or 'raspberrypi'.")

    # Set default time range if not provided (last 7 days)
    if start_date is None:
        start_date = datetime.utcnow() - timedelta(days=7)
    if end_date is None:
        end_date = datetime.utcnow()

    # Query the database
    try:
        query = {
            "deviceName": device_name,
            "timestamp_utc_dt": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        documents = collection.find(query)
        json_results = mongo_to_json(documents)
        
        if not json_results:
             raise HTTPException(status_code=404, detail="No data found for the specified range.")
             
        return json_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# To run this API server:
# 1. Make sure you have a .env file with your MongoDB credentials.
# 2. In your terminal, run: uvicorn api_server:app --reload
