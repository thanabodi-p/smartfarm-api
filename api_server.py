import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from urllib.parse import quote_plus
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from bson import ObjectId
import json

# --- Helper class to handle non-serializable MongoDB data ---
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

# --- Load Environment Variables ---
load_dotenv()

# --- App Initialization ---
app = FastAPI()

# --- MongoDB Connection Details ---
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")

if not all([MONGO_USER, MONGO_PASSWORD, MONGO_HOST]):
    raise ValueError("Missing MongoDB credentials in environment variables.")

MONGO_URI = f"mongodb://{MONGO_USER}:{quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:27019/?authSource=admin"
MONGO_DB_NAME = "Smart_Framing_Db"

client = None
db = None

# --- Lifespan event to manage database connection ---
@app.on_event("startup")
def startup_db_client():
    global client, db
    try:
        # Added serverSelectionTimeoutMS to fail faster if connection is bad
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB_NAME]
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("✅ Successfully connected to MongoDB.")
    except ConnectionFailure as e:
        print(f"❌ Could not connect to MongoDB: {e}")
        # We don't raise an exception here, so the app can start and report errors via API calls
        client = None
        db = None

@app.on_event("shutdown")
def shutdown_db_client():
    if client:
        client.close()
        print("MongoDB connection closed.")

# --- Helper function to fetch data ---
def get_data(collection_name: str, device_name: str):
    if not db or not client:
        raise HTTPException(status_code=503, detail="Database connection is not available.")
    
    try:
        collection = db[collection_name]
        end_date_utc = datetime.utcnow()
        start_date_utc = end_date_utc - timedelta(days=7)
        
        query = {
            "deviceName": device_name,
            "timestamp_utc": {
                "$gte": start_date_utc.strftime('%Y-%m-%dT%H:%M:%S'),
                "$lt": end_date_utc.strftime('%Y-%m-%dT%H:%M:%S')
            }
        }
        documents = list(collection.find(query).sort("_id", -1))
        
        # Use our custom JSONEncoder to handle ObjectId
        return json.loads(JSONEncoder().encode(documents))

    except OperationFailure as e:
        # This often happens with authentication errors
        raise HTTPException(status_code=500, detail=f"Database operation failed: {e.details}")
    except Exception as e:
        # Catch any other unexpected errors
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "SmartFarm API is running!"}

@app.get("/data/smartfarm")
def get_smartfarm_data():
    return get_data(collection_name="telemetry_data_clean", device_name="SmartFarm")

@app.get("/data/raspberrypi")
def get_rpi_data():
    return get_data(collection_name="raspberry_pi_telemetry_clean", device_name="raspberry_pi_status")