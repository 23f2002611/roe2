import pandas as pd
import functools
from fastapi import FastAPI, Request, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

# --- Constants ---
CSV_FILE = "q-fastapi-timeseries-cache.csv"

# --- Load Data Once ---
try:
    df = pd.read_csv(CSV_FILE)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"Successfully loaded and parsed {CSV_FILE}.")
except FileNotFoundError:
    print(f"Error: {CSV_FILE} not found. Please download it from the exam page.")
    exit()

# --- Caching Helper Function ---
@functools.lru_cache(maxsize=128)
def get_stats(location: Optional[str], sensor: Optional[str], 
              start_date: Optional[str], end_date: Optional[str]):
    """
    Performs the data filtering and aggregation.
    This function's results will be cached.
    """
    filtered_df = df.copy()
    
    if location:
        filtered_df = filtered_df[filtered_df['location'] == location]
    if sensor:
        filtered_df = filtered_df[filtered_df['sensor'] == sensor]
    if start_date:
        filtered_df = filtered_df[filtered_df['timestamp'] >= pd.to_datetime(start_date)]
    if end_date:
        # Add 1 day to end_date to make it inclusive
        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)
        filtered_df = filtered_df[filtered_df['timestamp'] < end_dt]

    count = len(filtered_df)
    
    if count == 0:
        return {"count": 0, "avg": 0, "min": 0, "max": 0}

    avg_val = filtered_df['value'].mean()
    min_val = filtered_df['value'].min()
    max_val = filtered_df['value'].max()

    return {
        "count": int(count),
        "avg": round(avg_val, 2),
        "min": round(min_val, 2),
        "max": round(max_val, 2)
    }

# --- FastAPI App ---
app = FastAPI(title="IoT Sensor Analytics API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Private Network header for local testing
@app.middleware("http")
async def add_pna_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response

# --- API Endpoint ---
@app.get("/stats")
async def stats_endpoint(
    response: Response,
    location: Optional[str] = Query(None),
    sensor: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """
    Analyzes sensor data with filters and response caching.
    """
    # Get cache info before the call
    cache_info_before = get_stats.cache_info()
    
    # Call the cached function
    stats_data = get_stats(location, sensor, start_date, end_date)
    
    # Get cache info after the call
    cache_info_after = get_stats.cache_info()

    # Set X-Cache header
    if cache_info_after.hits > cache_info_before.hits:
        response.headers["X-Cache"] = "HIT"
    else:
        response.headers["X-Cache"] = "MISS"

    return {"stats": stats_data}

@app.get("/")
def read_root():
    return {"message": "API is running. Use the /stats endpoint."}
