from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any
from pathlib import Path
import pandas as pd
import uvicorn
import hashlib
import json

CSV_PATH = Path("q-fastapi-timeseries-cache.csv")

app = FastAPI(title="SmartFactory Sensor Stats")

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load CSV once at startup and normalize
try:
    # parse timestamps as tz-aware (UTC) to avoid timezone comparison issues
    df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
except FileNotFoundError:
    raise RuntimeError(f"CSV file not found at {CSV_PATH}")
except Exception as e:
    raise RuntimeError(f"Failed to load CSV: {e}")

# Normalize column names (strip whitespace)
df.columns = [c.strip() for c in df.columns]

# Ensure timestamp column is timezone-aware (UTC)
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
else:
    raise RuntimeError("CSV must contain a 'timestamp' column.")

# Ensure value column exists and is numeric
if "value" not in df.columns:
    raise RuntimeError("CSV must contain a 'value' column.")
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Fill missing location/sensor with string versions to make filtering consistent
if "location" in df.columns:
    df["location"] = df["location"].astype(str)
else:
    df["location"] = ""

if "sensor" in df.columns:
    df["sensor"] = df["sensor"].astype(str)
else:
    df["sensor"] = ""

# Simple in-memory cache: key -> result (JSON-serializable dict)
_cache: Dict[str, Dict[str, Any]] = {}


def make_cache_key(location: Optional[str], sensor: Optional[str],
                   start_date: Optional[str], end_date: Optional[str]) -> str:
    payload = {
        "location": location if location is not None else None,
        "sensor": sensor if sensor is not None else None,
        "start_date": start_date if start_date is not None else None,
        "end_date": end_date if end_date is not None else None,
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def filter_dataframe(df_: pd.DataFrame,
                     location: Optional[str],
                     sensor: Optional[str],
                     start_date: Optional[str],
                     end_date: Optional[str]) -> pd.DataFrame:
    filtered = df_

    if location is not None:
        filtered = filtered[filtered["location"].astype(str) == str(location)]

    if sensor is not None:
        filtered = filtered[filtered["sensor"].astype(str) == str(sensor)]

    if start_date is not None:
        try:
            # parse as tz-aware UTC so comparisons succeed with df timestamps
            start_ts = pd.to_datetime(start_date, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start_date format. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).")
        filtered = filtered[filtered["timestamp"] >= start_ts]

    if end_date is not None:
        try:
            end_ts = pd.to_datetime(end_date, utc=True)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid end_date format. Use ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).")
        filtered = filtered[filtered["timestamp"] <= end_ts]

    return filtered


@app.get("/stats")
async def stats(response: Response,
                location: Optional[str] = Query(None),
                sensor: Optional[str] = Query(None),
                start_date: Optional[str] = Query(None),
                end_date: Optional[str] = Query(None)):
    """
    Compute basic stats (count, avg, min, max) for the 'value' column
    filtered by optional query params: location, sensor, start_date, end_date.

    Caching: identical requests return X-Cache: HIT
    """
    key = make_cache_key(location, sensor, start_date, end_date)

    # Return cached result if present
    if key in _cache:
        response.headers["X-Cache"] = "HIT"
        return _cache[key]

    # Not cached -> compute
    filtered = filter_dataframe(df, location, sensor, start_date, end_date)

    # Use pre-converted numeric 'value' column and drop NaNs
    series = filtered["value"].dropna()

    count = int(series.count())
    avg = float(series.mean()) if count > 0 else None
    minimum = float(series.min()) if count > 0 else None
    maximum = float(series.max()) if count > 0 else None

    result = {
        "stats": {
            "count": count,
            "avg": avg,
            "min": minimum,
            "max": maximum,
        }
    }

    # Cache the result (simple in-memory)
    _cache[key] = result

    response.headers["X-Cache"] = "MISS"
    return result


if __name__ == "__main__":
    # Run app with uvicorn when executed directly
    uvicorn.run("app:app", host="127.0.0.1", port=8000)
