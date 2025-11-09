import os
import pandas as pd
from fastapi import FastAPI, Query, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Tuple

DATA_PATH = "q-fastapi-timeseries-cache.csv"

app = FastAPI(title="SmartFactory IoT Stats API", version="1.0.0")

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache for computed stats
# Key: (location, sensor, start_iso, end_iso)
_stats_cache: Dict[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]], Dict] = {}

def _normalize_str(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = s.strip()
    if s2 == "":
        return None
    return s2.lower()

def _normalize_date(s: Optional[str]) -> Optional[str]:
    if s is None or s.strip() == "":
        return None
    try:
        ts = pd.to_datetime(s, utc=False, errors="raise")
        return ts.isoformat()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {s}. Error: {str(e)}")

def _load_df() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=500, detail=f"Data file not found at {DATA_PATH}")
    try:
        df = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load CSV: {e}")
    if "location" in df.columns:
        df["location_norm"] = df["location"].astype(str).str.strip().str.lower()
    else:
        raise HTTPException(status_code=500, detail="CSV missing 'location' column")
    if "sensor" in df.columns:
        df["sensor_norm"] = df["sensor"].astype(str).str.strip().str.lower()
    else:
        raise HTTPException(status_code=500, detail="CSV missing 'sensor' column")
    if "value" not in df.columns:
        raise HTTPException(status_code=500, detail="CSV missing 'value' column")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value", "timestamp"])
    return df

_df_cache = None
_df_mtime = None

def _get_df() -> pd.DataFrame:
    global _df_cache, _df_mtime
    try:
        mtime = os.path.getmtime(DATA_PATH)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"Data file not found at {DATA_PATH}")
    if _df_cache is None or _df_mtime != mtime:
        _df_cache = _load_df()
        _df_mtime = mtime
    return _df_cache

class StatsResponse(BaseModel):
    stats: dict

@app.get("/stats", response_model=StatsResponse)
def stats(
    response: Response,
    location: Optional[str] = Query(None, description="Location filter (e.g., zone-c)"),
    sensor: Optional[str] = Query(None, description="Sensor type (e.g., temperature)"),
    start_date: Optional[str] = Query(None, description="Start ISO date/time (e.g., 2025-01-01)"),
    end_date: Optional[str] = Query(None, description="End ISO date/time (e.g., 2025-12-31)"),
):
    df = _get_df()

    loc_norm = _normalize_str(location)
    sen_norm = _normalize_str(sensor)
    start_iso = _normalize_date(start_date) if start_date else None
    end_iso = _normalize_date(end_date) if end_date else None

    cache_key = (loc_norm, sen_norm, start_iso, end_iso)

    if cache_key in _stats_cache:
        result = _stats_cache[cache_key]
        response.headers["X-Cache"] = "HIT"
        return {"stats": result}

    filtered = df
    if loc_norm is not None:
        filtered = filtered[filtered["location_norm"] == loc_norm]
    if sen_norm is not None:
        filtered = filtered[filtered["sensor_norm"] == sen_norm]
    if start_iso is not None:
        start_ts = pd.to_datetime(start_iso)
        filtered = filtered[filtered["timestamp"] >= start_ts]
    if end_iso is not None:
        end_ts = pd.to_datetime(end_iso)
        filtered = filtered[filtered["timestamp"] <= end_ts]

    if filtered.empty:
        result = {"count": 0, "avg": None, "min": None, "max": None}
        _stats_cache[cache_key] = result
        response.headers["X-Cache"] = "MISS"
        return {"stats": result}

    vals = filtered["value"].astype(float)
    result = {
        "count": int(vals.count()),
        "avg": float(vals.mean()),
        "min": float(vals.min()),
        "max": float(vals.max()),
    }
    _stats_cache[cache_key] = result
    response.headers["X-Cache"] = "MISS"
    return {"stats": result}
