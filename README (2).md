
# SmartFactory IoT Stats API

## Run locally
```bash
pip install -r requirements.txt
# Ensure the CSV is available; defaults to /mnt/data/q-fastapi-timeseries-cache.csv
export DATA_PATH="/mnt/data/q-fastapi-timeseries-cache.csv"
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Example
```
http://127.0.0.1:8000/stats?location=zone-c&sensor=temperature&start_date=2025-01-01&end_date=2025-12-31
```

## Docker
```bash
docker build -t iot-stats .
docker run -it --rm -p 8000:8000 -v /mnt/data:/mnt/data -e DATA_PATH=/mnt/data/q-fastapi-timeseries-cache.csv iot-stats
```
