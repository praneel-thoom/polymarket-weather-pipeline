import requests
import json
import time
from datetime import datetime, timezone
import os
from kafka import KafkaProducer

API_KEY = os.environ["TOMORROW_API_KEY"]

LOCATIONS = {
    "miami": {"lat": 25.7617, "lon": -80.1918},
    "new_orleans": {"lat": 29.9511, "lon": -90.0715},
    "houston": {"lat": 29.7604, "lon": -95.3698}
}

FIELDS = [
    "windSpeed", "windGust", "windDirection",
    "pressureSeaLevel", "humidity", "precipitationProbability",
    "temperature", "weatherCode"
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_forecast(lat, lon):
    url = "https://api.tomorrow.io/v4/weather/forecast"
    params = {"location": f"{lat},{lon}", "apikey": API_KEY}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    latest = data["timelines"]["minutely"][0]
    return {field: latest["values"].get(field) for field in FIELDS}, latest["time"]

def poll():
    while True:
        timestamp = datetime.now(timezone.utc).isoformat()
        for location_id, coords in LOCATIONS.items():
            try:
                values, forecast_time = fetch_forecast(coords["lat"], coords["lon"])
                record = {
                    "ts": timestamp,
                    "location_id": location_id,
                    "forecast_time": forecast_time,
                    **values
                }
                producer.send("weather_forecasts", value=record)
                print(json.dumps(record))
            except Exception as e:
                print(f"ERROR {location_id}: {e}")
        producer.flush()
        time.sleep(300)

if __name__ == "__main__":
    poll()
