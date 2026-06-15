import requests
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

MARKETS = {
    "cat5_hurricane_us_2026": {
        "token_id": "42720822307141028194547821734867004050436183856119350616784794533947314314892",
        "question": "Will any Category 5 hurricane make landfall in the US before 2027?"
    }
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_price(token_id):
    url = f"https://clob.polymarket.com/price?token_id={token_id}&side=buy"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return float(response.json()["price"])

def poll():
    while True:
        timestamp = datetime.now(timezone.utc).isoformat()
        for market_id, market in MARKETS.items():
            try:
                price = fetch_price(market["token_id"])
                record = {
                    "ts": timestamp,
                    "market_id": market_id,
                    "token_id": market["token_id"],
                    "yes_price": price
                }
                producer.send("polymarket_events", value=record)
                print(json.dumps(record))
            except Exception as e:
                print(f"ERROR {market_id}: {e}")
        producer.flush()
        time.sleep(30)

if __name__ == "__main__":
    poll()
