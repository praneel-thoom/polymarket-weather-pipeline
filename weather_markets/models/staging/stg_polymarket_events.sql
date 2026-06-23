select
    event_time,
    market_id,
    yes_price,
    prev_price,
    price_change,
    price_change_pct,
    price_direction
from read_parquet('s3://polymarket-weather/silver/polymarket_events/**/*.parquet')
