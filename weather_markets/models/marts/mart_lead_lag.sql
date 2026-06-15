with polymarket as (
    select
        event_time,
        market_id,
        yes_price,
        price_change_pct,
        price_direction,
        cast(epoch(event_time) / 1800 as bigint) * 1800 as window_ts
    from {{ ref('stg_polymarket_events') }}
),

weather as (
    select
        event_time,
        location_id,
        wind_speed,
        pressure_sea_level,
        pressure_anomaly,
        humidity,
        precip_probability,
        wind_severity,
        cast(epoch(event_time) / 1800 as bigint) * 1800 as window_ts
    from {{ ref('stg_weather_forecasts') }}
),

poly_agg as (
    select
        window_ts,
        market_id,
        avg(yes_price) as avg_yes_price,
        avg(price_change_pct) as avg_price_change_pct,
        count(*) as tick_count
    from polymarket
    group by window_ts, market_id
),

weather_agg as (
    select
        window_ts,
        location_id,
        avg(wind_speed) as avg_wind_speed,
        avg(pressure_sea_level) as avg_pressure,
        avg(humidity) as avg_humidity,
        avg(precip_probability) as avg_precip_prob
    from weather
    group by window_ts, location_id
)

select
    p.window_ts,
    to_timestamp(p.window_ts) as window_start,
    p.market_id,
    p.avg_yes_price,
    p.avg_price_change_pct,
    p.tick_count,
    w.location_id,
    w.avg_wind_speed,
    w.avg_pressure,
    w.avg_humidity,
    w.avg_precip_prob
from poly_agg p
inner join weather_agg w on p.window_ts = w.window_ts
order by p.window_ts, w.location_id
