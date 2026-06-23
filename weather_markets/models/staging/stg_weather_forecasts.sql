select
    event_time,
    location_id,
    forecast_time,
    windSpeed as wind_speed,
    windGust as wind_gust,
    pressureSeaLevel as pressure_sea_level,
    pressure_anomaly,
    humidity,
    precipitationProbability as precip_probability,
    temperature,
    wind_severity
from read_parquet('s3://polymarket-weather/silver/weather_forecasts/**/*.parquet')
