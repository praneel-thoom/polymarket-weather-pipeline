import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import duckdb
import os

st.set_page_config(page_title="Hurricane Market Signal Pipeline", layout="wide")
st.title("Hurricane Prediction Market vs Weather Signal Dashboard")
st.caption("Polymarket Cat 5 Hurricane US Landfall odds vs atmospheric pressure")

# Only show data from when the pipeline was actually deployed as a stable,
# continuously-running streaming service, not the manually-backfilled history
# or the July 17 - Aug 2 gap when the old process had silently died.
LIVE_STREAM_START = pd.Timestamp("2026-08-02 04:45:00", tz="UTC")

def get_db():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    aws_key = st.secrets.get("AWS_ACCESS_KEY", os.environ.get("AWS_ACCESS_KEY", ""))
    aws_secret = st.secrets.get("AWS_SECRET_KEY", os.environ.get("AWS_SECRET_KEY", ""))
    aws_region = st.secrets.get("AWS_REGION", os.environ.get("AWS_REGION", "us-east-2"))
    bucket = st.secrets.get("AWS_BUCKET", os.environ.get("AWS_BUCKET", "polymarket-weather-pipeline"))
    con.execute(f"""
        SET s3_access_key_id='{aws_key}';
        SET s3_secret_access_key='{aws_secret}';
        SET s3_region='{aws_region}';
    """)
    return con, bucket

@st.cache_data(ttl=30)
def load_data():
    con, bucket = get_db()
    poly_df = con.execute(f"""
        SELECT event_time as ts, market_id, yes_price, price_change_pct, price_direction
        FROM read_parquet('s3://{bucket}/silver/polymarket_events/*.parquet', union_by_name=true)
        ORDER BY ts
    """).df()
    poly_df["ts"] = pd.to_datetime(poly_df["ts"], utc=True)
    poly_df = poly_df.dropna(subset=["ts"])
    poly_df = poly_df[poly_df["ts"] >= LIVE_STREAM_START]

    weather_df = con.execute(f"""
        SELECT event_time as ts, location_id, pressureSeaLevel, windSpeed, humidity, temperature
        FROM read_parquet('s3://{bucket}/silver/weather_forecasts/*.parquet', union_by_name=true)
        ORDER BY ts
    """).df()
    weather_df["ts"] = pd.to_datetime(weather_df["ts"], utc=True)
    weather_df = weather_df.dropna(subset=["ts"])
    weather_df = weather_df[weather_df["ts"] >= LIVE_STREAM_START]
    con.close()
    return poly_df, weather_df

poly_df, weather_df = load_data()

location = st.selectbox("Select Location", ["miami", "houston", "new_orleans"], index=0,
                        format_func=lambda x: x.replace("_", " ").title())

weather_filtered = weather_df[weather_df["location_id"] == location].copy()

merged = pd.merge_asof(
    poly_df[["ts", "yes_price"]],
    weather_filtered[["ts", "pressureSeaLevel"]],
    on="ts",
    direction="nearest",
    tolerance=pd.Timedelta("2h")
).dropna(subset=["pressureSeaLevel"])

corr = merged["yes_price"].corr(merged["pressureSeaLevel"])

current_price = poly_df["yes_price"].iloc[-1]
prev_price = poly_df["yes_price"].iloc[-2]
price_delta = current_price - prev_price

current_pressure = weather_filtered["pressureSeaLevel"].iloc[-1]
prev_pressure = weather_filtered["pressureSeaLevel"].iloc[-2]
pressure_delta = current_pressure - prev_pressure

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Yes Price", f"{current_price:.1%}", f"{price_delta:+.1%}")
col2.metric("Latest Pressure", f"{current_pressure:.1f} hPa", f"{pressure_delta:+.1f}")
col3.metric("Pressure \u2194 Price Correlation", f"{corr:.2f}")
col4.metric("Data Range", f"{poly_df['ts'].iloc[0].strftime('%b %d')} to {poly_df['ts'].iloc[-1].strftime('%b %d')}")

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(
    go.Scatter(x=poly_df["ts"], y=poly_df["yes_price"],
              name="Yes Price (Cat 5 Landfall)",
              line=dict(color="orange", width=2, shape="spline"),
              hovertemplate="%{x}<br>Price: %{y:.1%}<extra></extra>"),
    secondary_y=False
)
fig.add_trace(
    go.Scatter(x=weather_filtered["ts"], y=weather_filtered["pressureSeaLevel"],
              name=f"{location.replace('_', ' ').title()} Pressure (hPa)",
              line=dict(color="steelblue", width=2),
              hovertemplate="%{x}<br>Pressure: %{y:.1f} hPa<extra></extra>"),
    secondary_y=True
)
fig.update_layout(
    title=f"Polymarket Hurricane Odds vs {location.replace('_', ' ').title()} Atmospheric Pressure",
    xaxis_title="Date",
    height=500,
    legend=dict(x=0, y=1),
    hovermode="x unified"
)
fig.update_yaxes(title_text="Market Probability", tickformat=".0%", secondary_y=False)
fig.update_yaxes(title_text="Pressure (hPa)", secondary_y=True)
st.plotly_chart(fig, use_container_width=True)

st.subheader(f"Pressure vs Market Price ({location.replace('_', ' ').title()})")
m, b = np.polyfit(merged["pressureSeaLevel"], merged["yes_price"], 1)
x_line = np.linspace(merged["pressureSeaLevel"].min(), merged["pressureSeaLevel"].max(), 100)
y_line = m * x_line + b

scatter_fig = go.Figure()
scatter_fig.add_trace(go.Scatter(
    x=merged["pressureSeaLevel"], y=merged["yes_price"],
    mode="markers",
    marker=dict(color="orange", opacity=0.5, size=6),
    name="Observations",
    hovertemplate="Pressure: %{x:.1f} hPa<br>Price: %{y:.1%}<extra></extra>"
))
scatter_fig.add_trace(go.Scatter(
    x=x_line, y=y_line,
    mode="lines",
    line=dict(color="steelblue", width=2),
    name=f"Regression (r={corr:.2f})"
))
scatter_fig.update_layout(
    xaxis_title="Atmospheric Pressure (hPa)",
    yaxis_title="Market Probability",
    yaxis_tickformat=".0%",
    height=400
)
st.plotly_chart(scatter_fig, use_container_width=True)

if st.button("Refresh Now"):
    st.cache_data.clear()
    st.rerun()
