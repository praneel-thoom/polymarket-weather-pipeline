import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import duckdb
import pathlib

DATA_DIR = pathlib.Path(__file__).parent.parent / "data" / "gold_snapshot"

st.set_page_config(page_title="Hurricane Market Signal Pipeline", layout="wide")
st.title("Hurricane Prediction Market vs Weather Signal Dashboard")
st.caption("Polymarket Cat 5 Hurricane US Landfall odds vs atmospheric pressure")
st.info("Serving a snapshot from May 23 to Aug 17 2026 (30-min windows). Live streaming paused while AWS credits are refreshed.")

@st.cache_data(ttl=3600)
def load_data():
    con = duckdb.connect()
    poly_df = con.execute(f"""
        SELECT window_ts, market_id, avg_yes_price, tick_count
        FROM read_parquet('{DATA_DIR}/polymarket_windows.parquet')
        ORDER BY window_ts
    """).df()
    poly_df["ts"] = pd.to_datetime(poly_df["window_ts"], unit="s", utc=True)
    poly_df = poly_df.dropna(subset=["avg_yes_price"])

    weather_df = con.execute(f"""
        SELECT window_ts, location_id,
               avg_pressure, avg_wind_speed, avg_humidity, avg_temperature
        FROM read_parquet('{DATA_DIR}/weather_windows.parquet')
        ORDER BY window_ts
    """).df()
    weather_df["ts"] = pd.to_datetime(weather_df["window_ts"], unit="s", utc=True)
    weather_df = weather_df.dropna(subset=["avg_pressure"])
    con.close()
    return poly_df, weather_df

poly_df, weather_df = load_data()

location = st.selectbox("Select Location", ["miami", "houston", "new_orleans"], index=0,
                        format_func=lambda x: x.replace("_", " ").title())

weather_filtered = weather_df[weather_df["location_id"] == location].copy()

merged = poly_df[["window_ts", "ts", "avg_yes_price"]].merge(
    weather_filtered[["window_ts", "avg_pressure"]],
    on="window_ts",
    how="inner"
)

corr = merged["avg_yes_price"].corr(merged["avg_pressure"])

current_price = poly_df["avg_yes_price"].iloc[-1]
prev_price = poly_df["avg_yes_price"].iloc[-2]
price_delta = current_price - prev_price

current_pressure = weather_filtered["avg_pressure"].iloc[-1]
prev_pressure = weather_filtered["avg_pressure"].iloc[-2]
pressure_delta = current_pressure - prev_pressure

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Yes Price", f"{current_price:.1%}", f"{price_delta:+.1%}")
col2.metric("Latest Pressure", f"{current_pressure:.1f} hPa", f"{pressure_delta:+.1f}")
col3.metric("Pressure ↔ Price Correlation", f"{corr:.2f}")
col4.metric("Data Range", f"{poly_df['ts'].iloc[0].strftime('%b %d')} to {poly_df['ts'].iloc[-1].strftime('%b %d')}")

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(
    go.Scatter(x=poly_df["ts"], y=poly_df["avg_yes_price"],
              name="Yes Price (Cat 5 Landfall)",
              line=dict(color="orange", width=2, shape="spline"),
              hovertemplate="%{x}<br>Price: %{y:.1%}<extra></extra>"),
    secondary_y=False
)
fig.add_trace(
    go.Scatter(x=weather_filtered["ts"], y=weather_filtered["avg_pressure"],
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
fig.add_vrect(
    x0="2026-07-17 18:00:00", x1="2026-08-02 00:30:00",
    fillcolor="red", opacity=0.08, line_width=0,
)
fig.add_annotation(
    x="2026-07-25 09:00:00", y=1, yref="paper",
    text="Pipeline down", showarrow=False,
    font=dict(size=11, color="gray"), yanchor="top"
)
st.plotly_chart(fig, use_container_width=True)

st.subheader(f"Pressure vs Market Price ({location.replace('_', ' ').title()})")
m, b = np.polyfit(merged["avg_pressure"], merged["avg_yes_price"], 1)
x_line = np.linspace(merged["avg_pressure"].min(), merged["avg_pressure"].max(), 100)
y_line = m * x_line + b

scatter_fig = go.Figure()
scatter_fig.add_trace(go.Scatter(
    x=merged["avg_pressure"], y=merged["avg_yes_price"],
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
