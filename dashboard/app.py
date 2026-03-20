import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dashboard.analytics import (
    load_prices,
    latest_prices,
    compute_volatility,
    compute_correlation,
    price_pivot,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Market Monitor",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Crypto Market Monitor")
st.caption("Live data from Binance · Stored in PostgreSQL · Built with Prefect + Docker")

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)  # refresh every 60 seconds
def get_data():
    df = load_prices()
    return df

df = get_data()

if df.empty:
    st.error("No data found in the database. Make sure your pipeline has run.")
    st.stop()

# ── Section 1: Latest Prices + KPI Cards ──────────────────────────────────────
st.subheader("🔴 Live Snapshot")
latest = latest_prices(df)

cols = st.columns(len(latest))
for col, (_, row) in zip(cols, latest.iterrows()):
    delta_color = "normal"
    col.metric(
        label=row["symbol"],
        value=f"${row['price']:,.4f}",
        delta=f"{row['change_24h_%']:+.2f}% (24h)",
        delta_color=delta_color,
    )

st.divider()

# ── Section 2: Price History ──────────────────────────────────────────────────
st.subheader("📉 Price History")

selected_symbols = st.multiselect(
    "Select symbols to compare",
    options=sorted(df["symbol"].unique()),
    default=["BTC", "ETH", "SOL"],
)

if selected_symbols:
    filtered = df[df["symbol"].isin(selected_symbols)]

    # Normalize to 100 for fair comparison across price scales
    normalize = st.toggle("Normalize to 100 (compare relative performance)", value=True)

    if normalize:
        pivot = price_pivot(filtered)
        normalized = (pivot / pivot.iloc[0]) * 100
        fig = px.line(
            normalized,
            labels={"value": "Indexed Price (base=100)", "timestamp": ""},
            title="Normalized Price Performance",
        )
    else:
        fig = px.line(
            filtered,
            x="timestamp",
            y="price",
            color="symbol",
            labels={"price": "Price (USD)", "timestamp": ""},
            title="Absolute Price (USD)",
        )

    fig.update_layout(legend_title="Symbol", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Section 3: Rolling Volatility ────────────────────────────────────────────
st.subheader("⚡ Rolling Annualized Volatility")
st.caption("Standard deviation of 1-min returns × √525,600 — higher = more volatile")

vol_window = st.slider("Rolling window (minutes)", min_value=30, max_value=240, value=60, step=30)
vol_df = compute_volatility(df, window=vol_window)

if selected_symbols:
    vol_filtered = vol_df[vol_df["symbol"].isin(selected_symbols)].dropna(subset=["volatility"])
    fig_vol = px.line(
        vol_filtered,
        x="timestamp",
        y="volatility",
        color="symbol",
        labels={"volatility": "Annualized Volatility (%)", "timestamp": ""},
        title=f"Rolling {vol_window}-min Annualized Volatility",
    )
    fig_vol.update_layout(hovermode="x unified")
    st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# ── Section 4: Correlation Matrix ────────────────────────────────────────────
st.subheader("🔗 Return Correlation Matrix")
st.caption("Correlation of 1-min returns across all pairs — 1.0 = move together, -1.0 = move opposite")

corr = compute_correlation(df)

fig_corr = go.Figure(
    data=go.Heatmap(
        z=corr.values,
        x=corr.columns.tolist(),
        y=corr.index.tolist(),
        colorscale="RdBu",
        zmid=0,
        text=corr.values,
        texttemplate="%{text}",
        showscale=True,
    )
)
fig_corr.update_layout(title="Pairwise Return Correlation", height=450)
st.plotly_chart(fig_corr, use_container_width=True)

st.divider()

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    f"Data range: {df['timestamp'].min().strftime('%Y-%m-%d')} → "
    f"{df['timestamp'].max().strftime('%Y-%m-%d')} · "
    f"{len(df):,} total records across {df['symbol'].nunique()} symbols"
)
