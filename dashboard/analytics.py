import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def load_prices() -> pd.DataFrame:
    """Load all price data, sorted by symbol and time."""
    query = text("""
        SELECT symbol, timestamp, price::float
        FROM fact_crypto_prices
        ORDER BY symbol, timestamp
    """)
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["timestamp"])
    return df


def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Compute period-over-period % returns per symbol."""
    df = df.copy()
    df["return"] = df.groupby("symbol")["price"].pct_change() * 100
    return df


def latest_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Latest price + 1h and 24h change % per symbol."""
    results = []
    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("timestamp").reset_index(drop=True)
        latest = group.iloc[-1]

        # 1-hour change: find closest row ~60 rows back (1 row per min)
        idx_1h = max(len(group) - 60, 0)
        idx_24h = max(len(group) - 1440, 0)

        price_1h_ago = group.iloc[idx_1h]["price"]
        price_24h_ago = group.iloc[idx_24h]["price"]

        change_1h = ((latest["price"] - price_1h_ago) / price_1h_ago) * 100
        change_24h = ((latest["price"] - price_24h_ago) / price_24h_ago) * 100

        results.append({
            "symbol": symbol,
            "price": latest["price"],
            "change_1h_%": round(change_1h, 2),
            "change_24h_%": round(change_24h, 2),
            "last_updated": latest["timestamp"],
        })

    return pd.DataFrame(results).sort_values("symbol").reset_index(drop=True)


def compute_volatility(df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """
    Rolling annualized volatility per symbol.
    Uses std of returns over a rolling window, annualized for 1-min data.
    Minutes in a year = 525,600
    """
    df = compute_returns(df)
    results = []
    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("timestamp").copy()
        group["volatility"] = (
            group["return"]
            .rolling(window)
            .std()
            * np.sqrt(525_600)  # annualize 1-min returns
        )
        group["symbol"] = symbol
        results.append(group)
    return pd.concat(results).reset_index(drop=True)


def compute_correlation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Correlation matrix of returns across all symbols.
    Uses pivot so each column = one symbol's return series.
    """
    df = compute_returns(df)
    pivot = df.pivot_table(
        index="timestamp", columns="symbol", values="return"
    )
    return pivot.corr().round(2)


def price_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """Wide-format price table: rows=timestamp, cols=symbols."""
    return df.pivot_table(
        index="timestamp", columns="symbol", values="price"
    )