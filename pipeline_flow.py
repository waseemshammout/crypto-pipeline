import requests
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


@task(retries=3, retry_delay_seconds=5)
def get_prices(pairs):
    logger = get_run_logger()
    logger.info(f"Fetching prices for pairs: {pairs}")
    api_url = "https://api.binance.com/api/v3/ticker/price"
    response = requests.get(url=api_url)
    response.raise_for_status()
    result = response.json()
    filtered = [
        {"symbol": item["symbol"], "price": item["price"]}
        for item in result
        if item["symbol"] in pairs
    ]
    logger.info(f"Fetched {len(filtered)} records")
    return filtered

@task
def transform_data(raw_data):
    df = pd.DataFrame(raw_data)
    df["timestamp"] = datetime.now(timezone.utc)
    df["symbol"] = df["symbol"].str.replace("USDC", "")
    return df


@task
def save_to_db(data):
    logger = get_run_logger()
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set")
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        logger.info("Connecting to database...")
        engine.connect()
        logger.info("Connected successfully!")
        data.to_sql("fact_crypto_prices", con=engine, if_exists="append", index=False)
        logger.info("Prices saved successfully!")
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        raise

@flow
def crypto_pipeline():
    pairs = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "BNBUSDC", "ADAUSDC"]

    raw_data = get_prices(pairs)

    processed_df = transform_data(raw_data)

    save_to_db(processed_df)


if __name__ == "__main__":
    crypto_pipeline()
