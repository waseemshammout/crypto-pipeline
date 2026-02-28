import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_prices(pairs):
    api_url = f"https://api.binance.com/api/v3/ticker/price"
    response = requests.get(url=api_url)
    if response.status_code == 200:
        result = response.json()
    return [
        {"symbol": item["symbol"], "price": item["price"]}
        for item in result
        if item["symbol"] in pairs
    ]

try:
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set")
    else:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        print("Connecting...")
        engine.connect()
        print("Connected successfully!")

    pairs = ["BTCUSDC", "ETHUSDC", "SOLUSDC", "XRPUSDC", "BNBUSDC", "ADAUSDC"]

    data = pd.DataFrame(get_prices(pairs))
    data["timestamp"] = datetime.now()
    data["symbol"] = data["symbol"].str.replace("USDC", "")

    data.to_sql("fact_crypto_prices", con=engine, if_exists="append", index=False)
    print("Prices saved successfully!")
except Exception as e:
    print("Error occured:", type(e).__name__)
