import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

engine = create_engine(DATABASE_URL)
print('Successfully connected to remote database!')


def get_prices(pairs):
    api_url = f'https://api.binance.com/api/v3/ticker/price'
    response = requests.get(url=api_url)
    if response.status_code == 200:
        result = response.json()
    return [
        {"symbol": item["symbol"], "price": item["price"]}
        for item in result if item["symbol"] in pairs
        ]
        
pairs = ['BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC', 'BNBUSDC', 'ADAUSDC']
data = pd.DataFrame(get_prices(pairs))
data['timestamp'] = datetime.now()
data.to_sql('fact_crypto_prices', con=engine, if_exists='append', index=False)
print(data)