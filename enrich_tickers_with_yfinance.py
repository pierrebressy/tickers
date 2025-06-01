import sqlite3
import yfinance as yf
import pandas as pd
import time

DB_PATH = "data/tickers.db"
SOURCE_TABLE = "us_tickers"
TARGET_TABLE = "ticker_info"
SLEEP_TIME = 1  # Delay to avoid Yahoo Finance rate limits

FIELDS = [
    "symbol", "longName", "sector", "industry", "country",
    "marketCap", "currency", "isOptionable", "quoteType", "exchange"
]

def fetch_ticker_info(ticker):
    try:
        yf_obj = yf.Ticker(ticker)
        info = yf_obj.info
        # Fallback: check if options exist
        has_options = bool(yf_obj.options)

        return {
            "symbol": ticker,
            "longName": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "marketCap": info.get("marketCap"),
            "currency": info.get("currency"),
            "isOptionable": has_options,
            "quoteType": info.get("quoteType"),
            "exchange": info.get("exchange")
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

def enrich_tickers():
    conn = sqlite3.connect(DB_PATH)
    tickers = pd.read_sql(f"SELECT DISTINCT Symbol FROM {SOURCE_TABLE}", conn)["Symbol"].tolist()

    enriched_data = []
    count=0
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Fetching {ticker}...")
        data = fetch_ticker_info(ticker)
        if data:
            enriched_data.append(data)
        time.sleep(SLEEP_TIME)  # Sleep to avoid throttling
        count += 1
        #if count % 10 == 0:
        #    break
    df = pd.DataFrame(enriched_data, columns=FIELDS)
    df.to_sql(TARGET_TABLE, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Inserted {len(df)} records into {TARGET_TABLE}.")

def display_sample_tickers(n=10):
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql(f"SELECT * FROM {TARGET_TABLE} LIMIT {n}", conn)
        print(df)
    except Exception as e:
        print(f"Error displaying sample data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    enrich_tickers()
    display_sample_tickers(10)
