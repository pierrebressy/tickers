import sqlite3
import yfinance as yf
import pandas as pd
import time
import datetime
import yfinance as yf

DB_PATH = "data/tickers.db"
CANDIDATES_DB_PATH = "data/candidates.db"
SOURCE_TABLE = "us_tickers"
TARGET_TABLE = "ticker_info"
SLEEP_TIME = 1  # Delay to avoid Yahoo Finance rate limits

FIELDS = [
    "symbol", "longName", "sector", "industry", "country",
    "marketCap", "currency", "isOptionable", "quoteType", "exchange"
]

SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Energy": "XLE",
    "Consumer Defensive": "XLP",
    "Consumer Cyclical": "XLY",
    "Industrials": "XLI",
    "Utilities": "XLU",
    "Basic Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC"
}

def get_or_fetch_return(symbol, period, conn):
    today = datetime.date.today().isoformat()
    cur = conn.cursor()

    # Step 1: check cache
    cur.execute("""
        SELECT return_pct FROM price_cache
        WHERE symbol = ? AND period = ? AND last_updated = ?
    """, (symbol, period, today))
    row = cur.fetchone()
    if row:
        return row[0]

    # Step 2: fetch from yfinance
    try:
        hist = yf.Ticker(symbol).history(period=period)
        if hist.empty or len(hist) < 2:
            return None
        ret = (hist["Close"].iloc[-1] - hist["Close"].iloc[0]) / hist["Close"].iloc[0]
        return_pct = round(ret, 4)
    except:
        return None

    # Step 3: insert into cache
    cur.execute("""
        INSERT OR REPLACE INTO price_cache (symbol, period, return_pct, last_updated)
        VALUES (?, ?, ?, ?)
    """, (symbol, period, return_pct, today))
    conn.commit()
    return return_pct

def init_cache_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_cache (
            symbol TEXT,
            period TEXT,
            return_pct REAL,
            last_updated TEXT,
            PRIMARY KEY (symbol, period, last_updated)
        )
    """)
    conn.commit()

def alter_ticker_info_for_dividends(db_path):

    fields_to_add = {
        "has_dividend": "BOOLEAN",
        "next_dividend_date": "TEXT",
        "days_until_dividend": "INTEGER"
    }

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute("PRAGMA table_info(ticker_info)")
    existing_cols = [row[1] for row in cursor.fetchall()]

    for col, col_type in fields_to_add.items():
        if col not in existing_cols:
            print(f"Adding column '{col}' to ticker_info...")
            cursor.execute(f"ALTER TABLE ticker_info ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()
    print("✅ Table 'ticker_info' updated with dividend fields.")

def alter_ticker_info_add_last_check(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(ticker_info)")
    cols = [row[1] for row in cur.fetchall()]
    if "last_dividend_check" not in cols:
        cur.execute("ALTER TABLE ticker_info ADD COLUMN last_dividend_check TEXT")
        conn.commit()
        print("✅ Added column 'last_dividend_check'")
    conn.close()

def alter_price_cache_add_close_price(db_path):
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(price_cache)")
    cols = [row[1] for row in cur.fetchall()]
    if "close_price" not in cols:
        cur.execute("ALTER TABLE price_cache ADD COLUMN close_price REAL")
        conn.commit()
        print("✅ Added 'close_price' column to price_cache")
    else:
        print("ℹ️ 'close_price' already exists in price_cache")
    conn.close()

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

def enrich_tickers(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure 'processed' column exists in SOURCE_TABLE
    cursor.execute(f"PRAGMA table_info({SOURCE_TABLE})")
    columns = [col[1] for col in cursor.fetchall()]
    if 'processed' not in columns:
        print("Adding 'processed' column to SOURCE_TABLE...")
        cursor.execute(f"ALTER TABLE {SOURCE_TABLE} ADD COLUMN processed INTEGER DEFAULT 0")
        conn.commit()

    # Get unprocessed tickers
    query = f"SELECT DISTINCT Symbol FROM {SOURCE_TABLE} WHERE processed IS NULL OR processed = 0"
    tickers = pd.read_sql(query, conn)["Symbol"].tolist()

    if not tickers:
        print("All tickers already processed. Nothing to do.")
        conn.close()
        return

    enriched_data = []
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Fetching {ticker}...")
        data = fetch_ticker_info(ticker)
        if data:
            enriched_data.append(data)
            # Mark ticker as processed
            cursor.execute(f"UPDATE {SOURCE_TABLE} SET processed = 1 WHERE Symbol = ?", (ticker,))
            conn.commit()
        time.sleep(SLEEP_TIME)  # Avoid throttling

    if enriched_data:
        df = pd.DataFrame(enriched_data, columns=FIELDS)
        df.to_sql(TARGET_TABLE, conn, if_exists="append", index=False)
        print(f"Inserted {len(df)} new records into {TARGET_TABLE}.")
    else:
        print("No new data to insert.")

    conn.close()

def UNUSED_update_dividend_info(symbol, conn, force=False):
    from datetime import date, timedelta
    import yfinance as yf
    import pandas as pd

    today_str = date.today().isoformat()

    if not force:
        # Skip if already updated today
        cursor = conn.execute("""
            SELECT last_dividend_check FROM {TARGET_TABLE} WHERE symbol = ?
        """, (symbol,))
        row = cursor.fetchone()
        if row and row[0] == today_str:
            return  # Already updated

    try:
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
        divs = ticker.dividends

        has_dividend = not divs.empty
        next_div_date = None
        days_until = None

        # Try Yahoo ex-div date
        ex_date = cal.get("Ex-Dividend Date")
        if ex_date:
            ex_date = pd.to_datetime(ex_date).date()
            if ex_date >= date.today():
                next_div_date = ex_date.isoformat()
                days_until = (ex_date - date.today()).days
        elif has_dividend:
            last_date = divs.index[-1].date()
            if len(divs) >= 4:
                ex_date = last_date + timedelta(days=90)
                next_div_date = ex_date.isoformat()
                days_until = (ex_date - date.today()).days

        # Store in DB
        conn.execute("""
            UPDATE {TARGET_TABLE}
            SET has_dividend = ?, next_dividend_date = ?, days_until_dividend = ?, last_dividend_check = ?
            WHERE symbol = ?
        """, (has_dividend, next_div_date, days_until, today_str, symbol))
        conn.commit()

        print(f"💰 {symbol}: has_dividend={has_dividend}, next_div={next_div_date}, in {days_until}d")

    except Exception as e:
        print(f"❌ Dividend update failed for {symbol}: {e}")

def main():

    enrich_tickers(DB_PATH)
    alter_ticker_info_for_dividends(DB_PATH)
    alter_ticker_info_add_last_check(DB_PATH)
    alter_price_cache_add_close_price(DB_PATH)
    
if __name__ == "__main__":
    main()
