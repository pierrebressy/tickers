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

def list_large_optionable_tickers(min_cap=10_000_000):
    conn = sqlite3.connect(DB_PATH)
    try:
        query = f"""
            SELECT symbol, longName, marketCap, sector, industry, exchange
            FROM {TARGET_TABLE}
            WHERE marketCap > ? AND isOptionable = 1
            ORDER BY marketCap DESC
            LIMIT 200
        """
        df = pd.read_sql(query, conn, params=(min_cap,))
        print(df)
        return df
    except Exception as e:
        print(f"Error querying database: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def update_dividend_info(symbol, conn, force=False):
    from datetime import date, timedelta
    import yfinance as yf
    import pandas as pd

    today_str = date.today().isoformat()

    if not force:
        # Skip if already updated today
        cursor = conn.execute(f"""
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

def check_outperformance_vs_sector_etf(ticker_list, period="1mo"):
    import yfinance as yf
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(DB_PATH)

    init_cache_table(conn)

    try:
        placeholders = ",".join("?" for _ in ticker_list)
        query = f"SELECT symbol, sector FROM {TARGET_TABLE} WHERE symbol IN ({placeholders})"
        ticker_sectors = pd.read_sql(query, conn, params=ticker_list)

        if ticker_sectors.empty:
            print("No ticker info found.")
            return pd.DataFrame()

        results = []
        price_cache = {}

        def get_return(symbol):
            if symbol in price_cache:
                return price_cache[symbol]
            try:
                hist = yf.Ticker(symbol).history(period=period)
                if hist.empty or len(hist) < 2:
                    return None
                ret = (hist["Close"].iloc[-1] - hist["Close"].iloc[0]) / hist["Close"].iloc[0]
                price_cache[symbol] = ret
                return ret
            except:
                return None

        for idx, row in ticker_sectors.iterrows():
            symbol = row["symbol"]
            sector = row["sector"]
            print(f"[{idx+1}/{len(ticker_sectors)}] {symbol}: sector={sector}")

            # 1. Update dividend info
            update_dividend_info(symbol, conn)
            # Fetch updated dividend info
            cursor = conn.execute("""
                SELECT has_dividend, days_until_dividend
                FROM ticker_info
                WHERE symbol = ?
            """, (symbol,))
            div_row = cursor.fetchone()

            has_dividend = div_row[0] if div_row else None
            days_until = div_row[1] if div_row else None

            # 2. Get ticker return
            ticker_ret = get_or_fetch_return(symbol, period, conn)
            if ticker_ret is None:
                continue

            # 3. Get sector ETF return
            sector_etf = SECTOR_ETF_MAP.get(sector)
            if not sector_etf:
                print(f"⚠️ No ETF found for sector '{sector}'")
                continue

            etf_ret = get_or_fetch_return(sector_etf, period, conn)
            if etf_ret is None:
                continue

            try:
                price = yf.Ticker(symbol).history(period="1d")["Close"].iloc[-1]
                if price > 120:
                    print(f"⛔ {symbol} skipped (last price ${price:.2f} > 120)")
                    continue
            except:
                print(f"⚠️ Failed to get last price for {symbol}")
                continue


            results.append({
                "symbol": symbol,
                "sector": sector,
                "sector_etf": sector_etf,
                "return_pct": round(ticker_ret * 100, 2),
                "sector_etf_pct": round(etf_ret * 100, 2),
                "outperforming": ticker_ret > etf_ret,
                "has_dividend": has_dividend,
                "days_until_dividend": days_until
            })

        df = pd.DataFrame(results)
        df = df.sort_values(by="return_pct", ascending=False).reset_index(drop=True)
        # Add timestamp for traceability
        df["evaluated_at"] = today = datetime.date.today().isoformat()

        # Save to separate database
        candidates_db = "data/candidates.db"
        with sqlite3.connect(candidates_db) as out_conn:
            df.to_sql("candidates", out_conn, if_exists="replace", index=False)

        print(f"✅ Stored {len(df)} rows in {candidates_db} (table: candidates)")
        return df

    finally:
        conn.close()

def main():
    
    df=list_large_optionable_tickers(min_cap=100_000_000_000)
    tickers = df["symbol"].tolist()
    candidates = check_outperformance_vs_sector_etf(tickers, period="6mo")
    print(candidates)

if __name__ == "__main__":
    main()
