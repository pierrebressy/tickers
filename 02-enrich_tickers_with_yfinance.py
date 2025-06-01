import sqlite3
import yfinance as yf
import pandas as pd
import time
import datetime
import yfinance as yf

DB_PATH = "data/tickers.db"
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
        return_pct = round(ret * 100, 4)
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

def alter_ticker_info_for_dividends(db_path="data/tickers.db"):

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

def alter_ticker_info_add_last_check(db_path="data/tickers.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(ticker_info)")
    cols = [row[1] for row in cur.fetchall()]
    if "last_dividend_check" not in cols:
        cur.execute("ALTER TABLE ticker_info ADD COLUMN last_dividend_check TEXT")
        conn.commit()
        print("✅ Added column 'last_dividend_check'")
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

def list_large_optionable_tickers(min_cap=10_000_000):
    conn = sqlite3.connect(DB_PATH)
    try:
        query = f"""
            SELECT symbol, longName, marketCap, sector, industry, exchange
            FROM {TARGET_TABLE}
            WHERE marketCap > ? AND isOptionable = 1
            ORDER BY marketCap DESC
            LIMIT 100
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
        cursor = conn.execute("""
            SELECT last_dividend_check FROM ticker_info WHERE symbol = ?
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
            UPDATE ticker_info
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

def get_today_string():
    today = datetime.date.today().isoformat()
    return today

def get_candidates(limit=10, only_outperforming=False, only_with_dividends=False):
    db_path = "data/candidates.db"
    conn = sqlite3.connect(db_path)

    try:
        query = "SELECT * FROM candidates"
        filters = []

        if only_outperforming:
            filters.append("outperforming = 1")
        if only_with_dividends:
            filters.append("has_dividend = 1")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += f" ORDER BY return_pct DESC LIMIT {limit}"

        df = pd.read_sql(query, conn)
        return df

    finally:
        conn.close()


def main0():
    #alter_ticker_info_for_dividends()
    #alter_ticker_info_add_last_check()

    # load database with tickers information
    # enrich_tickers()

    # display a sample of tickers
    # display_sample_tickers(10)
    
    # extract large optionable tickers
    df=list_large_optionable_tickers()


    #tickers = ["AAPL", "JPM", "XOM", "PG", "TSLA"]
    tickers = df["symbol"].tolist()
    candidates=check_outperformance_vs_sector_etf(tickers, period="3mo")

def display_candidates_by_sector(only_outperforming=False, only_with_dividends=False):
    import sqlite3
    import pandas as pd

    db_path = "data/candidates.db"
    conn = sqlite3.connect(db_path)

    # Mapping sector → ETF
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

    try:
        # Build base query
        query = "SELECT * FROM candidates"
        filters = []

        if only_outperforming:
            filters.append("outperforming = 1")
        if only_with_dividends:
            filters.append("has_dividend = 1")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        df = pd.read_sql(query, conn)

        if df.empty:
            print("No data found.")
            return df

        # Group by sector
        summary = df.groupby("sector").agg(
            tickers=("symbol", list),
            avg_return_pct=("return_pct", "mean"),
            count=("symbol", "count"),
            dividend_count=("has_dividend", "sum"),
            avg_days_to_div=("days_until_dividend", lambda x: round(x.dropna().mean(), 1) if not x.dropna().empty else None)
        ).sort_values(by="avg_return_pct", ascending=False)

        # Add ETF name column
        summary["sector_etf"] = summary.index.map(SECTOR_ETF_MAP.get)

        # Print
        for sector, row in summary.iterrows():
            print(f"\n📊 {sector} (ETF: {row['sector_etf']})")
            print(f"   ➤ Tickers: {', '.join(row['tickers'])}")
            print(f"   ➤ Avg Return: {row['avg_return_pct']:.2f}%")
            print(f"   ➤ Total: {row['count']} tickers, {row['dividend_count']} with dividends")
            if row['avg_days_to_div'] is not None:
                print(f"   ➤ Avg days to dividend: {row['avg_days_to_div']} days")

        return summary

    finally:
        conn.close()


def tmain():
    print(get_today_string())  # ➜ "2025-06-01"

def main2():
    df = get_candidates(limit=100, only_outperforming=True, only_with_dividends=True)
    print(df)

def main3():
    display_candidates_by_sector(only_outperforming=True, only_with_dividends=True)

def main():

    # load database with tickers information
    enrich_tickers()
    alter_ticker_info_for_dividends()
    alter_ticker_info_add_last_check()

    df=list_large_optionable_tickers()
    tickers = df["symbol"].tolist()
    candidates = check_outperformance_vs_sector_etf(tickers, period="3mo")
    print(candidates)

if __name__ == "__main__":
    main()
