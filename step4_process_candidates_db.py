import sqlite3
import pandas as pd
import yfinance as yf
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DB_PATH = "data/candidates.db"

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
def alter_price_cache_add_close_price(db_path="data/tickers.db"):
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

def get_or_fetch_price(symbol, conn, today=None):
    import datetime
    import yfinance as yf

    if today is None:
        today = datetime.date.today().isoformat()

    # Step 1: check cache
    cur = conn.cursor()
    cur.execute("""
        SELECT close_price FROM price_cache
        WHERE symbol = ? AND period = '1d' AND last_updated = ?
    """, (symbol, today))
    row = cur.fetchone()
    if row and row[0] is not None:
        return round(row[0], 2)

    # Step 2: fetch from yfinance
    try:
        data = yf.Ticker(symbol).history(period="1d")
        if data.empty:
            return None
        price = round(data["Close"].iloc[-1], 2)
    except:
        return None

    # Step 3: store in cache
    cur.execute("""
        INSERT OR REPLACE INTO price_cache (symbol, period, return_pct, close_price, last_updated)
        VALUES (?, ?, NULL, ?, ?)
    """, (symbol, "1d", price, today))
    conn.commit()

    return price


def display_candidates_by_sector(only_outperforming=False, only_with_dividends=False):
    
    conn = sqlite3.connect(DB_PATH)

    # Mapping sector → ETF
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
            #print(f"   ➤ Avg Return: {row['avg_return_pct']:.2f}%")
            #print(f"   ➤ Total: {row['count']} tickers, {row['dividend_count']} with dividends")
            #if row['avg_days_to_div'] is not None:
            #    print(f"   ➤ Avg days to dividend: {row['avg_days_to_div']} days")

        return summary

    finally:
        conn.close()


def get_flat_candidate_table_with_prices(only_outperforming=False, only_with_dividends=False):
    db_path = "data/candidates.db"
    conn = sqlite3.connect(db_path)
    db_cache = "data/tickers.db"
    conn_cache = sqlite3.connect(db_cache)

    try:
        query = "SELECT symbol, sector, sector_etf, return_pct, sector_etf_pct, days_until_dividend FROM candidates"
        filters = []

        if only_outperforming:
            filters.append("outperforming = 1")
        if only_with_dividends:
            filters.append("has_dividend = 1")

        if filters:
            query += " WHERE " + " AND ".join(filters)

        df = pd.read_sql(query, conn)
        if df.empty:
            print("No candidates found.")
            return df

        df["diff_pct_vs_etf"] = df["return_pct"] - df["sector_etf_pct"]

        # Collect all unique tickers and ETFs to query once
        all_symbols = pd.unique(df[["symbol", "sector_etf"]].values.ravel())

        # Fetch latest prices
        today_str = datetime.date.today().isoformat()
        price_map = {sym: get_or_fetch_price(sym, conn_cache, today=today_str) for sym in all_symbols}

        # Map prices into DataFrame
        df["ticker_price"] = df["symbol"].map(price_map)
        df["etf_price"] = df["sector_etf"].map(price_map)

        # Final column order
        df = df[[
            "sector_etf", "symbol", "ticker_price", "etf_price",
            "return_pct", "sector_etf_pct", "diff_pct_vs_etf",
            "days_until_dividend"
        ]].sort_values(by="diff_pct_vs_etf", ascending=False).reset_index(drop=True)

        return df

    finally:
        conn.close()





def plot_sector_price_histories():

    PERIOD = "3mo"
    DB_CANDIDATES = "data/candidates.db"
    DB_TICKERS = "data/tickers.db"
    HISTORY_TABLE = "price_history"

    def ensure_price_history_table(conn):
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
                symbol TEXT,
                date TEXT,
                close REAL,
                period TEXT,
                PRIMARY KEY (symbol, date, period)
            )
        """)
        conn.commit()

    def get_or_cache_price_history(symbol, period, conn):
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT date, close FROM {HISTORY_TABLE}
            WHERE symbol = ? AND period = ?
            ORDER BY date
        """, (symbol, period))
        rows = cursor.fetchall()

        if rows:
            return pd.DataFrame(rows, columns=["Date", symbol]).set_index("Date")

        try:
            data = yf.Ticker(symbol).history(period=period)
            if data.empty:
                return pd.DataFrame()
            hist = data[["Close"]].copy()
            hist.reset_index(inplace=True)
            hist["symbol"] = symbol
            hist["period"] = period
            hist = hist.rename(columns={"Close": "close", "Date": "date"})
            hist[["symbol", "date", "close", "period"]].to_sql(HISTORY_TABLE, conn, if_exists="append", index=False)
            return hist[["date", "close"]].rename(columns={"close": symbol}).set_index("date")
        except Exception as e:
            print(f"⚠️ Failed to fetch history for {symbol}: {e}")
            return pd.DataFrame()

    # Connect to DBs
    conn_cand = sqlite3.connect(DB_CANDIDATES)
    conn_hist = sqlite3.connect(DB_TICKERS)
    ensure_price_history_table(conn_hist)

    # Read candidates
    df = pd.read_sql("SELECT symbol, sector, sector_etf FROM candidates", conn_cand)
    df = df.dropna(subset=["sector", "sector_etf"]).drop_duplicates()
    grouped = df.groupby("sector")

    for sector, group in grouped:
        all_symbols = group["symbol"].tolist() + group["sector_etf"].unique().tolist()
        price_frames = []

        for sym in set(all_symbols):
            df_hist = get_or_cache_price_history(sym, PERIOD, conn_hist)
            if not df_hist.empty:
                price_frames.append(df_hist)

        if price_frames:
            df_prices = pd.concat(price_frames, axis=1).dropna()
            df_prices.index = pd.to_datetime(df_prices.index)

            plt.figure(figsize=(12, 6))
            for col in df_prices.columns:
                plt.plot(df_prices.index, df_prices[col], label=col)
            plt.title(f"📈 3-Month Price History – {sector}")
            plt.xlabel("Date")
            plt.ylabel("Close Price (USD)")
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.show()

    conn_cand.close()
    conn_hist.close()


def color_percent(value):
    if value is None:
        return "--"
    elif value > 0:
        return f"\033[92m{value:10.2f}%\033[0m"  # Green
    elif value < 0:
        return f"\033[91m{value:10.2f}%\033[0m"  # Red
    else:
        return f"{value:10.2f}%"

def print_color_table_with_header(df, width=11):
    cols = list(df.columns)

    # Print header
    header = f"{'Ticker':>{width}} |"
    for col in cols[1:]:
        header += f" {col:^{width}} |"
    print(header)
    print("-" * len(header))

    # Print rows
    for _, row in df.iterrows():
        line = f"{row['Ticker']:>{width}} |"
        for col in cols[1:]:
            val = row[col]
            line += f" {color_percent(val):>{width}} |"
        print(line)

def get_performance_table(tickers):
    today = datetime.datetime.today()
    start_ytd = datetime.datetime(today.year, 1, 1)

    periods = {
        "Perf Week": 7,
        "Perf Month": 30,
        "Perf Quart": 90,
        "Perf Half": 180,
        "Perf Year": 365,
    }

    results = []

    for ticker in tickers:
        try:
            data = yf.Ticker(ticker).history(start=start_ytd - datetime.timedelta(days=370))
            if data.empty:
                continue

            latest_close = data["Close"].iloc[-1]
            row = {"Ticker": ticker}

            # Compute percent changes
            for label, days in periods.items():
                past_date = today - datetime.timedelta(days=days)
                data.index = data.index.tz_localize(None)  # ← This removes timezone info
                past_prices = data[data.index <= past_date]
                if not past_prices.empty:
                    past_close = past_prices["Close"].iloc[-1]
                    row[label] = round((latest_close - past_close) / past_close * 100, 2)
                else:
                    row[label] = None

            # Year-to-date
            ytd_data = data[data.index >= start_ytd]
            if not ytd_data.empty:
                ytd_start = ytd_data["Close"].iloc[0]
                row["Perf YTD"] = round((latest_close - ytd_start) / ytd_start * 100, 2)
            else:
                row["Perf YTD"] = None

            results.append(row)

        except Exception as e:
            print(f"⚠️ Error fetching data for {ticker}: {e}")

    return pd.DataFrame(results)


def main():
    display_candidates_by_sector(only_outperforming=True, only_with_dividends=True)
    df_flat = get_flat_candidate_table_with_prices(only_outperforming=True, only_with_dividends=True)
    print(df_flat.sort_values(by=["sector_etf","symbol"], ascending=True))
    grouped = df_flat.groupby("sector_etf")
    for etf, group in grouped:
        tickers = sorted(group["symbol"].unique())
        print(f"https://finviz.com/screener.ashx?v=211&t={etf},{','.join(tickers)}")

    #plot_sector_price_histories()


    tickers = sorted(df_flat["symbol"].unique().tolist())
    perf_df = get_performance_table(tickers)
    print_color_table_with_header(perf_df)


    sector_etfs = sorted(SECTOR_ETF_MAP.values())
    sector_perf_df = get_performance_table(sector_etfs)
    print_color_table_with_header(sector_perf_df)


if __name__ == "__main__":
    main()
