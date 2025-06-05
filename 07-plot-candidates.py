import sqlite3
import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt

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


def plot_etf_tickers(etf, tickers, base_path="./ticker_dbs"):
    if not tickers:
        print(f"No tickers found for ETF '{etf}'.")
        return

    plt.figure(figsize=(14, 7))
    
    for ticker in tickers:
        db_path = os.path.join(base_path, f"{ticker}.db")
        if not os.path.exists(db_path):
            print(f"DB for {ticker} not found. Skipping.")
            continue

        try:
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql("SELECT Date, Close FROM history", conn, parse_dates=["Date"])
                df.sort_values("Date", inplace=True)
                plt.plot(df["Date"], df["Close"], label=ticker)
        except Exception as e:
            print(f"Error reading {ticker}: {e}")
            continue

    plt.title(f"{etf} — 1 Year Price History")
    plt.xlabel("Date")
    plt.ylabel("Adjusted Close Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def unused_plot_etf_tickers_relative(etf, tickers, base_path="./ticker_dbs"):
    if not tickers:
        print(f"No tickers found for ETF '{etf}'.")
        return

    plt.figure(figsize=(14, 7))

    tickers = [etf] + [t for t in tickers if t != etf]

    for i, ticker in enumerate(tickers):
        db_path = os.path.join(base_path, f"{ticker}.db")
        if not os.path.exists(db_path):
            print(f"DB for {ticker} not found. Skipping.")
            continue

        try:
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql("SELECT Date, Close FROM history", conn, parse_dates=["Date"])
                df.sort_values("Date", inplace=True)

                # Normalize to % change from the first value
                df["Pct"] = (df["Close"] / df["Close"].iloc[0] - 1) * 100
                last_price = df["Close"].iloc[-1]
                label = f"{ticker} (${last_price:.0f})"
                if i == 0:
                    label += " (ETF)"
                    plt.plot(df["Date"], df["Pct"], label=label, linewidth=3.5, linestyle="--")
                else:
                    plt.plot(df["Date"], df["Pct"], label=label, linewidth=1.2)
        except Exception as e:
            print(f"Error reading {ticker}: {e}")
            continue

    plt.title(f"{etf} & Tickers — Relative 1-Year Performance (%)")
    plt.xlabel("Date")
    plt.ylabel("Performance vs start (%)")
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


import os
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3

def plot_etf_tickers_relative(etf, tickers, base_path="./ticker_dbs", output_dir="./etf_charts"):
    if not tickers:
        print(f"No tickers found for ETF '{etf}'.")
        return

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Ensure ETF is included and not duplicated
    tickers = [etf] + [t for t in tickers if t != etf]

    # Step 1: Collect last prices
    ticker_data = []
    for ticker in tickers:
        db_path = os.path.join(base_path, f"{ticker}.db")
        if not os.path.exists(db_path):
            print(f"DB for {ticker} not found. Skipping.")
            continue

        try:
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql("SELECT Date, Close FROM history", conn, parse_dates=["Date"])
                df.sort_values("Date", inplace=True)
                last_price = df["Close"].iloc[-1]
                ticker_data.append((ticker, df, last_price))
        except Exception as e:
            print(f"Error reading {ticker}: {e}")
            continue

    if not ticker_data:
        print("No valid data to plot.")
        return

    # Step 2: Sort by last price
    ticker_data.sort(key=lambda x: x[2])  # x[2] is last_price

    # Step 3: Plot
    plt.figure(figsize=(14, 7))

    for i, (ticker, df, last_price) in enumerate(ticker_data):
        df["Pct"] = (df["Close"] / df["Close"].iloc[0] - 1) * 100
        label = f"{ticker} (${last_price:.2f})"
        if ticker == etf:
            label += " (ETF)"
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=3.5, linestyle="--")
        else:
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=1.2)

    plt.title(f"{etf} & Tickers — Relative 1-Year Performance (%)")
    plt.xlabel("Date")
    plt.ylabel("Performance vs start (%)")
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Step 4: Save the plot
    save_path = os.path.join(output_dir, f"{etf}_relative.png")
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"Saved plot to {save_path}")
def main():
    df_flat = get_flat_candidate_table_with_prices(only_outperforming=True, only_with_dividends=True)
    print(df_flat.sort_values(by=["sector_etf","symbol"], ascending=True))
    grouped = df_flat.groupby("sector_etf")
    etf_tickers = {
        etf: sorted(group["symbol"].unique().tolist())
        for etf, group in grouped
    }
    for etf, tickers in etf_tickers.items():
        plot_etf_tickers_relative(etf, tickers)

if __name__ == "__main__":
    main()
    #with sqlite3.connect("ticker_dbs/ABBV.db") as conn:
    #    print(pd.read_sql("PRAGMA table_info(history);", conn))
