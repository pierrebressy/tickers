import matplotlib
matplotlib.use('Agg')  # IMPORTANT: use non-interactive backend BEFORE pyplot import
import matplotlib.pyplot as plt
import os
import sqlite3
import pandas as pd
import datetime


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

def get_start_date_from_period(period):
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if period.endswith("m"):
        months = int(period[:-1])
        return today - pd.DateOffset(months=months)
    elif period.endswith("y"):
        years = int(period[:-1])
        return today - pd.DateOffset(years=years)
    else:
        return None

def get_start_date_from_period_old(period):
    if period.endswith("d"):
        days = int(period[:-1])
        return datetime.datetime.today() - datetime.timedelta(days=days)
    elif period.endswith("mo"):
        months = int(period[:-2])
        return datetime.datetime.today() - datetime.timedelta(days=30 * months)
    elif period.endswith("y"):
        years = int(period[:-1])
        return datetime.datetime.today() - datetime.timedelta(days=365 * years)
    elif period == "ytd":
        return datetime.datetime(datetime.datetime.today().year, 1, 1)
    elif period == "max":
        return None  # keep all
    else:
        return datetime.datetime.today() - datetime.timedelta(days=365)  # fallback to 1y


def get_or_fetch_price(symbol, conn, today=None):

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
    db_path = "databases/candidates.db"
    conn = sqlite3.connect(db_path)
    db_cache = "databases/tickers.db"
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


def plot_etf_tickers(etf, tickers, base_path="./databases/ticker_dbs"):
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

def unused_plot_etf_tickers_relative(etf, tickers, base_path="./databases/ticker_dbs"):
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


def plot_etf_tickers_relative(etf, tickers, base_path="./databases/ticker_dbs", output_dir="./etf_charts", period="6m"):
    if not tickers:
        print(f"No tickers found for ETF '{etf}'.")
        return

    os.makedirs(output_dir, exist_ok=True)
    tickers = [etf] + [t for t in tickers if t != etf]

    start_date = get_start_date_from_period(period)
    start_date = pd.to_datetime(start_date).normalize()

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
                df["Date"] = pd.to_datetime(df["Date"])
                df = df[df["Date"] >= start_date]
                if df.empty:
                    print(f"No data for {ticker} after {start_date.date()}. Skipping.")
                    continue

                df["Pct"] = (df["Close"] / df["Close"].iloc[0] - 1) * 100
                last_price = df["Close"].iloc[-1]
                perf = df["Pct"].iloc[-1]
                ticker_data.append((ticker, df, last_price, perf))
        except Exception as e:
            print(f"Error reading {ticker}: {e}")
            continue

    if not ticker_data:
        print("No valid data to plot.")
        return

    # Sort by performance (ascending)
    ticker_data.sort(key=lambda x: x[3], reverse=True)

    plt.figure(figsize=(14, 7))

    for ticker, df, last_price, perf in ticker_data:
        label = f"{ticker} (${last_price:.2f}, {perf:+.1f}%)"
        if ticker == etf:
            label += " (ETF)"
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=3.5, linestyle="--")
        else:
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=1.2)

    plt.title(f"{etf} & Tickers — Relative {period.upper()} Performance (%)")
    plt.xlabel("Date")
    plt.ylabel("Performance vs start (%)")
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(output_dir, f"{etf}_relative.png")
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"✅ Saved plot to {save_path}")


def plot_etf_tickers_relative3(etf, tickers, base_path="./databases/ticker_dbs", output_dir="./etf_charts", period="6m"):
    if not tickers:
        print(f"No tickers found for ETF '{etf}'.")
        return

    os.makedirs(output_dir, exist_ok=True)
    tickers = [etf] + [t for t in tickers if t != etf]

    # Calculate start date
    start_date = get_start_date_from_period(period)
    start_date = pd.to_datetime(start_date).normalize()

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
                df["Date"] = pd.to_datetime(df["Date"])

                # Filter by period
                df = df[df["Date"] >= start_date]
                if df.empty:
                    print(f"No data for {ticker} after {start_date.date()}. Skipping.")
                    continue

                last_price = df["Close"].iloc[-1]
                ticker_data.append((ticker, df, last_price))
        except Exception as e:
            print(f"Error reading {ticker}: {e}")
            continue

    if not ticker_data:
        print("No valid data to plot.")
        return

    #ticker_data.sort(key=lambda x: x[2])
    ticker_data.sort(key=lambda x: x[2], reverse=True)

    plt.figure(figsize=(14, 7))

    for i, (ticker, df, last_price) in enumerate(ticker_data):
        df["Pct"] = (df["Close"] / df["Close"].iloc[0] - 1) * 100
        perf = df["Pct"].iloc[-1]
        label = f"{ticker} (${last_price:.2f}, {perf:+.1f}%)"
        if ticker == etf:
            label += " (ETF)"
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=3.5, linestyle="--")
        else:
            plt.plot(df["Date"], df["Pct"], label=label, linewidth=1.2)

    plt.title(f"{etf} & Tickers — Relative {period.upper()} Performance (%)")
    plt.xlabel("Date")
    plt.ylabel("Performance vs start (%)")
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    save_path = os.path.join(output_dir, f"{etf}_relative.png")
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"✅ Saved plot to {save_path}")

def plot_all_sector_etfs_relative(etf_list, base_path="./databases/ticker_dbs", output_path="static/graphs", period="6m"):

    plt.figure(figsize=(14, 7))

    data = []
    for etf in etf_list:
        start_date = get_start_date_from_period(period)
        db_path = os.path.join(base_path, f"{etf}.db")
        if not os.path.exists(db_path):
            print(f"DB for {etf} not found. Skipping.")
            continue

        try:
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql("SELECT Date, Close FROM history", conn, parse_dates=["Date"])
                df["Date"] = pd.to_datetime(df["Date"])

                df.sort_values("Date", inplace=True)

                if start_date:
                    df = df[df["Date"] >= start_date]
                if df.empty:
                    continue

                df["Pct"] = (df["Close"] / df["Close"].iloc[0] - 1) * 100
                perf = df["Pct"].iloc[-1]
                last_price = df["Close"].iloc[-1]
                #label = f"{etf} (${last_price:.2f})"
                label = f"{etf} (${last_price:.2f}, {perf:+.1f}%)"
                data.append((etf, df["Date"], df["Pct"], label))
        except Exception as e:
            print(f"Error reading {etf}: {e}")
            continue

    # Optional: sort by last performance
    data.sort(key=lambda x: x[2].iloc[-1])  # Sort by last % return
    for etf, dates, pct, label in data:
        plt.plot(dates, pct, label=label)

    plt.title("Sector ETFs — Relative Performance (%)")
    plt.xlabel("Date")
    plt.ylabel("Performance vs start (%)")
    plt.axhline(0, color='gray', linestyle='--', linewidth=0.7)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"✅ Saved sector ETF plot to {output_path}")



def main():

    output_dir = "static/graphs"
    df_flat = get_flat_candidate_table_with_prices(only_outperforming=True, only_with_dividends=True)
    print(df_flat.sort_values(by=["sector_etf","symbol"], ascending=True))
    grouped = df_flat.groupby("sector_etf")
    etf_tickers = {
        etf: sorted(group["symbol"].unique().tolist())
        for etf, group in grouped
    }
    for etf, tickers in etf_tickers.items():
        plot_etf_tickers_relative(etf, tickers, output_dir=output_dir)

    plot_all_sector_etfs_relative(
        list(SECTOR_ETF_MAP.values()),
        output_path=os.path.join(output_dir, "all_etfs_relative.png")
    )

if __name__ == "__main__":
    main()
