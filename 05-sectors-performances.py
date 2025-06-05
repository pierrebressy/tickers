import pandas as pd
import yfinance as yf
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

    sector_etfs = sorted(SECTOR_ETF_MAP.values())
    sector_perf_df = get_performance_table(sector_etfs)
    print_color_table_with_header(sector_perf_df)
    
if __name__ == "__main__":
    main()
