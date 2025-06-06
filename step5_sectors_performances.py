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


def get_performance_table(tickers):
    today = datetime.datetime.today()
    start_ytd = datetime.datetime(today.year, 1, 1)

    periods = {
        "Week": 7,
        "Month": 30,
        "Quarter": 90,
        "Half": 180,
        "Year": 365,
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
                data.index = data.index.tz_localize(None)  # remove timezone info
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
                row["YTD"] = round((latest_close - ytd_start) / ytd_start * 100, 2)
            else:
                row["YTD"] = None

            results.append(row)

        except Exception as e:
            print(f"⚠️ Error fetching data for {ticker}: {e}")

    return results


def get_sector_performance_table():
    sector_etfs = sorted(SECTOR_ETF_MAP.values())
    data = get_performance_table(sector_etfs)
    return "", data




def get_sector_performance_table_xxx():
    data = [
        # Remplace ceci par tes vraies données extraites dynamiquement
        {"Ticker": "XLB", "Week": 1.41, "Month": 4.61, "Quarter": 0.17, "Half": -3.79, "Year": -1.50, "YTD": 5.72},
        {"Ticker": "XLC", "Week": 1.41, "Month": 4.89, "Quarter": 2.41, "Half": 2.02, "Year": 21.93, "YTD": 5.57},
        # ...
    ]
    return "", data
