import sqlite3
import pandas as pd

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
            print(f"   ➤ Avg Return: {row['avg_return_pct']:.2f}%")
            print(f"   ➤ Total: {row['count']} tickers, {row['dividend_count']} with dividends")
            if row['avg_days_to_div'] is not None:
                print(f"   ➤ Avg days to dividend: {row['avg_days_to_div']} days")

        return summary

    finally:
        conn.close()



def main():
    display_candidates_by_sector(only_outperforming=True, only_with_dividends=True)

if __name__ == "__main__":
    main()
