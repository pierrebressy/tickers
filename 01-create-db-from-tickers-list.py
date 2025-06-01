import sqlite3
import pandas as pd

# Local file paths
NASDAQ_FILE = "data/nasdaqlisted.txt"
NYSE_FILE = "data/otherlisted.txt"
DB_FILE = "data/tickers.db"

def process_files(nasdaq_file, nyse_file):
    # Read NASDAQ file
    nasdaq_df = pd.read_csv(nasdaq_file, sep="|")
    nasdaq_df = nasdaq_df[:-1]  # Remove footer
    nasdaq_df["Exchange"] = "NASDAQ"

    # Read NYSE/Other file
    nyse_df = pd.read_csv(nyse_file, sep="|")
    nyse_df = nyse_df[:-1]  # Remove footer
    nyse_df["Exchange"] = nyse_df["Exchange"].map({"N": "NYSE", "A": "AMEX", "P": "NYSE ARCA"})

    # Normalize columns and concatenate
    nyse_df = nyse_df.rename(columns={"ACT Symbol": "Symbol"})
    combined_df = pd.concat([
        nasdaq_df[["Symbol", "Security Name", "Exchange"]],
        nyse_df[["Symbol", "Security Name", "Exchange"]]
    ], ignore_index=True)

    return combined_df

def store_in_database(df, db_file):
    with sqlite3.connect(db_file) as conn:
        df.to_sql("us_tickers", conn, if_exists="replace", index=False)
        print(f"Stored {len(df)} tickers in {db_file}.")



if __name__ == "__main__":
    tickers_df = process_files(NASDAQ_FILE, NYSE_FILE)
    store_in_database(tickers_df, DB_FILE)
