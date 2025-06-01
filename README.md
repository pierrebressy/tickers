# `Tickers` : find underlyings

## Modus operandi

### 1. Create a SQLite database from the list of tickers
The script `01-create-db-from-tickers-list.py` will use the  list of tickers from NASDAQ and create a SQLite database with the table `us_tickers`.

```bash
> python3 01-create-db-from-tickers-list.py
Stored 11376 tickers in data/tickers.db.
```

### 2. Fetch ticker information
The script `02-enrich_tickers_with_yfinance.py` will fetch information about each ticker from Yahoo Finance and store it in the `ticker_info` table of the SQLite database.

```bash
> python3 02-enrich_tickers_with_yfinance.py
  symbol      sector sector_etf  return_pct  sector_etf_pct  outperforming  has_dividend days_until_dividend evaluated_at
0   MSFT  Technology        XLK     1617.55          256.53           True             1                None   2025-06-01
1   AAPL  Technology        XLK    -1684.03          256.53          False             1                None   2025-06-01
```

### 3. Process the data
The script `03-process-candidates-db.py` will display various informations extracted from the database.

```bash
> python3 03-process-candidates-db.py
```


## sqlite3 db usage
```bash
sqlite3 data/tickers.db
```

### List of tables
```sql
.tables
```

### List of columns in a table
```sql
PRAGMA table_info(us_tickers);
PRAGMA table_info(ticker_info);
PRAGMA table_info(price_cache);
```

## Table `us_ticker`
List of tickers, from two main CSV files:
- nasdaqlisted.txt
- otherlisted.txt

The script `01-create-db-from-tickers-list.py` will download these files and create the table in the database.

```Txt
0|Symbol|TEXT|0||0
1|Security Name|TEXT|0||0
2|Exchange|TEXT|0||0```
```

## Table `ticker_info`
Information about each ticker, including market cap, sector, industry, and dividend information. This table is populated by the script `02-create-ticker-info.py`, which fetches data from Yahoo Finance.
```Txt
0|symbol|TEXT|0||0
1|longName|TEXT|0||0
2|sector|TEXT|0||0
3|industry|TEXT|0||0
4|country|TEXT|0||0
5|marketCap|REAL|0||0
6|currency|TEXT|0||0
7|isOptionable|INTEGER|0||0
8|quoteType|TEXT|0||0
9|exchange|TEXT|0||0
10|has_dividend|BOOLEAN|0||0
11|next_dividend_date|TEXT|0||0
12|days_until_dividend|INTEGER|0||0
13|last_dividend_check|TEXT|0||0
```

## Table price_cache
This table stores the latest price and return percentage for each ticker for the given `period`.
```Txt
0|symbol|TEXT|0||1
1|period|TEXT|0||2
2|return_pct|REAL|0||0
3|last_updated|TEXT|0||3```

