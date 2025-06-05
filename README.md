# `Tickers` : find underlyings

## Modus operandi

### 1. Create a SQLite database from the list of tickers

The script `01-create-db-from-tickers-list.py` will use the  list of tickers from NASDAQ and create a SQLite database with the table `us_tickers`.

To be run only once.

```bash
> python3 01-create-db-from-tickers-list.py
Stored 11376 tickers in data/tickers.db.
```

For postprocessing, remove some tickers that are not relevant for our analysis, such as ETFs or indices. You can do this by executing the following SQL commands in the SQLite database:
```bash
sqlite3 data/tickers.db
```
```sql
DELETE FROM us_tickers WHERE symbol = 'QQQ';
DELETE FROM us_tickers WHERE symbol = 'SPY';
DELETE FROM us_tickers WHERE symbol = 'VB';
DELETE FROM us_tickers WHERE symbol = 'VTI';
DELETE FROM us_tickers WHERE symbol = 'IVV';
```

### 2. Fetch ticker information
The script `02-enrich_tickers_with_yfinance.py` will fetch information about each ticker from `us_tickers` table, from Yahoo Finance and store it in the `ticker_info` table of the SQLite database.

To avoid long duration during update, the tickers that are already processed (`processed=1` in `us_tickers` table) will not be processed again.

When processed, the script update the `ticker_info` table with the following columns and sets the `processed` flag to 1 in the `us_tickers` table.

```bash
> python3 02-enrich_tickers_with_yfinance.py
  symbol      sector sector_etf  return_pct  sector_etf_pct  outperforming  has_dividend days_until_dividend evaluated_at
0   MSFT  Technology        XLK     1617.55          256.53           True             1                None   2025-06-01
1   AAPL  Technology        XLK    -1684.03          256.53          False             1                None   2025-06-01
```

### 3. Create a databa for candidates tickers

The script `03-create-candidates-db.py` will create a new SQLite database `data/candidates.db` from the `ticker_info` table, which will contain only the tickers that are relevant with specific criteria:
- Market cap greater than 100_000_000_000 USD
- Tickers that superform the sector ETF over the last 6 months

```bash
> python3 03-create-candidate-db.py
```





### 4. Process the candidates database

The script `04-process-candidates-db.py` will display various informations extracted from the database :
- list of tickers with their sector
- direct link to finviz.com to access the data, by sector,
- performance of each sector (week, monmth, quarter, half-year, year, YTD),

```bash
> python3 04-process-candidates-db.py
```

### 5. Get sectors performances
The script `05-sectors-performances.py` will calculate and display the performance of each sector.

```bash
> python3 05-sectors-performances.py
     Ticker |  Perf Week  | Perf Month  | Perf Quart  |  Perf Half  |  Perf Year  |  Perf YTD   |
-------------------------------------------------------------------------------------------------
        XLB |       0.75% |       1.66% |       1.11% |      -6.73% |      -3.85% |       4.26% |
        XLC |       1.21% |       4.09% |       1.52% |       1.72% |      23.16% |       4.54% |
        XLE |      -0.56% |      -0.55% |      -5.52% |     -10.16% |      -9.65% |      -5.19% |
        XLF |       1.82% |       2.41% |       2.45% |       2.20% |      24.29% |       6.09% |
        XLI |       1.41% |       6.12% |       8.72% |       0.93% |      17.35% |       9.11% |
        XLK |       1.81% |       6.60% |       5.87% |      -3.91% |      10.64% |      -0.28% |
        XLP |       1.51% |       1.56% |       1.42% |       1.75% |       9.88% |       6.25% |
       XLRE |       2.68% |      -0.48% |      -2.38% |      -3.39% |      13.48% |       4.24% |
        XLU |       1.06% |       2.72% |       5.72% |       3.41% |      16.09% |       8.17% |
        XLV |       1.76% |      -4.25% |     -10.15% |      -8.85% |      -6.21% |      -3.24% |
        XLY |       1.50% |       6.14% |       2.95% |      -5.02% |      22.58% |      -3.17% |

```

___
___
# Database structure and usage

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

