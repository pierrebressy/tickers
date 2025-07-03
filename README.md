# Tickers Finder with Flask server

## Create a virtual environment
```bash
python3 -m venv ticker_env
```
## Activate the virtual environment
```bash
source ticker_env/bin/activate
```
## Install dependencies
```bash
pip install -r requirements.txt
```
## Run the Flask server
```bash
source ticker_env/bin/activate
python3 app.py
```
Then open http://127.0.0.1:5052/

# Usage
- **Create Ticker DB** : required only once at the beginning of the project ; do not use it again
- **Enricher Ticker DB** : to update the database with properties of teh tickers using Yahoo Finance; do not use it again
- **Create Candidates DB** : to create a flat table with specified market cap, max prices and observation period
- **Process Candidates DB** : to process a flat table with tickers superforming their sector
- **Create Price History** : update the price history of the candidates
- **Generate graphs** : Generate graphs for the candidates