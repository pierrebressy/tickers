import yfinance as yf


def get_option_spread(ticker, expiry=None, strike=None, call=True):
    """
    Fetches bid-ask spread for a given option (call/put) on a ticker.

    Params:
        ticker  : str  — e.g. "AAPL"
        expiry  : str  — e.g. "2025-06-21"
        strike  : float — e.g. 200.0
        call    : bool — True for call, False for put

    Returns:
        dict with bid, ask, spread
    """
    tk = yf.Ticker(ticker)

    if expiry is None:
        expiry = tk.options[0]  # default: nearest expiry

    opt_chain = tk.option_chain(expiry)
    opt_df = opt_chain.calls if call else opt_chain.puts

    row = opt_df[opt_df['strike'] == strike]
    if row.empty:
        return {"error": f"No option found for strike {strike} on {expiry}"}

    bid = row['bid'].values[0]
    ask = row['ask'].values[0]
    spread = round(ask - bid, 2) if bid is not None and ask is not None else None

    return {
        "ticker": ticker,
        "type": "call" if call else "put",
        "expiry": expiry,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "spread": spread
    }


result = get_option_spread("AAPL", expiry="2025-06-20", strike=200.0, call=True)
print(result)
