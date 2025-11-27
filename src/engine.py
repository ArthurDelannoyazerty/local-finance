import polars as pl
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import date, timedelta, datetime
from src.database import get_db_path

# --- CACHING / MARKET DATA LOGIC ---

def update_market_data(tickers):
    """
    Downloads missing market data for the given tickers.
    """
    if not tickers:
        return

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    today = date.today()
    
    # Check what we already have to avoid re-downloading everything
    placeholders = ','.join(['?'] * len(tickers))
    query = f"SELECT ticker, MIN(date), MAX(date) FROM market_prices WHERE ticker IN ({placeholders}) GROUP BY ticker"
    cursor.execute(query, tickers)
    
    existing_data = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    for ticker in tickers:
        start_date = date(2020, 1, 1) # Default start if nothing exists
        
        if ticker in existing_data:
            last_date_str = existing_data[ticker][1]
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            if last_date >= today:
                continue # Up to date
            start_date = last_date + timedelta(days=1)
        
        if start_date > today: continue

        try:
            # Download
            df_yf = yf.download(ticker, start=start_date, end=today + timedelta(days=1), progress=False, auto_adjust=False)
            
            # Cleaning
            if not df_yf.empty:
                if isinstance(df_yf.columns, pd.MultiIndex):
                    try:
                        # Handle MultiIndex (e.g., ('Close', 'CW8.PA'))
                        if 'Close' in df_yf.columns.get_level_values(0): df_yf = df_yf['Close']
                        elif 'Adj Close' in df_yf.columns.get_level_values(0): df_yf = df_yf['Adj Close']
                    except: pass
                
                # Force to DataFrame with 1 column named 'price'
                if isinstance(df_yf, pd.Series): df_yf = df_yf.to_frame(name="price")
                elif "Close" in df_yf.columns: df_yf = df_yf[["Close"]].rename(columns={"Close": "price"})
                else: df_yf.columns = ["price"] # Blind assignment if structure is weird

                records = []
                for dt, row in df_yf.iterrows():
                    d_str = dt.strftime("%Y-%m-%d") if isinstance(dt, (datetime, pd.Timestamp)) else str(dt)
                    val = float(row.iloc[0]) if isinstance(row, pd.Series) else float(row['price'])
                    if not pd.isna(val):
                        records.append((d_str, ticker, val))
                
                if records:
                    cursor.executemany("INSERT OR REPLACE INTO market_prices (date, ticker, price) VALUES (?, ?, ?)", records)
                    print(f"✅ Updated {ticker}: {len(records)} days added.")
                    
        except Exception as e:
            print(f"⚠️ Error syncing {ticker}: {e}")

    conn.commit()
    conn.close()


# --- CORE ALGORITHM (The Robust Logic) ---

def calculate_wealth_evolution():
    """
    Generates the daily evolution of cash and assets using an Event-Sourcing approach.
    Returns a Polars DataFrame compatible with app.py.
    """
    conn = sqlite3.connect(get_db_path())

    # 1. LOAD ALL DATA (Into Pandas for the loop logic)
    accounts = pd.read_sql("SELECT name, initial_balance FROM accounts", conn)
    
    trans = pd.read_sql("SELECT date, account, amount, type FROM transactions WHERE is_excluded = 0", conn)
    trans['amount'] = pd.to_numeric(trans['amount'])
    
    transfers = pd.read_sql("SELECT date, source_account, target_account, amount FROM transfers", conn)
    transfers['amount'] = pd.to_numeric(transfers['amount'])
    
    investments = pd.read_sql("SELECT date, account, ticker, action, quantity, unit_price, fees FROM investments", conn)
    investments[['quantity', 'unit_price', 'fees']] = investments[['quantity', 'unit_price', 'fees']].apply(pd.to_numeric)
    
    # Get tickers to sync market data
    unique_tickers = investments['ticker'].unique().tolist() if not investments.empty else []
    conn.close() # Close to update market data safely
    
    # 2. SYNC MARKET PRICES
    if unique_tickers:
        update_market_data(unique_tickers)
        
    # Re-open connection to get fresh prices
    conn = sqlite3.connect(get_db_path())
    prices = pd.read_sql(f"SELECT date, ticker, price FROM market_prices", conn)
    conn.close()

    # 3. STANDARDIZE DATES
    for df in [trans, transfers, investments, prices]:
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])

    # Determine timeline
    all_dates = pd.concat([
        trans['date'], 
        transfers['date'], 
        investments['date']
    ]).dropna()
    
    if all_dates.empty and accounts.empty:
        return pl.DataFrame() # No data
        
    start_date = all_dates.min() if not all_dates.empty else pd.Timestamp(date.today())
    end_date = pd.Timestamp(date.today())
    
    # Create master timeline (Daily)
    timeline = pd.date_range(start=start_date, end=end_date, freq='D')

    # 4. PREPARE MARKET DATA MATRIX
    # Pivot: Index=Date, Cols=Ticker, Values=Price
    if not prices.empty:
        price_matrix = prices.pivot_table(index='date', columns='ticker', values='price', aggfunc='mean')
        # Reindex to full timeline and Forward Fill (handle weekends)
        price_matrix = price_matrix.reindex(timeline).ffill()
    else:
        price_matrix = pd.DataFrame(index=timeline)

    # 5. INITIALIZE STATE
    # Cash balances: { 'PEA': 1000.0, 'Compte Courant': 500.0 }
    current_cash = {row['name']: row['initial_balance'] for _, row in accounts.iterrows()}
    
    # Portfolio Inventory PER ACCOUNT: { 'PEA': {'CW8': 10}, 'CTO': {'TSLA': 5} }
    portfolio = {acc: {} for acc in current_cash}
    
    # FALLBACK PRICES: { 'CW8.PA': 450.20 } 
    # (Remembers the last purchase price if market data is missing)
    last_tx_prices = {}

    # Pre-group data by date for speed
    trans_g = trans.groupby('date') if not trans.empty else None
    transf_g = transfers.groupby('date') if not transfers.empty else None
    inv_g = investments.groupby('date') if not investments.empty else None

    history = []

    # 6. THE EVENT LOOP
    for day in timeline:
        day_ts = pd.Timestamp(day)
        
        # A. Process Cash Transactions (Income/Expense)
        if trans_g and day_ts in trans_g.groups:
            for _, row in trans_g.get_group(day_ts).iterrows():
                acc = row['account']
                if acc not in current_cash: 
                    current_cash[acc] = 0.0
                    portfolio[acc] = {}
                
                if row['type'] == 'INCOME':
                    current_cash[acc] += row['amount']
                else: # EXPENSE
                    current_cash[acc] -= row['amount']

        # B. Process Transfers
        if transf_g and day_ts in transf_g.groups:
            for _, row in transf_g.get_group(day_ts).iterrows():
                src, tgt = row['source_account'], row['target_account']
                if src in current_cash: current_cash[src] -= row['amount']
                if tgt in current_cash: current_cash[tgt] += row['amount']

        # C. Process Investments (Impact Cash AND Shares)
        if inv_g and day_ts in inv_g.groups:
            for _, row in inv_g.get_group(day_ts).iterrows():
                tkr = row['ticker']
                acc = row['account']
                qty = row['quantity']
                price = row['unit_price']
                fees = row['fees']
                
                total_cost = qty * price
                
                # Update Fallback Price
                last_tx_prices[tkr] = price

                if acc not in current_cash: 
                    current_cash[acc] = 0.0
                    portfolio[acc] = {}
                
                if tkr not in portfolio[acc]:
                    portfolio[acc][tkr] = 0.0

                if row['action'] == 'BUY':
                    current_cash[acc] -= (total_cost + fees)
                    portfolio[acc][tkr] += qty
                elif row['action'] == 'SELL':
                    current_cash[acc] += (total_cost - fees)
                    portfolio[acc][tkr] -= qty

        # D. Calculate Valuation (Snapshot)
        
        # 1. Calculate value of stocks held PER ACCOUNT
        account_invest_val = {acc: 0.0 for acc in current_cash}
        total_invest_val = 0.0
        
        for acc, holdings in portfolio.items():
            for tkr, qty in holdings.items():
                if qty > 0.000001:
                    # Get Market Price
                    mkt_price = 0.0
                    if tkr in price_matrix.columns:
                        val = price_matrix.at[day, tkr]
                        if not pd.isna(val):
                            mkt_price = val
                    
                    # Fallback
                    if mkt_price == 0.0:
                        mkt_price = last_tx_prices.get(tkr, 0.0)
                    
                    val = qty * mkt_price
                    account_invest_val[acc] += val
                    total_invest_val += val

        # 2. Create Record
        # CRITICAL FIX: The value of an account is Cash + Investment Value
        record = {
            'date': day,
            'Total Invest': total_invest_val, # Kept separate for the yellow dashed line
        }
        
        # Populate account columns with (Cash + Stock)
        for acc, cash_bal in current_cash.items():
            invest_bal = account_invest_val.get(acc, 0.0)
            record[acc] = cash_bal + invest_bal

        history.append(record)

    # 7. FORMAT OUTPUT FOR APP.PY
    if not history:
        return pl.DataFrame()

    df_hist = pd.DataFrame(history)
    
    # Calculate Total Wealth
    # Note: account columns already include investments now, so we just sum them.
    # We exclude 'date' and 'Total Invest' (which is just an indicator)
    account_cols = [c for c in df_hist.columns if c not in ['date', 'Total Invest']]
    df_hist['Total Wealth'] = df_hist[account_cols].sum(axis=1)
    
    # Convert to Polars
    pl_df = pl.from_pandas(df_hist)
    pl_df = pl_df.with_columns(pl.col("date").cast(pl.Date))
    
    return pl_df


def get_detailed_snapshot(target_date: date):
    """
    Returns a detailed breakdown of the portfolio at a specific date.
    Columns: [Account, Type, Ticker, Quantity, UnitPrice, Value]
    Type is either 'CASH' or 'INVEST'.
    """
    target_ts = pd.Timestamp(target_date)
    conn = sqlite3.connect(get_db_path())
    
    # 1. Get Cash Flow (Income, Expense, Transfers, Inv Buys/Sells)
    # -------------------------------------------------------------
    # Initial Balances
    accounts = pd.read_sql("SELECT name, initial_balance FROM accounts", conn)
    cash_balances = {row['name']: row['initial_balance'] for _, row in accounts.iterrows()}
    
    # Transactions (Income/Expense)
    tx_query = "SELECT account, amount, type FROM transactions WHERE date <= ? AND is_excluded = 0"
    txs = pd.read_sql(tx_query, conn, params=(target_date,))
    if not txs.empty:
        for _, row in txs.iterrows():
            acc = row['account']
            amt = row['amount']
            if acc in cash_balances:
                if row['type'] == 'INCOME': cash_balances[acc] += amt
                else: cash_balances[acc] -= amt

    # Transfers
    tr_query = "SELECT source_account, target_account, amount FROM transfers WHERE date <= ?"
    trs = pd.read_sql(tr_query, conn, params=(target_date,))
    if not trs.empty:
        for _, row in trs.iterrows():
            if row['source_account'] in cash_balances: cash_balances[row['source_account']] -= row['amount']
            if row['target_account'] in cash_balances: cash_balances[row['target_account']] += row['amount']

    # Investment Cash Impact & Share Quantities
    inv_query = "SELECT account, ticker, name, action, quantity, unit_price, fees FROM investments WHERE date <= ?"
    invs = pd.read_sql(inv_query, conn, params=(target_date,))
    
    portfolio_qty = {} # { (Account, Ticker): Quantity }
    ticker_names = {}  # { Ticker: Name }
    
    if not invs.empty:
        for _, row in invs.iterrows():
            acc = row['account']
            tkr = row['ticker']
            action = row['action']
            qty = row['quantity']
            price = row['unit_price']
            fees = row['fees']
            
            ticker_names[tkr] = row['name']
            
            # Cash Impact
            total_cost = qty * price
            if acc in cash_balances:
                if action == 'BUY': cash_balances[acc] -= (total_cost + fees)
                elif action == 'SELL': cash_balances[acc] += (total_cost - fees)
            
            # Quantity Impact
            key = (acc, tkr)
            if key not in portfolio_qty: portfolio_qty[key] = 0.0
            
            if action == 'BUY': portfolio_qty[key] += qty
            elif action == 'SELL': portfolio_qty[key] -= qty

    conn.close()

    # 2. Build Result Rows
    # --------------------
    rows = []
    
    # A. Add Cash Lines
    for acc, bal in cash_balances.items():
        if abs(bal) > 0.01:
            rows.append({
                "Account": acc,
                "Type": "Liquidités",
                "Ticker": "CASH",
                "Name": "Liquidités",
                "Quantity": 1.0,
                "UnitPrice": bal,
                "Value": bal
            })
            
    # B. Add Investment Lines (Need Prices)
    # Identify tickers with held quantity > 0
    active_tickers = [k[1] for k, v in portfolio_qty.items() if v > 0.000001]
    
    # Fetch price at date
    price_map = {}
    if active_tickers:
        unique_tickers = list(set(active_tickers))
        # Ensure data exists (Reuse existing function logic simplified)
        # Note: In a production app, we might trigger a download here, 
        # but for speed we assume 'calculate_wealth_evolution' has run at least once globally.
        
        conn = sqlite3.connect(get_db_path())
        placeholders = ','.join(['?'] * len(unique_tickers))
        # Find the closest price equal or before target_date
        p_query = f"""
            SELECT ticker, price 
            FROM market_prices 
            WHERE ticker IN ({placeholders}) AND date <= ?
            GROUP BY ticker 
            HAVING date = MAX(date)
        """
        # We append target_date to the params list
        params = unique_tickers + [target_date]
        prices_df = pd.read_sql(p_query, conn, params=params)
        conn.close()
        
        price_map = {row['ticker']: row['price'] for _, row in prices_df.iterrows()}

    for (acc, tkr), qty in portfolio_qty.items():
        if qty > 0.000001:
            # Get price or fallback to 0 (or could fallback to last purchase price if sophisticated)
            price = price_map.get(tkr, 0.0)
            val = qty * price
            rows.append({
                "Account": acc,
                "Type": "Investissement",
                "Ticker": tkr,
                "Name": ticker_names.get(tkr, tkr),
                "Quantity": qty,
                "UnitPrice": price,
                "Value": val
            })
            
    return pd.DataFrame(rows)