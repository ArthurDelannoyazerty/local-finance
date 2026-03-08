import sqlite3
from datetime import date, timedelta, datetime
from typing import List, Dict, Tuple, Any, Optional

import pandas as pd
import polars as pl
import yfinance as yf

from src.database import get_db_path


# --- CACHING / MARKET DATA LOGIC ---

def update_market_data(tickers: List[str]) -> None:
    """
    Downloads missing market data for the given tickers from Yahoo Finance 
    and caches it locally in the SQLite database.
    
    Args:
        tickers (List[str]): A list of ticker symbols (e.g.,['CW8.PA', 'AAPL']).
    """
    if not tickers:
        return

    today: date = date.today()
    
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        
        # Check what we already have to avoid re-downloading everything
        placeholders: str = ','.join(['?'] * len(tickers))
        query: str = f"""
            SELECT ticker, MIN(date), MAX(date) 
            FROM market_prices 
            WHERE ticker IN ({placeholders}) 
            GROUP BY ticker
        """
        cursor.execute(query, tickers)
        
        existing_data: Dict[str, Tuple[str, str]] = {
            row[0]: (row[1], row[2]) for row in cursor.fetchall()
        }

        for ticker in tickers:
            start_date: date = date(2020, 1, 1)  # Default start if nothing exists
            
            if ticker in existing_data:
                last_date_str = existing_data[ticker][1]
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
                if last_date >= today:
                    continue  # Up to date
                start_date = last_date + timedelta(days=1)
            
            if start_date > today:
                continue

            try:
                # Download data from Yahoo Finance
                df_yf = yf.download(
                    ticker, 
                    start=start_date, 
                    end=today + timedelta(days=1), 
                    progress=False, 
                    auto_adjust=True
                )
                
                # Cleaning and normalizing the dataframe structure
                if not df_yf.empty:
                    if isinstance(df_yf.columns, pd.MultiIndex):
                        try:
                            if 'Close' in df_yf.columns.get_level_values(0): 
                                df_yf = df_yf['Close']
                        except Exception:
                            pass                
                    
                    # Force to DataFrame with 1 column named 'price'
                    if isinstance(df_yf, pd.Series): 
                        df_yf = df_yf.to_frame(name="price")
                    elif "Close" in df_yf.columns: 
                        df_yf = df_yf[["Close"]].rename(columns={"Close": "price"})
                    else: 
                        df_yf.columns = ["price"]  # Blind assignment if structure is weird

                    records: List[Tuple[str, str, float]] =[]
                    for dt, row in df_yf.iterrows():
                        d_str = dt.strftime("%Y-%m-%d") if isinstance(dt, (datetime, pd.Timestamp)) else str(dt)
                        val = float(row.iloc[0]) if isinstance(row, pd.Series) else float(row['price'])
                        
                        if not pd.isna(val):
                            records.append((d_str, ticker, val))
                    
                    if records:
                        cursor.executemany(
                            "INSERT OR REPLACE INTO market_prices (date, ticker, price) VALUES (?, ?, ?)", 
                            records
                        )
                        print(f"✅ Updated {ticker}: {len(records)} days added.")
                        
            except Exception as e:
                print(f"⚠️ Error syncing {ticker}: {e}")

        conn.commit()


# --- CORE ALGORITHM (The Robust Logic) ---

def calculate_wealth_evolution(target_start: Optional[date] = None, target_end: Optional[date] = None) -> pl.DataFrame:
    """
    Generates the daily evolution of cash and assets.
    Optimized: Calculates only the active period, then manually stitches 
    padding rows for the past and future to fit the requested window.
    """
    # 1. LOAD ALL DATA
    with sqlite3.connect(get_db_path()) as conn:
        visible_acc_query = "SELECT name FROM accounts WHERE is_visible = 1"
        accounts = pd.read_sql("SELECT name, initial_balance FROM accounts WHERE is_visible = 1", conn)
        trans = pd.read_sql(f"SELECT date, account, amount, type FROM transactions WHERE is_excluded = 0 AND account IN ({visible_acc_query})", conn)
        transfers = pd.read_sql(f"SELECT date, source_account, target_account, amount FROM transfers WHERE source_account IN ({visible_acc_query}) AND target_account IN ({visible_acc_query})", conn)
        investments = pd.read_sql(f"SELECT date, account, ticker, action, quantity, unit_price, fees FROM investments WHERE account IN ({visible_acc_query})", conn)

    trans['amount'] = pd.to_numeric(trans['amount'])
    transfers['amount'] = pd.to_numeric(transfers['amount'])
    investments[['quantity', 'unit_price', 'fees']] = investments[['quantity', 'unit_price', 'fees']].apply(pd.to_numeric)

    unique_tickers: List[str] = investments['ticker'].unique().tolist() if not investments.empty else[]
    
    if unique_tickers:
        update_market_data(unique_tickers)
        
    with sqlite3.connect(get_db_path()) as conn:
        prices = pd.read_sql("SELECT date, ticker, price FROM market_prices", conn)

    # 3. STANDARDIZE DATES
    for df in [trans, transfers, investments, prices]:
        if not df.empty:
            df['date'] = pd.to_datetime(df['date']).dt.date

    all_dates = pd.concat([trans['date'], transfers['date'], investments['date']]).dropna()
    
    if all_dates.empty and accounts.empty:
        return pl.DataFrame()

    # 4. DETERMINE ACTIVE TIMELINE (Only where data exists)
    db_start = all_dates.min() if not all_dates.empty else date.today()
    db_end = all_dates.max() if not all_dates.empty else date.today()
    
    # Always include today to get latest prices
    if db_end < date.today():
        db_end = date.today()
        
    timeline_dates = pd.date_range(start=db_start, end=db_end, freq='D').date

    # 5. MARKET DATA MATRIX
    if not prices.empty:
        price_matrix = prices.pivot_table(index='date', columns='ticker', values='price', aggfunc='mean')
        idx = pd.to_datetime(timeline_dates)
        price_matrix.index = pd.to_datetime(price_matrix.index)
        price_matrix = price_matrix.reindex(idx).ffill().bfill().fillna(0.0)
        price_matrix.index = price_matrix.index.date
    else:
        price_matrix = pd.DataFrame(index=timeline_dates)

    # 6. INITIALIZE STATE
    current_cash: Dict[str, float] = {row['name']: float(row['initial_balance']) for _, row in accounts.iterrows()}
    portfolio: Dict[str, Dict[str, float]] = {acc: {} for acc in current_cash}
    last_tx_prices: Dict[str, float] = {}

    trans_g = trans.groupby('date') if not trans.empty else None
    transf_g = transfers.groupby('date') if not transfers.empty else None
    inv_g = investments.groupby('date') if not investments.empty else None

    history: List[Dict[str, Any]] =[]

    # 7. EVENT LOOP (Active period only)
    for day in timeline_dates:
        if trans_g and day in trans_g.groups:
            for _, row in trans_g.get_group(day).iterrows():
                acc = row['account']
                if acc not in current_cash: 
                    current_cash[acc] = 0.0
                    portfolio[acc] = {}
                if row['type'] == 'INCOME': current_cash[acc] += row['amount']
                else: current_cash[acc] -= row['amount']

        if transf_g and day in transf_g.groups:
            for _, row in transf_g.get_group(day).iterrows():
                src, tgt = row['source_account'], row['target_account']
                if src in current_cash: current_cash[src] -= row['amount']
                if tgt in current_cash: current_cash[tgt] += row['amount']

        if inv_g and day in inv_g.groups:
            for _, row in inv_g.get_group(day).iterrows():
                tkr, acc, qty, price, fees = row['ticker'], row['account'], row['quantity'], row['unit_price'], row['fees']
                last_tx_prices[tkr] = price
                if acc not in current_cash: 
                    current_cash[acc] = 0.0
                    portfolio[acc] = {}
                if tkr not in portfolio[acc]: portfolio[acc][tkr] = 0.0
                
                cost = qty * price
                if row['action'] == 'BUY':
                    current_cash[acc] -= (cost + fees)
                    portfolio[acc][tkr] += qty
                elif row['action'] == 'SELL':
                    current_cash[acc] += (cost - fees)
                    portfolio[acc][tkr] -= qty

        # Valuation
        account_invest_val = {acc: 0.0 for acc in current_cash}
        total_invest_val = 0.0
        
        for acc, holdings in portfolio.items():
            for tkr, qty in holdings.items():
                if qty > 0.000001:
                    mkt_price = 0.0
                    if tkr in price_matrix.columns:
                        val = price_matrix.at[day, tkr]
                        if not pd.isna(val): mkt_price = val
                    if mkt_price == 0.0: mkt_price = last_tx_prices.get(tkr, 0.0)
                    
                    val_assets = qty * mkt_price
                    account_invest_val[acc] += val_assets
                    total_invest_val += val_assets

        record = {'date': day, 'Total Invest': total_invest_val}
        for acc, cash_bal in current_cash.items():
            record[acc] = cash_bal + account_invest_val.get(acc, 0.0)
        history.append(record)

    if not history:
        return pl.DataFrame()

    df_hist = pd.DataFrame(history).fillna(0.0)
    account_cols = [c for c in df_hist.columns if c not in ['date', 'Total Invest']]
    df_hist['Total Wealth'] = df_hist[account_cols].sum(axis=1)

    # 8. ROBUST EXTENSION (MANUAL STITCHING)
    # Ensure strict datetime format for sorting and comparison
    df_hist['date'] = pd.to_datetime(df_hist['date'])
    df_hist = df_hist.sort_values('date')
    
    # Get the boundaries of the calculated data
    min_hist_date = df_hist['date'].iloc[0]
    max_hist_date = df_hist['date'].iloc[-1]
    
    # Prepare list of dataframes to concatenate
    dfs_to_concat = [df_hist]

    # A. Prepend Past Data (if requested start < data start)
    if target_start:
        t_start = pd.Timestamp(target_start)
        if t_start < min_hist_date:
            # Create a range from target_start up to the day before history starts
            past_dates = pd.date_range(start=t_start, end=min_hist_date - pd.Timedelta(days=1), freq='D')
            if not past_dates.empty:
                # Copy the first known row and replicate it
                first_row = df_hist.iloc[[0]].copy()
                past_df = pd.concat([first_row] * len(past_dates), ignore_index=True)
                past_df['date'] = past_dates
                dfs_to_concat.insert(0, past_df)

    # B. Append Future Data (if requested end > data end)
    if target_end:
        t_end = pd.Timestamp(target_end)
        if t_end > max_hist_date:
            # Create a range from day after history ends up to target_end
            future_dates = pd.date_range(start=max_hist_date + pd.Timedelta(days=1), end=t_end, freq='D')
            if not future_dates.empty:
                # Copy the last known row and replicate it
                last_row = df_hist.iloc[[-1]].copy()
                future_df = pd.concat([last_row] * len(future_dates), ignore_index=True)
                future_df['date'] = future_dates
                dfs_to_concat.append(future_df)

    # C. Final Stitching
    final_df = pd.concat(dfs_to_concat, ignore_index=True)

    # 9. CONVERT TO POLARS
    pl_df = pl.from_pandas(final_df).with_columns(pl.col("date").cast(pl.Date))
    
    return pl_df


def get_detailed_snapshot(target_date: date) -> pd.DataFrame:
    """
    Returns a detailed breakdown of the portfolio (cash and assets) at a specific date.
    
    Args:
        target_date (date): The exact date for the snapshot.
        
    Returns:
        pd.DataFrame: A DataFrame with columns[Account, Type, Ticker, Name, Quantity, UnitPrice, Value].
    """
    # 1. Get Cash Flow & Portfolio quantities up to target_date
    with sqlite3.connect(get_db_path()) as conn:
        accounts = pd.read_sql("SELECT name, initial_balance FROM accounts", conn)
        cash_balances: Dict[str, float] = {row['name']: float(row['initial_balance']) for _, row in accounts.iterrows()}
        
        # Transactions (Income/Expense)
        tx_query = "SELECT account, amount, type FROM transactions WHERE date <= ? AND is_excluded = 0"
        txs = pd.read_sql(tx_query, conn, params=(target_date,))
        if not txs.empty:
            for _, row in txs.iterrows():
                acc = row['account']
                amt = float(row['amount'])
                if acc in cash_balances:
                    if row['type'] == 'INCOME': 
                        cash_balances[acc] += amt
                    else: 
                        cash_balances[acc] -= amt

        # Transfers
        tr_query = "SELECT source_account, target_account, amount FROM transfers WHERE date <= ?"
        trs = pd.read_sql(tr_query, conn, params=(target_date,))
        if not trs.empty:
            for _, row in trs.iterrows():
                amt = float(row['amount'])
                if row['source_account'] in cash_balances: 
                    cash_balances[row['source_account']] -= amt
                if row['target_account'] in cash_balances: 
                    cash_balances[row['target_account']] += amt

        # Investments
        inv_query = "SELECT account, ticker, name, action, quantity, unit_price, fees FROM investments WHERE date <= ?"
        invs = pd.read_sql(inv_query, conn, params=(target_date,))
        
        portfolio_qty: Dict[Tuple[str, str], float] = {}  # { (Account, Ticker): Quantity }
        ticker_names: Dict[str, str] = {}                 # { Ticker: Name }
        
        if not invs.empty:
            for _, row in invs.iterrows():
                acc = row['account']
                tkr = row['ticker']
                action = row['action']
                qty = float(row['quantity'])
                price = float(row['unit_price'])
                fees = float(row['fees'])
                
                ticker_names[tkr] = row['name']
                
                # Cash Impact
                total_cost = qty * price
                if acc in cash_balances:
                    if action == 'BUY': 
                        cash_balances[acc] -= (total_cost + fees)
                    elif action == 'SELL': 
                        cash_balances[acc] += (total_cost - fees)
                
                # Quantity Impact
                key = (acc, tkr)
                if key not in portfolio_qty: 
                    portfolio_qty[key] = 0.0
                
                if action == 'BUY': 
                    portfolio_qty[key] += qty
                elif action == 'SELL': 
                    portfolio_qty[key] -= qty

    # 2. Build Result Rows
    rows: List[Dict[str, Any]] =[]
    
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
    active_tickers: List[str] = [k[1] for k, v in portfolio_qty.items() if v > 0.000001]
    price_map: Dict[str, float] = {}
    
    if active_tickers:
        unique_tickers = list(set(active_tickers))
        placeholders = ','.join(['?'] * len(unique_tickers))
        p_query = f"""
            SELECT ticker, price 
            FROM market_prices 
            WHERE ticker IN ({placeholders}) AND date <= ?
            GROUP BY ticker 
            HAVING date = MAX(date)
        """
        params = unique_tickers + [target_date]
        
        with sqlite3.connect(get_db_path()) as conn:
            prices_df = pd.read_sql(p_query, conn, params=params)
        
        price_map = {row['ticker']: float(row['price']) for _, row in prices_df.iterrows()}

    # Compile Final Rows
    for (acc, tkr), qty in portfolio_qty.items():
        if qty > 0.000001:
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