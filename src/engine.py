import polars as pl
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import date, timedelta, datetime
from src.database import get_db_path

def get_all_data():
    """Récupère toutes les tables en Polars DF"""
    conn = sqlite3.connect(get_db_path())
    
    # Transactions
    tx_query = "SELECT * FROM transactions WHERE is_excluded = 0"
    df_tx = pl.read_database(tx_query, conn).with_columns(pl.col("date").cast(pl.Date))
    
    # Transfers
    tr_query = "SELECT * FROM transfers"
    df_tr = pl.read_database(tr_query, conn).with_columns(pl.col("date").cast(pl.Date))
    
    # Investments
    inv_query = "SELECT * FROM investments"
    df_inv = pl.read_database(inv_query, conn).with_columns(pl.col("date").cast(pl.Date))
    
    # Accounts
    acc_query = "SELECT * FROM accounts"
    df_acc = pl.read_database(acc_query, conn)
    
    conn.close()
    return df_tx, df_tr, df_inv, df_acc

# --- NEW: CACHING LOGIC START ---

def update_market_data(tickers, min_date):
    """
    Smart Sync:
    1. Fills 'History Gaps' (if we bought stocks before our cache starts).
    2. Fills 'Recent Data' (if we haven't synced in a few days).
    """
    if not tickers:
        return

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    today = date.today()
    
    for ticker in tickers:
        # Check the Range of data we currently have
        cursor.execute("SELECT MIN(date), MAX(date) FROM market_prices WHERE ticker = ?", (ticker,))
        min_db, max_db = cursor.fetchone()
        
        # Ranges to download
        ranges_to_fetch = []
        
        # CASE A: No data at all
        if min_db is None:
            ranges_to_fetch.append((min_date, today))
        else:
            # Convert DB strings to Date objects
            min_db_date = datetime.strptime(min_db, "%Y-%m-%d").date()
            max_db_date = datetime.strptime(max_db, "%Y-%m-%d").date()
            
            # CASE B: We need older history (Backfill)
            # If our first investment (min_date) is older than what we have in DB
            if min_date < min_db_date:
                print(f"🔄 Backfilling history for {ticker}: {min_date} -> {min_db_date}")
                ranges_to_fetch.append((min_date, min_db_date))
            
            # CASE C: We need new data (Update)
            if max_db_date < today:
                # Start from the next day after what we have
                start_update = max_db_date + timedelta(days=1)
                if start_update <= today:
                    print(f"📈 Updating recent data for {ticker}: {start_update} -> {today}")
                    ranges_to_fetch.append((start_update, today))

        # Perform Downloads
        for start_d, end_d in ranges_to_fetch:
            # Safety: Don't download future dates relative to system time
            if start_d > today: continue
            
            # YFinance expects end date to be exclusive, so we add 1 day
            yf_end = end_d + timedelta(days=1)
            
            try:
                # auto_adjust=False keeps raw Close/Adj Close separation
                df_yf = yf.download(ticker, start=start_d, end=yf_end, progress=False, auto_adjust=False)
                
                if not df_yf.empty:
                    # --- Data Cleaning (Same as before) ---
                    if isinstance(df_yf.columns, pd.MultiIndex):
                        try:
                            if 'Close' in df_yf.columns.get_level_values(0): df_yf = df_yf['Close']
                            elif 'Adj Close' in df_yf.columns.get_level_values(0): df_yf = df_yf['Adj Close']
                        except: pass
                    
                    if isinstance(df_yf, pd.Series): df_yf = df_yf.to_frame(name="Close")
                    if "Close" in df_yf.columns: df_yf = df_yf[["Close"]]
                    
                    df_yf = df_yf.reset_index()
                    df_yf.columns = ["date", "price"]
                    
                    records = []
                    for _, row in df_yf.iterrows():
                        if pd.isnull(row['date']): continue
                        d_str = row['date'].strftime("%Y-%m-%d")
                        val = float(row['price'])
                        records.append((d_str, ticker, val))
                    
                    if records:
                        cursor.executemany("INSERT OR REPLACE INTO market_prices (date, ticker, price) VALUES (?, ?, ?)", records)
                        conn.commit()
                        print(f"✅ Saved {len(records)} days for {ticker}")
                        
            except Exception as e:
                print(f"❌ Error downloading {ticker} ({start_d} to {end_d}): {e}")

    conn.close()
    
def get_market_prices_df(tickers, min_date):
    """
    Syncs cache then reads everything from DB into Polars.
    Returns DataFrame: [date, TickerA, TickerB...] (Pivoted)
    """
    # 1. Sync Cache
    update_market_data(tickers, min_date)
    
    # 2. Read from DB
    conn = sqlite3.connect(get_db_path())
    
    # Placeholders string for SQL IN clause
    placeholders = ','.join('?' for _ in tickers)
    query = f"SELECT date, ticker, price FROM market_prices WHERE ticker IN ({placeholders}) ORDER BY date"
    
    try:
        df_long = pl.read_database(query, conn, execute_options={"parameters": list(tickers)})
    except Exception as e:
        print(f"Error reading market DB: {e}")
        conn.close()
        return pl.DataFrame()
        
    conn.close()
    
    if df_long.is_empty():
        return pl.DataFrame()

    # 3. Pivot to wide format (dates x tickers)
    df_long = df_long.with_columns(pl.col("date").cast(pl.Date))
    
    df_wide = df_long.pivot(
        values="price",
        index="date",
        on="ticker",
        aggregate_function="first" # Should be unique per pk anyway
    ).sort("date")
    
    return df_wide

# --- CACHING LOGIC END ---

def calculate_wealth_evolution(start_date=None):
    df_tx, df_tr, df_inv, df_acc = get_all_data()
    
    if df_tx.is_empty() and df_acc.is_empty():
        return pl.DataFrame()

    # 1. Timeline determination
    d1 = df_tx["date"].min() if not df_tx.is_empty() else date.today()
    d2 = df_tr["date"].min() if not df_tr.is_empty() else date.today()
    d3 = df_inv["date"].min() if not df_inv.is_empty() else date.today()
    
    global_min = min(filter(None, [d1, d2, d3])) or date.today()
    
    if start_date is None:
        min_date = global_min
    else:
        min_date = start_date

    calendar = pl.date_range(min_date, date.today(), interval="1d", eager=True).alias("date")
    df_timeline = pl.DataFrame({"date": calendar})

    # 2. CASH FLOW Calculation
    movements = []

    # Initial Balances
    for row in df_acc.rows(named=True):
        movements.append({"date": min_date, "account": row['name'], "amount": row['initial_balance']})

    # Transactions
    if not df_tx.is_empty():
        tx_clean = df_tx.select([
            pl.col("date"),
            pl.col("account"),
            pl.when(pl.col("type") == "EXPENSE").then(pl.col("amount") * -1).otherwise(pl.col("amount")).alias("amount")
        ])
        movements.extend(tx_clean.to_dicts())

    # Transfers
    if not df_tr.is_empty():
        tr_out = df_tr.select([pl.col("date"), pl.col("source_account").alias("account"), (pl.col("amount") * -1).alias("amount")])
        tr_in = df_tr.select([pl.col("date"), pl.col("target_account").alias("account"), pl.col("amount")])
        movements.extend(tr_out.to_dicts())
        movements.extend(tr_in.to_dicts())

    # Investments (Cash Impact)
    if not df_inv.is_empty():
        inv_buy = df_inv.filter(pl.col("action") == "BUY").select([
            pl.col("date"),
            pl.col("account"),
            ((pl.col("quantity") * pl.col("unit_price") * -1) - pl.col("fees")).alias("amount")
        ])
        inv_sell = df_inv.filter(pl.col("action") == "SELL").select([
            pl.col("date"),
            pl.col("account"),
            ((pl.col("quantity") * pl.col("unit_price")) - pl.col("fees")).alias("amount")
        ])
        movements.extend(inv_buy.to_dicts())
        movements.extend(inv_sell.to_dicts())

    if not movements:
        return pl.DataFrame()
        
    df_flow = pl.from_dicts(movements)
    df_daily_change = df_flow.group_by(["date", "account"]).agg(pl.col("amount").sum()).sort("date")
    df_pivot_cash = df_daily_change.pivot(index="date", on="account", values="amount").sort("date")
    df_full_cash = df_timeline.join(df_pivot_cash, on="date", how="left").fill_null(0)
    
    accounts_cols = [c for c in df_full_cash.columns if c != "date"]
    df_balances = df_full_cash.with_columns([
        pl.col(c).cum_sum().alias(c) for c in accounts_cols
    ])

    # 3. PORTFOLIO VALUE (Investments)
    if df_inv.is_empty():
        df_balances = df_balances.with_columns(
            pl.sum_horizontal(accounts_cols).alias("Total Wealth")
        )
        return df_balances

    # Calculate Quantity Held per Ticker per Day
    inv_qty = df_inv.with_columns([
        pl.when(pl.col("action") == "BUY").then(pl.col("quantity")).otherwise(pl.col("quantity") * -1).alias("signed_qty")
    ])
    
    df_qty_daily = inv_qty.group_by(["date", "ticker"]).agg(pl.col("signed_qty").sum()).sort("date")
    df_qty_pivot = df_qty_daily.pivot(index="date", on="ticker", values="signed_qty").sort("date")
    
    df_full_qty = df_timeline.join(df_qty_pivot, on="date", how="left").fill_null(0)
    tickers = [c for c in df_full_qty.columns if c != "date"]
    
    if not tickers:
        df_balances = df_balances.with_columns(pl.sum_horizontal(accounts_cols).alias("Total Wealth"))
        return df_balances

    df_holdings = df_full_qty.with_columns([
        pl.col(t).cum_sum().alias(t) for t in tickers
    ])

    # --- UPDATED: Fetch Prices via Cache ---
    
    # Retrieve prices from DB (handling download if missing)
    # We ask for data starting from the very first investment found
    inv_start_date = df_inv["date"].min()
    df_prices = get_market_prices_df(tickers, inv_start_date)

    if df_prices.is_empty():
        # Fallback if no internet or error: Value = 0 (or utilize purchase price if complex logic added)
        print("⚠️ No market data available. Investments valued at 0.")
        return df_balances.with_columns(
            pl.sum_horizontal(accounts_cols).alias("Total Wealth")
        )

    # 4. Multiply Quantity * Price
    
    # We need to ensure df_prices covers the whole timeline
    # Forward fill prices (if today is Sunday, use Friday's price)
    df_prices_filled = df_timeline.join(df_prices, on="date", how="left")
    
    # Forward fill strategy in Polars:
    df_prices_filled = df_prices_filled.with_columns([
        pl.col(t).forward_fill().backward_fill() for t in tickers if t in df_prices_filled.columns
    ])
    
    # Convert to Pandas for safe matrix multiplication (easiest way to handle dates alignment)
    pdf_holdings = df_holdings.to_pandas().set_index("date")[tickers]
    
    # Filter columns that exist in prices (in case a ticker failed to download)
    available_tickers = [t for t in tickers if t in df_prices_filled.columns]
    pdf_prices = df_prices_filled.to_pandas().set_index("date")[available_tickers]
    
    # Align headers
    pdf_holdings = pdf_holdings[available_tickers]
    
    # Multiplication
    pdf_val = pdf_holdings.mul(pdf_prices, fill_value=0).fillna(0)
    
    pdf_val["Total Invest"] = pdf_val.sum(axis=1)
    
    df_invest_res = pl.from_pandas(pdf_val.reset_index()).with_columns(pl.col("date").cast(pl.Date))
    
    # 5. Final Merge
    df_total = df_balances.join(df_invest_res.select(["date", "Total Invest"]), on="date", how="left").fill_null(0)
    
    cash_cols = [c for c in df_balances.columns if c != "date"]
    df_total = df_total.with_columns(
        (pl.sum_horizontal(cash_cols) + pl.col("Total Invest")).alias("Total Wealth")
    )
    
    return df_total