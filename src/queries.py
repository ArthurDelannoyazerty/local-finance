import polars as pl
from src.database import get_db_path
import sqlite3

def get_transactions_df():
    """Reads transactions from SQLite into Polars"""
    query = "SELECT * FROM transactions ORDER BY date DESC"
    raw_path = get_db_path()
    clean_path = raw_path.replace("\\", "/")
    uri = f"sqlite:///{clean_path}"
    return pl.read_database_uri(query, uri)

def get_investments_df():
    """Récupère l'historique des investissements pour affichage"""
    query = """
        SELECT date, action, ticker, name, quantity, unit_price, fees, account, comment 
        FROM investments 
        ORDER BY date DESC
    """
    raw_path = get_db_path()
    clean_path = raw_path.replace("\\", "/")
    uri = f"sqlite:///{clean_path}"
    try:
        return pl.read_database_uri(query, uri)
    except:
        return pl.DataFrame()
# -----------------

def update_exclusion(tx_id, is_excluded):
    """Updates the excluded status of a transaction"""
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    val = 1 if is_excluded else 0
    c.execute("UPDATE transactions SET is_excluded = ? WHERE id = ?", (val, tx_id))
    conn.commit()
    conn.close()

def get_daily_balance_evolution():
    """
    Complex logic: Reconstructs daily balance based on Income, Expenses, and Transfers.
    For now, we focus on Income/Expense aggregation.
    """
    df = get_transactions_df()
    
    # Filter out excluded
    df = df.filter(pl.col("is_excluded") == 0)
    
    # Adjust sign: Expense is negative
    df = df.with_columns(
        pl.when(pl.col("type") == "EXPENSE")
        .then(pl.col("amount") * -1)
        .otherwise(pl.col("amount"))
        .alias("signed_amount")
    )
    
    # Group by Date and Account
    # This gives the delta per day
    daily_delta = df.group_by(["date", "account"]).agg(
        pl.col("signed_amount").sum().alias("daily_change")
    ).sort("date")
    
    return daily_delta