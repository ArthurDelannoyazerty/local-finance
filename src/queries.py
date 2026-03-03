import sqlite3
import polars as pl
from src.database import get_db_path


def get_transactions_df() -> pl.DataFrame:
    """
    Retrieves all non-excluded transactions from the database.
    
    Returns:
        pl.DataFrame: A Polars DataFrame containing the transactions, 
                      ordered by date descending.
    """
    query: str = "SELECT * FROM transactions WHERE is_excluded = 0 ORDER BY date DESC"
    
    with sqlite3.connect(get_db_path()) as conn:
        df = pl.read_database(query, conn)
        
    # Safely cast the 'date' column to Date type if data exists
    if not df.is_empty() and "date" in df.columns:
        df = df.with_columns(pl.col("date").cast(pl.Date))
        
    return df


def get_investments_df() -> pl.DataFrame:
    """
    Retrieves the history of investments for display purposes.
    
    Returns:
        pl.DataFrame: A Polars DataFrame containing investment records.
    """
    query: str = """
        SELECT date, action, ticker, name, quantity, unit_price, fees, account, comment 
        FROM investments 
        ORDER BY date DESC
    """
    
    try:
        with sqlite3.connect(get_db_path()) as conn:
            df = pl.read_database(query, conn)
            
            if not df.is_empty() and "date" in df.columns:
                df = df.with_columns(pl.col("date").cast(pl.Date))
                
            return df
            
    except Exception as e:
        print(f"⚠️ Error reading investments: {e}")
        return pl.DataFrame()


def update_exclusion(tx_id: str, is_excluded: bool) -> None:
    """
    Updates the excluded status of a specific transaction.
    
    Args:
        tx_id (str): The unique identifier of the transaction.
        is_excluded (bool): True to exclude the transaction, False to include it.
    """
    val: int = 1 if is_excluded else 0
    
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE transactions SET is_excluded = ? WHERE id = ?", 
            (val, tx_id)
        )
        conn.commit()


def get_daily_balance_evolution() -> pl.DataFrame:
    """
    Reconstructs daily balance deltas based on Income and Expenses.
    
    Returns:
        pl.DataFrame: A Polars DataFrame with daily changes per account.
    """
    df = get_transactions_df()
    
    if df.is_empty():
        return pl.DataFrame()
    
    # Filter out excluded (already done in get_transactions_df, but kept for safety)
    df = df.filter(pl.col("is_excluded") == 0)
    
    # Adjust sign: Expenses become negative amounts
    df = df.with_columns(
        pl.when(pl.col("type") == "EXPENSE")
        .then(pl.col("amount") * -1)
        .otherwise(pl.col("amount"))
        .alias("signed_amount")
    )
    
    # Group by Date and Account to get the net daily delta
    daily_delta = (
        df.group_by(["date", "account"])
        .agg(pl.col("signed_amount").sum().alias("daily_change"))
        .sort("date")
    )
    
    return daily_delta