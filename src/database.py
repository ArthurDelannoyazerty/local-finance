import sqlite3
from pathlib import Path
from typing import Set, List

# Define the relative path to the database
DB_PATH: Path = Path("data/finance.db")


def get_db_path() -> str:
    """
    Returns the absolute path to the SQLite database.
    
    Returns:
        str: Absolute path to the database file.
    """
    return str(DB_PATH.resolve())


def sync_accounts_from_history() -> None:
    """
    Scans the historical transactions, transfers, and investments to 
    automatically create missing accounts in the `accounts` table.
    """
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    queries: List[str] =[
        "SELECT DISTINCT account FROM transactions WHERE account IS NOT NULL AND account != ''",
        "SELECT DISTINCT source_account FROM transfers WHERE source_account IS NOT NULL AND source_account != ''",
        "SELECT DISTINCT target_account FROM transfers WHERE target_account IS NOT NULL AND target_account != ''",
        "SELECT DISTINCT account FROM investments WHERE account IS NOT NULL AND account != ''"
    ]

    found_accounts: Set[str] = set()
    
    # Execute each query and collect unique account names
    for query in queries:
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                if row[0]:  # Ensure the account name is not empty
                    found_accounts.add(row[0])
        except sqlite3.Error:
            # Ignore errors if tables don't exist yet or are empty
            pass

    # Insert missing accounts
    count: int = 0
    for acc in found_accounts:
        cursor.execute(
            "INSERT OR IGNORE INTO accounts (name, initial_balance) VALUES (?, 0.0)", 
            (acc,)
        )
        if cursor.rowcount > 0:
            count += 1
            
    conn.commit()
    conn.close()
    
    if count > 0:
        print(f"✅ Base de données : {count} compte(s) récupéré(s) et ajouté(s).")


def init_db() -> None:
    """
    Initializes the SQLite database, creating the necessary tables 
    if they do not already exist. Also triggers account synchronization.
    """
    # Ensure the data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    # 1. Accounts Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            name TEXT PRIMARY KEY,
            initial_balance REAL DEFAULT 0.0,
            is_visible BOOLEAN DEFAULT 1
        )
    """)

    # 2. Transactions Table (Income & Expenses)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            date DATE,
            category TEXT,
            account TEXT,
            amount REAL,
            currency TEXT,
            comment TEXT,
            type TEXT, 
            is_excluded BOOLEAN DEFAULT 0
        )
    """)
    
    # 3. Transfers Table (Inter-account movements)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id TEXT PRIMARY KEY,
            date DATE,
            source_account TEXT,
            target_account TEXT,
            amount REAL,
            comment TEXT
        )
    """)

    # 4. Investments Table (Stock market transactions)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS investments (
            id TEXT PRIMARY KEY,
            date DATE,
            ticker TEXT,
            name TEXT,
            action TEXT,
            quantity REAL,
            unit_price REAL,
            fees REAL,
            account TEXT,
            comment TEXT
        )
    """)
    
    # 5. Market Prices Table (Historical data caching)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            date DATE,
            ticker TEXT,
            price REAL,
            PRIMARY KEY (date, ticker)
        )
    """)

    # 6. Projections Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projections (
            id TEXT PRIMARY KEY,
            name TEXT,
            created_at DATE,
            parameters_json TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    
    # Sync accounts from any existing history right after initialization
    sync_accounts_from_history()