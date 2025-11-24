import sqlite3
from pathlib import Path

DB_PATH = Path("data/finance.db")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. Transactions (Revenus & Dépenses)
    # We add 'is_excluded' for your checkbox feature
    c.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        date DATE,
        category TEXT,
        account TEXT,
        amount REAL,
        currency TEXT,
        comment TEXT,
        type TEXT, -- 'INCOME' or 'EXPENSE'
        is_excluded BOOLEAN DEFAULT 0
    )
    """)
    
    # 2. Transfers
    c.execute("""
    CREATE TABLE IF NOT EXISTS transfers (
        id TEXT PRIMARY KEY,
        date DATE,
        source_account TEXT,
        target_account TEXT,
        amount REAL,
        comment TEXT
    )
    """)

    # 3. Investments (Portefeuille) - For future use based on your requirements
    c.execute("""
    CREATE TABLE IF NOT EXISTS investments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE,
        ticker TEXT,
        action TEXT, -- BUY / SELL
        quantity REAL,
        unit_price REAL,
        fees REAL,
        account TEXT,
        comment TEXT
    )
    """)
    
    conn.commit()
    conn.close()

def get_db_path():
    return str(DB_PATH.resolve())