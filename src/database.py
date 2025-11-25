import sqlite3
from pathlib import Path

DB_PATH = Path("data/finance.db")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. Accounts (Pour définir le solde de départ, ex: Livret A a 10k€ au début)
    c.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        name TEXT PRIMARY KEY,
        type TEXT, -- 'CASH', 'INVEST'
        initial_balance REAL DEFAULT 0.0
    )
    """)

    # 2. Transactions (Revenus & Dépenses)
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
    
    # 3. Transfers
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

    # 4. Investments (Portefeuille)
    # Mise à jour avec tes champs demandés
    c.execute("""
    CREATE TABLE IF NOT EXISTS investments (
        id TEXT PRIMARY KEY,
        date DATE,
        ticker TEXT,
        name TEXT, -- Titre du produit
        action TEXT, -- 'BUY' or 'SELL'
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