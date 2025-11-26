import sqlite3
from pathlib import Path

DB_PATH = Path("data/finance.db")

def get_db_path():
    return str(DB_PATH.resolve())

def sync_accounts_from_history():
    """
    Scanne l'historique pour créer les comptes manquants.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    queries = [
        "SELECT DISTINCT account FROM transactions WHERE account IS NOT NULL AND account != ''",
        "SELECT DISTINCT source_account FROM transfers WHERE source_account IS NOT NULL AND source_account != ''",
        "SELECT DISTINCT target_account FROM transfers WHERE target_account IS NOT NULL AND target_account != ''",
        "SELECT DISTINCT account FROM investments WHERE account IS NOT NULL AND account != ''"
    ]

    found_accounts = set()
    for q in queries:
        try:
            rows = c.execute(q).fetchall()
            for r in rows:
                if r[0]:
                    found_accounts.add(r[0])
        except Exception:
            pass

    count = 0
    for acc in found_accounts:
        # MODIFICATION : On insère uniquement name et initial_balance
        c.execute("INSERT OR IGNORE INTO accounts (name, initial_balance) VALUES (?, 0.0)", (acc,))
        if c.rowcount > 0:
            count += 1
            
    conn.commit()
    conn.close()
    
    if count > 0:
        print(f"✅ Base de données : {count} comptes récupérés et ajoutés.")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # MODIFICATION : Suppression de la colonne 'type'
    c.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        name TEXT PRIMARY KEY,
        initial_balance REAL DEFAULT 0.0
    )
    """)

    c.execute("""
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

    c.execute("""
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
    
    conn.commit()
    conn.close()
    sync_accounts_from_history()