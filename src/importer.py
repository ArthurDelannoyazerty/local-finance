import hashlib
import sqlite3
from typing import Set, Dict, Any, BinaryIO

import fastexcel
import polars as pl

from src.database import get_db_path


def ensure_accounts_exist(conn: sqlite3.Connection, accounts_set: Set[str]) -> None:
    """
    Creates missing accounts in the 'accounts' table if they do not already exist.
    
    Args:
        conn (sqlite3.Connection): The active SQLite database connection.
        accounts_set (Set[str]): A set of account names to verify/insert.
    """
    cursor = conn.cursor()
    for acc in accounts_set:
        if acc:  # Ensure the account name is not empty or None
            cursor.execute(
                "INSERT OR IGNORE INTO accounts (name, initial_balance) VALUES (?, 0.0)", 
                (acc,)
            )


def generate_deterministic_id(row: Dict[str, Any], type_import: str) -> str:
    """
    Generates a unique MD5 ID based on transaction data to prevent duplicate imports.
    
    Args:
        row (Dict[str, Any]): A dictionary representing a single transaction row.
        type_import (str): The type of the sheet ('Revenus', 'Dépenses', or 'Transferts').
        
    Returns:
        str: A deterministic MD5 hash string.
    """
    if type_import in ['Revenus', 'Dépenses']:
        # Combine fields that make this transaction unique
        unique_string = (
            f"{row.get('date')}_{row.get('account')}_{row.get('amount')}_"
            f"{row.get('category')}_{row.get('comment')}"
        )
    else:
        # Transferts logic
        unique_string = (
            f"{row.get('date')}_{row.get('source_account')}_"
            f"{row.get('target_account')}_{row.get('amount')}"
        )
        
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()


def safe_amount(df: pl.DataFrame, col_name: str) -> pl.Expr:
    """
    Safely handles the conversion of amount columns (handling comma strings or floats).
    
    Args:
        df (pl.DataFrame): The Polars DataFrame being processed.
        col_name (str): The name of the column containing amounts.
        
    Returns:
        pl.Expr: A Polars expression representing the cleaned/cast column.
    """
    if col_name not in df.columns:
        return pl.lit(0.0)

    dtype = df.schema[col_name]
    if dtype == pl.String:
        return pl.col(col_name).str.replace(",", ".").cast(pl.Float64)
    else:
        return pl.col(col_name).cast(pl.Float64)


def safe_date(df: pl.DataFrame, col_name: str) -> pl.Expr:
    """
    Safely handles the conversion of string dates to Polars Date types.
    
    Args:
        df (pl.DataFrame): The Polars DataFrame being processed.
        col_name (str): The name of the column containing dates.
        
    Returns:
        pl.Expr: A Polars expression representing the parsed Date column.
    """
    if col_name not in df.columns:
        return pl.lit(None)

    dtype = df.schema[col_name]
    if dtype == pl.String:
        return pl.col(col_name).str.strptime(pl.Date, "%d/%m/%Y")
    else:
        return pl.col(col_name).cast(pl.Date)


def process_sheet(df: pl.DataFrame, type_import: str, conn: sqlite3.Connection) -> int:
    """
    Cleans, verifies, and inserts a Polars DataFrame into the SQLite database.
    
    Args:
        df (pl.DataFrame): The raw data loaded from the Excel sheet.
        type_import (str): The name of the sheet being processed.
        conn (sqlite3.Connection): The active SQLite connection.
        
    Returns:
        int: The number of rows successfully processed.
    """
    if df.is_empty():
        return 0

    # --- CLEANING COLUMNS ---
    # Remove invisible trailing/leading spaces (e.g., "Date " -> "Date")
    clean_cols = [c.strip() for c in df.columns]
    df.columns = clean_cols
    
    # --- VERIFICATION ---
    required_col = "Date et heure"
    if required_col not in df.columns:
        print(f"\n⚠️ ERREUR DANS LA FEUILLE '{type_import}'")
        print(f"   La ligne d'en-tête (ligne 2) a été lue, voici les colonnes trouvées :")
        print(f"   {df.columns}")
        raise ValueError(f"Colonne '{required_col}' introuvable dans '{type_import}'.")

    cursor = conn.cursor()
    rows_inserted = 0

    if type_import in ['Revenus', 'Dépenses']:
        t_type = 'INCOME' if type_import == 'Revenus' else 'EXPENSE'
        
        clean_df = df.select([
            safe_date(df, "Date et heure").alias("date"),
            pl.col("Catégorie").alias("category"),
            pl.col("Compte").alias("account"),
            safe_amount(df, "Montant dans la devise par défaut").alias("amount"),
            pl.col("Devise par défaut").alias("currency"),
            pl.col("Commentaire").alias("comment")
        ]).with_columns([
            pl.lit(t_type).alias("type"),
            pl.lit(0).alias("is_excluded"),
        ])

        rows = clean_df.to_dicts()
        for row in rows:
            row['id'] = generate_deterministic_id(row, type_import)
            if row['comment'] is None: 
                row['comment'] = ""

        cursor.executemany("""
            INSERT OR IGNORE INTO transactions 
            (id, date, category, account, amount, currency, comment, type, is_excluded)
            VALUES (:id, :date, :category, :account, :amount, :currency, :comment, :type, :is_excluded)
        """, rows)
        
        # Ensure related accounts exist
        accounts = set(clean_df["account"].unique().to_list())
        ensure_accounts_exist(conn, accounts)
        
        rows_inserted = len(rows)

    elif type_import == 'Transferts':
        clean_df = df.select([
            safe_date(df, "Date et heure").alias("date"),
            pl.col("Sortantes").alias("source_account"),
            pl.col("Entrantes").alias("target_account"),
            safe_amount(df, "Montant en devise sortante").alias("amount"),
            pl.col("Commentaire").alias("comment")
        ])

        rows = clean_df.to_dicts()
        for row in rows:
            row['id'] = generate_deterministic_id(row, type_import)
            if row['comment'] is None: 
                row['comment'] = ""

        cursor.executemany("""
            INSERT OR IGNORE INTO transfers
            (id, date, source_account, target_account, amount, comment)
            VALUES (:id, :date, :source_account, :target_account, :amount, :comment)
        """, rows)
        
        # Ensure related accounts exist
        sources = set(clean_df["source_account"].unique().to_list())
        targets = set(clean_df["target_account"].unique().to_list())
        ensure_accounts_exist(conn, sources.union(targets))
        
        rows_inserted = len(rows)
    
    return rows_inserted

    
def import_excel_file(file: BinaryIO) -> Dict[str, int]:
    """
    Reads an uploaded Excel file in memory and processes its specific sheets.
    
    Args:
        file (BinaryIO): A file-like object (e.g., Streamlit UploadedFile) containing Excel bytes.
        
    Returns:
        Dict[str, int]: A dictionary mapping sheet names to the number of rows inserted.
    """
    conn = sqlite3.connect(get_db_path())
    stats: Dict[str, int] = {"Revenus": 0, "Dépenses": 0, "Transferts": 0}
    
    try:
        # Streamlit provides a wrapper, fastexcel requires raw bytes.
        file.seek(0)             # Ensure we're at the beginning of the file
        file_bytes = file.read() # Read the whole file into memory (bytes)
        
        # Pass the raw bytes to fastexcel
        reader = fastexcel.read_excel(file_bytes)
        
        # 1. REVENUS
        try:
            # header_row=1 : skip row 0 (Title), take row 1 as header
            sheet = reader.load_sheet("Revenus", header_row=1)
            df_rev = sheet.to_polars()
            stats["Revenus"] = process_sheet(df_rev, "Revenus", conn)
        except Exception as e:
            # Log the error but continue (the sheet might just not exist)
            print(f"Info import Revenus: {e}")

        # 2. DEPENSES
        try:
            sheet = reader.load_sheet("Dépenses", header_row=1)
            df_dep = sheet.to_polars()
            stats["Dépenses"] = process_sheet(df_dep, "Dépenses", conn)
        except Exception as e:
            print(f"Info import Dépenses: {e}")

        # 3. TRANSFERTS
        try:
            sheet = reader.load_sheet("Transferts", header_row=1)
            df_trans = sheet.to_polars()
            stats["Transferts"] = process_sheet(df_trans, "Transferts", conn)
        except Exception as e:
            print(f"Info import Transferts: {e}")
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
        
    return stats