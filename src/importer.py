import polars as pl
import uuid
import sqlite3
import fastexcel
from src.database import get_db_path

def ensure_accounts_exist(conn, accounts_set):
    """Crée les comptes dans la table accounts s'ils n'existent pas"""
    cursor = conn.cursor()
    for acc in accounts_set:
        if acc:
            cursor.execute("INSERT OR IGNORE INTO accounts (name, type, initial_balance) VALUES (?, 'CASH', 0.0)", (acc,))

def generate_id():
    return str(uuid.uuid4())

def safe_amount(df, col_name):
    """Gère la conversion safe des montants (str avec virgule ou float)"""
    # Si la colonne n'existe pas (mauvais header), on ne crashe pas ici, 
    # l'erreur sera levée plus bas lors du select.
    if col_name not in df.columns:
        return pl.lit(0.0)

    dtype = df.schema[col_name]
    if dtype == pl.String:
        return pl.col(col_name).str.replace(",", ".").cast(pl.Float64)
    else:
        return pl.col(col_name).cast(pl.Float64)

def safe_date(df, col_name):
    """Gère la conversion safe des dates"""
    if col_name not in df.columns:
        return pl.lit(None)

    dtype = df.schema[col_name]
    if dtype == pl.String:
        return pl.col(col_name).str.strptime(pl.Date, "%d/%m/%Y")
    else:
        return pl.col(col_name).cast(pl.Date)

def process_sheet(df, type_import, conn):
    if df.is_empty():
        return 0

    # --- NETTOYAGE DES COLONNES ---
    # On enlève les espaces invisibles (ex: "Date " -> "Date")
    original_cols = df.columns
    clean_cols = [c.strip() for c in original_cols]
    df.columns = clean_cols
    
    # --- VERIFICATION ---
    required_col = "Date et heure"
    if required_col not in df.columns:
        print(f"\n⚠️ ERREUR DANS LA FEUILLE '{type_import}'")
        print(f"   La ligne d'en-tête (ligne 2) a été lue, voici les colonnes trouvées :")
        print(f"   {df.columns}")
        raise ValueError(f"Colonne '{required_col}' introuvable dans '{type_import}'.")

    cursor = conn.cursor()

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
            row['id'] = generate_id()
            if row['comment'] is None: row['comment'] = ""

        cursor.executemany("""
            INSERT OR IGNORE INTO transactions 
            (id, date, category, account, amount, currency, comment, type, is_excluded)
            VALUES (:id, :date, :category, :account, :amount, :currency, :comment, :type, :is_excluded)
        """, rows)
        
        accounts = set(clean_df["account"].unique().to_list())
        ensure_accounts_exist(conn, accounts)

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
            row['id'] = generate_id()
            if row['comment'] is None: row['comment'] = ""

        cursor.executemany("""
            INSERT OR IGNORE INTO transfers
            (id, date, source_account, target_account, amount, comment)
            VALUES (:id, :date, :source_account, :target_account, :amount, :comment)
        """, rows)
        sources = set(clean_df["source_account"].unique().to_list())
        targets = set(clean_df["target_account"].unique().to_list())
        ensure_accounts_exist(conn, sources.union(targets))
    
    return len(rows)

    
def import_excel_file(file):
    conn = sqlite3.connect(get_db_path())
    stats = {"Revenus": 0, "Dépenses": 0, "Transferts": 0}
    
    try:
        # CORRECTION : Streamlit donne un wrapper, fastexcel veut des bytes bruts.
        file.seek(0)           # On s'assure d'être au début du fichier
        file_bytes = file.read() # On lit tout le fichier en mémoire (bytes)
        
        # On passe les octets bruts à fastexcel
        reader = fastexcel.read_excel(file_bytes)
        
        # 1. REVENUS
        try:
            # header_row=1 : on saute la ligne 0 (Titre), on prend la ligne 1 comme header
            sheet = reader.load_sheet("Revenus", header_row=1)
            df_rev = sheet.to_polars()
            stats["Revenus"] = process_sheet(df_rev, "Revenus", conn)
        except Exception as e:
            # On log l'erreur mais on continue (peut-être que la feuille n'existe pas)
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

  