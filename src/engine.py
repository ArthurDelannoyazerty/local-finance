import polars as pl
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import date, timedelta
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
    
    # Accounts (Soldes initiaux)
    acc_query = "SELECT * FROM accounts"
    df_acc = pl.read_database(acc_query, conn)
    
    conn.close()
    return df_tx, df_tr, df_inv, df_acc

def calculate_wealth_evolution(start_date=None):
    df_tx, df_tr, df_inv, df_acc = get_all_data()
    
    # Si pas de données, retour vide
    if df_tx.is_empty() and df_acc.is_empty():
        return pl.DataFrame()

    # 1. Timeline
    if start_date is None:
        # Trouver la date min globale
        d1 = df_tx["date"].min() if not df_tx.is_empty() else date.today()
        d2 = df_tr["date"].min() if not df_tr.is_empty() else date.today()
        d3 = df_inv["date"].min() if not df_inv.is_empty() else date.today()
        min_date = min(filter(None, [d1, d2, d3])) or date.today()
    else:
        min_date = start_date

    # On crée un calendrier complet jusqu'à aujourd'hui
    calendar = pl.date_range(min_date, date.today(), interval="1d", eager=True).alias("date")
    df_timeline = pl.DataFrame({"date": calendar})

    # 2. Calcul du CASH FLOW par compte
    # On va créer une liste de mouvements : [Date, Account, Amount]
    
    movements = []

    # Soldes Initiaux (On les met à min_date)
    for row in df_acc.rows(named=True):
        movements.append({"date": min_date, "account": row['name'], "amount": row['initial_balance']})

    # Transactions (Income = +, Expense = -)
    if not df_tx.is_empty():
        tx_clean = df_tx.select([
            pl.col("date"),
            pl.col("account"),
            pl.when(pl.col("type") == "EXPENSE").then(pl.col("amount") * -1).otherwise(pl.col("amount")).alias("amount")
        ])
        movements.extend(tx_clean.to_dicts())

    # Transferts (Source = -, Target = +)
    if not df_tr.is_empty():
        tr_out = df_tr.select([pl.col("date"), pl.col("source_account").alias("account"), (pl.col("amount") * -1).alias("amount")])
        tr_in = df_tr.select([pl.col("date"), pl.col("target_account").alias("account"), pl.col("amount")])
        movements.extend(tr_out.to_dicts())
        movements.extend(tr_in.to_dicts())

    # Investissements (Impact Cash)
    # BUY = Cash sort (-), SELL = Cash rentre (+)
    # Montant impact = (Qty * Price) + Fees (si achat, fees augmentent le cout mais réduisent le cash dispo donc -Fees)
    # Simplification: Cash Change = -(Qty * Price) - Fees pour BUY
    # Cash Change = +(Qty * Price) - Fees pour SELL
    if not df_inv.is_empty():
        # BUY
        inv_buy = df_inv.filter(pl.col("action") == "BUY").select([
            pl.col("date"),
            pl.col("account"),
            ((pl.col("quantity") * pl.col("unit_price") * -1) - pl.col("fees")).alias("amount")
        ])
        # SELL
        inv_sell = df_inv.filter(pl.col("action") == "SELL").select([
            pl.col("date"),
            pl.col("account"),
            ((pl.col("quantity") * pl.col("unit_price")) - pl.col("fees")).alias("amount")
        ])
        movements.extend(inv_buy.to_dicts())
        movements.extend(inv_sell.to_dicts())

    # Création du DataFrame Cash Flow
    if not movements:
        return pl.DataFrame()
        
    df_flow = pl.from_dicts(movements)
    
    # Agrégation par jour et par compte
    df_daily_change = df_flow.group_by(["date", "account"]).agg(pl.col("amount").sum()).sort("date")
    
    # Pivot pour avoir les colonnes par compte
    df_pivot_cash = df_daily_change.pivot(index="date", on="account", values="amount").sort("date")
    
    # Jointure avec le calendrier pour boucher les trous
    df_full_cash = df_timeline.join(df_pivot_cash, on="date", how="left").fill_null(0)
    
    # CumSum pour avoir les soldes
    accounts_cols = [c for c in df_full_cash.columns if c != "date"]
    df_balances = df_full_cash.with_columns([
        pl.col(c).cum_sum().alias(c) for c in accounts_cols
    ])

    # 3. Calcul de la VALEUR PORTEFEUILLE (Investments)
    # On a besoin de : Date, Ticker, Quantity Held
    
    if df_inv.is_empty():
        # Si pas d'invest, le total = le cash
        df_balances = df_balances.with_columns(
            pl.sum_horizontal(accounts_cols).alias("Total Wealth")
        )
        return df_balances

    # Calcul quantité détenue par ticker par jour
    # BUY = +Qty, SELL = -Qty
    inv_qty = df_inv.with_columns([
        pl.when(pl.col("action") == "BUY").then(pl.col("quantity")).otherwise(pl.col("quantity") * -1).alias("signed_qty")
    ])
    
    df_qty_daily = inv_qty.group_by(["date", "ticker"]).agg(pl.col("signed_qty").sum()).sort("date")
    df_qty_pivot = df_qty_daily.pivot(index="date", on="ticker", values="signed_qty").sort("date")
    
    # Join timeline et cumsum
    df_full_qty = df_timeline.join(df_qty_pivot, on="date", how="left").fill_null(0)
    tickers = [c for c in df_full_qty.columns if c != "date"]
    
    df_holdings = df_full_qty.with_columns([
        pl.col(t).cum_sum().alias(t) for t in tickers
    ])

    # Récupération Prix Yahoo Finance
    # On convertit en Pandas pour yfinance
    if tickers:
        try:
            # Téléchargement data
            print(f"Téléchargement données marché pour: {tickers}")
            yf_data = yf.download(tickers, start=min_date, end=date.today() + timedelta(days=1), progress=False)['Close']
            
            # Si un seul ticker, yf renvoie une Series, on veut un DF
            if len(tickers) == 1:
                yf_data = pd.DataFrame({tickers[0]: yf_data})
                
            # Nettoyage index timezone
            yf_data.index = yf_data.index.tz_localize(None)
            
            # Reindex sur notre calendrier complet + forward fill (garder prix du vendredi pour le weekend)
            calendar_pd = pd.DatetimeIndex(df_timeline['date'].to_list())
            yf_data = yf_data.reindex(calendar_pd).ffill().bfill() # bfill pour le début si manque data
            
            # Retour en Polars
            df_prices = pl.from_pandas(yf_data.reset_index().rename(columns={"index": "date"}))
            # S'assurer que les dates sont bien Date et pas Datetime
            df_prices = df_prices.with_columns(pl.col("date").cast(pl.Date))

            # Calcul Valeur (Qty * Price)
            # On join holdings et prices
            df_val = df_holdings.join(df_prices, on="date", suffix="_price")
            
            stock_cols = []
            for t in tickers:
                # Si le ticker n'a pas été trouvé par YF, on ignore ou met 0
                if t in df_prices.columns:
                    col_name = f"Invest_{t}"
                    stock_cols.append(col_name)
                    df_val = df_val.with_columns(
                        (pl.col(t) * pl.col(t)).alias(col_name) # Astuce: ici pl.col(t) est la quantité. Attends, il faut le prix.
                    )
                    # Correction logique Polars dynamique
                    # On le fait plus simplement : 
                    # Val = Qty(t) * Price(t)
            
            # Pour faire propre en Polars dynamique :
            exprs = []
            for t in tickers:
                if t in df_prices.columns:
                    exprs.append((pl.col(t) * pl.col(t+"_right")).alias(f"Val_{t}")) # _right vient du join si conflit nom ? Non, suffix
            
            # Refaire le join proprement
            # df_holdings a [date, TickerA, TickerB...] (quantités)
            # df_prices a [date, TickerA, TickerB...] (prix)
            
            df_final_invest = df_holdings.join(df_prices, on="date", suffix="_price")
            
            val_expressions = []
            for t in tickers:
                # Vérifier si on a le prix
                if t in df_prices.columns:
                    # Qty * Price
                    val_expressions.append((pl.col(t) * pl.col(t)).alias(f"Val_{t}")) 
                    # Attention: join fait collision de nom si pas suffix.
            
            # Approche plus simple : Convertir en Pandas pour la multiplication matricielle (plus simple alignement)
            pdf_qty = df_holdings.to_pandas().set_index("date")[tickers]
            pdf_price = df_prices.to_pandas().set_index("date")[tickers]
            
            # Multiplication (alignement sur index et colonnes auto)
            pdf_val = pdf_qty * pdf_price
            pdf_val = pdf_val.fillna(0)
            
            # Ajouter une colonne Total Invest
            pdf_val["Total Invest"] = pdf_val.sum(axis=1)
            
            # Retour vers Polars
            df_invest_res = pl.from_pandas(pdf_val.reset_index()).with_columns(pl.col("date").cast(pl.Date))
            
            # FUSION FINALE : Cash + Invest
            df_total = df_balances.join(df_invest_res.select(["date", "Total Invest"]), on="date", how="left").fill_null(0)
            
            # Total Wealth
            cash_cols = [c for c in df_balances.columns if c != "date"]
            df_total = df_total.with_columns(
                (pl.sum_horizontal(cash_cols) + pl.col("Total Invest")).alias("Total Wealth")
            )
            
            return df_total

        except Exception as e:
            print(f"Erreur YFinance: {e}")
            # Fallback: retourne juste le cash
            return df_balances.with_columns(pl.sum_horizontal(accounts_cols).alias("Total Wealth"))
    
    return df_balances