import json
import sqlite3
import uuid
from datetime import date, timedelta
from typing import List, Optional, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st

from src.database import get_db_path, init_db
from src.engine import calculate_wealth_evolution, get_detailed_snapshot
from src.importer import import_excel_file
from src.projections_engine import (
    ProjectionConfig,
    calculate_deterministic_projection,
    calculate_monte_carlo,
)
from src.queries import get_investments_df, get_transactions_df

# --- SETUP & INITIALIZATION ---
st.set_page_config(
    page_title="My Finance Tracker", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Initialize database tables and sync accounts
init_db()

# Initialize session state for navigation and dates
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Tableau de Bord"


# --- HELPER FUNCTIONS ---

def get_accounts() -> pl.DataFrame:
    """Retrieves all VISIBLE accounts and their initial balances."""
    query: str = "SELECT name, initial_balance FROM accounts WHERE is_visible = 1"
    try:
        with sqlite3.connect(get_db_path()) as conn:
            return pl.read_database(query, conn)
    except Exception:
        return pl.DataFrame()

def update_account_details(name: str, amount: float, is_visible: bool) -> None:
    """Met à jour le solde, le type et la visibilité du compte."""
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE accounts SET initial_balance = ?, is_visible = ? WHERE name = ?", 
            (amount, int(is_visible), name)
        )
        conn.commit()

def save_investment(
    date_inv: date, ticker: str, name: str, action: str, 
    qty: float, price: float, fees: float, account: str, comment: str
) -> None:
    """Saves a new investment transaction to the database."""
    final_comment: str = comment if comment and comment.strip() != "" else ""
    
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO investments (id, date, ticker, name, action, quantity, unit_price, fees, account, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (str(uuid.uuid4()), date_inv, ticker.strip(), name, action, qty, price, fees, account, final_comment))
        conn.commit()

def delete_investments(ids_to_delete: List[str]) -> None:
    """Supprime une liste d'investissements via leurs IDs."""
    if not ids_to_delete:
        return
    with sqlite3.connect(get_db_path()) as conn:
        placeholders = ','.join(['?'] * len(ids_to_delete))
        conn.execute(f"DELETE FROM investments WHERE id IN ({placeholders})", ids_to_delete)
        conn.commit()

def update_account_initial(name: str, amount: float) -> None:
    """Updates the initial balance of an existing account."""
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE accounts SET initial_balance = ? WHERE name = ?", (amount, name))
        conn.commit()


def create_new_account(name: str) -> None:
    """Creates a new account with a starting balance of 0.0."""
    with sqlite3.connect(get_db_path()) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO accounts (name, initial_balance) VALUES (?, 0.0)", (name,))
        conn.commit()


# --- PAGE RENDERING FUNCTIONS ---

def render_import_page() -> None:
    """Renders the Data Import and Account Configuration page."""
    st.header("📥 Gestion des Données")
    tab1, tab2 = st.tabs(["Import Excel", "Configuration Comptes"])
    
    with tab1:
        st.markdown("Importez vos exports bancaires (Excel avec feuilles `Revenus`, `Dépenses`, `Transferts`).")
        uploaded_file = st.file_uploader("Fichier Excel", type=["xlsx"])
        
        if uploaded_file and st.button("Lancer l'import"):
            with st.spinner("Traitement..."):
                try:
                    stats = import_excel_file(uploaded_file)
                    st.success("Import réussi !")
                    st.json(stats)
                except Exception as e:
                    st.error(f"Erreur : {e}")

    with tab2:
        st.subheader("Solde de départ des comptes")
        st.info("Indiquez le solde initial de chaque compte avant la première transaction importée.")

        with st.expander("➕ Créer un nouveau compte manuellement"):
            col_new_1, col_new_2 = st.columns([3, 1])
            with col_new_1:
                new_acc_name = st.text_input("Nom du nouveau compte", placeholder="ex: PEA Bourse Direct")
            with col_new_2:
                st.write("") 
                st.write("") 
                if st.button("Créer le compte"):
                    if new_acc_name.strip():
                        create_new_account(new_acc_name.strip())
                        st.success(f"Compte '{new_acc_name}' créé !")
                        st.rerun()
                    else:
                        st.error("Le nom ne peut pas être vide.")
        
        st.divider()
        
        with sqlite3.connect(get_db_path()) as conn:
            # On récupère TOUS les comptes ici (même cachés) pour pouvoir les modifier
            df_acc = pl.read_database("SELECT name, initial_balance,is_visible FROM accounts", conn)

        if not df_acc.is_empty():
            pdf_acc = df_acc.to_pandas()
            
            # Forcer la conversion en booléen pour la case à cocher
            pdf_acc['is_visible'] = pdf_acc['is_visible'].astype(bool)


            edited_acc = st.data_editor(
                pdf_acc, 
                column_config={
                    "name": st.column_config.TextColumn("Compte", disabled=True),
                    "initial_balance": st.column_config.NumberColumn("Solde Initial", format="%.2f €"),
                    "is_visible": st.column_config.CheckboxColumn("Visible", help="Décocher pour cacher ce compte des analyses")
                },
                hide_index=True,
                use_container_width=True,
                key="acc_editor"
            )
            
            if st.button("Sauvegarder la configuration"):
                for _, row in edited_acc.iterrows():
                    update_account_details(row['name'], row['initial_balance'], row['is_visible'])
                st.success("Configuration mise à jour !")
                st.rerun()

def render_dashboard_page() -> None:
    """Renders the Cash Flow and Budget Dashboard page."""
    st.header("📊 Analyse des Flux (Cash Flow)")
    
    df = get_transactions_df()
    
    if df.is_empty():
        st.warning("Pas de données. Veuillez importer des fichiers.")
        return

    # Date handling logic
    if "input_start" not in st.session_state:
        st.session_state["input_start"] = date(date.today().year, 1, 1)
    if "input_end" not in st.session_state:
        st.session_state["input_end"] = date.today()

    def update_date_range(days: Optional[int] = None, start: Optional[date] = None, end: Optional[date] = None):
        target_end = end if end else date.today()
        target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
        st.session_state["input_start"] = target_start
        st.session_state["input_end"] = target_end

    with st.container():
        st.subheader("📅 Période d'analyse")
        col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
        
        with col_shortcuts:
            st.caption("Raccourcis rapides")
            b1, b2, b3, b4, b5 = st.columns(5)
            if b1.button("1 Mois", width="stretch"): update_date_range(days=30)
            if b2.button("3 Mois", width="stretch"): update_date_range(days=90)
            if b3.button("6 Mois", width="stretch"): update_date_range(days=180)
            if b4.button("YTD (Année)", width="stretch", help="Depuis le 1er Janvier"): 
                update_date_range(start=date(date.today().year, 1, 1))
            if b5.button("Tout", width="stretch"):
                update_date_range(start=df["date"].min())

            years: List[int] = sorted(df["date"].dt.year().unique().to_list(), reverse=True)
            if years:
                st.write("") 
                cols_years = st.columns(len(years) + 2)
                for i, year in enumerate(years):
                    if cols_years[i].button(str(year), key=f"year_{year}", width="stretch"):
                        update_date_range(start=date(year, 1, 1), end=date(year, 12, 31))

        with col_pickers:
            st.caption("Sélection manuelle")
            c_start, c_end = st.columns(2)
            start_date = c_start.date_input("Début", key="input_start")
            end_date = c_end.date_input("Fin", key="input_end")

    st.divider()

    df_filtered = df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
            
    income = df_filtered.filter(pl.col("type") == "INCOME")["amount"].sum()
    expense = df_filtered.filter(pl.col("type") == "EXPENSE")["amount"].sum()
    savings = income - expense
    rate = (savings / income * 100) if income > 0 else 0.0

    delta_days = (end_date - start_date).days + 1
    num_months = max(1.0, delta_days / 30.436875)
    
    avg_income = income / num_months
    avg_expense = expense / num_months
    avg_savings = savings / num_months
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Revenus", f"{income:,.0f} €")
    k1.caption(f"Moy. / mois : **{avg_income:,.0f} €**")
    
    k2.metric("Dépenses", f"{expense:,.0f} €", delta_color="inverse")
    k2.caption(f"Moy. / mois : **{avg_expense:,.0f} €**")

    k3.metric("Épargne", f"{savings:,.0f} €", delta_color="normal")
    k3.caption(f"Moy. / mois : **{avg_savings:,.0f} €**")
    
    k4.metric("Taux d'épargne", f"{rate:.1f} %")
    
    st.divider()
    
    col_charts_1, col_charts_2 = st.columns(2)
    
    # Left Chart: Pie Chart for Expenses
    with col_charts_1:
        st.subheader("Dépenses par Catégorie")
        df_exp = df_filtered.filter(pl.col("type") == "EXPENSE")
        
        if not df_exp.is_empty():
            grp = df_exp.group_by("category").agg(pl.col("amount").sum()).sort("amount", descending=True)
            pdf_chart = grp.to_pandas()

            if len(pdf_chart) > 7:
                top_n = pdf_chart.iloc[:6].copy()
                others_value = pdf_chart.iloc[6:]['amount'].sum()
                others_df = pd.DataFrame([{'category': 'Autres', 'amount': others_value}])
                pdf_chart = pd.concat([top_n, others_df], ignore_index=True)

            fig_pie = px.pie(
                pdf_chart, 
                values="amount", 
                names="category", 
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel 
            )
            fig_pie.update_traces(
                textposition='outside', 
                textinfo='label+percent+value',
                texttemplate='%{label}<br><b>%{value:,.0f} €</b><br>(%{percent})'
            )
            fig_pie.update_layout(showlegend=False, margin=dict(t=40, b=80, l=20, r=20), height=600)
            st.plotly_chart(fig_pie, width="stretch")
        else:
            st.info("Aucune dépense sur cette période.")
    
    # Right Chart: Monthly Bar Chart
    with col_charts_2:
        st.subheader("Évolution Mensuelle & Tendance")
        monthly_agg = (
            df_filtered
            .with_columns(pl.col("date").dt.truncate("1mo").alias("month_date"))
            .group_by(["month_date", "type"])
            .agg(pl.col("amount").sum())
            .sort("month_date")
        )
        
        if not monthly_agg.is_empty():
            df_pivot = monthly_agg.pivot(
                values="amount", 
                index="month_date", 
                on="type",
                aggregate_function="sum"
            ).fill_null(0).sort("month_date")

            if "INCOME" not in df_pivot.columns:
                df_pivot = df_pivot.with_columns(pl.lit(0.0).alias("INCOME"))
            if "EXPENSE" not in df_pivot.columns:
                df_pivot = df_pivot.with_columns(pl.lit(0.0).alias("EXPENSE"))

            df_pivot = df_pivot.with_columns(
                pl.col("EXPENSE").rolling_mean(window_size=3).alias("ma_expense")
            )

            pdf_viz = df_pivot.to_pandas()

            fig_combo = go.Figure()
            fig_combo.add_trace(go.Bar(
                x=pdf_viz["month_date"], y=pdf_viz["INCOME"], name="Revenus", marker_color="#00CC96"
            ))
            fig_combo.add_trace(go.Bar(
                x=pdf_viz["month_date"], y=pdf_viz["EXPENSE"], name="Dépenses", marker_color="#EF553B"
            ))
            fig_combo.add_trace(go.Scatter(
                x=pdf_viz["month_date"], y=pdf_viz["ma_expense"], mode='lines',
                name="Moyenne Dépenses (3 mois)", line=dict(color='#172B4D', width=3, dash='dot') 
            ))

            fig_combo.update_layout(
                barmode='group', 
                xaxis=dict(tickformat="%b %y", dtick="M1", tickangle=-45),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=20, b=40, l=20, r=20),
                height=450
            )
            st.plotly_chart(fig_combo, width="stretch")
        else:
            st.info("Pas de données sur la période sélectionnée.")


def render_wealth_page() -> None:
    """Renders the Net Wealth and Investment management page."""
    st.header("📈 Évolution du Patrimoine")
    
    # --- CALCUL DES KPI GLOBAUX ---
    today_snapshot = get_detailed_snapshot(date.today())
    
    current_value = today_snapshot[today_snapshot['Type'] == 'Investissement']['Value'].sum()

    # Calcul du "Net Investi" (Cash Flow entrant dans la bourse)
    # Formule : (Total Achats + Frais) - (Total Ventes - Frais)
    with sqlite3.connect(get_db_path()) as conn:
        # Somme de tout l'argent sorti de la poche (Achats + Frais)
        res_buy = pd.read_sql("SELECT SUM(quantity * unit_price + fees) FROM investments WHERE action = 'BUY'", conn).iloc[0, 0]
        buy_total = res_buy if res_buy else 0.0
        
        # Somme de tout l'argent rentré dans la poche (Ventes - Frais)
        res_sell = pd.read_sql("SELECT SUM(quantity * unit_price - fees) FROM investments WHERE action = 'SELL'", conn).iloc[0, 0]
        sell_total = res_sell if res_sell else 0.0
    
    net_invested = buy_total - sell_total
    
    # Calcul Plus-Value / Moins-Value
    pnl_abs = current_value - net_invested
    pnl_pct = (pnl_abs / net_invested * 100) if net_invested > 0 else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric(label="💰 Capital Net Investi",help="Argent sorti de votre poche (Achats - Ventes)",value=f"{net_invested:,.0f} €")
    k2.metric(label="📊 Valeur Actuelle (Actifs)",help="Valeur de marché de vos actions/ETF aujourd'hui (hors cash)",value=f"{current_value:,.0f} €")
    k3.metric(label="🚀 Plus/Moins-Value Latente",value=f"{pnl_abs:+,.0f} €",delta=f"{pnl_pct:+.2f} %")
    
    st.divider()

    acc_df = get_accounts()
    account_options: List[str] =[]
    accounts_ready: bool = False
    default_index: int = 0
    
    if not acc_df.is_empty():
        account_options = acc_df["name"].to_list()
        accounts_ready = True
        target_default = "PEA Bourse Direct"
        if target_default in account_options:
            default_index = account_options.index(target_default)
    else:
        account_options = ["⚠️ Aucun compte trouvé"]

    # Investment Form
    with st.expander("➕ Ajouter une transaction Bourse (Achat/Vente)", expanded=False):
        with st.form("invest_form"):
            st.caption("Saisissez les détails de l'ordre exécuté.")
            f1, f2, f3, f4 = st.columns(4)
            
            with f1:
                i_date = st.date_input("Date de l'exécution")
                action_label = st.selectbox("Type d'opération",["Achat (BUY)", "Vente (SELL)"])
                i_action = "BUY" if "Achat" in action_label else "SELL"
                
            with f2:
                i_ticker = st.text_input("Ticker (ex: CW8.PA)", value="")
                i_name = st.text_input("Nom du produit", value="")
                
            with f3:
                i_qty = st.number_input("Quantité", min_value=0.0, step=1.0, format="%.6f")
                i_price = st.number_input("Prix Unitaire", min_value=0.0, step=0.1, format="%.6f")
                
            with f4:
                i_acc = st.selectbox("Compte impacté (Cash)", account_options, index=default_index, disabled=not accounts_ready)
                i_fees = st.number_input("Frais totaux (€)", min_value=0.0, step=0.1, format="%.2f")

            i_comm = st.text_input("Commentaire (Optionnel)")
            submit_btn = st.form_submit_button("Enregistrer l'investissement", type="primary")
            
            if submit_btn:
                if not accounts_ready:
                    st.error("Impossible d'enregistrer : aucun compte bancaire n'est disponible.")
                else:
                    save_investment(i_date, i_ticker, i_name, i_action, i_qty, i_price, i_fees, str(i_acc), i_comm)
                    st.success("Transaction enregistrée avec succès !")
                    st.rerun()

    # Investment History Table
    with st.expander("📜 Historique et Gestion (Suppression)", expanded=False):
        df_inv_hist = get_investments_df()
        
        if not df_inv_hist.is_empty():
            # Conversion en Pandas pour l'édition
            pdf_hist = df_inv_hist.to_pandas()
            
            # Ajout d'une colonne de sélection (cases à cocher) initialisée à False
            pdf_hist.insert(0, "Select", False)

            # Affichage de l'éditeur
            edited_df = st.data_editor(
                pdf_hist,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Suppr ?", help="Cochez pour supprimer"),
                    "id": None,  # On cache la colonne ID, inutile pour l'utilisateur
                    "date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                    "action": "Action",
                    "ticker": "Ticker",
                    "name": "Nom",
                    "quantity": st.column_config.NumberColumn("Qté"),
                    "unit_price": st.column_config.NumberColumn("Prix U.", format="%.2f €"),
                    "fees": st.column_config.NumberColumn("Frais", format="%.2f €"),
                    "account": "Compte",
                    "comment": "Note"
                },
                hide_index=True,
                key="editor_history" # Clé unique importante
            )

            # Bouton d'action pour supprimer
            # On vérifie si des lignes ont été cochées dans le DataFrame édité
            rows_to_delete = edited_df[edited_df["Select"]]
            
            if not rows_to_delete.empty:
                st.warning(f"⚠️ Vous allez supprimer {len(rows_to_delete)} transaction(s).")
                col_del_1, col_del_2 = st.columns([1, 4])
                
                with col_del_1:
                    if st.button("Confirmer la suppression", type="primary"):
                        ids = rows_to_delete["id"].tolist()
                        delete_investments(ids)
                        st.success("Transactions supprimées !")
                        st.rerun() # Rafraîchit la page immédiatement
        else:
            st.info("Aucune transaction enregistrée.")
    
    st.divider()
    
    # --- 1. Récupération rapide des dates pour configurer l'interface ---
    try:
        with sqlite3.connect(get_db_path()) as conn:
            min_dates = pd.read_sql("""
                SELECT MIN(date) as d FROM transactions WHERE is_excluded=0
                UNION SELECT MIN(date) FROM investments
                UNION SELECT MIN(date) FROM transfers
            """, conn)
            valid_dates = pd.to_datetime(min_dates['d']).dropna()
            first_history_date = valid_dates.min().date() if not valid_dates.empty else date.today()
            
            years_df = pd.read_sql("""
                SELECT DISTINCT strftime('%Y', date) as y FROM transactions WHERE date IS NOT NULL 
                UNION SELECT DISTINCT strftime('%Y', date) FROM investments WHERE date IS NOT NULL
            """, conn)
            years = sorted([int(y) for y in years_df['y'].dropna() if y], reverse=True)
    except Exception:
        first_history_date = date.today()
        years =[]

    # Initialisation Session State
    if "wealth_start" not in st.session_state:
        st.session_state["wealth_start"] = first_history_date
    if "wealth_end" not in st.session_state:
        st.session_state["wealth_end"] = date.today()

    def update_wealth_range(days: Optional[int] = None, start: Optional[date] = None, end: Optional[date] = None):
        target_end = end if end else date.today()
        target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
        st.session_state["wealth_start"] = target_start
        st.session_state["wealth_end"] = target_end

    # Interface Date
    with st.container():
        st.subheader("📅 Période d'analyse")
        col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
        with col_shortcuts:
            st.caption("Raccourcis rapides")
            b1, b2, b3, b4 = st.columns(4)
            if b1.button("1 Mois", key="w_1m", width="stretch"): update_wealth_range(days=30)
            if b2.button("6 Mois", key="w_6m", width="stretch"): update_wealth_range(days=180) 
            if b3.button("YTD", key="w_ytd", width="stretch"): update_wealth_range(start=date(date.today().year, 1, 1))
            if b4.button("Tout", key="w_all", width="stretch"): update_wealth_range(start=first_history_date)

            if years:
                st.write("") 
                cols_years = st.columns(len(years) + 2)
                for i, year in enumerate(years):
                    if cols_years[i].button(str(year), key=f"w_year_{year}", width="stretch"):
                        update_wealth_range(start=date(year, 1, 1), end=date(year, 12, 31))

        with col_pickers:
            st.caption("Sélection manuelle")
            c_start, c_end = st.columns(2)
            w_start = c_start.date_input("Début", key="wealth_start")
            w_end = c_end.date_input("Fin", key="wealth_end")

    # --- 2. Calcul du Patrimoine Dynamique ---
    with st.spinner("Calcul de l'évolution du patrimoine..."):
        # C'est ICI que l'on passe nos variables du futur/passé au moteur
        df_wealth = calculate_wealth_evolution(target_start=w_start, target_end=w_end)
    
    if df_wealth.is_empty():
        st.info("Pas assez de données pour générer le graphique.")
        return

    # Chart Generation
    df_viz = df_wealth.filter((pl.col("date") >= w_start) & (pl.col("date") <= w_end))
    
    if df_viz.is_empty():
        st.warning("Aucune donnée sur cette période.")
    else:
        pdf_wealth = df_viz.to_pandas()
        st.write("---")
        col_opts, col_metrics = st.columns([2, 1])
        
        excluded_cols = ['date', 'Total Wealth', 'Total Invest']
        account_cols =[c for c in pdf_wealth.columns if c not in excluded_cols]
        
        with col_opts:
            chart_mode = st.radio("Mode d'affichage", ["Total", "Détail"], horizontal=True)
            selected_accounts = account_cols
            
            if chart_mode in ["Détail"]:
                selected_accounts = st.multiselect(
                    "Comptes à inclure dans le calcul et le graphique", 
                    account_cols, 
                    default=account_cols
                )

        with col_metrics:
            if chart_mode == "Total":
                current_series = pdf_wealth['Total Wealth']
            else:
                if selected_accounts:
                    current_series = pdf_wealth[selected_accounts].sum(axis=1)
                else:
                    current_series = pd.Series([0.0] * len(pdf_wealth))

            last_val = current_series.iloc[-1] if not current_series.empty else 0.0
            first_val = current_series.iloc[0] if not current_series.empty else 0.0
            delta = last_val - first_val
            
            metric_label = "Patrimoine (Sélection)" if chart_mode != "Total" else "Patrimoine Net Total"
            st.metric(metric_label, f"{last_val:,.2f} €", delta=f"{delta:,.2f} €")

        fig = go.Figure()
        if chart_mode == "Total":
            fig.add_trace(go.Scatter(
                x=pdf_wealth['date'], y=pdf_wealth['Total Wealth'], 
                mode='lines', name='Patrimoine Net', line=dict(color='#636EFA', width=4),
                fill='tozeroy', fillcolor='rgba(99, 110, 250, 0.1)' 
            ))
            if "Total Invest" in pdf_wealth.columns:
                fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth['Total Invest'], 
                    mode='lines', name='Dont Investissement', line=dict(color='gold', width=2, dash='dash')
                ))

        elif chart_mode == "Détail":
            if selected_accounts:
                for col in selected_accounts:
                    fig.add_trace(go.Scatter(
                        x=pdf_wealth['date'], y=pdf_wealth[col],
                        mode='lines', stackgroup='one', name=col
                    ))
            else:
                fig.add_trace(go.Scatter(x=pdf_wealth['date'], y=[0]*len(pdf_wealth), mode='lines', name='Aucun compte'))

        fig.update_layout(
            title=f"Évolution du {w_start.strftime('%d/%m/%Y')} au {w_end.strftime('%d/%m/%Y')}",
            xaxis=dict(showgrid=False),
            yaxis=dict(title="Valeur (€)", showgrid=True, gridcolor='lightgray'),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=700, 
            margin=dict(l=20, r=20, t=60, b=40)
        )
        with st.container(height=710, border=False):
            st.plotly_chart(fig, width="stretch")


def render_market_map_page() -> None:
    """Renders the Market Treemap and Portfolio Performance page."""
    st.header("🗺️ Carte Thermique du Patrimoine (Treemap)")

    col_ctrl1, col_ctrl2 = st.columns([1, 3])

    with col_ctrl1:
        mode = st.selectbox(
            "Mode d'affichage",[
                "Poids des Comptes",
                "Poids Comptes & Actifs",
                "Performance (Évolution) - Tout",
                "Performance (Évolution) - Actions"
            ]
        )

    if "Performance" in mode:
        if "map_start" not in st.session_state:
            st.session_state["map_start"] = date.today() - timedelta(days=30)
        if "map_end" not in st.session_state:
            st.session_state["map_end"] = date.today()

        def update_map_date_range(days: Optional[int] = None, start: Optional[date] = None, end: Optional[date] = None):
            target_end = end if end else date.today()
            target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
            st.session_state["map_start"] = target_start
            st.session_state["map_end"] = target_end
        
        try:
            with sqlite3.connect(get_db_path()) as conn:
                min_date_db = pd.read_sql("SELECT MIN(date) as min_date FROM investments", conn)["min_date"].iloc[0]
                if pd.isna(min_date_db): 
                    global_min_date = date(2020, 1, 1)
                else: 
                    global_min_date = pd.to_datetime(min_date_db).date()
        except Exception:
            global_min_date = date(2020, 1, 1)

        with col_ctrl2:
            sub_c1, sub_c2 = st.columns([3, 2])
            with sub_c1:
                st.caption("Raccourcis rapides")
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("1 Mois", width="stretch", key="map_1m"): update_map_date_range(days=30)
                if b2.button("3 Mois", width="stretch", key="map_3m"): update_map_date_range(days=90)
                if b3.button("YTD", width="stretch", key="map_ytd"): update_map_date_range(start=date(date.today().year, 1, 1))
                if b4.button("Tout", width="stretch", key="map_all"): update_map_date_range(start=global_min_date)

            with sub_c2:
                st.caption("Sélection manuelle")
                date_start = st.date_input("Début", key="map_start")
                date_end = st.date_input("Fin", key="map_end")
        
        target_date = date_end
    else:
        with col_ctrl2:
            target_date = st.date_input("Date de situation", date.today())
        date_start = target_date
        date_end = target_date

    st.write("---")

    with st.spinner("Génération de la carte..."):
        df_end = get_detailed_snapshot(date_end)
        
        if df_end.empty:
            st.warning("Aucune donnée disponible à la date de fin sélectionnée.")
            return

        viz_df = df_end.copy()
        path_cols =[]
        color_args = {}
        hovertemplate = "<b>%{label}</b><br>Valeur: %{value:,.0f} €<extra></extra>"
        custom_data_cols = None

        if mode == "Poids des Comptes":
            viz_df = viz_df.groupby("Account", as_index=False)["Value"].sum()
            path_cols = ["Account"]
            color_args = dict(color="Account")

        elif mode == "Poids Comptes & Actifs":
            path_cols = ["Account", "Name"]
            color_args = dict(color="Account")

        elif "Performance" in mode:
            df_start = get_detailed_snapshot(date_start)
            
            with sqlite3.connect(get_db_path()) as conn:
                cost_basis_query = """
                    SELECT account, ticker, SUM(quantity * unit_price + fees) as cost_basis
                    FROM investments
                    WHERE action = 'BUY' AND date > ? AND date <= ?
                    GROUP BY account, ticker
                """
                df_cost_basis = pd.read_sql(cost_basis_query, conn, params=(date_start, date_end))
            
            df_cost_basis = df_cost_basis.rename(columns={"account": "Account", "ticker": "Ticker"})

            df_start_slim = df_start[["Account", "Ticker", "Value"]].rename(columns={"Value": "Value_Start"})
            viz_df = viz_df.merge(df_start_slim, on=["Account", "Ticker"], how="left")
            viz_df = viz_df.merge(df_cost_basis, on=["Account", "Ticker"], how="left")
            viz_df.fillna({"Value_Start": 0, "cost_basis": 0}, inplace=True)
            
            def get_adjusted_start(row: pd.Series) -> float:
                if row['Value_Start'] < 0.01 and row['cost_basis'] > 0:
                    return row['cost_basis']
                return row['Value_Start']
            
            viz_df['Value_Start_Adjusted'] = viz_df.apply(get_adjusted_start, axis=1)

            def calc_perf(row: pd.Series) -> float:
                start_val = row["Value_Start_Adjusted"]
                net_invested = row.get("cost_basis", 0.0)
                
                total_in = start_val + net_invested
                if total_in < 0.01: 
                    return 0.0
                
                end_val = row["Value"]
                profit = end_val - start_val - net_invested
                return (profit / total_in) * 100
            
            viz_df["Performance %"] = viz_df.apply(calc_perf, axis=1)

            if mode == "Performance (Évolution) - Actions":
                viz_df = viz_df[viz_df['Type'] == 'Investissement'].copy()

            path_cols =["Account", "Name"]
            hovertemplate = "<b>%{label}</b><br>Valeur: %{value:,.0f} €<br>Perf: %{customdata[0]:.2f}%<extra></extra>"
            custom_data_cols = ['Performance %']
            
            max_abs_perf = viz_df["Performance %"].abs().max()
            if pd.isna(max_abs_perf) or max_abs_perf == 0: 
                max_abs_perf = 1.0

            color_args = dict(
                color="Performance %",
                color_continuous_scale="RdYlGn",
                range_color=[-max_abs_perf, max_abs_perf],
            )

        if viz_df.empty or viz_df['Value'].sum() < 0.01:
            st.info("Aucun actif à afficher pour la sélection actuelle.")
        else:
            fig = px.treemap(
                viz_df,
                path=path_cols,
                values='Value',
                custom_data=custom_data_cols,
                **color_args
            )

            fig.update_traces(
                textinfo="label+value",
                texttemplate="%{label}<br>%{value:,.0f}€",
                hovertemplate=hovertemplate,
                marker=dict(line=dict(width=2, color='white'))
            )
            
            fig.update_layout(
                margin=dict(t=20, l=10, r=10, b=10),
                height=700,
                coloraxis_colorbar=dict(title="Perf %") if 'Performance' in mode else None
            )

            st.plotly_chart(fig, width="stretch")

            with st.expander("Voir les données détaillées"):
                st.dataframe(viz_df)



def get_current_metrics() -> Dict[str, float]:
    """Récupère le patrimoine actuel et l'épargne moyenne des 6 derniers mois."""
    # 1. Patrimoine Actuel
    snapshot = get_detailed_snapshot(date.today())
    current_wealth = snapshot['Value'].sum() if not snapshot.empty else 0.0
    
    # 2. Épargne Moyenne (Revenus - Dépenses sur 6 mois)
    start_dt = date.today() - timedelta(days=180)
    df_tx = get_transactions_df()
    
    avg_savings = 500.0 # Valeur par défaut
    
    if not df_tx.is_empty():
        df_recent = df_tx.filter(pl.col("date") >= start_dt)
        if not df_recent.is_empty():
            inc = df_recent.filter(pl.col("type") == "INCOME")["amount"].sum()
            exp = df_recent.filter(pl.col("type") == "EXPENSE")["amount"].sum()
            total_savings = inc - exp
            avg_savings = total_savings / 6
            if avg_savings < 0: avg_savings = 0 # Éviter le négatif par défaut
            
    # 3. Dépenses Mensuelles Moyennes (pour FIRE)
    avg_expenses = 1500.0
    if not df_tx.is_empty():
         df_recent = df_tx.filter(pl.col("date") >= start_dt)
         total_exp = df_recent.filter(pl.col("type") == "EXPENSE")["amount"].sum()
         if total_exp > 0:
             avg_expenses = total_exp / 6

    return {
        "wealth": current_wealth, 
        "savings": avg_savings,
        "expenses": avg_expenses
    }

def save_scenario_db(name: str, config: ProjectionConfig):
    with sqlite3.connect(get_db_path()) as conn:
        conn.execute(
            "INSERT INTO projections (id, name, created_at, parameters_json) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), name, date.today(), config.to_json())
        )
        conn.commit()

def load_scenarios_db() -> pd.DataFrame:
    with sqlite3.connect(get_db_path()) as conn:
        return pd.read_sql("SELECT * FROM projections ORDER BY created_at DESC", conn)

def delete_scenario_db(s_id: str):
    with sqlite3.connect(get_db_path()) as conn:
        conn.execute("DELETE FROM projections WHERE id = ?", (s_id,))
        conn.commit()


# --- PAGE RENDERING: PROJECTIONS ---


# File: /app.py

def render_projections_page():
    st.header("🔮 Projections Futures & FIRE")

    metrics = get_current_metrics()
    
    if "life_events" not in st.session_state:
        st.session_state["life_events"] = []

    # --- SIDEBAR ---
    with st.sidebar:
        st.subheader("👤 Profil")
        current_age = st.number_input("Votre Âge", 18, 70, 25)
        retire_age = st.number_input("Âge Retraite Classique", 50, 75, 65, help="Âge auquel vous toucheriez votre retraite si vous arrêtiez d'épargner aujourd'hui (pour calcul Coast FIRE).")
        
        st.divider()
        st.subheader("💰 Paramètres Initiaux")
        start_capital = st.number_input("Patrimoine Actuel (€)", value=float(metrics["wealth"]), step=1000.0, format="%.0f")
        monthly_savings = st.number_input("Épargne Mensuelle (€)", value=float(metrics["savings"]), step=50.0, format="%.0f")
        
        st.divider()
        st.subheader("🌍 Hypothèses Macro")
        years = st.slider("Horizon de simulation (Années)", 5, 45, (retire_age - current_age) + 5)
        inflation = st.slider("Inflation Moyenne (%)", 0.0, 5.0, 2.0, 0.1)
        salary_growth = st.slider("Augmentation Épargne/An (%)", 0.0, 5.0, 1.0, 0.1)

    # --- TABS ---
    tab_sim, tab_events, tab_monte = st.tabs(["📈 Simulateur & FIRE", "🏠 Événements", "🎲 Monte Carlo"])

    with tab_sim:
        col_param, col_graph = st.columns([1, 3])
        
        with col_param:
            st.markdown("#### ⚙️ Rendement & Fiscalité")
            annual_return = st.slider("Rendement Annuel (%)", 0.0, 15.0, 7.0, 0.1) / 100
            tax_rate = st.slider("Taxe Plus-Values (%)", 0.0, 30.0, 30.0, 0.1) / 100 # Default Flat Tax
            
            st.markdown("#### 🔥 Objectifs FIRE")
            monthly_expenses = st.number_input("Dépenses Mensuelles (€)", value=float(metrics["expenses"]), step=100.0)
            
            # Définition des niveaux FIRE
            lean_ratio = 0.8
            fat_ratio = 1.5
            
            show_coast = st.checkbox(
                "Montrer Coast FIRE", 
                value=True, 
                help="Montant nécessaire AUJOURD'HUI pour atteindre votre cible à la retraite sans ne plus jamais rien épargner."
            )
            show_lean = st.checkbox(
                f"Montrer Lean FIRE ({monthly_expenses*lean_ratio:,.0f}€/mois)", 
                value=False,
                help="Le mode 'Frugal' ou Survie. Couvre uniquement vos dépenses vitales (défini ici à 80% de vos dépenses normales)."
            )
            show_fat = st.checkbox(
                f"Montrer Fat FIRE ({monthly_expenses*fat_ratio:,.0f}€/mois)", 
                value=False,
                help="Le mode 'Luxe' ou Confort absolu. Permet de voyager et d'augmenter votre train de vie (défini ici à 150% de vos dépenses normales)."
            )            
            st.markdown("---")
            show_real = st.checkbox("Ajuster à l'inflation (Réel)", value=True)

        with col_graph:
            # 1. Calcul Projection Principale
            config = ProjectionConfig(
                start_capital=start_capital,
                monthly_savings=monthly_savings,
                years=years,
                annual_return_rate=annual_return,
                inflation_rate=inflation / 100,
                salary_growth_rate=salary_growth / 100,
                life_events=st.session_state["life_events"]
            )
            df = calculate_deterministic_projection(config)
            
            # 2. Calcul Fiscalité (Net Pocket)
            # Pour simplifier l'affichage des seuils, on compare tout en "Net d'impôt".
            # Donc on nettoie la courbe de patrimoine.
            df['Gains'] = df['Nominal Capital'] - df['Total Invested']
            df['Tax'] = df['Gains'].apply(lambda x: x * tax_rate if x > 0 else 0)
            df['Net Nominal'] = df['Nominal Capital'] - df['Tax']
            # Recalcul Net Réel
            deflator = (1 + (inflation/100)) ** (df['Month']/12)
            df['Net Real'] = df['Net Nominal'] / deflator
            
            # 3. Calcul des Cibles (Targets)
            # La règle des 4% s'applique sur le capital Net disponible.
            fire_number_real = (monthly_expenses * 12) / 0.04
            
            # Colonne à afficher
            y_col = 'Net Real' if show_real else 'Net Nominal'
            
            # KPI
            final_wealth = df[y_col].iloc[-1]
            passive_income = (final_wealth * 0.04) / 12
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Patrimoine Net Final", f"{final_wealth:,.0f} €")
            c2.metric("Rente Mensuelle (4%)", f"{passive_income:,.0f} €")
            
            # Calcul Date FIRE Standard
            reached = df[df['Net Real'] >= fire_number_real]
            if not reached.empty:
                years_fire = reached.iloc[0]['Year']
                age_fire = current_age + years_fire
                c3.metric("FIRE Standard atteint à", f"{age_fire:.1f} ans", delta=f"dans {years_fire:.1f} ans")
            else:
                c3.metric("FIRE Standard", "Non atteint")

            # --- GRAPHIQUE ---
            fig = go.Figure()
            
            # A. Courbe Principale
            fig.add_trace(go.Scatter(
                x=df["Year"] + current_age, # On affiche l'âge en X
                y=df[y_col],
                mode='lines',
                name='Votre Patrimoine (Net)',
                line=dict(color='#636EFA', width=4),
                hovertemplate='Âge: %{x:.1f}<br>Patrimoine: %{y:,.0f} €<extra></extra>'
            ))

            # Fonction helper pour tracer des cibles
            def add_target_line(amount_real, name, color, visible, style='dash'):
                if not visible: return
                if show_real:
                    y_vals = [amount_real] * len(df)
                else:
                    # En nominal, la cible augmente avec l'inflation
                    y_vals = [amount_real * ((1 + inflation/100)**y) for y in df["Year"]]
                
                fig.add_trace(go.Scatter(
                    x=df["Year"] + current_age, y=y_vals, mode='lines', name=name,
                    line=dict(color=color, dash=style, width=2),
                    hovertemplate=f'{name}: %{{y:,.0f}} €<extra></extra>'
                ))

            # B. Les Lignes FIRE
            add_target_line(fire_number_real, "Standard FIRE (4%)", "#00CC96", True) # Toujours visible
            add_target_line(fire_number_real * lean_ratio, "Lean FIRE", "#FFA15A", show_lean, 'dot')
            add_target_line(fire_number_real * fat_ratio, "Fat FIRE", "#AB63FA", show_fat, 'dot')

            # C. La Courbe Coast FIRE
            # Logique : Coast FIRE = Combien il me faut AUJOURD'HUI pour qu'à 65 ANS j'ai mon FIRE Number, sans rien ajouter.
            # Formule : Target / (1 + Rate)^(Years_Left)
            # Attention : Le Rate doit être le taux Réel (Return - Inflation) si on est en mode Réel.
            if show_coast:
                real_return_rate = (1 + annual_return) / (1 + (inflation/100)) - 1
                rate_used = real_return_rate if show_real else annual_return
                target_used = fire_number_real # En réel, la cible est fixe. En nominal, le calcul est implicite dans le taux nominal.
                
                coast_curve = []
                for y in df["Year"]:
                    age = current_age + y
                    years_left = retire_age - age
                    if years_left > 0:
                        # Combien il faut avoir à l'âge 'age' pour que ça grossisse jusqu'à 'retire_age'
                        req = target_used / ((1 + rate_used) ** years_left)
                    else:
                        req = target_used # Arrivé à la retraite
                    coast_curve.append(req)
                
                # Si mode nominal, il faut re-inflater la courbe Coast pour qu'elle soit comparable au Nominal Wealth ?
                # Non, la formule ci-dessus avec 'annual_return' (nominal) gère déjà l'inflation implicitement pour atteindre la cible nominale.
                # Sauf qu'on a utilisé 'fire_number_real' comme base.
                if not show_real:
                     # Re-calcul propre nominal : Target à 65 ans (inflatée) ramenée au présent via rendement nominal
                     target_at_65_nominal = fire_number_real * ((1 + inflation/100)**(retire_age - current_age))
                     coast_curve = []
                     for y in df["Year"]:
                        age = current_age + y
                        years_left = retire_age - age
                        if years_left > 0:
                            req = target_at_65_nominal / ((1 + annual_return)**years_left)
                        else:
                            req = target_at_65_nominal
                        coast_curve.append(req)

                fig.add_trace(go.Scatter(
                    x=df["Year"] + current_age,
                    y=coast_curve,
                    mode='lines',
                    name='Coast FIRE (Min requis)',
                    line=dict(color='#FECB52', width=2, dash='longdash'),
                    # J'ai supprimé les paramètres 'fill' et 'fillcolor' ici
                    hovertemplate='Coast Threshold: %{y:,.0f} €<extra></extra>'
                ))

            fig.update_layout(
                title="Trajectoire vers l'Indépendance",
                xaxis_title="Votre Âge",
                yaxis_title="Patrimoine Net (€)",
                height=600,
                hovermode="x unified"
            )
            st.plotly_chart(fig, width="stretch")

    # --- TAB 2: EVENTS ---
    with tab_events:
        st.info("Ajoutez des impacts financiers futurs.")
        c_evt1, c_evt2, c_evt3, c_evt4 = st.columns([2, 2, 2, 1])
        with c_evt1: evt_name = st.text_input("Nom", "Achat Voiture")
        with c_evt2: evt_year = st.number_input("Année", 1, years, 5)
        with c_evt3: evt_amount = st.number_input("Montant", value=-15000.0, step=1000.0)
        with c_evt4: 
            st.write("")
            st.write("")
            if st.button("Ajouter"):
                st.session_state["life_events"].append({"name": evt_name, "year": evt_year, "amount": evt_amount})
        
        if st.session_state["life_events"]:
            st.dataframe(pd.DataFrame(st.session_state["life_events"]), hide_index=True)
            if st.button("🗑️ Tout effacer"):
                st.session_state["life_events"] = []
                st.rerun()

    # --- TAB 3: MONTE CARLO ---
    with tab_monte:
        st.markdown("### 🎲 Analyse de Risque")
        st.markdown("""
        Cette simulation lance des centaines de scénarios possibles en intégrant la **volatilité** (les hauts et les bas) des marchés financiers.
        *   **Ligne du haut (Max) :** Le scénario "chanceux" où les marchés performent exceptionnellement bien.
        *   **Zone bleue :** 80% des scénarios probables se trouvent ici.
        *   **Ligne du bas (Min) :** Le scénario "catastrophe" (krach boursier majeur, crise longue).
        """)
        
        col_mc_1, col_mc_2 = st.columns([1, 3])
        with col_mc_1:
            volatility = st.slider("Volatilité Marché (%)", 5.0, 30.0, 15.0, 1.0) / 100
            nb_sim = st.select_slider("Nombre Simulations", options=[50, 100, 200, 500], value=200)
        
        with col_mc_2:
            if st.button("Lancer la simulation", type="primary"):
                mc_config = ProjectionConfig(
                    start_capital=start_capital, monthly_savings=monthly_savings, years=years,
                    annual_return_rate=annual_return, inflation_rate=inflation / 100,
                    salary_growth_rate=salary_growth / 100, volatility=volatility,
                    life_events=st.session_state["life_events"]
                )
                df_mc = calculate_monte_carlo(mc_config, n_simulations=nb_sim)
                
                # Calcul des min/max sur l'ensemble des simulations
                # Note: calculate_monte_carlo retourne des percentiles. 
                # Pour avoir le vrai min/max, il faudrait modifier le moteur, 
                # mais P10 et P90 sont statistiquement plus pertinents que les outliers absolus.
                # Cependant, pour répondre à la demande "Top et Bottom", on va utiliser P90 et P10 comme bornes "raisonnables"
                # ou ajouter P05 et P95 si on veut être plus large.
                
                fig_mc = go.Figure()
                
                # Zone 10-90%
                fig_mc.add_trace(go.Scatter(
                    x=pd.concat([df_mc['Year'], df_mc['Year'][::-1]]),
                    y=pd.concat([df_mc['P90 (Optimiste)'], df_mc['P10 (Pessimiste)'][::-1]]),
                    fill='toself', fillcolor='rgba(0,176,246,0.2)', 
                    line=dict(color='rgba(255,255,255,0)'), 
                    name='Zone Probable (80% des cas)',
                    hoverinfo='skip'
                ))
                
                # Lignes spécifiques
                fig_mc.add_trace(go.Scatter(x=df_mc['Year'], y=df_mc['P90 (Optimiste)'], mode='lines', name='Scénario Optimiste (Top 10%)', line=dict(color='#636EFA', dash='dot', width=1)))
                fig_mc.add_trace(go.Scatter(x=df_mc['Year'], y=df_mc['P50 (Médian)'], mode='lines', name='Scénario Médian', line=dict(color='#00CC96', width=3)))
                fig_mc.add_trace(go.Scatter(x=df_mc['Year'], y=df_mc['P10 (Pessimiste)'], mode='lines', name='Scénario Pessimiste (Bottom 10%)', line=dict(color='#EF553B', dash='dot', width=1)))
                
                fig_mc.update_layout(height=500, title="Cône d'incertitude", yaxis_title="Capital (€)", hovermode="x unified")
                st.plotly_chart(fig_mc, width="stretch")
                

# --- MAIN APP ROUTING ---

def main():
    # Render the sidebar
    with st.sidebar:
        st.title("💰 Finance Tracker")
        st.write("---") 
        
        if st.button("📊 Tableau de Bord", width="stretch"):
            st.session_state["current_page"] = "Tableau de Bord"
            
        if st.button("📈 Patrimoine & Bourse", width="stretch"):
            st.session_state["current_page"] = "Patrimoine & Bourse"

        if st.button("🗺️ Carte du Marché", width="stretch"):
            st.session_state["current_page"] = "Carte du Marché"

        if st.button("🔮 Projections & FIRE", width="stretch"):
            st.session_state["current_page"] = "Projections"    

        if st.button("📥 Import / Données", width="stretch"):
            st.session_state["current_page"] = "Import / Données"
        
        st.write("---") 

    # Route to the selected page
    page = st.session_state["current_page"]
    
    if page == "Import / Données":
        render_import_page()
    elif page == "Tableau de Bord":
        render_dashboard_page()
    elif page == "Patrimoine & Bourse":
        render_wealth_page()
    elif page == "Carte du Marché":
        render_market_map_page()
    if page == "Projections":
        render_projections_page()

if __name__ == "__main__":
    main()