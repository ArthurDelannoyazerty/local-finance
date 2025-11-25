import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import uuid
import sqlite3
from datetime import date

from src.database import init_db, get_db_path
from src.importer import import_excel_file 
from src.queries import get_transactions_df, update_exclusion
from src.engine import calculate_wealth_evolution

# --- SETUP ---
st.set_page_config(page_title="My Finance Tracker", layout="wide", initial_sidebar_state="expanded")
init_db()

# --- SIDEBAR ---
st.sidebar.title("💰 Finance Tracker")
page = st.sidebar.radio("Navigation", ["Tableau de Bord", "Patrimoine & Bourse", "Import / Données"])

# --- HELPERS ---
def get_accounts():
    conn = sqlite3.connect(get_db_path())
    df = pl.read_database("SELECT * FROM accounts", conn)
    conn.close()
    return df

def save_investment(date_inv, ticker, name, action, qty, price, fees, account, comment):
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    c.execute("""
        INSERT INTO investments (id, date, ticker, name, action, quantity, unit_price, fees, account, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (str(uuid.uuid4()), date_inv, ticker, name, action, qty, price, fees, account, comment))
    conn.commit()
    conn.close()

def update_account_initial(name, amount):
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    c.execute("UPDATE accounts SET initial_balance = ? WHERE name = ?", (amount, name))
    conn.commit()
    conn.close()

# --- PAGE 1: IMPORT / DATA ---
if page == "Import / Données":
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
        st.info("Pour que les graphiques de patrimoine soient justes, indiquez le solde initial de chaque compte avant la première transaction importée.")
        
        df_acc = get_accounts()
        if not df_acc.is_empty():
            # On utilise data_editor pour modifier rapidement
            pdf_acc = df_acc.to_pandas()
            edited_acc = st.data_editor(
                pdf_acc, 
                column_config={
                    "name": st.column_config.TextColumn("Compte", disabled=True),
                    "initial_balance": st.column_config.NumberColumn("Solde Initial", format="%.2f €")
                },
                hide_index=True,
                key="acc_editor"
            )
            
            if st.button("Sauvegarder les soldes"):
                for index, row in edited_acc.iterrows():
                    update_account_initial(row['name'], row['initial_balance'])
                st.success("Soldes mis à jour !")
                st.rerun()
        else:
            st.warning("Aucun compte détecté. Importez d'abord des fichiers.")

# --- PAGE 2: DASHBOARD (BUDGET) ---
elif page == "Tableau de Bord":
    st.header("📊 Analyse des Flux (Cash Flow)")
    
    df = get_transactions_df()
    
    if df.is_empty():
        st.warning("Pas de données.")
    else:
        # --- FILTERS ---
        c1, c2, c3 = st.columns(3)
        with c1:
            years = sorted(df["date"].dt.year().unique().to_list(), reverse=True)
            selected_year = st.selectbox("Année", years)
        
        # Filter Data
        df_year = df.filter(pl.col("date").dt.year() == selected_year)
        
        # --- OUTLIER DETECTION ---
        with st.expander("🛠️ Gestion des Transactions Exceptionnelles (Outliers)"):
            threshold = st.slider("Seuil de détection (€)", 500, 10000, 2000, step=100)
            
            # Find outliers
            outliers = df_year.filter(pl.col("amount").abs() >= threshold).sort("date", descending=True)
            
            if not outliers.is_empty():
                st.write(f"{len(outliers)} transactions détectées au dessus de {threshold}€")
                
                # Editor
                pdf_out = outliers.to_pandas()
                edited_out = st.data_editor(
                    pdf_out,
                    column_config={
                        "is_excluded": st.column_config.CheckboxColumn("Exclure ?", help="Cochez pour retirer des stats"),
                        "amount": st.column_config.NumberColumn("Montant", format="%.2f €"),
                        "id": None
                    },
                    disabled=["date", "category", "account", "amount", "currency", "type"],
                    hide_index=True
                )
                
                if st.button("Mettre à jour les exclusions"):
                    count = 0
                    for i, row in edited_out.iterrows():
                        orig = df.filter(pl.col("id") == row['id']).select("is_excluded").item()
                        if bool(row['is_excluded']) != bool(orig):
                            update_exclusion(row['id'], bool(row['is_excluded']))
                            count += 1
                    if count > 0:
                        st.success(f"{count} transactions mises à jour.")
                        st.rerun()
            else:
                st.info("Aucune transaction au dessus de ce seuil.")

        # --- CHARTS ---
        # Data clean (without excluded)
        df_clean = df_year.filter(pl.col("is_excluded") == 0)
        
        # KPI
        income = df_clean.filter(pl.col("type") == "INCOME")["amount"].sum()
        expense = df_clean.filter(pl.col("type") == "EXPENSE")["amount"].sum()
        savings = income - expense
        rate = (savings / income * 100) if income > 0 else 0
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Revenus", f"{income:,.0f} €")
        k2.metric("Dépenses", f"{expense:,.0f} €", delta_color="inverse")
        k3.metric("Épargne", f"{savings:,.0f} €", delta_color="normal")
        k4.metric("Taux d'épargne", f"{rate:.1f} %")
        
        st.divider()
        
        col_charts_1, col_charts_2 = st.columns(2)
        
        with col_charts_1:
            st.subheader("Dépenses par Catégorie")
            df_exp = df_clean.filter(pl.col("type") == "EXPENSE")
            if not df_exp.is_empty():
                grp = df_exp.group_by("category").agg(pl.col("amount").sum()).sort("amount", descending=True)
                fig_pie = px.pie(grp.to_pandas(), values="amount", names="category", hole=0.4)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col_charts_2:
            st.subheader("Évolution Mensuelle")
            monthly = df_clean.group_by([pl.col("date").dt.month().alias("month"), "type"]).agg(pl.col("amount").sum()).sort("month")
            fig_bar = px.bar(monthly.to_pandas(), x="month", y="amount", color="type", barmode="group",
                             color_discrete_map={"INCOME": "#00CC96", "EXPENSE": "#EF553B"})
            st.plotly_chart(fig_bar, use_container_width=True)

# --- PAGE 3: PATRIMOINE & INVEST ---
elif page == "Patrimoine & Bourse":
    st.header("📈 Évolution du Patrimoine")
    
    # 1. ADD TRANSACTION FORM
    with st.expander("➕ Ajouter une transaction Bourse (Achat/Vente)"):
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            i_date = st.date_input("Date")
            i_action = st.selectbox("Action", ["BUY", "SELL"])
        with f2:
            i_ticker = st.text_input("Ticker (ex: CW8.PA)", value="CW8.PA")
            i_name = st.text_input("Nom du produit", value="Amundi MSCI World")
        with f3:
            i_qty = st.number_input("Quantité", min_value=0.01, step=1.0)
            i_price = st.number_input("Prix Unitaire", min_value=0.01, step=0.1)
        with f4:
            accs = get_accounts()["name"].to_list() if not get_accounts().is_empty() else ["Défaut"]
            i_acc = st.selectbox("Compte", accs)
            i_fees = st.number_input("Frais", min_value=0.0, step=0.1)
        
        i_comm = st.text_input("Commentaire")
        
        if st.button("Enregistrer Investissement"):
            save_investment(i_date, i_ticker, i_name, i_action, i_qty, i_price, i_fees, i_acc, i_comm)
            st.success("Transaction enregistrée !")
            st.rerun()

    st.divider()
    
    # 2. CALCULATION ENGINE
    with st.spinner("Calcul de l'évolution du patrimoine (intégration données Yahoo Finance)..."):
        df_wealth = calculate_wealth_evolution()
    
    if df_wealth.is_empty():
        st.info("Pas assez de données pour générer le graphique.")
    else:
        # 3. TRADINGVIEW STYLE CHART
        st.subheader("Evolution Globale (Cash + Actifs)")
        
        # Filtre Temporel
        range_opts = ["1M", "3M", "6M", "YTD", "1Y", "ALL"]
        # (Pour simplifier ici on utilise le zoom interactif de Plotly, pas besoin de filtrer le DF manuellement sauf si très lourd)
        
        # Conversion Pandas pour Plotly
        pdf_wealth = df_wealth.to_pandas()
        
        # Graphique principal
        fig = go.Figure()
        
        # Zone empilée pour les comptes (Cash)
        # On peut choisir de montrer le Total Wealth en ligne ou le détail empilé
        
        chart_mode = st.radio("Vue", ["Patrimoine Total (Ligne)", "Détail Comptes & Invest (Empilé)"], horizontal=True)
        
        if chart_mode == "Patrimoine Total (Ligne)":
            fig.add_trace(go.Scatter(x=pdf_wealth['date'], y=pdf_wealth['Total Wealth'], 
                                     mode='lines', name='Total Net Worth',
                                     line=dict(color='#636EFA', width=3)))
            fig.update_layout(title="Patrimoine Net Total")
            
        else:
            # Stacked Area
            # Colonnes à plotter : tout sauf date et Total Wealth
            cols = [c for c in pdf_wealth.columns if c not in ['date', 'Total Wealth', 'Total Invest']]
            
            for c in cols:
                fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth[c],
                    mode='lines', stackgroup='one', name=c
                ))
            
            # Ajouter la ligne d'investissement global par dessus ? 
            # Non, si on a 'Total Invest' dans le dataframe, on peut le montrer séparément
            if "Total Invest" in pdf_wealth.columns:
                 fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth["Total Invest"],
                    mode='lines', name='Val. Portefeuille (Actions)',
                    line=dict(color='gold', width=2, dash='dash')
                ))

        # Range Slider style TradingView
        fig.update_layout(
            hovermode="x unified",
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
                type="date"
            ),
            yaxis=dict(title="Valeur (€)")
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Dernières valeurs
        last_row = pdf_wealth.iloc[-1]
        st.metric("Patrimoine Actuel", f"{last_row['Total Wealth']:,.2f} €")