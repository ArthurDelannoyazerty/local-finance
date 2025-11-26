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

# --- SESSION STATE NAVIGATION ---
# 1. Initialize the current page in session state if it doesn't exist
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Tableau de Bord"

# --- SIDEBAR ---
with st.sidebar:
    st.title("💰 Finance Tracker")
    st.write("---") # Divider
    
    # 2. Create standard buttons for navigation
    # We use a callback logic: clicking a button updates the session state
    
    if st.button("📊 Tableau de Bord", use_container_width=True):
        st.session_state["current_page"] = "Tableau de Bord"
        
    if st.button("📈 Patrimoine & Bourse", use_container_width=True):
        st.session_state["current_page"] = "Patrimoine & Bourse"
        
    if st.button("📥 Import / Données", use_container_width=True):
        st.session_state["current_page"] = "Import / Données"
    
    st.write("---") # Bottom divider

# 3. Retrieve the active page from session state
page = st.session_state["current_page"]

# --- HELPER FUNCTIONS ---
def get_accounts():
    conn = sqlite3.connect(get_db_path())
    try:
        df = pl.read_database("SELECT * FROM accounts", conn)
    except:
        df = pl.DataFrame()
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
        st.info("Indiquez le solde initial de chaque compte avant la première transaction importée.")
        
        df_acc = get_accounts()
        if not df_acc.is_empty():
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
        st.warning("Pas de données. Veuillez importer des fichiers.")
    else:
        # 1. Filtre Global (Année)
        c_year, _ = st.columns([1, 3])
        with c_year:
            years = sorted(df["date"].dt.year().unique().to_list(), reverse=True)
            if not years:
                years = [date.today().year]
            selected_year = st.selectbox("Année", years)
        
        df_year = df.filter(pl.col("date").dt.year() == selected_year)
        
        # 2. Gestion des Exclusions (Outliers)
        with st.expander("🛠️ Gestion des Transactions Exceptionnelles (Outliers)", expanded=True):
            
            # --- NOUVEAU : Filtres pour le tableau d'exclusion ---
            c_ex_1, c_ex_2 = st.columns(2)
            with c_ex_1:
                threshold = st.slider("Seuil de détection (€)", 500, 10000, 2000, step=100)
            with c_ex_2:
                # Menu demandé : Choix entre Dépenses (Defaut) et Revenus
                filter_type_label = st.radio(
                    "Type de transaction :", 
                    ["Dépenses", "Revenus"], 
                    horizontal=True, # Affiche les options côte à côte
                    index=0
                )            
            # Conversion du choix en valeur DB
            db_type = "EXPENSE" if filter_type_label == "Dépenses" else "INCOME"

            # Filtre Polars : Année + Seuil + Type
            outliers = df_year.filter(
                (pl.col("amount").abs() >= threshold) & 
                (pl.col("type") == db_type)
            ).sort("date", descending=True)
            
            if not outliers.is_empty():
                st.write(f"**{len(outliers)}** {filter_type_label.lower()} détectées au dessus de {threshold}€")
                
                pdf_out = outliers.to_pandas()
                
                edited_out = st.data_editor(
                    pdf_out,
                    column_config={
                        "is_excluded": st.column_config.CheckboxColumn("Exclure ?", help="Cocher pour retirer des graphiques"),
                        "amount": st.column_config.NumberColumn("Montant", format="%.2f €"),
                        "date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                        "category": st.column_config.TextColumn("Catégorie"),
                        "comment": st.column_config.TextColumn("Commentaire"),
                        "id": None # On cache l'ID technique
                    },
                    # On autorise l'édition de 'is_excluded' et 'comment' (pratique pour annoter)
                    disabled=["date", "category", "account", "amount", "currency", "type"],
                    hide_index=True,
                    use_container_width=True
                )
                
                if st.button("Mettre à jour les exclusions"):
                    count = 0
                    for i, row in edited_out.iterrows():
                        # On compare avec la valeur actuelle en base
                        orig = df.filter(pl.col("id") == row['id']).select("is_excluded").item()
                        if bool(row['is_excluded']) != bool(orig):
                            update_exclusion(row['id'], bool(row['is_excluded']))
                            count += 1
                    
                    if count > 0:
                        st.success(f"{count} transactions mises à jour.")
                        st.rerun()
            else:
                st.info(f"Aucune transaction de type '{filter_type_label}' ne dépasse {threshold}€.")

        st.divider()

        # 3. Graphiques (Calculés sur les données NON exclues)
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
            if not monthly.is_empty():
                fig_bar = px.bar(monthly.to_pandas(), x="month", y="amount", color="type", barmode="group",
                                 color_discrete_map={"INCOME": "#00CC96", "EXPENSE": "#EF553B"})
                st.plotly_chart(fig_bar, use_container_width=True)

# --- PAGE 3: PATRIMOINE & INVEST ---
elif page == "Patrimoine & Bourse":
    st.header("📈 Évolution du Patrimoine")
    
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
            # Safe get accounts
            acc_df = get_accounts()
            accs = acc_df["name"].to_list() if not acc_df.is_empty() else ["Défaut"]
            i_acc = st.selectbox("Compte", accs)
            i_fees = st.number_input("Frais", min_value=0.0, step=0.1)
        
        i_comm = st.text_input("Commentaire")
        
        if st.button("Enregistrer Investissement"):
            save_investment(i_date, i_ticker, i_name, i_action, i_qty, i_price, i_fees, i_acc, i_comm)
            st.success("Transaction enregistrée !")
            st.rerun()

    st.divider()
    
    with st.spinner("Calcul de l'évolution du patrimoine..."):
        df_wealth = calculate_wealth_evolution()
    
    if df_wealth.is_empty():
        st.info("Pas assez de données pour générer le graphique.")
    else:
        st.subheader("Evolution Globale")
        pdf_wealth = df_wealth.to_pandas()
        
        fig = go.Figure()
        
        chart_mode = st.radio("Vue", ["Total (Ligne)", "Détail (Empilé)"], horizontal=True)
        
        if chart_mode == "Total (Ligne)":
            fig.add_trace(go.Scatter(x=pdf_wealth['date'], y=pdf_wealth['Total Wealth'], 
                                     mode='lines', name='Total Net Worth',
                                     line=dict(color='#636EFA', width=3)))
            fig.update_layout(title="Patrimoine Net Total")
            
        else:
            cols = [c for c in pdf_wealth.columns if c not in ['date', 'Total Wealth', 'Total Invest']]
            for c in cols:
                fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth[c],
                    mode='lines', stackgroup='one', name=c
                ))
            if "Total Invest" in pdf_wealth.columns:
                 fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth["Total Invest"],
                    mode='lines', name='Actions (Val.)',
                    line=dict(color='gold', width=2, dash='dash')
                ))

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
        
        last_row = pdf_wealth.iloc[-1]
        st.metric("Patrimoine Actuel", f"{last_row['Total Wealth']:,.2f} €")