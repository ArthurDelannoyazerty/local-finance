import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import uuid
import sqlite3
from datetime import date
from datetime import date, timedelta 

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
        # --- GESTION DES DATES & RACCOURCIS ---
        
        # 1. Initialisation des clés du widget si elles n'existent pas
        if "input_start" not in st.session_state:
            st.session_state["input_start"] = date(date.today().year, 1, 1)
        if "input_end" not in st.session_state:
            st.session_state["input_end"] = date.today()

        # 2. Fonction de Callback pour synchroniser les boutons et les calendriers
        def update_date_range(days=None, start=None, end=None):
            """Met à jour directement les clés utilisées par les widgets st.date_input"""
            target_end = date.today()
            if end:
                target_end = end
                
            target_start = target_end # fallback
            
            if start:
                target_start = start
            elif days:
                target_start = target_end - timedelta(days=days)
            
            # C'est ici que la magie opère : on met à jour les clés des widgets
            st.session_state["input_start"] = target_start
            st.session_state["input_end"] = target_end

        # 3. Interface UI améliorée (Layout horizontal)
        with st.container():
            st.subheader("📅 Période d'analyse")
            
            # On divise en 2 colonnes : Raccourcis à gauche, Calendriers à droite
            col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
            
            with col_shortcuts:
                st.caption("Raccourcis rapides")
                # Ligne 1 : Périodes glissantes
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("1 Mois", use_container_width=True):
                    update_date_range(days=30)
                if b2.button("3 Mois", use_container_width=True):
                    update_date_range(days=90)
                if b3.button("YTD (Année)", use_container_width=True, help="Depuis le 1er Janvier"):
                    update_date_range(start=date(date.today().year, 1, 1))
                if b4.button("Tout", use_container_width=True):
                    min_date = df["date"].min()
                    update_date_range(start=min_date)

                # Ligne 2 : Années spécifiques (Affichées sous forme de petits "tags")
                years = sorted(df["date"].dt.year().unique().to_list(), reverse=True)
                if years:
                    st.write("") # Petit espacement
                    cols_years = st.columns(len(years) + 2) # +2 pour éviter que ce soit trop large
                    for i, year in enumerate(years):
                        if cols_years[i].button(str(year), key=f"year_{year}", use_container_width=True):
                            update_date_range(start=date(year, 1, 1), end=date(year, 12, 31))

            with col_pickers:
                st.caption("Sélection manuelle")
                c_start, c_end = st.columns(2)
                # Notez que nous n'avons plus besoin de 'value=' car la 'key' gère l'état
                # Le widget lira automatiquement st.session_state["input_start"]
                start_date = c_start.date_input("Début", key="input_start")
                end_date = c_end.date_input("Fin", key="input_end")

        st.divider()

        # 4. Filtrage des données (On utilise directement les variables issues des widgets)
        df_filtered = df.filter(
            (pl.col("date") >= start_date) & 
            (pl.col("date") <= end_date)
        )
                
        # KPI
        income = df_filtered.filter(pl.col("type") == "INCOME")["amount"].sum()
        expense = df_filtered.filter(pl.col("type") == "EXPENSE")["amount"].sum()
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
            df_exp = df_filtered.filter(pl.col("type") == "EXPENSE")
            
            if not df_exp.is_empty():
                # 1. Agrégation
                grp = df_exp.group_by("category").agg(pl.col("amount").sum()).sort("amount", descending=True)
                pdf_chart = grp.to_pandas()

                # 2. Stratégie UX "Top N + Autres" pour éviter la surcharge
                # Si on a plus de 7 catégories, on garde les 6 plus grosses et on groupe le reste
                if len(pdf_chart) > 7:
                    top_n = pdf_chart.iloc[:6].copy()
                    others_value = pdf_chart.iloc[6:]['amount'].sum()
                    others_df = pd.DataFrame([{'category': 'Autres', 'amount': others_value}])
                    pdf_chart = pd.concat([top_n, others_df], ignore_index=True)

                # 3. Création du graphique
                fig_pie = px.pie(
                    pdf_chart, 
                    values="amount", 
                    names="category", 
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel # Couleurs douces
                )

                # 4. Configuration avancée du texte
                fig_pie.update_traces(
                    textposition='outside', # Place le texte à l'extérieur pour voir la couleur
                    textinfo='label+percent+value',
                    # On utilise <br> pour sauter des lignes et <b> pour le gras
                    # %{value:.0f} € -> Affiche la valeur arrondie avec le sigle €
                    texttemplate='%{label}<br><b>%{value:,.0f} €</b><br>(%{percent})'
                )
                
                # Suppression de la légende (car l'info est déjà sur le graph) pour gagner de la place
                fig_pie.update_layout(showlegend=False, margin=dict(t=40, b=80, l=20, r=20), height=600)
                
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Aucune dépense sur cette période.")
        
        with col_charts_2:
            st.subheader("Évolution Mensuelle & Tendance")
            
            # 1. Préparation des données : Groupement par Mois-Année (et non juste par mois)
            # On utilise dt.truncate("1mo") pour garder l'info de l'année (ex: 2023-01-01)
            monthly_agg = (
                df_filtered
                .with_columns(pl.col("date").dt.truncate("1mo").alias("month_date"))
                .group_by(["month_date", "type"])
                .agg(pl.col("amount").sum())
                .sort("month_date")
            )
            
            if not monthly_agg.is_empty():
                # 2. Pivot : Transformer les lignes INCOME/EXPENSE en colonnes
                # Cela nous permet de manipuler les séries distinctement
                df_pivot = monthly_agg.pivot(
                    values="amount", 
                    index="month_date", 
                    columns="type", 
                    aggregate_function="sum"
                ).fill_null(0).sort("month_date")

                # Sécurisation : Vérifier que les colonnes existent (si on a que des dépenses par ex)
                if "INCOME" not in df_pivot.columns:
                    df_pivot = df_pivot.with_columns(pl.lit(0.0).alias("INCOME"))
                if "EXPENSE" not in df_pivot.columns:
                    df_pivot = df_pivot.with_columns(pl.lit(0.0).alias("EXPENSE"))

                # 3. Calcul de la Moyenne Mobile (Ex: Moyenne des dépenses sur 3 mois)
                # Cela crée la courbe de tendance
                df_pivot = df_pivot.with_columns(
                    pl.col("EXPENSE").rolling_mean(window_size=3).alias("ma_expense")
                )

                # Conversion en Pandas pour Plotly
                pdf_viz = df_pivot.to_pandas()

                # 4. Construction du Graphique avancé (Graph Objects)
                fig_combo = go.Figure()

                # Barres des Revenus
                fig_combo.add_trace(go.Bar(
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["INCOME"],
                    name="Revenus",
                    marker_color="#00CC96"
                ))

                # Barres des Dépenses
                fig_combo.add_trace(go.Bar(
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["EXPENSE"],
                    name="Dépenses",
                    marker_color="#EF553B"
                ))

                # Ligne de Tendance (Moyenne Mobile Dépenses)
                fig_combo.add_trace(go.Scatter(
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["ma_expense"],
                    mode='lines',
                    name="Moyenne Dépenses (3 mois)",
                    line=dict(color='#172B4D', width=3, dash='dot') # Bleu foncé, pointillé
                ))

                # 5. Configuration du Layout pour forcer l'affichage de tous les mois
                fig_combo.update_layout(
                    barmode='group', # Barres côte à côte
                    xaxis=dict(
                        tickformat="%b %y", # Format : Jan 24
                        dtick="M1",         # Force un tick par mois
                        tickangle=-45       # Inclinaison pour lisibilité
                    ),
                    legend=dict(
                        orientation="h",    # Légende horizontale en bas
                        yanchor="bottom", y=1.02,
                        xanchor="right", x=1
                    ),
                    margin=dict(t=20, b=40, l=20, r=20),
                    height=450
                )

                st.plotly_chart(fig_combo, use_container_width=True)
            else:
                st.info("Pas de données sur la période sélectionnée.")

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
    
    # --- 1. CALCUL DES DONNÉES (FULL HISTORY) ---
    with st.spinner("Calcul de l'évolution du patrimoine..."):
        df_wealth = calculate_wealth_evolution()
    
    if df_wealth.is_empty():
        st.info("Pas assez de données pour générer le graphique.")
    
    else:
        # --- GESTION DES DATES (COPIE EXACTE DU DASHBOARD) ---
        
        # 1. Initialisation des clés spécifiques à cette page
        if "wealth_start" not in st.session_state:
            st.session_state["wealth_start"] = df_wealth["date"].min() # Par défaut : tout l'historique
        if "wealth_end" not in st.session_state:
            st.session_state["wealth_end"] = date.today()

        # 2. Fonction de Callback pour synchroniser
        def update_wealth_range(days=None, start=None, end=None):
            target_end = end if end else date.today()
            target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
            
            st.session_state["wealth_start"] = target_start
            st.session_state["wealth_end"] = target_end

        # 3. Interface UI (Layout identique au Dashboard)
        with st.container():
            st.subheader("📅 Période d'analyse")
            
            col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
            
            with col_shortcuts:
                st.caption("Raccourcis rapides")
                # Ligne 1 : Périodes glissantes
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("1 Mois", key="w_1m", use_container_width=True):
                    update_wealth_range(days=30)
                if b2.button("6 Mois", key="w_6m", use_container_width=True):
                    update_wealth_range(days=180) # Adapté pour le patrimoine (plus long terme)
                if b3.button("YTD", key="w_ytd", use_container_width=True):
                    update_wealth_range(start=date(date.today().year, 1, 1))
                if b4.button("Tout", key="w_all", use_container_width=True):
                    update_wealth_range(start=df_wealth["date"].min())

                # Ligne 2 : Années spécifiques (Basées sur les données de richesse)
                # On extrait les années disponibles dans l'historique de patrimoine
                years = sorted(df_wealth["date"].dt.year().unique().to_list(), reverse=True)
                if years:
                    st.write("") 
                    cols_years = st.columns(len(years) + 2)
                    for i, year in enumerate(years):
                        if cols_years[i].button(str(year), key=f"w_year_{year}", use_container_width=True):
                            update_wealth_range(start=date(year, 1, 1), end=date(year, 12, 31))

            with col_pickers:
                st.caption("Sélection manuelle")
                c_start, c_end = st.columns(2)
                # On lie les widgets aux clés de session définies plus haut
                w_start = c_start.date_input("Début", key="wealth_start")
                w_end = c_end.date_input("Fin", key="wealth_end")

        # --- 4. FILTRAGE ---
        df_viz = df_wealth.filter(
            (pl.col("date") >= w_start) & 
            (pl.col("date") <= w_end)
        )
        
        if df_viz.is_empty():
            st.warning("Aucune donnée sur cette période.")
        else:
            pdf_wealth = df_viz.to_pandas()
            
            # --- 5. CONFIGURATION ET GRAPHIQUE (Le reste inchangé) ---
            st.write("---")
            col_opts, col_metrics = st.columns([2, 1])
            
            # Identification des colonnes "Comptes"
            excluded_cols = ['date', 'Total Wealth', 'Total Invest']
            account_cols = [c for c in pdf_wealth.columns if c not in excluded_cols]
            
            with col_opts:
                chart_mode = st.radio(
                    "Mode d'affichage", 
                    ["Global (Total)", "Détail (Empilé)", "Comparaison (Sélection)"], 
                    horizontal=True
                )
                
                selected_accounts = account_cols
                if chart_mode == "Comparaison (Sélection)":
                    selected_accounts = st.multiselect(
                        "Sélectionnez les comptes à afficher", 
                        account_cols, 
                        default=account_cols[:3] if len(account_cols)>3 else account_cols
                    )

            with col_metrics:
                last_val = pdf_wealth.iloc[-1]['Total Wealth']
                first_val = pdf_wealth.iloc[0]['Total Wealth']
                delta = last_val - first_val
                st.metric("Patrimoine Fin de Période", f"{last_val:,.2f} €", delta=f"{delta:,.2f} €")

            # --- GRAPHIQUE PLOTLY ---
            fig = go.Figure()

            if chart_mode == "Global (Total)":
                fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth['Total Wealth'], 
                    mode='lines', name='Patrimoine Net',
                    line=dict(color='#636EFA', width=4),
                    fill='tozeroy', fillcolor='rgba(99, 110, 250, 0.1)' 
                ))
                if "Total Invest" in pdf_wealth.columns:
                    fig.add_trace(go.Scatter(
                        x=pdf_wealth['date'], y=pdf_wealth['Total Invest'], 
                        mode='lines', name='Dont Investissement',
                        line=dict(color='gold', width=2, dash='dash')
                    ))

            elif chart_mode == "Détail (Empilé)":
                for col in account_cols:
                    fig.add_trace(go.Scatter(
                        x=pdf_wealth['date'], y=pdf_wealth[col],
                        mode='lines', stackgroup='one', name=col
                    ))

            elif chart_mode == "Comparaison (Sélection)":
                if selected_accounts:
                    for col in selected_accounts:
                        fig.add_trace(go.Scatter(
                            x=pdf_wealth['date'], y=pdf_wealth[col],
                            mode='lines', name=col
                        ))

            fig.update_layout(
                title=f"Évolution du {w_start.strftime('%d/%m/%Y')} au {w_end.strftime('%d/%m/%Y')}",
                xaxis=dict(showgrid=False),
                yaxis=dict(title="Valeur (€)", showgrid=True, gridcolor='lightgray'),
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=700, # Hauteur demandée
                margin=dict(l=20, r=20, t=60, b=40)
            )

            st.plotly_chart(fig, use_container_width=True)

  