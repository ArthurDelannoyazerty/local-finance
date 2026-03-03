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
from src.queries import get_transactions_df, update_exclusion, get_investments_df
from src.engine import calculate_wealth_evolution, get_detailed_snapshot

# --- SETUP ---
st.set_page_config(page_title="My Finance Tracker", layout="wide", initial_sidebar_state="expanded")
init_db()

# --- SESSION STATE NAVIGATION ---
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Tableau de Bord"

# --- SIDEBAR ---
with st.sidebar:
    st.title("💰 Finance Tracker")
    st.write("---") 
    
    if st.button("📊 Tableau de Bord", width="stretch"):
        st.session_state["current_page"] = "Tableau de Bord"
        
    if st.button("📈 Patrimoine & Bourse", width="stretch"):
        st.session_state["current_page"] = "Patrimoine & Bourse"

    if st.button("🗺️ Carte du Marché", width="stretch"):
        st.session_state["current_page"] = "Carte du Marché"
        
    if st.button("📥 Import / Données", width="stretch"):
        st.session_state["current_page"] = "Import / Données"
    
    st.write("---") 

page = st.session_state["current_page"]

# --- HELPER FUNCTIONS ---
def get_accounts():
    conn = sqlite3.connect(get_db_path())
    try:
        df = pl.read_database("SELECT name, initial_balance FROM accounts", conn)
    except:
        df = pl.DataFrame()
    conn.close()
    return df

def save_investment(date_inv, ticker, name, action, qty, price, fees, account, comment):
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    
    # CORRECTION : Gestion du commentaire vide pour éviter l'erreur SQL
    final_comment = comment if comment and comment.strip() != "" else ""

    c.execute("""
        INSERT INTO investments (id, date, ticker, name, action, quantity, unit_price, fees, account, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (str(uuid.uuid4()), date_inv, ticker, name, action, qty, price, fees, account, final_comment))
    conn.commit()
    conn.close()

def update_account_initial(name, amount):
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    c.execute("UPDATE accounts SET initial_balance = ? WHERE name = ?", (amount, name))
    conn.commit()
    conn.close()

def create_new_account(name):
    conn = sqlite3.connect(get_db_path())
    c = conn.cursor()
    # On crée le compte s'il n'existe pas
    c.execute("INSERT OR IGNORE INTO accounts (name, initial_balance) VALUES (?, 0.0)", (name,))
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

        # --- AJOUT : FORMULAIRE DE CRÉATION ---
        with st.expander("➕ Créer un nouveau compte manuellement"):
            col_new_1, col_new_2 = st.columns([3, 1])
            with col_new_1:
                new_acc_name = st.text_input("Nom du nouveau compte", placeholder="ex: PEA Bourse Direct")
            with col_new_2:
                st.write("") # Espacement pour aligner le bouton
                st.write("") 
                if st.button("Créer le compte"):
                    if new_acc_name.strip():
                        create_new_account(new_acc_name.strip())
                        st.success(f"Compte '{new_acc_name}' créé !")
                        st.rerun() # Recharge la page pour mettre à jour la liste
                    else:
                        st.error("Le nom ne peut pas être vide.")
        
        st.divider()
        # --------------------------------------
        
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
        if "input_start" not in st.session_state:
            st.session_state["input_start"] = date(date.today().year, 1, 1)
        if "input_end" not in st.session_state:
            st.session_state["input_end"] = date.today()

        def update_date_range(days=None, start=None, end=None):
            target_end = date.today()
            if end:
                target_end = end
            target_start = target_end 
            if start:
                target_start = start
            elif days:
                target_start = target_end - timedelta(days=days)
            st.session_state["input_start"] = target_start
            st.session_state["input_end"] = target_end

        with st.container():
            st.subheader("📅 Période d'analyse")
            col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
            
            with col_shortcuts:
                st.caption("Raccourcis rapides")
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("1 Mois", width="stretch"):
                    update_date_range(days=30)
                if b2.button("3 Mois", width="stretch"):
                    update_date_range(days=90)
                if b3.button("YTD (Année)", width="stretch", help="Depuis le 1er Janvier"):
                    update_date_range(start=date(date.today().year, 1, 1))
                if b4.button("Tout", width="stretch"):
                    min_date = df["date"].min()
                    update_date_range(start=min_date)

                years = sorted(df["date"].dt.year().unique().to_list(), reverse=True)
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

        df_filtered = df.filter(
            (pl.col("date") >= start_date) & 
            (pl.col("date") <= end_date)
        )
                
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
                # CORRECTION POLARS : 'columns' -> 'on'
                df_pivot = monthly_agg.pivot(
                    values="amount", 
                    index="month_date", 
                    on="type",  # <-- Changement ici
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
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["INCOME"],
                    name="Revenus",
                    marker_color="#00CC96"
                ))

                fig_combo.add_trace(go.Bar(
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["EXPENSE"],
                    name="Dépenses",
                    marker_color="#EF553B"
                ))

                fig_combo.add_trace(go.Scatter(
                    x=pdf_viz["month_date"], 
                    y=pdf_viz["ma_expense"],
                    mode='lines',
                    name="Moyenne Dépenses (3 mois)",
                    line=dict(color='#172B4D', width=3, dash='dot') 
                ))

                fig_combo.update_layout(
                    barmode='group', 
                    xaxis=dict(
                        tickformat="%b %y", 
                        dtick="M1",         
                        tickangle=-45       
                    ),
                    legend=dict(
                        orientation="h",    
                        yanchor="bottom", y=1.02,
                        xanchor="right", x=1
                    ),
                    margin=dict(t=20, b=40, l=20, r=20),
                    height=450
                )

                st.plotly_chart(fig_combo, width="stretch")
            else:
                st.info("Pas de données sur la période sélectionnée.")

# --- PAGE 3: PATRIMOINE & INVEST ---
elif page == "Patrimoine & Bourse":
    st.header("📈 Évolution du Patrimoine")
    
    # 1. Préparation des données pour les menus déroulants
    acc_df = get_accounts()
    
    # Configuration par défaut
    account_options = []
    accounts_ready = False
    default_index = 0  # Par défaut, le premier de la liste
    
    if not acc_df.is_empty():
        account_options = acc_df["name"].to_list()
        accounts_ready = True
        
        # --- AJOUT : LOGIQUE DE PRÉ-SÉLECTION ---
        target_default = "PEA Bourse Direct" # Le nom exact que vous cherchez
        if target_default in account_options:
            default_index = account_options.index(target_default)
        # ----------------------------------------
    else:
        account_options = ["⚠️ Aucun compte trouvé"]
        accounts_ready = False

    # 2. Formulaire d'ajout
    with st.expander("➕ Ajouter une transaction Bourse (Achat/Vente)", expanded=False):
        
        # On utilise st.form pour éviter le rechargement à chaque clic
        with st.form("invest_form"):
            st.caption("Saisissez les détails de l'ordre exécuté.")
            
            f1, f2, f3, f4 = st.columns(4)
            
            with f1:
                i_date = st.date_input("Date de l'exécution")
                # Menu strict pour l'action
                action_label = st.selectbox("Type d'opération", ["Achat (BUY)", "Vente (SELL)"])
                # On traduit l'affichage en valeur DB
                i_action = "BUY" if "Achat" in action_label else "SELL"
                
            with f2:
                i_ticker = st.text_input("Ticker (ex: CW8.PA)", value="CW8.PA")
                i_name = st.text_input("Nom du produit", value="Amundi MSCI World")
                
            with f3:
                i_qty = st.number_input("Quantité", min_value=0.0001, step=1.0, format="%.4f")
                i_price = st.number_input("Prix Unitaire", min_value=0.0001, step=0.1, format="%.2f")
                
            with f4:
                # On ajoute le paramètre index=default_index
                i_acc = st.selectbox(
                    "Compte impacté (Cash)", 
                    account_options, 
                    index=default_index,
                    disabled=not accounts_ready
                )
                i_fees = st.number_input("Frais totaux (€)", min_value=0.0, step=0.1, format="%.2f")

            i_comm = st.text_input("Commentaire (Optionnel)")
            
            # Bouton de validation
            submit_btn = st.form_submit_button("Enregistrer l'investissement", type="primary")
            
            if submit_btn:
                # Vérification bloquante avant sauvegarde
                if not accounts_ready:
                    st.error("Impossible d'enregistrer : aucun compte bancaire n'est disponible. Veuillez importer un fichier Excel dans l'onglet 'Import / Données'.")
                else:
                    # Gestion du commentaire vide (évite erreur SQL)
                    safe_comm = i_comm if i_comm and i_comm.strip() != "" else ""
                    
                    save_investment(i_date, i_ticker, i_name, i_action, i_qty, i_price, i_fees, i_acc, safe_comm)
                    st.success("Transaction enregistrée avec succès !")
                    st.rerun()

    # 3. Tableau Historique (Collapsible)
    with st.expander("📜 Historique des transactions produits financiers", expanded=False):
        # Nécessite d'avoir importé get_investments_df depuis src.queries
        # Si vous ne l'avez pas fait, ajoutez "from src.queries import get_investments_df" en haut du fichier
        try:
            df_inv_hist = get_investments_df()
            if not df_inv_hist.is_empty():
                st.dataframe(
                    df_inv_hist.to_pandas(),
                    width="stretch",
                    column_config={
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
                    hide_index=True
                )
            else:
                st.info("Aucune transaction enregistrée.")
        except NameError:
            st.error("Fonction 'get_investments_df' manquante. Vérifiez src/queries.py.")

    st.divider()
    
    # 4. Calculs et Graphiques (Reste inchangé)
    with st.spinner("Calcul de l'évolution du patrimoine..."):
        df_wealth = calculate_wealth_evolution()
    
    if df_wealth.is_empty():
        st.info("Pas assez de données pour générer le graphique.")
    else:
        # ... (Le code existant pour le graphique reste ici) ...
        # Copiez-collez la fin de votre fichier original ici (gestion wealth_start, graphique Plotly, etc.)
        if "wealth_start" not in st.session_state:
            st.session_state["wealth_start"] = df_wealth["date"].min()
        if "wealth_end" not in st.session_state:
            st.session_state["wealth_end"] = date.today()

        def update_wealth_range(days=None, start=None, end=None):
            target_end = end if end else date.today()
            target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
            st.session_state["wealth_start"] = target_start
            st.session_state["wealth_end"] = target_end

        with st.container():
            st.subheader("📅 Période d'analyse")
            col_shortcuts, col_pickers = st.columns([3, 2], gap="large")
            with col_shortcuts:
                st.caption("Raccourcis rapides")
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("1 Mois", key="w_1m", width="stretch"):
                    update_wealth_range(days=30)
                if b2.button("6 Mois", key="w_6m", width="stretch"):
                    update_wealth_range(days=180) 
                if b3.button("YTD", key="w_ytd", width="stretch"):
                    update_wealth_range(start=date(date.today().year, 1, 1))
                if b4.button("Tout", key="w_all", width="stretch"):
                    update_wealth_range(start=df_wealth["date"].min())

                years = sorted(df_wealth["date"].dt.year().unique().to_list(), reverse=True)
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

        df_viz = df_wealth.filter((pl.col("date") >= w_start) & (pl.col("date") <= w_end))
        
        if df_viz.is_empty():
            st.warning("Aucune donnée sur cette période.")
        else:
            pdf_wealth = df_viz.to_pandas()
            
            st.write("---")
            col_opts, col_metrics = st.columns([2, 1])
            
            # Identify account columns (exclude metadata columns)
            excluded_cols = ['date', 'Total Wealth', 'Total Invest']
            account_cols = [c for c in pdf_wealth.columns if c not in excluded_cols]
            
            with col_opts:
                chart_mode = st.radio(
                    "Mode d'affichage", 
                    ["Total", "Détail"], 
                    horizontal=True
                )
                
                # UPDATE 1: Allow selection for both Detail and Comparison modes
                selected_accounts = account_cols # Default to all
                
                if chart_mode in ["Détail"]:
                    selected_accounts = st.multiselect(
                        "Comptes à inclure dans le calcul et le graphique", 
                        account_cols, 
                        default=account_cols
                    )

            with col_metrics:
                # UPDATE 2: Dynamic calculation of the metric
                if chart_mode == "Total":
                    # In Global mode, we usually look at the absolute total
                    current_series = pdf_wealth['Total Wealth']
                else:
                    # In Detail/Comparison, we sum ONLY the selected accounts
                    if selected_accounts:
                        current_series = pdf_wealth[selected_accounts].sum(axis=1)
                    else:
                        current_series = pd.Series([0.0] * len(pdf_wealth))

                # Safe access to values
                if not current_series.empty:
                    last_val = current_series.iloc[-1]
                    first_val = current_series.iloc[0]
                else:
                    last_val = 0.0
                    first_val = 0.0
                    
                delta = last_val - first_val
                
                # Dynamic Label based on mode
                metric_label = "Patrimoine (Sélection)" if chart_mode != "Total" else "Patrimoine Net Total"
                st.metric(metric_label, f"{last_val:,.2f} €", delta=f"{delta:,.2f} €")

            # UPDATE 3: Generate the chart based on the selection
            fig = go.Figure()

            if chart_mode == "Total":
                fig.add_trace(go.Scatter(
                    x=pdf_wealth['date'], y=pdf_wealth['Total Wealth'], 
                    mode='lines', name='Patrimoine Net',
                    line=dict(color='#636EFA', width=4),
                    fill='tozeroy', fillcolor='rgba(99, 110, 250, 0.1)' 
                ))
                # Optional: Show investment part if available
                if "Total Invest" in pdf_wealth.columns:
                    fig.add_trace(go.Scatter(
                        x=pdf_wealth['date'], y=pdf_wealth['Total Invest'], 
                        mode='lines', name='Dont Investissement',
                        line=dict(color='gold', width=2, dash='dash')
                    ))

            elif chart_mode == "Détail":
                # Stack only selected accounts
                if selected_accounts:
                    for col in selected_accounts:
                        fig.add_trace(go.Scatter(
                            x=pdf_wealth['date'], y=pdf_wealth[col],
                            mode='lines', stackgroup='one', name=col
                        ))
                else:
                    # Handle empty selection
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
    

    
# =========================================================
# PAGE: CARTE DU MARCHÉ (CORRECTED WITH KEYERROR FIX)
# =========================================================
elif page == "Carte du Marché":
    st.header("🗺️ Carte Thermique du Patrimoine (Treemap)")

    # 1. Controls
    # ----------------------------------------------------------------
    col_ctrl1, col_ctrl2 = st.columns([1, 3])

    with col_ctrl1:
        mode = st.selectbox(
            "Mode d'affichage",
            [
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

        def update_map_date_range(days=None, start=None, end=None):
            target_end = end if end else date.today()
            target_start = start if start else (target_end - timedelta(days=days) if days else target_end)
            st.session_state["map_start"] = target_start
            st.session_state["map_end"] = target_end
        
        conn = sqlite3.connect(get_db_path())
        try:
            min_date_db = pd.read_sql("SELECT MIN(date) as min_date FROM investments", conn)["min_date"].iloc[0]
            if pd.isna(min_date_db): global_min_date = date(2020, 1, 1)
            else: global_min_date = pd.to_datetime(min_date_db).date()
        except:
            global_min_date = date(2020, 1, 1)
        conn.close()

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

    # 2. Data Processing
    # ----------------------------------------------------------------
    with st.spinner("Génération de la carte..."):
        df_end = get_detailed_snapshot(date_end)
        
        if df_end.empty:
            st.warning("Aucune donnée disponible à la date de fin sélectionnée.")
        else:
            viz_df = df_end.copy()
            path_cols = []
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
                
                conn = sqlite3.connect(get_db_path())
                cost_basis_query = """
                    SELECT account, ticker, SUM(quantity * unit_price + fees) as cost_basis
                    FROM investments
                    WHERE action = 'BUY' AND date > ? AND date <= ?
                    GROUP BY account, ticker
                """
                df_cost_basis = pd.read_sql(cost_basis_query, conn, params=(date_start, date_end))
                conn.close()
                
                df_cost_basis = df_cost_basis.rename(columns={"account": "Account", "ticker": "Ticker"})

                df_start_slim = df_start[["Account", "Ticker", "Value"]].rename(columns={"Value": "Value_Start"})
                viz_df = viz_df.merge(df_start_slim, on=["Account", "Ticker"], how="left")
                viz_df = viz_df.merge(df_cost_basis, on=["Account", "Ticker"], how="left")
                viz_df.fillna({"Value_Start": 0, "cost_basis": 0}, inplace=True)
                
                def get_adjusted_start(row):
                    if row['Value_Start'] < 0.01 and row['cost_basis'] > 0:
                        return row['cost_basis']
                    return row['Value_Start']
                
                viz_df['Value_Start_Adjusted'] = viz_df.apply(get_adjusted_start, axis=1)

                def calc_perf(row):
                    start_val = row["Value_Start_Adjusted"]
                    net_invested = row.get("cost_basis", 0.0) # Assume cost_basis is Net Cashflows (Buys - Sells)
                    
                    # Total money put in = Starting value + new cash injected
                    total_in = start_val + net_invested
                    
                    if total_in < 0.01: return 0.0
                    
                    end_val = row["Value"]
                    
                    # Profit = Final Value - What we started with - What we added
                    profit = end_val - start_val - net_invested
                    
                    return (profit / total_in) * 100
                viz_df["Performance %"] = viz_df.apply(calc_perf, axis=1)

                if mode == "Performance (Évolution) - Actions":
                    viz_df = viz_df[viz_df['Type'] == 'Investissement'].copy()

                path_cols = ["Account", "Name"]
                hovertemplate = "<b>%{label}</b><br>Valeur: %{value:,.0f} €<br>Perf: %{customdata[0]:.2f}%<extra></extra>"
                custom_data_cols = ['Performance %']
                
                max_abs_perf = viz_df["Performance %"].abs().max()
                if pd.isna(max_abs_perf) or max_abs_perf == 0: max_abs_perf = 1.0

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

  

