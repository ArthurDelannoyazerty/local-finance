import streamlit as st
import polars as pl
import plotly.express as px
from src.database import init_db
from src.importer import import_excel_file 
from src.queries import get_transactions_df, update_exclusion, get_daily_balance_evolution

# Initialize DB on startup
init_db()

st.set_page_config(page_title="Local Finance", layout="wide")

# Sidebar
st.sidebar.title("💰 Local Finance")
page = st.sidebar.radio("Menu", ["Tableau de Bord", "Import Données", "Investissements"])

# --- PAGE: IMPORT ---
if page == "Import Données":
    st.header("Importer le fichier Excel Global")
    
    st.markdown("""
    Le fichier doit être un **.xlsx** contenant obligatoirement les feuilles :
    - `Revenus`
    - `Dépenses`
    - `Transferts`
    """)
    
    uploaded_file = st.file_uploader("Choisir le fichier Excel", type=["xlsx"])
    
    if uploaded_file and st.button("Lancer l'importation"):
        with st.spinner("Traitement du fichier Excel en cours..."):
            try:
                stats = import_excel_file(uploaded_file)
                st.success("Importation terminée avec succès !")
                
                # Show results in nice columns
                c1, c2, c3 = st.columns(3)
                c1.metric("Revenus ajoutés", stats["Revenus"])
                c2.metric("Dépenses ajoutées", stats["Dépenses"])
                c3.metric("Transferts ajoutés", stats["Transferts"])
                
            except Exception as e:
                st.error(f"Erreur lors de l'import : {str(e)}")
                
# --- PAGE: DASHBOARD ---
elif page == "Tableau de Bord":
    st.header("Analyse Dépenses & Revenus")
    
    # 1. Load Data
    df = get_transactions_df()
    
    if df.is_empty():
        st.warning("Aucune donnée. Veuillez importer des fichiers.")
    else:
        # 2. Outlier Detection Logic
        st.subheader("🔍 Validation des transactions")
        
        # We create a threshold slider
        threshold = st.slider("Seuil de détection 'Grosse Dépense' (€)", 100, 5000, 1000)
        
        # Pre-calculate potential outliers for UI highlight (Visual only)
        # In Polars, we convert to Pandas for Streamlit editor compatibility (Streamlit plays nicer with Pandas for editing)
        pdf = df.to_pandas()
        
        # Configure the editor
        # Users can toggle 'is_excluded' here
        edited_df = st.data_editor(
            pdf,
            column_config={
                "is_excluded": st.column_config.CheckboxColumn("Ignorer ?", help="Exclure des stats"),
                "amount": st.column_config.NumberColumn("Montant", format="%.2f €"),
                "id": None # Hide ID
            },
            disabled=["date", "category", "account", "amount", "currency", "comment", "type"],
            hide_index=True,
            key="data_editor"
        )
        
        # Update DB if changes occurred
        # (Naive implementation: In a real app, compare changes. Here we just update rows that changed)
        if st.button("Sauvegarder les exclusions"):
            # Iterate and update
            # This is slow for big data, better to use session state diffs, but works for local
            for index, row in edited_df.iterrows():
                # Only update if different from original (optimization needed here for scale)
                current_val = row['is_excluded']
                original_val = df.filter(pl.col("id") == row['id']).select("is_excluded").item()
                
                if bool(current_val) != bool(original_val):
                    update_exclusion(row['id'], bool(current_val))
            st.rerun()

        # 3. Filter for Charts
        df_clean = pl.from_pandas(edited_df).filter(pl.col("is_excluded") == 0)
        
        st.divider()
        
        # 4. Charts
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("Répartition Dépenses")
            expenses = df_clean.filter(pl.col("type") == "EXPENSE")
            if not expenses.is_empty():
                grouped = expenses.group_by("category").agg(pl.col("amount").sum())
                fig = px.pie(grouped.to_pandas(), values='amount', names='category', hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            st.subheader("Évolution Solde Cash")
            # Calculate cumulative sum
            # Note: This graph is relative to the start date (doesn't know initial bank balance)
            daily = get_daily_balance_evolution()
            if not daily.is_empty():
                # Cumulative sum per account
                daily = daily.with_columns(
                    pl.col("daily_change").cum_sum().over("account").alias("balance")
                )
                fig_line = px.line(daily.to_pandas(), x="date", y="balance", color="account", markers=True)
                st.plotly_chart(fig_line, use_container_width=True)

# --- PAGE: INVESTISSEMENTS ---
elif page == "Investissements":
    st.header("Suivi Portefeuille (PEA / CTO)")
    st.info("Fonctionnalité à venir : Ajout des tickers et connexion Yahoo Finance.")
    
    # Skeleton for manual entry
    with st.expander("Ajouter un achat/vente manuellement"):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.date_input("Date")
            st.text_input("Ticker (ex: CW8.PA)")
        with c2:
            st.number_input("Quantité")
            st.number_input("Prix Unitaire")
        with c3:
            st.selectbox("Compte", ["PEA Bourse Direct", "CTO Trade Republic"])
            st.button("Enregistrer Transaction")