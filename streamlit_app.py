import streamlit as st
import pandas as pd
from snowflake.snowpark import Session, context
from datetime import datetime

# Inicializace session
@st.cache_resource
def get_session():
    return context.get_active_session()

# Výpis schémat
@st.cache_data
def list_schemas(_session):
    result = _session.sql("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA").collect()
    return [row['SCHEMA_NAME'] for row in result]

# Výpis tabulek
@st.cache_data
def list_tables(_session, schema_name):
    if not schema_name:
        return []
    result = _session.sql(f"""
        SELECT TABLE_SCHEMA AS TABLE_SCHEMA, TABLE_NAME AS TABLE_NAME, TABLE_SCHEMA || '.' || TABLE_NAME AS TABLE_ID
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_name}'
    """).collect()
    return {row['TABLE_NAME']: row['TABLE_ID'] for row in result}

# Načtení dat z tabulky s volitelným filtrem
def load_table_filtered(_session, table_id, where=None):
    tbl = _session.table(table_id)
    if where:
        tbl = tbl.filter(where)
    return tbl.to_pandas()

# Výchozí načtení dat
@st.cache_data(ttl=3600)
def load_table(_session, table_id):
    return _session.table(table_id).to_pandas()

# Přepsání celé tabulky
def replace_table(session, table_id, df):
    schema_name, table_id = table_id.split('.', 1)
    session.write_pandas(df, table_id, schema=schema_name, overwrite=True)

# Funkce pro vykreslení data editoru s dynamickým klíčem
def display_data_editor(df_to_edit, editor_key):
    edited_df = st.data_editor(
        df_to_edit,
        num_rows="dynamic",
        use_container_width=True,
        key=editor_key
    )
    return edited_df

# Callback pro zrušení filtru
def clear_filter_callback():
    st.session_state.where_input = ""
    st.session_state.where_clause = ""
    st.session_state.filter_applied = False
    st.session_state.reload_data = True

# Aplikace
def main():
    st.set_page_config(layout="wide")
    st.title("📊 Data browser")

    session = get_session()

    # Zpráva (např. COMMIT / ROLLBACK)
    if "message" in st.session_state:
        st.success(st.session_state.message)
        del st.session_state.message

    # Inicializace stavu
    if "editor_key_counter" not in st.session_state:
        st.session_state.editor_key_counter = 0
    if "filter_applied" not in st.session_state:
        st.session_state.filter_applied = False
    if "where_clause" not in st.session_state:
        st.session_state.where_clause = ""

    # Výběr schématu a tabulky
    schemas = list_schemas(session)
    selected_schema = st.selectbox(
        "📁 Vyber schéma",
        schemas,
        index=schemas.index("L_META") if "L_META" in schemas else 0,
        key="selected_schema"
    )

    tables_dict = list_tables(session, selected_schema)

    if not tables_dict:
        st.info("Zvolené schéma neobsahuje žádnou tabulku.")
        st.stop()

    selected_table_name = st.selectbox("📂 Vyber tabulku", options=list(tables_dict.keys()))
    selected_table_id = tables_dict[selected_table_name]

    if not selected_table_id:
        st.info("Nebyla vybrána žádná validní tabulka.")
        st.stop()
        
    if selected_table_id:
        st.subheader(f"Obsah tabulky: `{selected_table_name}`")

        st.markdown("<style>div.stHorizontalBlock {align-items: end;}</style>", unsafe_allow_html=True)      
        col_expander, col2, col3, _, _ = st.columns([2.5, 1, 1, 0.5, 0.5])

        # WHERE-like filtr s podbarvením a dynamickým popiskem
        with col_expander:
            expander_label = "🔍 Filtrováno" if st.session_state.filter_applied else "🔍 Filtr"
            expander_style = (
                "background-color: rgba(255, 255, 0, 0.1); border-radius: 5px;"
                if st.session_state.filter_applied else ""
            )
            st.markdown(f"<style>div.stExpander:nth-of-type(2), div.stExpander:nth-of-type(6), div.stExpander:nth-of-type(7), div.stExpander:nth-of-type(8) {{ {expander_style} }}</style>", unsafe_allow_html=True)

            with st.expander(expander_label):
                where_clause = st.text_input(
                    "Zadej WHERE podmínku (bez klíčového slova 'WHERE')",
                    placeholder="např. amount > 100 AND status = 'active'",
                    key="where_input"
                )
                col_clear_btn, col_filter_btn = st.columns(2)
                with col_clear_btn:
                    clear_filter = st.button("❌ Zrušit filtr", key="clear_filter_button", on_click=clear_filter_callback)
                with col_filter_btn:
                    apply_filter = st.button("🔽 Filtrovat", key="filter_button")

        # Načítání dat – podle filtru nebo běžně
        if "reload_data" not in st.session_state:
            st.session_state.reload_data = True

        df = None

        if apply_filter and where_clause:
            st.session_state.where_clause = where_clause
            st.session_state.filter_applied = True
            st.session_state.reload_data = True
            st.rerun()

        elif st.session_state.reload_data:
            if st.session_state.filter_applied and st.session_state.where_clause:
                df = load_table_filtered(session, selected_table_id, st.session_state.where_clause)
            else:
                df = load_table(session, selected_table_id)
            st.session_state.reload_data = False

        if df is None:
            df = load_table(session, selected_table_id)

        # Zobrazíme editor s dynamickým klíčem
        editor_key = f"editor_{st.session_state.editor_key_counter}"
        edited_df = display_data_editor(df, editor_key)

        # ROLLBACK
        if col2.button("🔁 ROLLBACK", use_container_width=True):
            load_table.clear()
            st.session_state.reload_data = True
            st.session_state.editor_key_counter += 1
            st.session_state.message = "Změny byly zahozeny (ROLLBACK) – data byla znovu načtena z databáze."
            st.rerun()

        # COMMIT
        if col3.button("💾 COMMIT", use_container_width=True):
            try:
                datetime_cols = edited_df.select_dtypes(include=['datetime64[ns]']).columns
                for col in datetime_cols:
                    edited_df[col] = pd.to_datetime(edited_df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

                replace_table(session, selected_table_id, edited_df)
                load_table.clear()
                st.session_state.reload_data = True
                st.session_state.editor_key_counter += 1
                st.session_state.message = "Změny byly uloženy (COMMIT)."
                st.rerun()
            except Exception as e:
                st.error(f"Chyba při COMMITu: {e}")

        # Export CSV
        with st.expander("⬇️ Export do CSV"):
            csv = edited_df.to_csv(index=False).encode('utf-8')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{selected_table_name}_{timestamp}.csv"
            st.download_button(
                "📥 Stáhnout aktuální pohled jako CSV",
                csv,
                file_name=file_name,
                mime='text/csv'
            )

        # Import CSV
        with st.expander("⬆️ Import CSV – přepsání tabulky"):
            uploaded_file = st.file_uploader("Vyber CSV soubor", type="csv")
            if uploaded_file:
                try:
                    imported_df = pd.read_csv(uploaded_file)
                    st.dataframe(imported_df, use_container_width=True)
                    if st.button("🚨 Nahradit celou tabulku importovanými daty"):
                        replace_table(session, selected_table_id, imported_df)
                        load_table.clear()
                        st.session_state.reload_data = True
                        st.session_state.editor_key_counter += 1
                        st.session_state.message = "Tabulka byla nahrazena."
                        st.rerun()
                except Exception as e:
                    st.error(f"Chyba při importu: {e}")

if __name__ == "__main__":
    main()