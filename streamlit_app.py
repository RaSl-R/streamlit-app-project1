import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from passlib.context import CryptContext
import os

# Připojení k databázi (Neon nebo jiná PostgreSQL)
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_NAME = st.secrets["DB_NAME"]

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Nastavení kontextu pro bezpečné hashování hesel
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def check_login(email: str, password: str, conn) -> str | None:
    result = conn.execute(text("SELECT password_hash, role FROM auth.users WHERE email = :email"), {"email": email}).fetchone()
    if not result:
        return None
    stored_hash, role = result
    if verify_password(password, stored_hash):
        return role
    return None

def login_form():
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Heslo", type="password")
        submitted = st.form_submit_button("Přihlásit")
    if submitted:
        with engine.begin() as conn:
            role = check_login(email, password, conn)
        if role:
            st.session_state.logged_in = True
            st.session_state.user_email = email
            st.session_state.user_role = role
            st.success(f"Přihlášen jako {email} ({role})")
        else:
            st.error("Neplatné přihlašovací údaje")

def logout():
    if st.button("Odhlásit", use_container_width=True):
        st.session_state.clear()
        st.rerun()

def viewer_ui():
    st.subheader("Zobrazení dat")
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM cars.vehicles ORDER BY id", conn)
    st.dataframe(df)

def editor_ui():
    viewer_ui()
    st.subheader("Import dat (jen pro editory)")
    uploaded_file = st.file_uploader("Vyber CSV soubor", type="csv")
    if uploaded_file and st.button("Importovat", use_container_width=True):
        df = pd.read_csv(uploaded_file)
        with engine.begin() as conn:
            df.to_sql("vehicles", conn, schema="cars", if_exists="append", index=False)
        st.success("Data importována.")

def main():
    st.title("Databázová aplikace s autentizací")

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        logout()
        role = st.session_state.user_role
        if role == "viewer":
            viewer_ui()
        elif role == "editor":
            editor_ui()
        else:
            st.error("Neznámá role")
    else:
        login_form()

if __name__ == "__main__":
    main()
