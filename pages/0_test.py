"""Minimal test page - hardcoded values only."""
import streamlit as st

st.set_page_config(page_title="Test", page_icon="🧪")
st.title("🧪 Test Page - Hardcoded Values")

# Hardcoded values
HOST = "jeen-pg-dev-weu.postgres.database.azure.com"
DATABASE = "postgres"
USERNAME = "jeen_pg_dev_admin"

st.write(f"HOST variable = {HOST}")
st.write(f"DATABASE variable = {DATABASE}")
st.write(f"USERNAME variable = {USERNAME}")

st.subheader("Form with hardcoded values")

with st.form("test_form"):
    host = st.text_input("Host", value=HOST)
    database = st.text_input("Database", value=DATABASE)
    username = st.text_input("Username", value=USERNAME)
    
    if st.form_submit_button("Submit"):
        st.write(f"Submitted: {host}, {database}, {username}")
