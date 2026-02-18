"""
Debug script - traces exactly what happens when the page loads.
Run this INSTEAD of the Streamlit app to see what values are loaded.
"""
import os
import sys

print("=" * 60)
print("DEBUG: Simulating page load")
print("=" * 60)

# Step 1: Check current directory
print(f"\n[1] Current directory: {os.getcwd()}")

# Step 2: Check __file__ equivalent
script_path = os.path.abspath(__file__)
print(f"[2] This script path: {script_path}")

# Step 3: Simulate the BASE_DIR calculation from the page
# In pages/1_connect.py: BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pages_dir = os.path.dirname(os.path.dirname(script_path))  # tests -> db-migrator
base_dir_simulated = pages_dir
print(f"[3] Simulated BASE_DIR (from tests/): {base_dir_simulated}")

# But the actual page is in pages/, so let's simulate that
actual_page_path = os.path.join(pages_dir, "pages", "1_connect.py")
print(f"[4] Actual page path: {actual_page_path}")
print(f"    Exists: {os.path.exists(actual_page_path)}")

# What BASE_DIR would be from pages/1_connect.py
actual_base_dir = os.path.dirname(os.path.dirname(actual_page_path))
print(f"[5] BASE_DIR from pages/1_connect.py: {actual_base_dir}")

# Step 4: Check .env file
env_path = os.path.join(actual_base_dir, ".env")
print(f"\n[6] .env path: {env_path}")
print(f"    Exists: {os.path.exists(env_path)}")

# Step 5: Read .env with dotenv_values
print("\n[7] Reading .env with dotenv_values...")
from dotenv import dotenv_values
config = dotenv_values(env_path)
print(f"    Keys found: {list(config.keys())}")
print(f"    SOURCE_DB_HOST: {config.get('SOURCE_DB_HOST', 'NOT FOUND')}")
print(f"    SOURCE_DB_DATABASE: {config.get('SOURCE_DB_DATABASE', 'NOT FOUND')}")
print(f"    SOURCE_DB_USERNAME: {config.get('SOURCE_DB_USERNAME', 'NOT FOUND')}")

# Step 6: Check secrets.toml
secrets_path = os.path.join(actual_base_dir, ".streamlit", "secrets.toml")
print(f"\n[8] secrets.toml path: {secrets_path}")
print(f"    Exists: {os.path.exists(secrets_path)}")

# Step 7: Try importing the page module to see what happens
print("\n[9] Attempting to import page module...")
sys.path.insert(0, actual_base_dir)

# Don't actually import (it will start Streamlit), just check the file
print(f"\n[10] Reading 1_connect.py to check BASE_DIR definition...")
with open(actual_page_path, 'r') as f:
    content = f.read()
    
# Find BASE_DIR definition
for i, line in enumerate(content.split('\n'), 1):
    if 'BASE_DIR' in line and '=' in line:
        print(f"    Line {i}: {line.strip()}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Expected .env values to load:")
print(f"  host: {config.get('SOURCE_DB_HOST')}")
print(f"  database: {config.get('SOURCE_DB_DATABASE')}")  
print(f"  username: {config.get('SOURCE_DB_USERNAME')}")
print(f"  prefix: {config.get('TABLE_PREFIX')}")
