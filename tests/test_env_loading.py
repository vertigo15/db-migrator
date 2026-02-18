"""
Run with: python tests/test_env_loading.py
Make sure no Streamlit instance is running on port 8501 before running.
Selenium test to verify .env values are loaded correctly in the Streamlit app.
"""
import os
import time
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from utils.config import get_env_connection_defaults, get_env_table_prefix

import subprocess
import signal


def start_streamlit():
    """Start Streamlit server in background."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app_path = os.path.join(base_dir, "app.py")
    
    process = subprocess.Popen(
        ["streamlit", "run", app_path, "--server.headless=true", "--server.port=8501"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
    )
    return process


def stop_streamlit(process):
    """Stop Streamlit server."""
    if process:
        if os.name == 'nt':
            process.terminate()
        else:
            process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)


def test_env_values_loaded():
    """Test that .env values are loaded into the connection form."""
    
    # Get expected values from .env
    expected = get_env_connection_defaults()
    expected_prefix = get_env_table_prefix()
    
    print("Expected values from .env:")
    print(f"  Host: {expected['host']}")
    print(f"  Port: {expected['port']}")
    print(f"  Database: {expected['database']}")
    print(f"  Username: {expected['username']}")
    print(f"  Table Prefix: {expected_prefix}")
    print()
    
    # Setup Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Uncomment for headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    
    # Initialize driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    
    try:
        # Clear localStorage BEFORE loading the page
        # First load a blank page to set localStorage
        driver.get("http://localhost:8501")
        time.sleep(2)
        
        print("Clearing localStorage...")
        driver.execute_script("""
            for (let i = localStorage.length - 1; i >= 0; i--) {
                const key = localStorage.key(i);
                if (key && key.startsWith('db_migrator_')) {
                    localStorage.removeItem(key);
                }
            }
        """)
        
        # Now navigate to the Connect page with fresh localStorage
        url = "http://localhost:8501/connect"
        print(f"Opening {url}...")
        driver.get(url)
        
        # Wait for Streamlit to fully load
        time.sleep(5)
        
        # Look for debug output
        try:
            debug_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ENV_DEFAULTS')]")
            for el in debug_elements:
                print(f"DEBUG FOUND: {el.text}")
        except:
            print("Could not find debug elements")
        
        # Wait for form inputs to be present
        wait = WebDriverWait(driver, 15)
        
        # Find input fields by their labels
        # Streamlit generates inputs with data-testid attributes
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'], input[type='number']")
        
        print(f"Found {len(inputs)} input fields")
        
        # Get actual values from form
        actual_values = {}
        for inp in inputs:
            value = inp.get_attribute("value")
            placeholder = inp.get_attribute("placeholder") or ""
            aria_label = inp.get_attribute("aria-label") or ""
            
            # Try to identify field by placeholder or nearby label
            if "localhost" in placeholder.lower() or "host" in aria_label.lower():
                actual_values["host"] = value
            elif "my_database" in placeholder.lower() or "database" in aria_label.lower():
                actual_values["database"] = value
            elif "postgres" in placeholder.lower() or "username" in aria_label.lower():
                actual_values["username"] = value
            elif "jeen_dev" in placeholder.lower() or "prefix" in aria_label.lower():
                actual_values["table_prefix"] = value
            
            print(f"  Input: value='{value}', placeholder='{placeholder}'")
        
        print()
        print("Actual values in form:")
        for k, v in actual_values.items():
            print(f"  {k}: {v}")
        
        # Verify values
        print()
        print("Verification:")
        all_passed = True
        
        if actual_values.get("host") == expected["host"]:
            print(f"  ✅ Host matches: {expected['host']}")
        else:
            print(f"  ❌ Host mismatch: expected '{expected['host']}', got '{actual_values.get('host')}'")
            all_passed = False
        
        if actual_values.get("database") == expected["database"]:
            print(f"  ✅ Database matches: {expected['database']}")
        else:
            print(f"  ❌ Database mismatch: expected '{expected['database']}', got '{actual_values.get('database')}'")
            all_passed = False
        
        if actual_values.get("username") == expected["username"]:
            print(f"  ✅ Username matches: {expected['username']}")
        else:
            print(f"  ❌ Username mismatch: expected '{expected['username']}', got '{actual_values.get('username')}'")
            all_passed = False
        
        if actual_values.get("table_prefix") == expected_prefix:
            print(f"  ✅ Table Prefix matches: {expected_prefix}")
        else:
            print(f"  ❌ Table Prefix mismatch: expected '{expected_prefix}', got '{actual_values.get('table_prefix')}'")
            all_passed = False
        
        print()
        if all_passed:
            print("🎉 ALL TESTS PASSED!")
        else:
            print("⚠️ SOME TESTS FAILED")
        
        return all_passed
        
    finally:
        driver.quit()


if __name__ == "__main__":
    print("=" * 60)
    print("ENV Loading Test")
    print("=" * 60)
    
    # Start fresh Streamlit instance
    print("Starting fresh Streamlit server...")
    streamlit_process = start_streamlit()
    
    try:
        # Wait for server to be ready
        time.sleep(6)
        success = test_env_values_loaded()
    finally:
        print("\nStopping Streamlit server...")
        stop_streamlit(streamlit_process)
    
    sys.exit(0 if success else 1)
