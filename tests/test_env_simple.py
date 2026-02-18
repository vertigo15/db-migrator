"""
Simplified Selenium test - just checks if values appear on page.
"""
import os
import sys
import time
import subprocess

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Expected values (from .streamlit/secrets.toml)
EXPECTED_HOST = "jeen-pg-dev-weu.postgres.database.azure.com"
EXPECTED_DB = "postgres"
EXPECTED_USER = "jeen_pg_dev_admin"


def main():
    print("Starting Streamlit server...")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app_path = os.path.join(base_dir, "app.py")
    
    proc = subprocess.Popen(
        ["streamlit", "run", app_path, "--server.headless=true"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    try:
        time.sleep(6)  # Wait for server
        
        # Setup browser
        options = Options()
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=options)
        
        try:
            # Try the renamed page
            driver.get("http://localhost:8501/source_connect")
            time.sleep(5)
            
            # Save screenshot
            screenshot_path = os.path.join(base_dir, "test_screenshot.png")
            driver.save_screenshot(screenshot_path)
            print(f"Screenshot saved to: {screenshot_path}")
            
            # Get page source and check if expected values are present
            page_source = driver.page_source
            
            print("\n=== Checking for expected values in page ===")
            
            if EXPECTED_HOST in page_source:
                print(f"✅ Found host: {EXPECTED_HOST}")
            else:
                print(f"❌ Host NOT found: {EXPECTED_HOST}")
                
            if EXPECTED_DB in page_source:
                print(f"✅ Found database: {EXPECTED_DB}")
            else:
                print(f"❌ Database NOT found: {EXPECTED_DB}")
                
            if EXPECTED_USER in page_source:
                print(f"✅ Found username: {EXPECTED_USER}")
            else:
                print(f"❌ Username NOT found: {EXPECTED_USER}")
            
            # Check for debug output
            if "DEBUG - ENV_HOST=" in page_source:
                import re
                match = re.search(r'DEBUG - ENV_HOST=([^,]+), ENV_DB=([^,]+), ENV_USER=([^"<]+)', page_source)
                if match:
                    print(f"\n=== DEBUG OUTPUT FOUND ===")
                    print(f"  ENV_HOST: {match.group(1)}")
                    print(f"  ENV_DB: {match.group(2)}")
                    print(f"  ENV_USER: {match.group(3)}")
            else:
                print("\n❌ DEBUG output not found in page")
            
            # Also get all input values
            print("\n=== All input values found ===")
            inputs = driver.find_elements("css selector", "input")
            for i, inp in enumerate(inputs):
                val = inp.get_attribute("value")
                inp_type = inp.get_attribute("type")
                if val and inp_type != "hidden":
                    print(f"  Input {i}: type={inp_type}, value={val[:50]}...")
                    
        finally:
            driver.quit()
            
    finally:
        proc.terminate()
        print("\nStopped server.")


if __name__ == "__main__":
    main()
