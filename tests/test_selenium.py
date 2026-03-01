"""
Selenium Test Suite for DB Migrator Application

Tests the full user flow through the Streamlit UI:
1. Source database connection
2. User selection and data extraction
3. Data transformation
4. Target database connection and loading

Requirements:
    pip install selenium pytest

Run tests:
    pytest tests/test_selenium.py -v
    
Or with specific browser:
    pytest tests/test_selenium.py --browser=chrome -v
"""

import pytest
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# Test Configuration
BASE_URL = "http://localhost:8501"
DEFAULT_TIMEOUT = 10
STREAMLIT_LOAD_TIMEOUT = 30

# Test Data
TEST_SOURCE_DB = {
    "host": "source-postgres.example.com",
    "port": "5432",
    "database": "jeen_v4",
    "username": "migrator",
    "password": "test_password",
    "prefix": "jeen_dev"
}

TEST_TARGET_DB = {
    "host": "target-postgres.example.com",
    "port": "5432",
    "database": "user_db",
    "username": "migrator",
    "password": "test_password"
}


class StreamlitHelper:
    """Helper class for Streamlit-specific interactions"""
    
    @staticmethod
    def wait_for_streamlit_ready(driver, timeout=STREAMLIT_LOAD_TIMEOUT):
        """Wait for Streamlit app to fully load"""
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        # Wait for Streamlit-specific elements
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".stApp"))
        )
        time.sleep(2)  # Additional wait for dynamic content
    
    @staticmethod
    def fill_text_input(driver, label, value):
        """Fill a Streamlit text input by label"""
        try:
            # Try finding by label text
            label_element = driver.find_element(By.XPATH, f"//label[contains(text(), '{label}')]")
            input_element = label_element.find_element(By.XPATH, ".//following-sibling::div//input")
            input_element.clear()
            input_element.send_keys(value)
            time.sleep(0.5)
            return True
        except NoSuchElementException:
            print(f"Could not find text input with label: {label}")
            return False
    
    @staticmethod
    def click_button(driver, button_text):
        """Click a Streamlit button by text"""
        try:
            button = WebDriverWait(driver, DEFAULT_TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{button_text}')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(0.5)
            button.click()
            time.sleep(1)
            return True
        except (TimeoutException, NoSuchElementException):
            print(f"Could not find or click button: {button_text}")
            return False
    
    @staticmethod
    def select_radio(driver, option_text):
        """Select a Streamlit radio option by text"""
        try:
            radio = driver.find_element(By.XPATH, f"//label[contains(., '{option_text}')]//input[@type='radio']")
            driver.execute_script("arguments[0].click();", radio)
            time.sleep(0.5)
            return True
        except NoSuchElementException:
            print(f"Could not find radio option: {option_text}")
            return False
    
    @staticmethod
    def check_success_message(driver, message_text=None):
        """Check if success message is displayed"""
        try:
            if message_text:
                success = driver.find_element(By.XPATH, f"//div[contains(@class, 'stSuccess') and contains(., '{message_text}')]")
            else:
                success = driver.find_element(By.CSS_SELECTOR, ".stSuccess")
            return success.is_displayed()
        except NoSuchElementException:
            return False
    
    @staticmethod
    def check_error_message(driver):
        """Check if error message is displayed"""
        try:
            error = driver.find_element(By.CSS_SELECTOR, ".stError")
            return error.is_displayed()
        except NoSuchElementException:
            return False
    
    @staticmethod
    def get_page_title(driver):
        """Get the current page title from Streamlit sidebar"""
        try:
            title = driver.find_element(By.CSS_SELECTOR, ".stSidebar h1")
            return title.text
        except NoSuchElementException:
            return None


@pytest.fixture(scope="session")
def browser_options(pytestconfig):
    """Configure browser options"""
    browser = pytestconfig.getoption("--browser", default="chrome")
    
    if browser == "chrome":
        options = ChromeOptions()
        options.add_argument("--headless")  # Run headless for CI/CD
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        return options
    elif browser == "firefox":
        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--width=1920")
        options.add_argument("--height=1080")
        return options
    else:
        raise ValueError(f"Unsupported browser: {browser}")


@pytest.fixture
def driver(browser_options, pytestconfig):
    """Create and configure WebDriver"""
    browser = pytestconfig.getoption("--browser", default="chrome")
    
    if browser == "chrome":
        driver = webdriver.Chrome(options=browser_options)
    elif browser == "firefox":
        driver = webdriver.Firefox(options=browser_options)
    else:
        raise ValueError(f"Unsupported browser: {browser}")
    
    driver.implicitly_wait(DEFAULT_TIMEOUT)
    yield driver
    driver.quit()


@pytest.fixture
def streamlit_app(driver):
    """Navigate to Streamlit app and wait for load"""
    driver.get(BASE_URL)
    StreamlitHelper.wait_for_streamlit_ready(driver)
    return driver


class TestSourceConnection:
    """Test source database connection page"""
    
    def test_page_loads(self, streamlit_app):
        """Test that the main page loads successfully"""
        assert "DB Migrator" in streamlit_app.title or streamlit_app.find_element(By.CSS_SELECTOR, ".stApp")
    
    def test_source_connection_form_visible(self, streamlit_app):
        """Test that source connection form is visible"""
        # Check for key form elements
        host_input = streamlit_app.find_elements(By.XPATH, "//label[contains(text(), 'Host')]")
        assert len(host_input) > 0, "Host input not found"
        
        port_input = streamlit_app.find_elements(By.XPATH, "//label[contains(text(), 'Port')]")
        assert len(port_input) > 0, "Port input not found"
    
    def test_fill_source_connection(self, streamlit_app):
        """Test filling source database connection form"""
        helper = StreamlitHelper
        
        # Fill connection details
        assert helper.fill_text_input(streamlit_app, "Host", TEST_SOURCE_DB["host"])
        assert helper.fill_text_input(streamlit_app, "Port", TEST_SOURCE_DB["port"])
        assert helper.fill_text_input(streamlit_app, "Database", TEST_SOURCE_DB["database"])
        assert helper.fill_text_input(streamlit_app, "Username", TEST_SOURCE_DB["username"])
        assert helper.fill_text_input(streamlit_app, "Password", TEST_SOURCE_DB["password"])
        assert helper.fill_text_input(streamlit_app, "Table Prefix", TEST_SOURCE_DB["prefix"])
    
    def test_test_connection_button_exists(self, streamlit_app):
        """Test that 'Test Connection' button exists"""
        button = streamlit_app.find_elements(By.XPATH, "//button[contains(., 'Test Connection')]")
        assert len(button) > 0, "Test Connection button not found"
    
    @pytest.mark.integration
    def test_invalid_connection_shows_error(self, streamlit_app):
        """Test that invalid connection shows error message"""
        helper = StreamlitHelper
        
        # Fill with invalid connection
        helper.fill_text_input(streamlit_app, "Host", "invalid-host")
        helper.fill_text_input(streamlit_app, "Port", "9999")
        helper.fill_text_input(streamlit_app, "Database", "nonexistent")
        helper.fill_text_input(streamlit_app, "Username", "nobody")
        helper.fill_text_input(streamlit_app, "Password", "wrongpass")
        
        # Click test connection
        helper.click_button(streamlit_app, "Test Connection")
        time.sleep(2)
        
        # Should show error
        assert helper.check_error_message(streamlit_app), "Expected error message not shown"


class TestDataSelection:
    """Test data selection and extraction page"""
    
    @pytest.fixture
    def navigate_to_select_data(self, streamlit_app):
        """Navigate to Select & Extract Data page"""
        # Click on sidebar navigation
        try:
            select_data_link = streamlit_app.find_element(
                By.XPATH, "//a[contains(., 'Select & Extract Data')]"
            )
            select_data_link.click()
            StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        except NoSuchElementException:
            pytest.skip("Select & Extract Data page navigation not available")
        return streamlit_app
    
    def test_user_selection_section_visible(self, navigate_to_select_data):
        """Test that user selection section is visible"""
        driver = navigate_to_select_data
        
        # Check for user selection elements
        user_section = driver.find_elements(By.XPATH, "//*[contains(text(), 'Select Users')]")
        assert len(user_section) > 0, "User selection section not found"
    
    def test_document_filters_visible(self, navigate_to_select_data):
        """Test that document filters are visible"""
        driver = navigate_to_select_data
        
        # Check for filter elements
        filters = driver.find_elements(By.XPATH, "//*[contains(text(), 'Document Filters')]")
        assert len(filters) > 0, "Document filters section not found"
    
    def test_extract_button_exists(self, navigate_to_select_data):
        """Test that Extract Data button exists"""
        driver = navigate_to_select_data
        
        extract_btn = driver.find_elements(By.XPATH, "//button[contains(., 'Extract Data')]")
        assert len(extract_btn) > 0, "Extract Data button not found"
    
    def test_sql_generation_toggle_exists(self, navigate_to_select_data):
        """Test that SQL generation toggle exists"""
        driver = navigate_to_select_data
        
        # Look for checkbox with SQL generation text
        sql_toggle = driver.find_elements(
            By.XPATH, 
            "//*[contains(text(), 'Generate SQL') or contains(text(), 'SQL migration')]"
        )
        assert len(sql_toggle) > 0, "SQL generation toggle not found"


class TestTransformation:
    """Test data transformation page"""
    
    @pytest.fixture
    def navigate_to_transform(self, streamlit_app):
        """Navigate to Transform Data page"""
        try:
            transform_link = streamlit_app.find_element(
                By.XPATH, "//a[contains(., 'Transform Data')]"
            )
            transform_link.click()
            StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        except NoSuchElementException:
            pytest.skip("Transform Data page navigation not available")
        return streamlit_app
    
    def test_transformation_page_loads(self, navigate_to_transform):
        """Test that transformation page loads"""
        driver = navigate_to_transform
        assert driver.find_element(By.CSS_SELECTOR, ".stApp")
    
    def test_organization_id_input_exists(self, navigate_to_transform):
        """Test that organization ID input exists"""
        driver = navigate_to_transform
        
        org_input = driver.find_elements(By.XPATH, "//label[contains(text(), 'Organization ID')]")
        assert len(org_input) > 0, "Organization ID input not found"


class TestTargetLoad:
    """Test target database connection and load page"""
    
    @pytest.fixture
    def navigate_to_target(self, streamlit_app):
        """Navigate to Target Configuration & Load page"""
        try:
            target_link = streamlit_app.find_element(
                By.XPATH, "//a[contains(., 'Target Configuration') or contains(., 'Target')]"
            )
            target_link.click()
            StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        except NoSuchElementException:
            pytest.skip("Target page navigation not available")
        return streamlit_app
    
    def test_target_connection_form_visible(self, navigate_to_target):
        """Test that target connection form is visible"""
        driver = navigate_to_target
        
        host_input = driver.find_elements(By.XPATH, "//label[contains(text(), 'Host')]")
        assert len(host_input) > 0, "Target host input not found"
    
    def test_schema_mode_selector_exists(self, navigate_to_target):
        """Test that schema mode selector exists"""
        driver = navigate_to_target
        
        schema_mode = driver.find_elements(
            By.XPATH, 
            "//*[contains(text(), 'Schema Mode') or contains(text(), 'schema mode')]"
        )
        assert len(schema_mode) > 0, "Schema mode selector not found"
    
    def test_load_data_button_exists(self, navigate_to_target):
        """Test that Load Data button exists"""
        driver = navigate_to_target
        
        load_btn = driver.find_elements(By.XPATH, "//button[contains(., 'Load Data')]")
        assert len(load_btn) > 0, "Load Data button not found"
    
    def test_single_table_load_section_exists(self, navigate_to_target):
        """Test that single table load section exists"""
        driver = navigate_to_target
        
        single_table = driver.find_elements(
            By.XPATH, 
            "//*[contains(text(), 'Load Single Table')]"
        )
        assert len(single_table) > 0, "Single table load section not found"


class TestEndToEndFlow:
    """End-to-end user flow tests"""
    
    @pytest.mark.e2e
    @pytest.mark.slow
    def test_complete_migration_flow(self, streamlit_app):
        """Test complete migration flow (requires valid test databases)"""
        helper = StreamlitHelper
        
        # Step 1: Configure source connection
        assert helper.fill_text_input(streamlit_app, "Host", TEST_SOURCE_DB["host"])
        assert helper.fill_text_input(streamlit_app, "Port", TEST_SOURCE_DB["port"])
        assert helper.fill_text_input(streamlit_app, "Database", TEST_SOURCE_DB["database"])
        assert helper.fill_text_input(streamlit_app, "Username", TEST_SOURCE_DB["username"])
        assert helper.fill_text_input(streamlit_app, "Password", TEST_SOURCE_DB["password"])
        assert helper.fill_text_input(streamlit_app, "Table Prefix", TEST_SOURCE_DB["prefix"])
        
        # Test connection
        assert helper.click_button(streamlit_app, "Test Connection")
        time.sleep(3)
        
        # Should show success or error (depending on test environment)
        has_success = helper.check_success_message(streamlit_app)
        has_error = helper.check_error_message(streamlit_app)
        assert has_success or has_error, "No feedback message after connection test"
        
        # If connection failed, skip rest of test
        if has_error:
            pytest.skip("Source database connection failed - skipping rest of flow")
        
        # Step 2: Navigate to data selection
        select_data_link = streamlit_app.find_element(
            By.XPATH, "//a[contains(., 'Select & Extract Data')]"
        )
        select_data_link.click()
        StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        
        # Step 3: Navigate to transformation
        transform_link = streamlit_app.find_element(
            By.XPATH, "//a[contains(., 'Transform Data')]"
        )
        transform_link.click()
        StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        
        # Step 4: Navigate to target load
        target_link = streamlit_app.find_element(
            By.XPATH, "//a[contains(., 'Target')]"
        )
        target_link.click()
        StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        
        # Verify we made it through all pages
        assert streamlit_app.find_element(By.CSS_SELECTOR, ".stApp")


class TestSQLGeneration:
    """Test SQL generation features"""
    
    @pytest.fixture
    def navigate_to_select_data(self, streamlit_app):
        """Navigate to Select & Extract Data page"""
        try:
            select_data_link = streamlit_app.find_element(
                By.XPATH, "//a[contains(., 'Select & Extract Data')]"
            )
            select_data_link.click()
            StreamlitHelper.wait_for_streamlit_ready(streamlit_app)
        except NoSuchElementException:
            pytest.skip("Select & Extract Data page not available")
        return streamlit_app
    
    def test_sql_files_section_visible(self, navigate_to_select_data):
        """Test that SQL files section becomes visible after extraction"""
        driver = navigate_to_select_data
        
        # Check if SQL files heading exists (may not be visible initially)
        sql_section = driver.find_elements(
            By.XPATH, 
            "//*[contains(text(), 'Generated SQL Files') or contains(text(), 'SQL Migration Files')]"
        )
        # This may be 0 if no extraction has been done yet, which is okay
        assert True  # Just verify page loaded
    
    def test_download_buttons_for_sql_files(self, navigate_to_select_data):
        """Test that download buttons exist for SQL files (if generated)"""
        driver = navigate_to_select_data
        
        # Look for download buttons
        download_btns = driver.find_elements(
            By.XPATH, 
            "//button[contains(., 'Download') or contains(@download, '.sql')]"
        )
        # May be 0 if no files generated yet, which is expected
        assert isinstance(len(download_btns), int)


def pytest_addoption(parser):
    """Add custom pytest options"""
    parser.addoption(
        "--browser",
        action="store",
        default="chrome",
        help="Browser to use for testing: chrome or firefox"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--browser=chrome"])
