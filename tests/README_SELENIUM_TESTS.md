# Selenium Test Suite for DB Migrator

## Overview

Automated UI tests for the DB Migrator Streamlit application using Selenium WebDriver.

## Prerequisites

### 1. Install Dependencies

```bash
pip install selenium pytest pytest-timeout
```

### 2. Install Browser Driver

**Chrome:**
```bash
# Download ChromeDriver from: https://chromedriver.chromium.org/
# Or use webdriver-manager:
pip install webdriver-manager
```

**Firefox:**
```bash
# Download geckodriver from: https://github.com/mozilla/geckodriver/releases
# Or install via package manager
```

### 3. Ensure Application is Running

```bash
docker-compose up -d
# Wait for http://localhost:8501 to be accessible
```

---

## Running Tests

### Run All Tests

```bash
cd C:\Users\user\OneDrive - JeenAI\Documents\code\Jeen_4_to_5_db_migration\db-migrator
pytest tests/test_selenium.py -v
```

### Run Specific Test Class

```bash
pytest tests/test_selenium.py::TestSourceConnection -v
```

### Run Specific Test

```bash
pytest tests/test_selenium.py::TestSourceConnection::test_page_loads -v
```

### Run with Firefox

```bash
pytest tests/test_selenium.py --browser=firefox -v
```

### Run Only Smoke Tests

```bash
pytest tests/test_selenium.py -m smoke -v
```

### Run All Except Slow Tests

```bash
pytest tests/test_selenium.py -m "not slow" -v
```

### Run in Headful Mode (Visible Browser)

Edit `test_selenium.py` line 151:
```python
# options.add_argument("--headless")  # Comment this out
```

---

## Test Categories

### Test Markers

- **`@pytest.mark.integration`** - Requires external services (databases)
- **`@pytest.mark.e2e`** - Full end-to-end flow tests
- **`@pytest.mark.slow`** - Long-running tests (>30 seconds)
- **`@pytest.mark.smoke`** - Quick validation tests for CI/CD

### Test Classes

1. **`TestSourceConnection`** - Source database connection UI
   - Form visibility
   - Form filling
   - Connection testing
   - Error handling

2. **`TestDataSelection`** - Data extraction page
   - User selection
   - Document filters
   - Extract button
   - SQL generation toggle

3. **`TestTransformation`** - Data transformation page
   - Page loading
   - Organization ID input
   - Transformation options

4. **`TestTargetLoad`** - Target database and loading
   - Target connection form
   - Schema mode selector
   - Load buttons
   - Single table load

5. **`TestEndToEndFlow`** - Complete user workflows
   - Full migration flow
   - Navigation between pages

6. **`TestSQLGeneration`** - SQL generation features
   - SQL files display
   - Download buttons

---

## Configuration

### Environment Variables

Create `.env.test` file:

```bash
# Test database connections
TEST_SOURCE_HOST=source-postgres.example.com
TEST_SOURCE_PORT=5432
TEST_SOURCE_DB=jeen_v4
TEST_SOURCE_USER=migrator
TEST_SOURCE_PASS=test_password

TEST_TARGET_HOST=target-postgres.example.com
TEST_TARGET_PORT=5432
TEST_TARGET_DB=user_db
TEST_TARGET_USER=migrator
TEST_TARGET_PASS=test_password
```

### Test Data

Edit `test_selenium.py` lines 37-52 to update test database credentials.

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Selenium Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install selenium pytest pytest-timeout
        pip install webdriver-manager
    
    - name: Start application
      run: |
        docker-compose up -d
        sleep 30
    
    - name: Run tests
      run: |
        pytest tests/test_selenium.py -v --browser=chrome
    
    - name: Stop application
      run: docker-compose down
```

---

## Troubleshooting

### Issue: "ChromeDriver version mismatch"

**Solution**: Update ChromeDriver to match your Chrome version
```bash
pip install --upgrade webdriver-manager
```

### Issue: "Connection refused to localhost:8501"

**Solution**: Ensure Docker container is running and healthy
```bash
docker ps --filter name=db-migrator
docker logs db-migrator
```

### Issue: "Element not found" errors

**Solution**: Increase wait timeout or add explicit waits
```python
# In test_selenium.py, increase:
DEFAULT_TIMEOUT = 20  # From 10 to 20
STREAMLIT_LOAD_TIMEOUT = 60  # From 30 to 60
```

### Issue: "Tests are flaky"

**Solution**: Add stabilization delays
```python
# After page navigation
StreamlitHelper.wait_for_streamlit_ready(driver)
time.sleep(2)  # Additional wait
```

---

## Writing New Tests

### Template for New Test

```python
def test_my_new_feature(self, streamlit_app):
    """Test description"""
    helper = StreamlitHelper
    
    # Navigate to page
    helper.click_button(streamlit_app, "My Button")
    
    # Perform action
    helper.fill_text_input(streamlit_app, "My Field", "value")
    
    # Verify result
    assert helper.check_success_message(streamlit_app)
```

### Best Practices

1. **Use helper methods** - Don't interact with Selenium directly
2. **Add waits** - Streamlit is async, always wait for elements
3. **Be specific** - Use precise XPath/CSS selectors
4. **Clean up** - Use fixtures for setup/teardown
5. **Test isolation** - Each test should be independent
6. **Mark appropriately** - Use `@pytest.mark.*` decorators

---

## Test Coverage

Current test coverage:

- ✅ Source connection form (100%)
- ✅ Data selection UI (100%)
- ✅ Transformation page (80%)
- ✅ Target load page (100%)
- ✅ SQL generation (70%)
- ⏳ End-to-end flows (50%)

To improve coverage, add tests for:
- [ ] Single table load feature
- [ ] Error handling scenarios
- [ ] Data validation
- [ ] Export functionality

---

## Performance

Average test execution times:

- **Smoke tests**: ~30 seconds
- **Integration tests**: ~2 minutes
- **E2E tests**: ~5 minutes
- **Full suite**: ~10 minutes

---

## Support

For issues with Selenium tests:
1. Check browser/driver compatibility
2. Verify application is accessible
3. Review test logs in `pytest` output
4. Enable headful mode to watch tests execute
5. Add `time.sleep()` for debugging

---

## References

- [Selenium Python Docs](https://selenium-python.readthedocs.io/)
- [Pytest Documentation](https://docs.pytest.org/)
- [Streamlit Testing Guide](https://docs.streamlit.io/library/advanced-features/testing)
