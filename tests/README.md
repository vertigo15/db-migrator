# DB Migrator - Selenium Test Suite

## Overview
Automated UI testing for the DB Migrator Streamlit application using Selenium WebDriver with Chrome.

## Test Coverage

### `test_basic_flow.py`
Comprehensive end-to-end test covering the complete migration workflow:

#### ✅ **Step 1: Application Launch**
- Opens Streamlit app at `http://localhost:8501`
- Verifies page loads successfully

#### ✅ **Step 2: Connect Page Navigation**
- Navigates to Connect page via sidebar
- Verifies page title and content

#### ✅ **Step 3: Source Database Connection**
- Locates and clicks "🔗 Test Connection" button
- Verifies connection attempt (result depends on database availability)

#### ✅ **Step 4: Select Data Page Navigation**
- Navigates to Select Data page
- Verifies page title "📋 Select Data to Migrate"
- Confirms page is working

#### ⚠️ **Step 5: User Selection** (Limited)
- Attempts to select first 3 users
- **Known Limitation**: Streamlit's `st.data_editor()` uses custom rendering (canvas-based) that Selenium cannot interact with
- Checkboxes are not standard HTML `<input type="checkbox">` elements
- Test can only find and interact with limited UI elements

#### ⚠️ **Step 6: Start Extraction** (Depends on Step 5)
- Attempts to click "🚀 Start Extraction" button
- **Known Limitation**: Button may not appear without successful user selection
- Test continues even if button is not found

#### ✅ **Step 7: SQL File Verification**
- Checks `output/migrations/` directory for generated SQL files
- Verifies files `01_*.sql` through `06_*.sql` exist
- **Note**: File `00_*.sql` does not exist by design (migration schema setup is embedded in each SQL file)

#### ✅ **Step 8: Target Page Testing**
- Navigates to Target page
- Verifies page title "🎯 Target Configuration & Load"
- Checks for database structure information (optional - may be rendered dynamically)
- Locates and clicks "🔗 Test Connection" button
- Verifies connection test completes

## Running Tests

### Prerequisites
```bash
# Install dependencies
pip install selenium pytest

# ChromeDriver is automatically managed by Selenium 4+
```

### Start Streamlit App
```bash
# Terminal 1: Run the app
streamlit run app.py
```

### Run Tests
```bash
# Terminal 2: Run the test
pytest tests/test_basic_flow.py -v -s

# Or run directly
python tests/test_basic_flow.py
```

## Test Output

### Screenshots
All test steps capture screenshots saved to `tests/screenshots/`:
- `01_homepage.png` - Initial app load
- `02_connect_page.png` - Connect page
- `03_connection_tested.png` - After clicking Test Connection
- `04_select_data_page.png` - Select Data page
- `05_users_selected.png` - After user selection attempt
- `06_extraction_started.png` or `06_no_extraction_button.png` - Extraction step
- `07_extraction_complete.png` - SQL files verified
- `08_target_page.png` - Target page
- `08_target_connection_tested.png` - After Target connection test
- `error_stepX.png` - If any step fails

### Console Output
Detailed step-by-step progress with:
- ✓ Success indicators
- ⚠️ Warnings for optional/skipped steps
- ❌ Errors if critical steps fail
- SQL file listings
- Button detection details

## Known Limitations

### 1. Streamlit `st.data_editor()` Interaction
**Issue**: Streamlit's data editor uses custom JavaScript/Canvas rendering for checkboxes and interactive elements. These are not standard HTML form elements.

**Impact**: 
- Cannot programmatically select users in the data editor
- Cannot test row selection functionality
- Manual testing required for data selection features

**Workaround Options**:
- Use Streamlit's built-in testing framework (if available)
- Modify UI to use standard HTML checkboxes (would change design)
- Manual testing for this specific feature

### 2. Dynamic Content Rendering
**Issue**: Some Streamlit components (`st.info()`, `st.expander()`) may render content dynamically after initial page load or in collapsible sections.

**Impact**:
- Database structure info box may not be immediately detectable
- Content verification relies on timing and DOM state

**Workaround**: 
- Tests include wait times and multiple detection strategies
- Non-critical content checks are marked as optional

### 3. Connection-Dependent Tests
**Issue**: Connection tests depend on actual database availability.

**Impact**:
- Tests may show warnings if databases are not available
- This is expected behavior for environments without databases

**Solution**: 
- Tests verify that connection attempt was made
- Success/failure messages are informational, not blocking

## Test Maintenance

### Updating Selectors
If UI changes break tests, update these common selectors in `test_basic_flow.py`:

```python
# Page navigation links
"//a[contains(@href, 'connect') or contains(text(), 'connect')]"
"//a[contains(@href, 'select_data') or contains(text(), 'select data')]"
"//a[contains(@href, 'target') or contains(text(), 'target')]"

# Buttons
"//button[contains(., 'Test Connection')]"
"//button[contains(., 'Start Extraction')]"

# Page titles
driver.find_element(By.TAG_NAME, "h1").text
```

### Adding New Tests
Follow the existing pattern:
1. Add step with descriptive print statement
2. Use `WebDriverWait` for element detection
3. Capture screenshot before and after action
4. Handle exceptions gracefully with informational messages
5. Use `try/except` blocks for optional checks

## Alternative Testing Approaches

For comprehensive testing of Streamlit-specific features:

1. **Streamlit Testing Framework**: If/when available, use official Streamlit testing tools
2. **Component Testing**: Test `utils/` modules directly with pytest (unit tests)
3. **Manual Testing Checklist**: Document manual test cases for UI-specific features
4. **Playwright**: Consider Playwright as an alternative to Selenium (better async support)

## Contributing

When adding tests:
- Document all known limitations in comments
- Mark optional checks with `⚠️` warnings
- Create screenshots at each critical step
- Handle errors gracefully to allow test continuation
- Update this README with new test coverage details
