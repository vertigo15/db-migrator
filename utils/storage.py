"""
localStorage helpers using streamlit-javascript for persistent browser storage.
"""
import json
from typing import Any, Optional
import streamlit as st
from streamlit_javascript import st_javascript

from utils.config import STORAGE_PREFIX


def save_to_storage(key: str, value: Any) -> bool:
    """
    Save a value to browser localStorage.
    
    Args:
        key: The storage key (will be prefixed with db_migrator_)
        value: The value to store (will be JSON serialized)
    
    Returns:
        True if successful
    """
    # Ensure key has prefix
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    
    try:
        json_value = json.dumps(value)
        js_code = f"""
            localStorage.setItem('{full_key}', '{json_value}');
            return true;
        """
        result = st_javascript(js_code)
        return result == True
    except Exception as e:
        st.error(f"Failed to save to localStorage: {e}")
        return False


def load_from_storage(key: str, default: Any = None) -> Any:
    """
    Load a value from browser localStorage.
    
    Args:
        key: The storage key (will be prefixed with db_migrator_)
        default: Default value if key not found
    
    Returns:
        The deserialized value or default
    """
    # Ensure key has prefix
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    
    try:
        js_code = f"""
            const value = localStorage.getItem('{full_key}');
            return value;
        """
        result = st_javascript(js_code)
        
        if result is None or result == "null" or result == "":
            return default
        
        return json.loads(result)
    except json.JSONDecodeError:
        return default
    except Exception as e:
        st.error(f"Failed to load from localStorage: {e}")
        return default


def remove_from_storage(key: str) -> bool:
    """
    Remove a value from browser localStorage.
    
    Args:
        key: The storage key to remove
    
    Returns:
        True if successful
    """
    full_key = key if key.startswith(STORAGE_PREFIX) else f"{STORAGE_PREFIX}{key}"
    
    try:
        js_code = f"""
            localStorage.removeItem('{full_key}');
            return true;
        """
        result = st_javascript(js_code)
        return result == True
    except Exception as e:
        st.error(f"Failed to remove from localStorage: {e}")
        return False


def clear_all_storage() -> bool:
    """
    Clear all db_migrator_ prefixed keys from localStorage.
    
    Returns:
        True if successful
    """
    try:
        js_code = f"""
            const keysToRemove = [];
            for (let i = 0; i < localStorage.length; i++) {{
                const key = localStorage.key(i);
                if (key && key.startsWith('{STORAGE_PREFIX}')) {{
                    keysToRemove.push(key);
                }}
            }}
            keysToRemove.forEach(key => localStorage.removeItem(key));
            return keysToRemove.length;
        """
        result = st_javascript(js_code)
        return result is not None
    except Exception as e:
        st.error(f"Failed to clear localStorage: {e}")
        return False


def get_all_storage_keys() -> list:
    """
    Get all db_migrator_ prefixed keys from localStorage.
    
    Returns:
        List of keys
    """
    try:
        js_code = f"""
            const keys = [];
            for (let i = 0; i < localStorage.length; i++) {{
                const key = localStorage.key(i);
                if (key && key.startsWith('{STORAGE_PREFIX}')) {{
                    keys.push(key);
                }}
            }}
            return JSON.stringify(keys);
        """
        result = st_javascript(js_code)
        if result:
            return json.loads(result)
        return []
    except Exception:
        return []


# Convenience functions for common storage operations
def save_connection(connection_type: str, connection_data: dict) -> bool:
    """Save source or target connection details."""
    key = f"{connection_type}_connection"
    # Don't store password in localStorage for security
    safe_data = {k: v for k, v in connection_data.items() if k != "password"}
    return save_to_storage(key, safe_data)


def load_connection(connection_type: str) -> Optional[dict]:
    """Load source or target connection details."""
    key = f"{connection_type}_connection"
    return load_from_storage(key, default=None)


def save_selected_users(user_emails: list) -> bool:
    """Save selected user emails."""
    return save_to_storage("selected_users", user_emails)


def load_selected_users() -> list:
    """Load selected user emails."""
    return load_from_storage("selected_users", default=[])


def save_document_filters(filters: dict) -> bool:
    """Save document filter settings."""
    return save_to_storage("document_filters", filters)


def load_document_filters() -> dict:
    """Load document filter settings."""
    return load_from_storage("document_filters", default={})


def save_mapping_config(config: dict) -> bool:
    """Save column mapping configuration."""
    return save_to_storage("mapping_config", config)


def load_mapping_config() -> Optional[dict]:
    """Load column mapping configuration."""
    return load_from_storage("mapping_config", default=None)
