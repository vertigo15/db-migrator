import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import get_table_name


def test_empty_prefix_uses_unprefixed_table_names():
    assert get_table_name("users", "") == "users"
    assert get_table_name("custom_documents", "") == "custom_documents"
    assert get_table_name("logs", "") == "logs"
    assert get_table_name("translate", "") == "translate"


def test_empty_prefix_embeddings_falls_back_to_logical_name():
    assert get_table_name("embeddings", "") == "embeddings"


def test_non_empty_prefix_keeps_existing_behavior():
    assert get_table_name("users", "jeen_dev") == "jeen_dev_users"
    assert get_table_name("embeddings", "jeen_dev") == "jeen_dev"
