import pytest

from utils.user_batching import (
    MISSING_BUCKET,
    OTHER_BUCKET,
    select_user_letter_batch,
    user_batch_bucket,
    user_bucket_counts,
)


def test_buckets_email_local_part_case_insensitively():
    assert user_batch_bucket(" Alice@example.com ", "email") == "A"
    assert user_batch_bucket("alice@other.example", "email") == "A"
    assert user_batch_bucket("bob@example.com", "email") == "B"


def test_buckets_digits_unicode_and_missing_values():
    assert user_batch_bucket("0521234567@example.com", "email") == "0"
    assert user_batch_bucket("שרה", "name") == "ש"
    assert user_batch_bucket("_service@example.com", "email") == OTHER_BUCKET
    assert user_batch_bucket("", "name") == MISSING_BUCKET
    assert user_batch_bucket(None, "name") == MISSING_BUCKET


def test_numeric_email_local_parts_sort_numerically():
    records = [
        {"name": "Ten", "email": "10@example.com"},
        {"name": "Two", "email": "2@example.com"},
        {"name": "Twenty", "email": "20@example.com"},
    ]

    assert select_user_letter_batch(
        records,
        field="email",
        buckets=["1", "2"],
        limit=10,
    ) == [
        "10@example.com",
        "2@example.com",
        "20@example.com",
    ]


def test_selection_respects_bucket_order_limit_exclusions_and_duplicates():
    records = [
        {"name": "Bob", "email": "bob@example.com"},
        {"name": "Alice", "email": "alice@example.com"},
        {"name": "Amy", "email": "AMY@example.com"},
        {"name": "Duplicate", "email": "alice@example.com"},
        {"name": "Charlie", "email": "charlie@example.com"},
    ]

    assert select_user_letter_batch(
        records,
        field="name",
        buckets=["A", "B"],
        limit=2,
        excluded_emails=["amy@example.com"],
    ) == ["alice@example.com", "bob@example.com"]


def test_bucket_counts_include_digits_and_dynamic_unicode_letters():
    records = [
        {"name": "Alice", "email": "alice@example.com"},
        {"name": "adam", "email": "adam@example.com"},
        {"name": "שרה", "email": "sara@example.com"},
        {"name": "Numeric", "email": "0521@example.com"},
    ]

    assert user_bucket_counts(records, "name") == {"A": 2, "N": 1, "ש": 1}
    assert user_bucket_counts(records, "email") == {
        "A": 2,
        "S": 1,
        "0": 1,
    }


def test_rejects_unknown_fields_and_non_positive_limits():
    with pytest.raises(ValueError, match="Unsupported"):
        user_batch_bucket("value", "company")
    with pytest.raises(ValueError, match="positive"):
        select_user_letter_batch([], "email", ["A"], 0)
