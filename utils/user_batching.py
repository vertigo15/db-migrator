"""Pure helpers for deterministic user batching by email or name."""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
import unicodedata


OTHER_BUCKET = "# Other"
MISSING_BUCKET = "∅ Missing"
SUPPORTED_FIELDS = {"email", "name"}


def _clean_scalar(value) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if text.casefold() in {"", "nan", "nat", "none", "<na>"}:
        return ""
    return text


def _grouping_text(value, field: str) -> str:
    if field not in SUPPORTED_FIELDS:
        raise ValueError(f"Unsupported user batch field: {field}")
    text = _clean_scalar(value)
    if field == "email" and text:
        return text.split("@", 1)[0].strip()
    return text


def user_batch_bucket(value, field: str) -> str:
    """Return a case-insensitive first-character bucket for one scalar."""
    text = _grouping_text(value, field)
    if not text:
        return MISSING_BUCKET
    initial = text[0]
    if initial.isalpha():
        return initial.upper()
    if initial.isdigit():
        return initial
    return OTHER_BUCKET


def user_bucket_sort_key(bucket: str) -> tuple:
    """Order letter buckets first, then digits, symbols, and missing values."""
    if bucket == OTHER_BUCKET:
        return (2, "")
    if bucket == MISSING_BUCKET:
        return (3, "")
    if bucket.isalpha():
        return (0, bucket.casefold())
    if bucket.isdigit():
        return (1, bucket)
    return (2, bucket.casefold())


def _user_value_sort_key(value, field: str) -> tuple:
    text = _grouping_text(value, field)
    if not text:
        return (2, "")
    if field == "email" and text.isdecimal():
        # Phone-number-style email local-parts should sort as numbers rather
        # than lexically (2 before 10, not 10 before 2).
        return (0, int(text), text)
    return (1, text.casefold(), text)


def user_bucket_counts(
    records: Iterable[Mapping[str, object]],
    field: str,
) -> dict[str, int]:
    """Count selectable users in each first-character bucket."""
    counts = Counter()
    for record in records:
        if not _clean_scalar(record.get("email")):
            continue
        counts[user_batch_bucket(record.get(field), field)] += 1
    return dict(sorted(counts.items(), key=lambda item: user_bucket_sort_key(item[0])))


def select_user_letter_batch(
    records: Iterable[Mapping[str, object]],
    field: str,
    buckets: Iterable[str],
    limit: int,
    excluded_emails: Iterable[str] = (),
) -> list[str]:
    """Select one deterministic, de-duplicated letter batch.

    ``excluded_emails`` is normally the set of terminally migrated users.
    Buckets are processed in their displayed order; numeric-only email local
    parts are sorted numerically within each digit bucket.
    """
    if field not in SUPPORTED_FIELDS:
        raise ValueError(f"Unsupported user batch field: {field}")
    if int(limit) < 1:
        raise ValueError("Letter batch limit must be positive")

    wanted = {str(bucket) for bucket in buckets}
    excluded = {
        _clean_scalar(email).casefold()
        for email in excluded_emails
        if _clean_scalar(email)
    }
    candidates = []
    seen = set()
    for record in records:
        email = _clean_scalar(record.get("email"))
        normalized_email = email.casefold()
        if not email or normalized_email in excluded or normalized_email in seen:
            continue
        bucket = user_batch_bucket(record.get(field), field)
        if bucket not in wanted:
            continue
        seen.add(normalized_email)
        candidates.append(
            (
                user_bucket_sort_key(bucket),
                _user_value_sort_key(record.get(field), field),
                normalized_email,
                email,
            )
        )

    candidates.sort(key=lambda candidate: candidate[:3])
    return [candidate[3] for candidate in candidates[: int(limit)]]
