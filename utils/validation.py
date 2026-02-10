"""
Validation utilities for migration data integrity checks.
"""
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import uuid


class ValidationResult:
    """Result of a single validation check."""
    
    def __init__(self, name: str, status: str, message: str, details: Optional[Dict] = None):
        """
        Initialize validation result.
        
        Args:
            name: Name of the validation check
            status: 'pass', 'fail', 'warning', or 'skipped'
            message: Human-readable message
            details: Optional additional details
        """
        self.name = name
        self.status = status
        self.message = message
        self.details = details or {}
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "details": self.details,
        }


class DataValidator:
    """
    Validator for migration data integrity.
    """
    
    def __init__(self, extract_dir: str, transform_dir: str):
        """
        Initialize validator.
        
        Args:
            extract_dir: Directory containing extracted CSV files
            transform_dir: Directory containing transformed CSV files
        """
        self.extract_dir = extract_dir
        self.transform_dir = transform_dir
        self.results: List[ValidationResult] = []
    
    def _find_latest_file(self, directory: str, prefix: str) -> Optional[str]:
        """Find the most recent file with given prefix."""
        if not os.path.exists(directory):
            return None
        files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith('.csv')]
        if not files:
            return None
        files.sort(reverse=True)
        return os.path.join(directory, files[0])
    
    def _read_csv_safe(self, filepath: str) -> Optional[pd.DataFrame]:
        """Safely read a CSV file."""
        try:
            return pd.read_csv(filepath)
        except:
            return None
    
    def validate_row_counts(self) -> ValidationResult:
        """Validate that extracted and transformed row counts match."""
        tables = ["users", "folders", "documents", "embeddings", "agents", "users_groups"]
        mismatches = []
        
        for table in tables:
            extract_file = self._find_latest_file(self.extract_dir, f"{table}_")
            transform_file = self._find_latest_file(self.transform_dir, f"{table}_")
            
            if not extract_file or not transform_file:
                continue
            
            extract_df = self._read_csv_safe(extract_file)
            transform_df = self._read_csv_safe(transform_file)
            
            if extract_df is None or transform_df is None:
                continue
            
            if len(extract_df) != len(transform_df):
                mismatches.append({
                    "table": table,
                    "extracted": len(extract_df),
                    "transformed": len(transform_df),
                })
        
        if mismatches:
            result = ValidationResult(
                name="Row Count Consistency",
                status="fail",
                message=f"Row count mismatch in {len(mismatches)} table(s)",
                details={"mismatches": mismatches}
            )
        else:
            result = ValidationResult(
                name="Row Count Consistency",
                status="pass",
                message="All extracted and transformed row counts match"
            )
        
        self.results.append(result)
        return result
    
    def validate_required_columns_users(self) -> ValidationResult:
        """Validate that users have no null values in required columns."""
        transform_file = self._find_latest_file(self.transform_dir, "users_")
        
        if not transform_file:
            result = ValidationResult(
                name="Users Required Columns",
                status="skipped",
                message="No users file found"
            )
            self.results.append(result)
            return result
        
        df = self._read_csv_safe(transform_file)
        if df is None:
            result = ValidationResult(
                name="Users Required Columns",
                status="skipped",
                message="Could not read users file"
            )
            self.results.append(result)
            return result
        
        issues = []
        
        # Check for null IDs
        if "id" in df.columns:
            null_ids = df["id"].isnull().sum()
            if null_ids > 0:
                issues.append(f"{null_ids} rows with null ID")
        
        # Check for null emails
        if "email" in df.columns:
            null_emails = df["email"].isnull().sum()
            if null_emails > 0:
                issues.append(f"{null_emails} rows with null email")
        
        if issues:
            result = ValidationResult(
                name="Users Required Columns",
                status="fail",
                message="; ".join(issues),
                details={"issues": issues}
            )
        else:
            result = ValidationResult(
                name="Users Required Columns",
                status="pass",
                message="All users have valid ID and email"
            )
        
        self.results.append(result)
        return result
    
    def validate_required_columns_documents(self) -> ValidationResult:
        """Validate that documents have no null values in required columns."""
        transform_file = self._find_latest_file(self.transform_dir, "documents_")
        
        if not transform_file:
            result = ValidationResult(
                name="Documents Required Columns",
                status="skipped",
                message="No documents file found"
            )
            self.results.append(result)
            return result
        
        df = self._read_csv_safe(transform_file)
        if df is None:
            result = ValidationResult(
                name="Documents Required Columns",
                status="skipped",
                message="Could not read documents file"
            )
            self.results.append(result)
            return result
        
        issues = []
        
        # Check for null IDs (doc_id -> id)
        id_col = "id" if "id" in df.columns else "doc_id" if "doc_id" in df.columns else None
        if id_col:
            null_ids = df[id_col].isnull().sum()
            if null_ids > 0:
                issues.append(f"{null_ids} rows with null {id_col}")
        
        if issues:
            result = ValidationResult(
                name="Documents Required Columns",
                status="fail",
                message="; ".join(issues),
                details={"issues": issues}
            )
        else:
            result = ValidationResult(
                name="Documents Required Columns",
                status="pass",
                message="All documents have valid IDs"
            )
        
        self.results.append(result)
        return result
    
    def validate_referential_integrity_docs_users(self) -> ValidationResult:
        """Validate that all document owner_ids exist in users."""
        users_file = self._find_latest_file(self.extract_dir, "users_")
        docs_file = self._find_latest_file(self.extract_dir, "documents_")
        
        if not users_file or not docs_file:
            result = ValidationResult(
                name="Document-User Referential Integrity",
                status="skipped",
                message="Missing users or documents file"
            )
            self.results.append(result)
            return result
        
        # Need to exclude users_groups file
        if "groups" in users_file:
            files = [f for f in os.listdir(self.extract_dir) 
                     if f.startswith("users_") and "groups" not in f and f.endswith('.csv')]
            if files:
                files.sort(reverse=True)
                users_file = os.path.join(self.extract_dir, files[0])
            else:
                result = ValidationResult(
                    name="Document-User Referential Integrity",
                    status="skipped",
                    message="No users file found"
                )
                self.results.append(result)
                return result
        
        users_df = self._read_csv_safe(users_file)
        docs_df = self._read_csv_safe(docs_file)
        
        if users_df is None or docs_df is None:
            result = ValidationResult(
                name="Document-User Referential Integrity",
                status="skipped",
                message="Could not read files"
            )
            self.results.append(result)
            return result
        
        user_ids = set(users_df["id"].dropna().astype(str).tolist())
        doc_owner_ids = set(docs_df["owner_id"].dropna().astype(str).tolist())
        
        orphaned = doc_owner_ids - user_ids
        
        if orphaned:
            result = ValidationResult(
                name="Document-User Referential Integrity",
                status="fail",
                message=f"{len(orphaned)} documents have owner_id not in users",
                details={"orphaned_owner_ids": list(orphaned)[:10]}  # First 10
            )
        else:
            result = ValidationResult(
                name="Document-User Referential Integrity",
                status="pass",
                message="All document owner_ids exist in users"
            )
        
        self.results.append(result)
        return result
    
    def validate_referential_integrity_embeddings_docs(self) -> ValidationResult:
        """Validate that all embedding doc_ids exist in documents."""
        docs_file = self._find_latest_file(self.extract_dir, "documents_")
        embeddings_file = self._find_latest_file(self.extract_dir, "embeddings_")
        
        if not docs_file or not embeddings_file:
            result = ValidationResult(
                name="Embedding-Document Referential Integrity",
                status="skipped",
                message="Missing documents or embeddings file"
            )
            self.results.append(result)
            return result
        
        docs_df = self._read_csv_safe(docs_file)
        embeddings_df = self._read_csv_safe(embeddings_file)
        
        if docs_df is None or embeddings_df is None:
            result = ValidationResult(
                name="Embedding-Document Referential Integrity",
                status="skipped",
                message="Could not read files"
            )
            self.results.append(result)
            return result
        
        doc_ids = set(docs_df["doc_id"].dropna().astype(str).tolist())
        
        # Extract doc_id from metadata JSON
        embedding_doc_ids = set()
        if "metadata" in embeddings_df.columns:
            for metadata in embeddings_df["metadata"].dropna():
                try:
                    if isinstance(metadata, str):
                        meta_dict = json.loads(metadata)
                        if "doc_id" in meta_dict:
                            embedding_doc_ids.add(str(meta_dict["doc_id"]))
                except:
                    pass
        
        orphaned = embedding_doc_ids - doc_ids
        
        if orphaned:
            result = ValidationResult(
                name="Embedding-Document Referential Integrity",
                status="fail",
                message=f"{len(orphaned)} embeddings have doc_id not in documents",
                details={"orphaned_doc_ids": list(orphaned)[:10]}
            )
        else:
            result = ValidationResult(
                name="Embedding-Document Referential Integrity",
                status="pass",
                message="All embedding doc_ids exist in documents"
            )
        
        self.results.append(result)
        return result
    
    def validate_uuid_format(self) -> ValidationResult:
        """Validate that UUID columns contain valid UUIDs."""
        issues = []
        
        # Check users IDs
        users_file = self._find_latest_file(self.transform_dir, "users_")
        if users_file and "groups" not in users_file:
            df = self._read_csv_safe(users_file)
            if df is not None and "id" in df.columns:
                invalid = 0
                for val in df["id"].dropna():
                    try:
                        # Try to parse as UUID - some may be string IDs, not UUIDs
                        # Only check if it looks like a UUID (has dashes)
                        if "-" in str(val):
                            uuid.UUID(str(val))
                    except:
                        invalid += 1
                if invalid > 0:
                    issues.append(f"users: {invalid} invalid UUIDs in id column")
        
        # Check documents IDs
        docs_file = self._find_latest_file(self.transform_dir, "documents_")
        if docs_file:
            df = self._read_csv_safe(docs_file)
            if df is not None:
                id_col = "id" if "id" in df.columns else "doc_id" if "doc_id" in df.columns else None
                if id_col:
                    invalid = 0
                    for val in df[id_col].dropna():
                        try:
                            if "-" in str(val):
                                uuid.UUID(str(val))
                        except:
                            invalid += 1
                    if invalid > 0:
                        issues.append(f"documents: {invalid} invalid UUIDs in {id_col} column")
        
        if issues:
            result = ValidationResult(
                name="UUID Format Validation",
                status="warning",
                message="; ".join(issues),
                details={"issues": issues}
            )
        else:
            result = ValidationResult(
                name="UUID Format Validation",
                status="pass",
                message="All UUID columns contain valid formats"
            )
        
        self.results.append(result)
        return result
    
    def validate_timestamp_format(self) -> ValidationResult:
        """Validate that timestamp columns can be parsed."""
        issues = []
        
        tables_to_check = [
            ("users", "created_at"),
            ("documents", "created_at"),
            ("folders", "created_at"),
        ]
        
        for table, col in tables_to_check:
            file = self._find_latest_file(self.transform_dir, f"{table}_")
            if not file:
                continue
            if table == "users" and "groups" in file:
                continue
            
            df = self._read_csv_safe(file)
            if df is None or col not in df.columns:
                continue
            
            invalid = 0
            for val in df[col].dropna():
                try:
                    pd.to_datetime(val)
                except:
                    invalid += 1
            
            if invalid > 0:
                issues.append(f"{table}.{col}: {invalid} unparseable timestamps")
        
        if issues:
            result = ValidationResult(
                name="Timestamp Format Validation",
                status="warning",
                message="; ".join(issues),
                details={"issues": issues}
            )
        else:
            result = ValidationResult(
                name="Timestamp Format Validation",
                status="pass",
                message="All timestamp columns are parseable"
            )
        
        self.results.append(result)
        return result
    
    def run_all_validations(self) -> Dict:
        """
        Run all validation checks.
        
        Returns:
            Dictionary with validation results and summary
        """
        self.results = []
        
        # Run all validations
        self.validate_row_counts()
        self.validate_required_columns_users()
        self.validate_required_columns_documents()
        self.validate_referential_integrity_docs_users()
        self.validate_referential_integrity_embeddings_docs()
        self.validate_uuid_format()
        self.validate_timestamp_format()
        
        # Build summary
        summary = {
            "total": len(self.results),
            "passed": sum(1 for r in self.results if r.status == "pass"),
            "failed": sum(1 for r in self.results if r.status == "fail"),
            "warnings": sum(1 for r in self.results if r.status == "warning"),
            "skipped": sum(1 for r in self.results if r.status == "skipped"),
        }
        
        return {
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "results": [r.to_dict() for r in self.results],
            "summary": summary,
            "overall_status": "fail" if summary["failed"] > 0 else "pass",
        }


def generate_migration_report(
    extraction_results: Dict,
    transformation_results: Dict,
    validation_results: Dict,
    load_results: Optional[Dict] = None,
    duration_seconds: Optional[float] = None
) -> Dict:
    """
    Generate a comprehensive migration report.
    
    Args:
        extraction_results: Results from extraction
        transformation_results: Results from transformation
        validation_results: Results from validation
        load_results: Optional results from loading
        duration_seconds: Optional total duration
        
    Returns:
        Report dictionary
    """
    report = {
        "generated_at": datetime.now().isoformat(),
        "duration_seconds": duration_seconds,
        "extraction": {
            "timestamp": extraction_results.get("timestamp"),
            "tables": extraction_results.get("summary", {}),
            "errors": extraction_results.get("errors", []),
        },
        "transformation": {
            "timestamp": transformation_results.get("timestamp"),
            "tables": transformation_results.get("summary", {}),
            "errors": transformation_results.get("errors", []),
        },
        "validation": validation_results,
        "overall_status": "success",
    }
    
    if load_results:
        report["load"] = {
            "timestamp": load_results.get("timestamp"),
            "tables": {
                name: {
                    "loaded": result.get("rows_loaded", 0),
                    "failed": result.get("rows_failed", 0),
                    "status": result.get("status", "N/A"),
                }
                for name, result in load_results.get("tables", {}).items()
            },
            "summary": load_results.get("summary", {}),
            "errors": load_results.get("errors", []),
        }
    
    # Determine overall status
    if extraction_results.get("errors"):
        report["overall_status"] = "failed"
    elif transformation_results.get("errors"):
        report["overall_status"] = "failed"
    elif validation_results.get("overall_status") == "fail":
        report["overall_status"] = "failed"
    elif load_results and load_results.get("errors"):
        report["overall_status"] = "partial"
    
    return report
