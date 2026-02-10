"""
Data loader for loading transformed data into target database.
"""
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Callable, Any
import pandas as pd

from utils.db import ConnectionConfig, get_connection, truncate_table, execute_insert


# Load order (respecting foreign key dependencies)
LOAD_ORDER = [
    "users_groups",
    "users",
    "folders",
    "documents",
    "embeddings",
    "agents",
]

# Default target table configuration
TARGET_TABLES = {
    "users_groups": {
        "target_table": "users_groups",
        "target_schema": "user_db",
        "conflict_columns": ["id"],
    },
    "users": {
        "target_table": "users",
        "target_schema": "user_db",
        "conflict_columns": ["id"],
    },
    "folders": {
        "target_table": "folders",
        "target_schema": "document_db",
        "conflict_columns": ["id"],
    },
    "documents": {
        "target_table": "documents",
        "target_schema": "document_db",
        "conflict_columns": ["id"],
    },
    "embeddings": {
        "target_table": "embeddings",
        "target_schema": "document_db",
        "conflict_columns": ["id"],
    },
    "agents": {
        "target_table": "agents",
        "target_schema": "completion_db",
        "conflict_columns": ["id"],
    },
}


class DataLoader:
    """
    Engine for loading transformed data into target database.
    """
    
    def __init__(
        self,
        config: ConnectionConfig,
        input_dir: str,
        schema_mode: str = "schemas",  # 'schemas' or 'databases'
        progress_callback: Optional[Callable[[str, int, int, str], None]] = None
    ):
        """
        Initialize data loader.
        
        Args:
            config: Target database connection configuration
            input_dir: Directory containing transformed CSV files
            schema_mode: 'schemas' if target uses schemas, 'databases' if separate databases
            progress_callback: Optional callback for progress updates (table, current, total, status)
        """
        self.config = config
        self.input_dir = input_dir
        self.schema_mode = schema_mode
        self.progress_callback = progress_callback
    
    def _report_progress(self, table_name: str, current: int, total: int, status: str = ""):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(table_name, current, total, status)
    
    def _find_latest_file(self, prefix: str) -> Optional[str]:
        """Find the most recent file with given prefix in input directory."""
        files = [f for f in os.listdir(self.input_dir) if f.startswith(prefix) and f.endswith('.csv')]
        if not files:
            return None
        files.sort(reverse=True)
        return os.path.join(self.input_dir, files[0])
    
    def _get_full_table_name(self, logical_name: str) -> str:
        """Get the full table name including schema."""
        table_config = TARGET_TABLES.get(logical_name, {})
        target_table = table_config.get("target_table", logical_name)
        
        if self.schema_mode == "schemas":
            schema = table_config.get("target_schema", "public")
            return f"{schema}.{target_table}"
        else:
            # For separate databases, just use table name (connection handles DB)
            return target_table
    
    def load_table(
        self,
        logical_name: str,
        load_mode: str = "truncate",  # 'truncate' or 'upsert'
        dry_run: bool = False
    ) -> Dict:
        """
        Load a single table.
        
        Args:
            logical_name: Logical table name
            load_mode: 'truncate' for truncate & load, 'upsert' for upsert
            dry_run: If True, only generate SQL without executing
            
        Returns:
            Result dictionary with stats and any errors
        """
        result = {
            "table": logical_name,
            "rows_loaded": 0,
            "rows_failed": 0,
            "status": "pending",
            "sql_preview": None,
            "error": None,
        }
        
        # Find input file
        input_file = self._find_latest_file(f"{logical_name}_")
        if not input_file:
            result["status"] = "skipped"
            result["error"] = f"No input file found for {logical_name}"
            return result
        
        # Read data
        try:
            df = pd.read_csv(input_file)
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"Failed to read CSV: {str(e)}"
            return result
        
        if df.empty:
            result["status"] = "skipped"
            result["error"] = "No data to load"
            return result
        
        # Get table configuration
        table_config = TARGET_TABLES.get(logical_name, {})
        full_table_name = self._get_full_table_name(logical_name)
        conflict_columns = table_config.get("conflict_columns", ["id"])
        
        # Generate SQL preview
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join(columns)
        
        if load_mode == "truncate":
            truncate_sql = f"TRUNCATE TABLE {full_table_name} CASCADE;"
            insert_sql = f"INSERT INTO {full_table_name} ({column_names}) VALUES ({placeholders})"
            result["sql_preview"] = f"{truncate_sql}\n{insert_sql}\n-- {len(df)} rows"
        else:  # upsert
            conflict_cols = ", ".join(conflict_columns)
            update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
            upsert_sql = f"""
INSERT INTO {full_table_name} ({column_names})
VALUES ({placeholders})
ON CONFLICT ({conflict_cols}) DO UPDATE SET
{", ".join(update_cols)}
            """.strip()
            result["sql_preview"] = f"{upsert_sql}\n-- {len(df)} rows"
        
        if dry_run:
            result["status"] = "dry_run"
            result["rows_loaded"] = len(df)
            return result
        
        # Execute load
        try:
            conn = get_connection(self.config)
            cursor = conn.cursor()
            
            if load_mode == "truncate":
                # Truncate table first
                cursor.execute(f"TRUNCATE TABLE {full_table_name} CASCADE;")
                conn.commit()
            
            # Prepare insert/upsert statement
            if load_mode == "upsert" and conflict_columns:
                conflict_cols = ", ".join(conflict_columns)
                update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
                if update_cols:
                    insert_sql = f"""
                        INSERT INTO {full_table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT ({conflict_cols}) DO UPDATE SET
                        {", ".join(update_cols)}
                    """
                else:
                    insert_sql = f"""
                        INSERT INTO {full_table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT ({conflict_cols}) DO NOTHING
                    """
            else:
                insert_sql = f"INSERT INTO {full_table_name} ({column_names}) VALUES ({placeholders})"
            
            # Insert rows
            loaded = 0
            failed = 0
            
            for _, row in df.iterrows():
                try:
                    # Convert row values, handling NaN
                    values = []
                    for val in row:
                        if pd.isna(val):
                            values.append(None)
                        else:
                            values.append(val)
                    
                    cursor.execute(insert_sql, tuple(values))
                    loaded += 1
                except Exception as e:
                    failed += 1
                    result["error"] = str(e)  # Store last error
            
            conn.commit()
            cursor.close()
            conn.close()
            
            result["rows_loaded"] = loaded
            result["rows_failed"] = failed
            result["status"] = "success" if failed == 0 else "partial"
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
        
        return result
    
    def load_all(
        self,
        load_modes: Dict[str, str],
        dry_run: bool = False,
        strict_mode: bool = True
    ) -> Dict:
        """
        Load all tables in dependency order.
        
        Args:
            load_modes: Dict mapping table name to load mode ('truncate' or 'upsert')
            dry_run: If True, only generate SQL without executing
            strict_mode: If True, stop on first error
            
        Returns:
            Results dictionary with per-table results and summary
        """
        results = {
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "dry_run": dry_run,
            "tables": {},
            "summary": {
                "total_loaded": 0,
                "total_failed": 0,
                "tables_succeeded": 0,
                "tables_failed": 0,
            },
            "errors": [],
        }
        
        total = len(LOAD_ORDER)
        
        for i, table_name in enumerate(LOAD_ORDER):
            self._report_progress(table_name, i + 1, total, "loading")
            
            load_mode = load_modes.get(table_name, "truncate")
            table_result = self.load_table(table_name, load_mode, dry_run)
            
            results["tables"][table_name] = table_result
            
            if table_result["status"] in ["success", "partial", "dry_run"]:
                results["summary"]["total_loaded"] += table_result["rows_loaded"]
                results["summary"]["total_failed"] += table_result["rows_failed"]
                results["summary"]["tables_succeeded"] += 1
            elif table_result["status"] == "error":
                results["summary"]["tables_failed"] += 1
                if table_result["error"]:
                    results["errors"].append(f"{table_name}: {table_result['error']}")
                
                if strict_mode and not dry_run:
                    self._report_progress(table_name, i + 1, total, "stopped")
                    break
        
        self._report_progress("complete", total, total, "done")
        return results


def get_target_table_info(config: ConnectionConfig, schema_mode: str = "schemas") -> Dict[str, Dict]:
    """
    Get information about target tables (existence, row count, schema).
    
    Args:
        config: Target database connection
        schema_mode: 'schemas' or 'databases'
        
    Returns:
        Dict mapping table name to info dict
    """
    info = {}
    
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        
        for logical_name, table_config in TARGET_TABLES.items():
            target_table = table_config.get("target_table", logical_name)
            schema = table_config.get("target_schema", "public") if schema_mode == "schemas" else "public"
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                );
            """, (schema, target_table))
            exists = cursor.fetchone()[0]
            
            table_info = {
                "exists": exists,
                "schema": schema,
                "table_name": target_table,
                "full_name": f"{schema}.{target_table}",
                "row_count": 0,
                "columns": [],
            }
            
            if exists:
                # Get row count
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{target_table};")
                    table_info["row_count"] = cursor.fetchone()[0]
                except:
                    pass
                
                # Get column info
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """, (schema, target_table))
                table_info["columns"] = [
                    {"name": row[0], "type": row[1], "nullable": row[2]}
                    for row in cursor.fetchall()
                ]
            
            info[logical_name] = table_info
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        # Return empty info on error
        pass
    
    return info
