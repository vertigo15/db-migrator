"""
Database connection helpers for PostgreSQL.
"""
import subprocess
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st

from utils.config import get_all_table_names, TABLE_DEFINITIONS


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionConfig":
        return cls(
            host=data.get("host", "localhost"),
            port=int(data.get("port", 5432)),
            database=data.get("database", ""),
            username=data.get("username", ""),
            password=data.get("password", ""),
        )


def get_connection(config: ConnectionConfig) -> psycopg2.extensions.connection:
    """
    Create a database connection.
    
    Args:
        config: Connection configuration
        
    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.username,
        password=config.password,
    )


def test_connection(config: ConnectionConfig) -> Tuple[bool, str]:
    """
    Test database connection.
    
    Args:
        config: Connection configuration
        
    Returns:
        Tuple of (success, message)
    """
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return True, f"Connected! PostgreSQL version: {version[:50]}..."
    except psycopg2.OperationalError as e:
        return False, f"Connection failed: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"


def test_target_databases_and_tables(config: ConnectionConfig, schema_mode: str = "databases") -> Dict:
    """
    Comprehensive test of target databases and tables.
    
    For schema_mode='databases', checks:
    - Server connectivity
    - user_db, document_db, completion_db existence
    - Required tables in each database
    
    Args:
        config: Connection configuration (base database)
        schema_mode: 'databases' or 'schemas'
        
    Returns:
        Dict with test results including:
        - success: bool
        - message: str
        - server_connected: bool
        - databases: Dict[db_name, {exists, tables}]
    """
    from utils.loader import TARGET_TABLES
    
    result = {
        "success": False,
        "message": "",
        "server_connected": False,
        "databases": {},
        "version": None
    }
    
    # Test server connection
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        result["version"] = version
        result["server_connected"] = True
        cursor.close()
        conn.close()
    except Exception as e:
        result["message"] = f"Server connection failed: {str(e)}"
        return result
    
    if schema_mode == "databases":
        # Group tables by target database
        db_tables = {}
        for table_name, table_config in TARGET_TABLES.items():
            target_db = table_config.get("target_schema", "public")
            target_table = table_config.get("target_table", table_name)
            if target_db not in db_tables:
                db_tables[target_db] = []
            db_tables[target_db].append({"logical": table_name, "physical": target_table})
        
        # Test each database
        all_dbs_ok = True
        messages = []
        
        for db_name, expected_tables in db_tables.items():
            db_result = {
                "exists": False,
                "tables": {},
                "error": None
            }
            
            # Try to connect to this database
            try:
                db_config = ConnectionConfig(
                    host=config.host,
                    port=config.port,
                    database=db_name,
                    username=config.username,
                    password=config.password
                )
                conn = get_connection(db_config)
                db_result["exists"] = True
                
                cursor = conn.cursor()
                
                # Check each expected table
                for table_info in expected_tables:
                    table_name = table_info["physical"]
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        );
                    """, (table_name,))
                    exists = cursor.fetchone()[0]
                    
                    # Get row count if exists
                    row_count = 0
                    if exists:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM public.{table_name};")
                            row_count = cursor.fetchone()[0]
                        except:
                            row_count = -1
                    
                    db_result["tables"][table_info["logical"]] = {
                        "physical_name": table_name,
                        "exists": exists,
                        "row_count": row_count
                    }
                    
                    if not exists:
                        all_dbs_ok = False
                        messages.append(f"❌ {db_name}.{table_name} not found")
                    else:
                        messages.append(f"✅ {db_name}.{table_name} ({row_count} rows)")
                
                cursor.close()
                conn.close()
                
            except psycopg2.OperationalError as e:
                db_result["error"] = f"Cannot connect: {str(e)}"
                all_dbs_ok = False
                messages.append(f"❌ Database '{db_name}' not accessible")
            except Exception as e:
                db_result["error"] = str(e)
                all_dbs_ok = False
                messages.append(f"❌ Error checking {db_name}: {str(e)}")
            
            result["databases"][db_name] = db_result
        
        result["success"] = all_dbs_ok
        result["message"] = "\n".join(messages)
        
    else:  # schema_mode == "schemas"
        # For schemas mode, just test if schemas exist
        try:
            conn = get_connection(config)
            cursor = conn.cursor()
            
            for schema_name in ["user_db", "document_db", "completion_db"]:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.schemata 
                        WHERE schema_name = %s
                    );
                """, (schema_name,))
                exists = cursor.fetchone()[0]
                result["databases"][schema_name] = {"exists": exists, "tables": {}}
            
            cursor.close()
            conn.close()
            result["success"] = all(db["exists"] for db in result["databases"].values())
            
        except Exception as e:
            result["message"] = f"Error checking schemas: {str(e)}"
    
    return result


def check_tables_exist(config: ConnectionConfig, prefix: str) -> Dict[str, bool]:
    """
    Check which tables exist in the database for the given prefix.
    
    Args:
        config: Connection configuration
        prefix: Table prefix (e.g., 'jeen_dev')
        
    Returns:
        Dict mapping logical table names to existence boolean
    """
    table_names = get_all_table_names(prefix)
    results = {}
    
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        
        for logical_name, actual_name in table_names.items():
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (actual_name,))
            exists = cursor.fetchone()[0]
            results[logical_name] = {
                "actual_name": actual_name,
                "exists": exists
            }
        
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        st.error(f"Error checking tables: {e}")
        return {}


def get_table_row_count(config: ConnectionConfig, table_name: str) -> int:
    """Get the row count for a table."""
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM public.{table_name};")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception:
        return -1


def execute_query(config: ConnectionConfig, query: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute a query and return results as a DataFrame.
    
    Args:
        config: Connection configuration
        query: SQL query string
        params: Query parameters
        
    Returns:
        pandas DataFrame with results
    """
    conn = get_connection(config)
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()


def execute_query_chunked(
    config: ConnectionConfig, 
    query: str, 
    params: tuple = None,
    chunk_size: int = 10000
) -> pd.DataFrame:
    """
    Execute a query and return results in chunks (for large tables).
    
    Args:
        config: Connection configuration
        query: SQL query string
        params: Query parameters
        chunk_size: Number of rows per chunk
        
    Returns:
        Generator yielding DataFrame chunks
    """
    conn = get_connection(config)
    try:
        for chunk in pd.read_sql_query(query, conn, params=params, chunksize=chunk_size):
            yield chunk
    finally:
        conn.close()


def get_table_schema(config: ConnectionConfig, table_name: str) -> List[Dict]:
    """
    Get the schema (column info) for a table.
    
    Args:
        config: Connection configuration
        table_name: Name of the table
        
    Returns:
        List of column definitions
    """
    query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = %s
        ORDER BY ordinal_position;
    """
    conn = get_connection(config)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, (table_name,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(row) for row in results]


def run_pg_dump(
    config: ConnectionConfig,
    output_dir: str,
    tables: Optional[List[str]] = None,
    compress: bool = True
) -> Tuple[bool, str, Optional[str]]:
    """
    Run pg_dump to backup the database.
    
    Args:
        config: Connection configuration
        output_dir: Directory to save backup
        tables: Optional list of specific tables to backup
        compress: Whether to compress the output with gzip
        
    Returns:
        Tuple of (success, message, output_file_path)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if tables:
        tables_suffix = "_partial"
    else:
        tables_suffix = "_full"
    
    filename = f"{config.database}{tables_suffix}_{timestamp}.sql"
    if compress:
        filename += ".gz"
    
    output_path = os.path.join(output_dir, filename)
    
    # Build pg_dump command
    cmd = [
        "pg_dump",
        "-h", config.host,
        "-p", str(config.port),
        "-U", config.username,
        "-d", config.database,
        "-F", "p",  # Plain text format
    ]
    
    # Add specific tables if provided
    if tables:
        for table in tables:
            cmd.extend(["-t", f"public.{table}"])
    
    # Set password via environment variable
    env = os.environ.copy()
    env["PGPASSWORD"] = config.password
    
    try:
        if compress:
            # Pipe through gzip
            with open(output_path, "wb") as f:
                dump_proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env
                )
                gzip_proc = subprocess.Popen(
                    ["gzip"],
                    stdin=dump_proc.stdout,
                    stdout=f,
                    stderr=subprocess.PIPE
                )
                dump_proc.stdout.close()
                gzip_proc.communicate()
                dump_proc.wait()
                
                if dump_proc.returncode != 0:
                    stderr = dump_proc.stderr.read().decode()
                    return False, f"pg_dump failed: {stderr}", None
        else:
            with open(output_path, "w") as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    env=env
                )
                if result.returncode != 0:
                    return False, f"pg_dump failed: {result.stderr.decode()}", None
        
        file_size = os.path.getsize(output_path)
        size_mb = file_size / (1024 * 1024)
        return True, f"Backup created: {filename} ({size_mb:.2f} MB)", output_path
        
    except FileNotFoundError:
        return False, "pg_dump not found. Please ensure PostgreSQL client tools are installed.", None
    except Exception as e:
        return False, f"Backup failed: {str(e)}", None


def execute_insert(
    config: ConnectionConfig,
    table_name: str,
    df: pd.DataFrame,
    on_conflict: Optional[str] = None,
    conflict_columns: Optional[List[str]] = None
) -> Tuple[int, int]:
    """
    Insert DataFrame rows into a table.
    
    Args:
        config: Connection configuration
        table_name: Target table name
        df: DataFrame to insert
        on_conflict: 'update' for upsert, None for regular insert
        conflict_columns: Columns to check for conflict (for upsert)
        
    Returns:
        Tuple of (rows_inserted, rows_failed)
    """
    conn = get_connection(config)
    cursor = conn.cursor()
    
    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)
    
    if on_conflict == "update" and conflict_columns:
        update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
        conflict_cols = ", ".join(conflict_columns)
        query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols}) DO UPDATE SET
            {", ".join(update_cols)}
        """
    else:
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    
    inserted = 0
    failed = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute(query, tuple(row))
            inserted += 1
        except Exception as e:
            failed += 1
            # Log error but continue
            
    conn.commit()
    cursor.close()
    conn.close()
    
    return inserted, failed


def truncate_table(config: ConnectionConfig, table_name: str) -> bool:
    """
    Truncate a table.
    
    Args:
        config: Connection configuration
        table_name: Table to truncate
        
    Returns:
        True if successful
    """
    try:
        conn = get_connection(config)
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Failed to truncate {table_name}: {e}")
        return False
