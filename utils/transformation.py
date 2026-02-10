"""
Transformation engine for mapping source V4 schema to target V5 schema.
"""
import os
import copy
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Callable, Any
import pandas as pd
import yaml

from utils.config import DEFAULT_MAPPINGS


class TransformationEngine:
    """
    Engine for transforming extracted data according to mapping configuration.
    """
    
    def __init__(
        self,
        mapping_config: Dict,
        input_dir: str,
        output_dir: str,
        constant_columns: Optional[Dict[str, Dict[str, Any]]] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        Initialize transformation engine.
        
        Args:
            mapping_config: Column mapping configuration
            input_dir: Directory containing extracted CSV files
            output_dir: Directory to save transformed CSV files
            constant_columns: Dict mapping table names to dict of constant column name -> value
            progress_callback: Optional callback for progress updates
        """
        self.mapping_config = mapping_config
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.constant_columns = constant_columns or {}
        self.progress_callback = progress_callback
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def _report_progress(self, table_name: str, current: int, total: int):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(table_name, current, total)
    
    def _find_latest_file(self, prefix: str) -> Optional[str]:
        """Find the most recent file with given prefix in input directory."""
        files = [f for f in os.listdir(self.input_dir) if f.startswith(prefix) and f.endswith('.csv')]
        if not files:
            return None
        # Sort by timestamp in filename
        files.sort(reverse=True)
        return os.path.join(self.input_dir, files[0])
    
    def _apply_column_mapping(
        self,
        df: pd.DataFrame,
        table_name: str,
        mapping: Dict
    ) -> pd.DataFrame:
        """
        Apply column mapping to a dataframe.
        
        Args:
            df: Source dataframe
            table_name: Name of the table for logging
            mapping: Mapping configuration for this table
            
        Returns:
            Transformed dataframe
        """
        columns = mapping.get("columns", [])
        
        if not columns:
            return df
        
        # Build rename mapping and select columns
        rename_map = {}
        select_cols = []
        
        for col_map in columns:
            source_col = col_map.get("source")
            target_col = col_map.get("target")
            
            if source_col in df.columns:
                rename_map[source_col] = target_col
                select_cols.append(source_col)
        
        # Select and rename columns
        if select_cols:
            result_df = df[select_cols].rename(columns=rename_map)
        else:
            result_df = df.copy()
        
        # Add constant columns for this table
        if table_name in self.constant_columns:
            for col_name, col_value in self.constant_columns[table_name].items():
                result_df[col_name] = col_value
        
        return result_df
    
    def transform_users(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform users table."""
        input_file = self._find_latest_file("users_")
        if not input_file or "groups" in input_file:
            # Try to find non-groups file
            files = [f for f in os.listdir(self.input_dir) 
                     if f.startswith("users_") and "groups" not in f and f.endswith('.csv')]
            if files:
                files.sort(reverse=True)
                input_file = os.path.join(self.input_dir, files[0])
            else:
                return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("users", {})
        
        transformed_df = self._apply_column_mapping(df, "users", mapping)
        
        output_path = os.path.join(self.output_dir, f"users_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def transform_users_groups(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform users_groups table."""
        input_file = self._find_latest_file("users_groups_")
        if not input_file:
            return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("users_groups", {})
        
        transformed_df = self._apply_column_mapping(df, "users_groups", mapping)
        
        output_path = os.path.join(self.output_dir, f"users_groups_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def transform_folders(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform folders table."""
        input_file = self._find_latest_file("folders_")
        if not input_file:
            return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("folders", {})
        
        transformed_df = self._apply_column_mapping(df, "folders", mapping)
        
        output_path = os.path.join(self.output_dir, f"folders_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def transform_documents(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform documents table."""
        input_file = self._find_latest_file("documents_")
        if not input_file:
            return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("custom_documents", {})
        
        transformed_df = self._apply_column_mapping(df, "custom_documents", mapping)
        
        output_path = os.path.join(self.output_dir, f"documents_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def transform_embeddings(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform embeddings table."""
        input_file = self._find_latest_file("embeddings_")
        if not input_file:
            return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("embeddings", {})
        
        transformed_df = self._apply_column_mapping(df, "embeddings", mapping)
        
        output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def transform_agents(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Transform agents table."""
        input_file = self._find_latest_file("agents_")
        if not input_file:
            return None, None
        
        df = pd.read_csv(input_file)
        mapping = self.mapping_config.get("agents", {})
        
        transformed_df = self._apply_column_mapping(df, "agents", mapping)
        
        output_path = os.path.join(self.output_dir, f"agents_{self.timestamp}.csv")
        transformed_df.to_csv(output_path, index=False)
        
        return transformed_df, output_path
    
    def run_full_transformation(self) -> Dict:
        """
        Run full transformation pipeline.
        
        Returns:
            Dictionary with transformation results
        """
        results = {
            "timestamp": self.timestamp,
            "files": {},
            "summary": {},
            "errors": []
        }
        
        transforms = [
            ("users_groups", self.transform_users_groups),
            ("users", self.transform_users),
            ("folders", self.transform_folders),
            ("documents", self.transform_documents),
            ("embeddings", self.transform_embeddings),
            ("agents", self.transform_agents),
        ]
        
        total = len(transforms)
        
        for i, (name, transform_func) in enumerate(transforms):
            self._report_progress(name, i + 1, total)
            
            try:
                df, path = transform_func()
                if df is not None and path:
                    results["files"][name] = path
                    results["summary"][name] = len(df)
                else:
                    results["summary"][name] = 0
            except Exception as e:
                results["errors"].append(f"Error transforming {name}: {str(e)}")
                results["summary"][name] = 0
        
        return results


def get_default_mapping_config() -> Dict:
    """Get a copy of the default mapping configuration."""
    return copy.deepcopy(DEFAULT_MAPPINGS)


def save_mapping_config(config: Dict, filepath: str) -> bool:
    """
    Save mapping configuration to a YAML file.
    
    Args:
        config: Mapping configuration dictionary
        filepath: Path to save the YAML file
        
    Returns:
        True if successful
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        return True
    except Exception as e:
        return False


def load_mapping_config(filepath: str) -> Optional[Dict]:
    """
    Load mapping configuration from a YAML file.
    
    Args:
        filepath: Path to the YAML file
        
    Returns:
        Mapping configuration dictionary or None if failed
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        return None


def mapping_to_dataframe(mapping: Dict, table_name: str) -> pd.DataFrame:
    """
    Convert mapping configuration for a table to a dataframe for display/editing.
    
    Args:
        mapping: Full mapping configuration
        table_name: Name of the table
        
    Returns:
        DataFrame with columns: source_col, target_col, type, flag
    """
    table_config = mapping.get(table_name, {})
    columns = table_config.get("columns", [])
    
    rows = []
    for col in columns:
        rows.append({
            "source_col": col.get("source", ""),
            "target_col": col.get("target", ""),
            "type": col.get("type", ""),
            "flag": col.get("flag", ""),
        })
    
    return pd.DataFrame(rows)


def dataframe_to_mapping(df: pd.DataFrame, existing_mapping: Dict, table_name: str) -> Dict:
    """
    Convert edited dataframe back to mapping configuration.
    
    Args:
        df: Edited dataframe
        existing_mapping: Existing mapping configuration
        table_name: Name of the table
        
    Returns:
        Updated mapping configuration
    """
    mapping = copy.deepcopy(existing_mapping)
    
    if table_name not in mapping:
        mapping[table_name] = {}
    
    columns = []
    for _, row in df.iterrows():
        col_config = {
            "source": row.get("source_col", ""),
            "target": row.get("target_col", ""),
            "type": row.get("type", ""),
        }
        if row.get("flag"):
            col_config["flag"] = row.get("flag")
        columns.append(col_config)
    
    mapping[table_name]["columns"] = columns
    
    return mapping


def get_flagged_fields(mapping: Dict) -> List[Dict]:
    """
    Get all fields that have flags/warnings.
    
    Args:
        mapping: Mapping configuration
        
    Returns:
        List of flagged fields with table name, column info, and flag
    """
    flagged = []
    
    for table_name, table_config in mapping.items():
        # Table-level flag
        if table_config.get("flag"):
            flagged.append({
                "table": table_name,
                "column": "(entire table)",
                "flag": table_config["flag"]
            })
        
        # Column-level flags
        for col in table_config.get("columns", []):
            if col.get("flag"):
                flagged.append({
                    "table": table_name,
                    "column": f"{col['source']} → {col['target']}",
                    "flag": col["flag"]
                })
    
    return flagged
