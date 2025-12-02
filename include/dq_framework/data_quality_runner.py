# data_quality_runner.py
from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, Union
import os
import sys
import pandas as pd
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
from scripts.get_environment_config import ProjectConfig
from dq_framework.gx_yaml_dq_loader import gx_load_yaml_config
from gx_pandas_validator import PandasYamlValidator
from column_utils import normalize_columns
from dq_result_saver import DQResultSaver
from dq_framework.gx_yaml_dq_loader import create_expectation_objects

def load_csv_as_df(
    csv_path: Union[str, "os.PathLike"],
    *,
    sep: str = ";",
    encoding: str = "utf-8",
    na_values=None,
    dtype: Optional[dict] = None,
    normalize_colnames: bool = True,
) -> pd.DataFrame:
    """Load CSV file into DataFrame with optional column normalization."""
    df = pd.read_csv(
        csv_path,
        sep=sep,
        encoding=encoding,
        na_values=na_values,
        dtype=dtype,
    )
    if normalize_colnames:
        df = normalize_columns(df)
    return df

def run_data_quality_from_yaml_and_dataframe(
    df: pd.DataFrame,
    save_to_db: bool = True,
    dataset_id: Optional[str] = None,
) -> Dict:
    """
    Run data quality validation using YAML config and DataFrame.
    
    Args:
        yaml_str_or_path: YAML config string or file path
        df: DataFrame with data to validate
        save_to_db: Whether to save results to database
        dataset_id: Dataset identifier
        normalize_colnames: Whether to normalize column names
    
    Returns:
        Dictionary with validation results
    """
    try:
        cfg = ProjectConfig()
        yaml_validation_path = cfg.yaml_validation_path
        yaml_cfg = gx_load_yaml_config(yaml_validation_path)
        
        start_time = time.time()

        expectations = create_expectation_objects(yaml_cfg)
        validator = PandasYamlValidator()
        results = validator.validate(
            df=df,
            expectations=expectations,
            config=yaml_cfg,
        )
        
        validation_duration = time.time() - start_time
        
        if save_to_db:
            saver = DQResultSaver()
            success = saver.save_results(
                dataset_id=dataset_id,
                expectation_suite_name=yaml_cfg.expectation_suite_name,
                validation_results=results,
                validation_duration=validation_duration
            )
            
            if success:
                print(f"DQ results saved to DB (dataset: {dataset_id})")
            else:
                print("Error saving results to DB")
        
        return results
        
    except Exception as e:
        print(f"Error in data quality validation: {e}")
        raise
