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

from yaml_dq_loader import load_yaml_config
from gx_pandas_validator import PandasYamlValidator
from column_utils import normalize_columns
from dq_result_saver import DQResultSaver
from yaml_dq_loader import create_expectation_objects

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


def run_data_quality_from_yaml_and_csv(
    yaml_str_or_path: Union[str, "os.PathLike"],
    csv_path: Union[str, "os.PathLike"],
    sep: str = ";",
    encoding: str = "utf-8",
    na_values=None,
    dtype: Optional[dict] = None,
    run_id: Optional[str] = None,
    save_to_db: bool = True,
    dataset_id: Optional[str] = None,
) -> Dict:
    """
    Run data quality validation using YAML config and CSV data.
    
    Args:
        yaml_str_or_path: YAML config string or file path
        csv_path: CSV file path
        sep: CSV delimiter
        encoding: File encoding
        na_values: Values to treat as NA
        dtype: Column data types
        run_id: Validation run identifier
        primary_keys: List of primary key columns
        result_format: GE result format
        save_to_db: Whether to save results to database
        dataset_id: Dataset identifier
    
    Returns:
        Dictionary with validation results
    """
    dq_conn = os.environ["AIRFLOW_CONN_DQ_POSTGRES"]
    cfg = load_yaml_config(yaml_str_or_path)
    df = load_csv_as_df(
        csv_path,
        sep=sep,
        encoding=encoding,
        na_values=na_values,
        dtype=dtype,
    )

    if dataset_id is None:
        dataset_id = os.path.basename(csv_path).replace('.csv', '')
    
    if run_id is None:
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    start_time = time.time()

    expectations = create_expectation_objects(cfg)
    validator = PandasYamlValidator()
    results = validator.validate(
        df=df,
        expectations=expectations,
        config=cfg,
        run_id=run_id
    )
    
    validation_duration = time.time() - start_time
    
    if save_to_db:
        saver = DQResultSaver(dq_conn)
        success = saver.save_results(
            dataset_id=dataset_id,
            run_id=run_id,
            expectation_suite_name=cfg.expectation_suite_name,
            validation_results=results,
            validation_duration=validation_duration
        )
        
        if success:
            print(f"DQ results saved to DB (dataset: {dataset_id}, run: {run_id})")
        else:
            print("Error saving results to DB")
    
    return results