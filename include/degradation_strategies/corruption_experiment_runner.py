# corruption_experiment_runner.py
import pandas as pd
from degradation_strategies.data_corruptor_conjoined_corruptions import create_conjoined_corruption
from degradation_strategies.data_corruptor_missing_values import introduce_missing_values
from degradation_strategies.data_corruptor_outliers import introduce_outliers
from degradation_strategies.data_corruptor_categorical_errors import introduce_categorical_errors
from degradation_strategies.data_corruptor_label_errors import introduce_label_errors
from degradation_strategies.data_corruptor_relationship_violations import introduce_relationship_violations

from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_dataframe
from sql.postgres_manager import PostgresManager


def run_corruption_experiment(
    original_df: pd.DataFrame,
    target_table_name: str,
    dataset_id: str,
    scenario: str,
    corruption_function_name: str = None
) -> dict:
    """
    Run a full corruption experiment:
    - apply selected corruption strategy
    - save corrupted dataset to database
    - evaluate data quality impact via DQ framework

    Args:
        original_df: Clean input dataset to corrupt.
        target_table_name: Database table name used to store corrupted data.
        dataset_id: Unique identifier for this corruption run.
        scenario: Corruption scenario descriptor (e.g., 'mild', 'severe').
        corruption_function_name: Name of corruption function to apply.

    Returns:
        Dictionary containing DQ results or error messages per scenario.
    """
    db_manager = PostgresManager()

    # Available corruption functions
    corruption_functions = {
        "missing_values": introduce_missing_values,
        "outliers": introduce_outliers,
        "categorical_errors": introduce_categorical_errors,
        "label_errors": introduce_label_errors,
        "relationship_violation": introduce_relationship_violations,
        "conjoined_corruption": create_conjoined_corruption,
    }

    corruption_function = corruption_functions[corruption_function_name]
    results = {}

    print(f"\n=== Running {scenario.upper()} corruption scenario ===")

    # Apply corruption strategy
    corrupted_df = corruption_function(original_df, scenario)

    # Save corrupted dataset
    db_manager.save_corrupted_data(
        df=corrupted_df,
        table_name=target_table_name,
        dataset_id=dataset_id,
        corruption_type=corruption_function_name,
        corruption_scenario=scenario,
    )

    # Run data quality validation
    try:
        dq_results = run_data_quality_from_yaml_and_dataframe(
            corrupted_df,
            dataset_id=dataset_id,
            save_to_db=True,
        )

        results[scenario] = {
            "success_rate": dq_results.get("success", False),
            "statistics": dq_results.get("statistics", {}),
            "file_path": target_table_name,
        }

        print(f"{scenario} corruption completed - DQ validation run")

    except Exception as e:
        print(f"Error in {scenario} scenario: {e}")
        results[scenario] = {"error": str(e)}

    return results
