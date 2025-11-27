# corruption_experiment_runner.py
import pandas as pd
from degradation_strategies.data_corruptor_conjoined_corruptions import create_conjoined_corruption
from degradation_strategies.data_corruptor_missing_values import introduce_missing_values
from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_csv, load_csv_as_df
from degradation_strategies.data_corruptor_outliers import introduce_outliers
from degradation_strategies.data_corruptor_categorical_errors import introduce_categorical_errors
from degradation_strategies.data_corruptor_label_errors import introduce_label_errors
from degradation_strategies.data_corruptor_relationship_violations import introduce_relationship_violations
import tempfile
import os

def run_corruption_experiment(original_csv_path: str, yaml_config_path: str, 
                              output_dir: str = "corrupted_datasets"):
    """
    Run comprehensive data corruption experiment and evaluate DQ impact.
    
    Args:
        original_csv_path: Path to original German Credit CSV
        yaml_config_path: Path to DQ YAML configuration
        output_dir: Output directory for corrupted datasets
    """

    os.makedirs(output_dir, exist_ok=True)
    
    original_df = load_csv_as_df(
        original_csv_path,
        sep=';',
    )
    
    scenarios = ["light", "medium", "severe"]
    
    results = {}
    
    for scenario in scenarios:
        print(f"\n=== Running {scenario.upper()} corruption scenario ===")
        
        # create and save the corrupted df
        corrupted_df = create_conjoined_corruption(original_df, scenario)
        
        corrupted_csv_path = os.path.join(output_dir, f"corrupted_data_{scenario}.csv")
        corrupted_df.to_csv(corrupted_csv_path, sep=';', index=False)
        
        # run DQ validation on corrupted data
        try:
            dq_results = run_data_quality_from_yaml_and_csv(
                yaml_config_path,
                corrupted_csv_path,
                dataset_id=f"corrupted_data_{scenario}",
                run_id=f"corrupted_data_{scenario}",
                save_to_db=True
            )
            
            results[scenario] = {
                'success_rate': dq_results.get('success', False),
                'statistics': dq_results.get('statistics', {}),
                'file_path': corrupted_csv_path
            }
            
            print(f"{scenario} corruption completed - DQ validation run")
            
        except Exception as e:
            print(f"Error in {scenario} scenario: {e}")
            results[scenario] = {'error': str(e)}
    
    return results
