# dq_improvement_pipeline.py
import pandas as pd
import numpy as np
import json
from data_quality_improver import DataQualityImprover
from advanced_data_cleaning import AdvancedDataCleaning
from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_csv
from dq_framework.yaml_dq_loader import load_yaml_config
import os
import logging

class DQImprovementPipeline:
    """
    Complete pipeline for improving data quality.
    """
    
    def __init__(self, yaml_config_path: str):
        """
        Initialize the DQ improvement pipeline.
        
        Args:
            yaml_config_path: Path to YAML configuration file
        """
        self.yaml_config_path = yaml_config_path
        self.validation_rules = self._load_validation_rules()
        self.basic_improver = DataQualityImprover(self.validation_rules)
        self.advanced_cleaner = AdvancedDataCleaning()
    
    def _load_validation_rules(self) -> dict:
        """
        Load validation rules from YAML configuration.
        
        Returns:
            Dictionary with validation rules
        """
        cfg = load_yaml_config(self.yaml_config_path)
        return {
            'expectations': cfg.expectations,
        }
    
    def run_improvement_pipeline(self, input_csv_path: str, output_csv_path: str = None) -> dict:
        """
        Run complete data quality improvement pipeline.
        
        Args:
            input_csv_path: Path to input CSV file with data quality issues
            output_csv_path: Path for output improved CSV file
            
        Returns:
            Dictionary with improvement results and statistics
        """

        df_original = pd.read_csv(input_csv_path, sep=';')
        
        # DQ validation on original data and further improvement
        print("Running initial DQ validation...")
        initial_results = run_data_quality_from_yaml_and_csv(
            self.yaml_config_path,
            input_csv_path,
            dataset_id="before_improvement",
            run_id="initial_check",
            save_to_db=False
        )

        df_improved = self.basic_improver.improve_data_quality(df_original)
        
        # Advanced cleaning part
        numeric_columns = df_improved.select_dtypes(include=[np.number]).columns.tolist()
        if 'category' in numeric_columns:
            numeric_columns.remove('category')
        
        df_improved = self.advanced_cleaner.multivariate_outlier_detection(df_improved)
        df_improved = self.advanced_cleaner.smart_categorical_imputation(df_improved)
        
        # Save improved data and run DQ validation on them
        if output_csv_path is None:
            base_name = os.path.splitext(input_csv_path)[0]
            output_csv_path = f"{base_name}_improved.csv"
        
        df_improved.to_csv(output_csv_path, sep=';', index=False)

        final_results = run_data_quality_from_yaml_and_csv(
            self.yaml_config_path,
            output_csv_path,
            dataset_id="after_improvement", 
            run_id="final_check",
            save_to_db=False
        )
        
        # Calculate improvement metrics
        improvement_stats = self._calculate_improvement(initial_results, final_results)
        
        return {
            'initial_results': initial_results,
            'final_results': final_results,
            'improvement_stats': improvement_stats,
            'improved_file_path': output_csv_path
        }
    

    def _calculate_improvement(self, initial_results: dict, final_results: dict) -> dict:
        """
        Calculate data quality improvement metrics.
        
        Args:
            initial_results: DQ results before improvement
            final_results: DQ results after improvement
            
        Returns:
            Dictionary with improvement statistics
        """
        initial_stats = initial_results.get('statistics', {})
        final_stats = final_results.get('statistics', {})
        
        initial_success_rate = initial_stats.get('success_percent', 0)
        final_success_rate = final_stats.get('success_percent', 0)
        
        return {
            'initial_success_rate': initial_success_rate,
            'final_success_rate': final_success_rate,
            'improvement': final_success_rate - initial_success_rate,
            'initial_failures': initial_stats.get('unsuccessful_expectations', 0),
            'final_failures': final_stats.get('unsuccessful_expectations', 0),
            'failures_fixed': initial_stats.get('unsuccessful_expectations', 0) - final_stats.get('unsuccessful_expectations', 0)
        }