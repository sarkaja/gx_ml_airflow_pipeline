# optimized_dq_pipeline.py
from typing import Dict
import ast
import logging

import pandas as pd

from unified_data_quality_improver import UnifiedDataQualityImprover
from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_dataframe
from dq_framework.gx_yaml_dq_loader import gx_load_yaml_config
from scripts.get_environment_config import ProjectConfig
from sql.postgres_manager import PostgresManager
from ml_pipeline.stats_check import evaluate_remediation_bias

logger = logging.getLogger("airflow.task")


class DQImprovementPipeline:
    """
    Pipeline for conditional DQ remediation and re-validation.
    """

    def __init__(self) -> None:
        """Initialize pipeline with config, validation rules, and improver."""
        cfg = ProjectConfig()
        self.corruption_type = cfg.get_corruption_function()
        self.corruption_scenario = cfg.get_scenario()
        self.yaml_validation_path = cfg.yaml_validation_path
        self.validation_rules = self._load_validation_rules()
        self.improver = UnifiedDataQualityImprover(self.validation_rules)

    def _load_validation_rules(self) -> Dict:
        """
        Load validation rules from GX YAML file.

        Returns:
            Dict mapping expectation names to rules.
        """
        yaml_cfg = gx_load_yaml_config(self.yaml_validation_path)
        return {"expectations": yaml_cfg.expectations}

    def run_improvement_pipeline(
        self,
        df: pd.DataFrame,
        target_table_name: str,
        dataset_id: str,
        failed_expectations: str,
        initial_unsuccessful_expectations: int,
        initial_success_rate: int,
    ) -> Dict:
        """
        Run remediation, save improved data, and perform final DQ validation.

        Args:
            df: Input dataframe to remediate.
            target_table_name: DB table for saving remediated data.
            dataset_id: Identifier for this run.
            failed_expectations: Expectation failures from initial validation.
            initial_unsuccessful_expectations: Original failed expectation count.
            initial_success_rate: Original DQ success rate.

        Returns:
            Dict with final validation results and improvement metrics.
        """
        db_manager = PostgresManager()

        logger.info(
            "Running conditional remediation for failed expectations: %s",
            failed_expectations,
        )
        failed_expectations = ast.literal_eval(failed_expectations)

        # Configure improver
        self.improver.set_failed_expectations(failed_expectations)
        logger.info(
            "Running conditional remediation for %d failed expectations",
            len(failed_expectations),
        )

        # Apply remediation rules
        df_improved = self.improver.improve_data_quality(df)

        # Drift evaluation
        logger.info("Evaluating remediation distribution drift...")
        drift_report = evaluate_remediation_bias(df_improved)
        for col, metrics in drift_report.items():
            dominant_growth = metrics.get("dominant_growth", 0)
            psi = metrics.get("psi", 0)
            kl = metrics.get("kl_divergence", 0)

            if abs(dominant_growth) > 0.10:
                logger.warning(
                    "[%s] dominant value increased by %.2f → potential bias",
                    col, dominant_growth
                )
            if psi > 0.25:
                logger.warning("[%s] PSI=%.3f → strong drift", col, psi)
            if kl > 0.2:
                logger.warning("[%s] KL divergence=%.3f → large distribution shift", col, kl)

        # Save improved data
        db_manager.save_corrupted_data(
            df=df_improved,
            table_name=target_table_name,
            corruption_type=self.corruption_type,
            corruption_scenario=self.corruption_scenario,
            dataset_id=dataset_id,
        )

        # Final validation
        final_results = run_data_quality_from_yaml_and_dataframe(
            df_improved,
            dataset_id=dataset_id,
            save_to_db=True,
        )

        # Improvement metrics
        improvement_stats = self._calculate_improvement(
            initial_success_rate,
            initial_unsuccessful_expectations,
            final_results,
        )

        return {
            "final_results": final_results,
            "improvement_stats": improvement_stats,
            "improved_file_path": target_table_name,
            "remediated_dataframe": df_improved,
            "remediated_expectations": len(failed_expectations),
        }

    def _calculate_improvement(
        self,
        initial_success_rate: int,
        initial_unsuccessful_expectations: int,
        final_results: Dict,
    ) -> Dict:
        """
        Compute before/after DQ success metrics.

        Args:
            initial_success_rate: DQ success rate before remediation.
            initial_unsuccessful_expectations: Number of failed expectations before remediation.
            final_results: Final DQ validation results.

        Returns:
            Dict with success deltas, failure reductions, and percentages.
        """
        final_stats = final_results.get("statistics", {})
        final_success_rate = final_stats.get("success_percent", 0)
        final_failures = final_stats.get("unsuccessful_expectations", 0)

        improvement = final_success_rate - initial_success_rate
        failures_fixed = initial_unsuccessful_expectations - final_failures
        improvement_percentage = (
            (improvement / initial_success_rate * 100)
            if initial_success_rate > 0
            else 0
        )

        return {
            "initial_success_rate": initial_success_rate,
            "final_success_rate": final_success_rate,
            "improvement": improvement,
            "initial_failures": initial_unsuccessful_expectations,
            "final_failures": final_failures,
            "failures_fixed": failures_fixed,
            "improvement_percentage": improvement_percentage,
        }

