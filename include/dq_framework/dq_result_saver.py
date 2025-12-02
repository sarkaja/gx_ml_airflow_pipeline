# dq_result_saver.py
from __future__ import annotations

import json
from typing import Dict, List, Any, Optional
import logging
import os

from scripts.get_environment_config import ProjectConfig
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("airflow.task")


class DQResultSaver:
    """
    Persist Data Quality (DQ) validation results and analysis metrics into PostgreSQL.
    Handles statistics computation, result analysis, and database insertion.
    """

    def __init__(self):
        """
        Initialize the DQ result saver with database connection and run metadata.
        """
        cfg = ProjectConfig()
        self.connection_string = os.getenv("AIRFLOW_CONN_DQ_POSTGRES")
        self.run_id = cfg.get_run_id()

        self.engine = create_engine(self.connection_string)
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def _get_connection(self):
        """
        Create a new SQLAlchemy session.

        Returns:
            Database session object.
        """
        return self.SessionLocal()

    def _calculate_statistics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute DQ statistics when results do not contain a 'statistics' block.

        Args:
            results: Raw DQ validation output.

        Returns:
            Dictionary containing summary statistics.
        """
        results_list = results.get("results", [])

        total = len(results_list)
        successful = sum(1 for r in results_list if r.get("success", False))
        failed = total - successful

        success_rate = (successful / total * 100) if total > 0 else 0.0

        return {
            "evaluated_expectations": total,
            "successful_expectations": successful,
            "unsuccessful_expectations": failed,
            "success_percent": success_rate,
        }

    def analyze_validation_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze validation results to extract detailed quality metrics:
        failures, severity, key column problems, and completeness/consistency health.

        Args:
            results: DQ validation results.

        Returns:
            Dictionary of aggregated analysis metrics.
        """
        logger.info(f"Full results: {results}")

        # Use existing statistics or compute manually
        stats = results.get("statistics") or self._calculate_statistics(results)

        total_expectations = stats.get("evaluated_expectations", 0)
        successful = stats.get("successful_expectations", 0)
        failed = stats.get("unsuccessful_expectations", 0)
        success_rate = stats.get("success_percent", 0.0)

        failed_columns = set()
        critical_failures = 0
        warning_failures = 0
        key_columns_results = {}
        unexpected_count_total = 0

        key_columns = {
            "checking_account_status", "credit_history", "purpose",
            "credit_amount", "duration_in_month", "age_in_years",
            "savings_account_bonds", "foreign_worker", "category",
        }

        for result in results.get("results", []):
            expectation_config = result.get("expectation_config", {})
            kwargs = expectation_config.get("kwargs", {})
            column = kwargs.get("column")

            result_inner = result.get("result", {})
            unexpected_count_total += result_inner.get("unexpected_count", 0)

            if not result.get("success", False):
                severity = expectation_config.get("severity", "critical")

                if severity == "critical":
                    critical_failures += 1
                else:
                    warning_failures += 1

                if column:
                    failed_columns.add(column)

                    if column in key_columns:
                        key_columns_results[column] = {
                            "expectation_type": expectation_config.get("type", ""),
                            "success": False,
                            "severity": severity,
                        }

        # Completeness check
        completeness_ok = all(
            r.get("success", False)
            for r in results.get("results", [])
            if "expect_column_values_to_not_be_null" in r.get("expectation_config", {}).get("type", "")
        )

        # Consistency check
        consistency_ok = all(
            r.get("success", False)
            for r in results.get("results", [])
            if "expect_column_values_to_be_in_set" in r.get("expectation_config", {}).get("type", "")
        )

        # Real success rate including proportion of unexpected values
        record_count = self._get_record_count(results)
        total_examined_values = total_expectations * (record_count or 1)
        real_success_rate = (
            ((total_examined_values - unexpected_count_total) / total_examined_values) * 100
            if total_examined_values > 0 else 0.0
        )

        logger.info(f"Success rate with proportion adjustment: {real_success_rate:.2f}%")

        return {
            "total_expectations": total_expectations,
            "successful_expectations": successful,
            "failed_expectations": failed,
            "success_rate": success_rate,
            "overall_success": results.get("success", False),
            "completeness_ok": completeness_ok,
            "consistency_ok": consistency_ok,
            "failed_columns": list(failed_columns),
            "critical_failures": critical_failures,
            "warning_failures": warning_failures,
            "key_columns_validation": key_columns_results,
            "records_processed": record_count,
        }

    def _get_record_count(self, results: Dict[str, Any]) -> Optional[int]:
        """
        Extract number of processed records from DQ results.

        Args:
            results: DQ validation output.

        Returns:
            Integer count of processed records, if available.
        """
        for r in results.get("results", []):
            result_data = r.get("result", {})
            if "element_count" in result_data:
                return result_data["element_count"]
        return None

    def save_results(
        self,
        dataset_id: str,
        expectation_suite_name: str,
        validation_results: Dict[str, Any],
        validation_duration: Optional[float] = None,
    ) -> bool:
        """
        Persist validation results and analysis to PostgreSQL.

        Args:
            dataset_id: Unique dataset identifier.
            expectation_suite_name: Expectation suite name.
            validation_results: Raw validation result dictionary.
            validation_duration: Time taken for validation (seconds).

        Returns:
            True if save succeeded, False otherwise.
        """
        try:
            logger.info(f"Saving DQ results: dataset={dataset_id}, run_id={self.run_id}")

            analysis = self.analyze_validation_results(validation_results)

            insert_query = text("""
                INSERT INTO public.dq_results (
                    dataset_id, run_id, expectation_suite_name,
                    total_expectations, successful_expectations, failed_expectations,
                    success_rate, overall_success, completeness_ok, consistency_ok,
                    failed_columns, critical_failures, warning_failures,
                    key_columns_validation, validation_duration_seconds, records_processed
                ) VALUES (
                    :dataset_id, :run_id, :suite,
                    :total, :successful, :failed,
                    :rate, :overall, :complete, :consistent,
                    :failed_cols, :critical, :warning,
                    :key_cols, :duration, :records
                )
            """)

            with self._get_connection() as session:
                session.execute(
                    insert_query,
                    {
                        "dataset_id": dataset_id,
                        "run_id": self.run_id,
                        "suite": expectation_suite_name,
                        "total": analysis["total_expectations"],
                        "successful": analysis["successful_expectations"],
                        "failed": analysis["failed_expectations"],
                        "rate": analysis["success_rate"],
                        "overall": analysis["overall_success"],
                        "complete": analysis["completeness_ok"],
                        "consistent": analysis["consistency_ok"],
                        "failed_cols": analysis["failed_columns"],
                        "critical": analysis["critical_failures"],
                        "warning": analysis["warning_failures"],
                        "key_cols": json.dumps(analysis["key_columns_validation"]),
                        "duration": validation_duration,
                        "records": analysis["records_processed"],
                    },
                )
                session.commit()

            logger.info("DQ results saved successfully.")
            return True

        except Exception as e:
            logger.error(f"Failed to save DQ results: {e}")
            return False


    def get_recent_failures(self, dataset_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent failures for a dataset.

        Args:
            dataset_id: Dataset identifier
            limit: Maximum number of results to return

        Returns:
            List of failure records
        """
        try:
            query = text("""
                SELECT run_id, expectation_suite_name, failed_columns,
                       critical_failures, success_rate, created_at
                FROM public.dq_results
                WHERE dataset_id = :dataset_id AND overall_success = false
                ORDER BY created_at DESC
                LIMIT :limit
            """)

            with self._get_connection() as session:
                result = session.execute(
                    query,
                    {"dataset_id": dataset_id, "limit": limit}
                )
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error reading failures: {e}")
            return []
