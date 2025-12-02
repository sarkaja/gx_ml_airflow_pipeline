"""
Airflow DAG for Data Quality (DQ) validation of the German Credit dataset.

This DAG:
- Loads corrupted data from PostgreSQL,
- Executes YAML-based data quality validation,
- Computes DQ score and identifies failing expectations,
- Decides whether ML can run or remediation is required,
- Triggers either the ML pipeline or the DQ remediation DAG.

Triggered automatically whenever a new corrupted dataset is produced
(uses DQ_RESULTS_DATASET as schedule dependency).
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from sql.postgres_manager import PostgresManager
from datasets import DQ_RESULTS_DATASET

logger = logging.getLogger("airflow.task")


def run_data_quality_validation(**context):
    """
    Run DQ validation on corrupted German Credit data.

    Steps:
        1. Load corrupted dataset from PostgreSQL.
        2. Run YAML-configured DQ expectations.
        3. Extract summary statistics and failing expectations.
        4. Push metadata to XCom for follow-up tasks.

    Args:
        context: Airflow task execution context

    Returns:
        dict: Summary with success rate and overall DQ decision
    """
    try:
        logger.info("=== STARTING DQ VALIDATION ===")

        from scripts.get_environment_config import ProjectConfig
        from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_dataframe

        cfg = ProjectConfig()

        table_name = cfg.table_names["corrupted"]
        corruption_type = cfg.get_corruption_function()
        corruption_scenario = cfg.get_scenario()
        dq_threshold = cfg.get_dq_threshold() or 90.0

        db_manager = PostgresManager()
        df = db_manager.load_clean_data(
            corruption_scenario=corruption_scenario,
            corruption_type=corruption_type,
            table_name=table_name,
        )

        dataset_id = df["dataset_id"].iloc[0]

        logger.info(f"Running DQ validation for table: {table_name}")
        logger.info(f"Scenario: {corruption_scenario}, Type: {corruption_type}")
        logger.info(f"DQ threshold: {dq_threshold}%")
        logger.info(f"Dataset ID: {dataset_id}")

        # Run Great Expectations validation
        results = run_data_quality_from_yaml_and_dataframe(
            df=df,
            dataset_id=dataset_id,
            save_to_db=True,
        )

        stats = results.get("statistics", {})
        success_rate = stats.get("success_percent", 0.0)
        overall_success = success_rate >= dq_threshold

        # Collect failing expectations
        failed_expectations = []
        for r in results.get("results", []):
            if not r.get("success"):
                cfg_raw = r.get("expectation_config", {})
                col = cfg_raw.get("kwargs", {}).get("column")
                if col:
                    failed_expectations.append(
                        {
                            "type": cfg_raw.get("type"),
                            "column": col,
                        }
                    )

        dq_summary = {
            "success_rate": success_rate,
            "overall_success": overall_success,
            "total_expectations": stats.get("evaluated_expectations", 0),
            "successful_expectations": stats.get("successful_expectations", 0),
        }

        logger.info(f"DQ validation completed: {success_rate:.1f}% success")
        logger.info(f"Failed expectations: {failed_expectations}")

        ti = context["task_instance"]
        ti.xcom_push("dq_summary", dq_summary)
        ti.xcom_push("failed_expectations", failed_expectations)
        ti.xcom_push("table_name", table_name)
        ti.xcom_push("dataset_id", dataset_id)
        ti.xcom_push("initial_unsuccessful_expectations", stats.get("unsuccessful_expectations"))
        ti.xcom_push("initial_success_rate", stats.get("success_percent"))

        return dq_summary

    except Exception as e:
        logger.error(f"DQ validation failed: {e}")
        context["task_instance"].xcom_push(
            key="dq_summary",
            value={"overall_success": False, "error": str(e)},
        )
        raise


def evaluate_dq_results(**context):
    """
    Evaluate DQ summary and choose the next pipeline step.

    Args:
        context: Airflow execution context

    Returns:
        str: task_id of next branch ("successful_dq" or "insufficient_dq")
    """
    try:
        dq_summary = context["task_instance"].xcom_pull(
            task_ids="run_data_quality_validation",
            key="dq_summary",
        )

        if not dq_summary:
            logger.error("DQ summary missing from XCom.")
            return "fail"

        success_rate = dq_summary.get("success_rate", 0)
        overall_success = dq_summary.get("overall_success", False)

        logger.info(f"DQ Evaluation: {success_rate:.1f}% success (OK={overall_success})")

        return "successful_dq" if overall_success else "insufficient_dq"

    except Exception as e:
        logger.error(f"Error evaluating DQ results: {e}")
        return "end_dq"


def trigger_ml(**context):
    """Placeholder for manual ML trigger if needed."""
    logger.info("Proceeding with ML — DQ successful.")


def trigger_remediation(**context):
    """Placeholder for manual remediation trigger if needed."""
    logger.info("Proceeding with remediation — DQ insufficient.")


# ------------------------- DAG Definition ------------------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "german_credit_data_quality",
    default_args=default_args,
    description="Data Quality validation pipeline for German Credit dataset",
    schedule=[DQ_RESULTS_DATASET],  # triggered by upstream corruption DAG
    catchup=False,
    tags=["data_quality"],
) as dag:

    start = EmptyOperator(task_id="start_dq")

    data_quality_task = PythonOperator(
        task_id="run_data_quality_validation",
        python_callable=run_data_quality_validation,
    )

    evaluation_task = BranchPythonOperator(
        task_id="evaluate_dq_results",
        python_callable=evaluate_dq_results,
    )

    trigger_ml_task = TriggerDagRunOperator(
        task_id="trigger_ml_pipeline",
        trigger_dag_id="german_credit_ml_pipeline",
        conf={
            "table_name": "{{ ti.xcom_pull('run_data_quality_validation', key='table_name') }}",
            "dataset_id": "{{ ti.xcom_pull('run_data_quality_validation', key='dataset_id') }}",
            "source_dag": "german_credit_data_quality",
        },
        wait_for_completion=False,
    )

    trigger_remediation_task = TriggerDagRunOperator(
        task_id="trigger_remediation_pipeline",
        trigger_dag_id="german_credit_dq_remediation",
        conf={
            "failed_expectations": "{{ ti.xcom_pull('run_data_quality_validation', key='failed_expectations') }}",
            "initial_success_rate": "{{ ti.xcom_pull('run_data_quality_validation', key='initial_success_rate') }}",
            "initial_unsuccessful_expectations": "{{ ti.xcom_pull('run_data_quality_validation', key='initial_unsuccessful_expectations') }}",
            "dataset_id": "{{ ti.xcom_pull('run_data_quality_validation', key='dataset_id') }}",
            "source_dag": "german_credit_data_quality",
        },
        wait_for_completion=False,
    )

    success_branch = EmptyOperator(task_id="successful_dq")
    fail_branch = EmptyOperator(task_id="insufficient_dq")
    end = EmptyOperator(task_id="end_dq", trigger_rule="none_failed")

    # DAG structure
    start >> data_quality_task >> evaluation_task
    evaluation_task >> [success_branch, fail_branch, end]
    success_branch >> trigger_ml_task >> end
    fail_branch >> trigger_remediation_task >> end
