"""
Data Quality Remediation DAG for the German Credit dataset.

This DAG is triggered when the main DQ pipeline detects insufficient data quality.
It performs the following steps:

1. Load the corrupted dataset version.
2. Run the remediation pipeline to improve data quality.
3. Recalculate DQ metrics and evaluate whether remediation succeeded.
4. If the final DQ score meets the threshold (>= 90%), trigger the ML pipeline.
5. Otherwise, stop or retry depending on the configuration.

Trigger configuration must include:
    dataset_id
    initial_success_rate
    initial_unsuccessful_expectations
    failed_expectations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import os
import sys
import logging

logger = logging.getLogger("airflow.task")


def run_dq_remediation(**context):
    """
    Execute remediation pipeline on the corrupted dataset.

    Steps:
        - Load corrupted dataset from PostgreSQL
        - Apply remediation rules based on failed expectations
        - Save remediated dataset
        - Recalculate final DQ metrics

    Args:
        context: Airflow execution context

    Returns:
        dict: Summary with initial/final success rate and improvement indicators
    """
    try:
        logger.info("=== STARTING DQ REMEDIATION PIPELINE ===")

        from scripts.get_environment_config import ProjectConfig
        from sql.postgres_manager import PostgresManager

        cfg = ProjectConfig()
        dag_run = context.get("dag_run")

        dataset_id = str(dag_run.conf.get("dataset_id"))
        corruption_type = cfg.get_corruption_function()
        corruption_scenario = cfg.get_scenario()
        table_name = cfg.table_names["corrupted"]

        db_manager = PostgresManager()

        # dynamic import for remediation strategies
        project_path = cfg.project_path
        sys.path.append(os.path.join(project_path, "include", "remediation_strategies"))
        from remediation_strategies.dq_improvement_pipeline import DQImprovementPipeline

        # load corrupted dataset
        df = db_manager.load_clean_data(
            table_name=table_name,
            corruption_scenario=corruption_scenario,
            corruption_type=corruption_type,
            dataset_id=dataset_id,
        )

        initial_unsuccessful = float(dag_run.conf.get("initial_unsuccessful_expectations"))
        initial_success_rate = float(dag_run.conf.get("initial_success_rate"))
        failed_expectations = dag_run.conf.get("failed_expectations")

        logger.info(f"Failed expectations: {failed_expectations}")
        logger.info(f"Initial unsuccessful: {initial_unsuccessful}")

        target_table_name = table_name.replace("corrupted", "remediated")
        remed_dataset_id = dataset_id.replace("corrupt", "remed")

        # run remediation
        remediation_pipeline = DQImprovementPipeline()
        results = remediation_pipeline.run_improvement_pipeline(
            df=df,
            target_table_name=target_table_name,
            initial_success_rate=initial_success_rate,
            initial_unsuccessful_expectations=initial_unsuccessful,
            failed_expectations=failed_expectations,
            dataset_id=remed_dataset_id,
        )

        improvement_stats = results["improvement_stats"]
        final_rate = improvement_stats["final_success_rate"]
        final_fails = improvement_stats["final_failures"]

        summary = {
            "initial_success_rate": improvement_stats["initial_success_rate"],
            "final_success_rate": final_rate,
            "improvement": improvement_stats["improvement"],
            "failures_fixed": improvement_stats["failures_fixed"],
            "remediation_success": final_rate >= 90.0,
        }

        # log outcomes
        logger.info(f"DQ Remediation completed")
        logger.info(f"Initial: {summary['initial_success_rate']:.1f}% → Final: {final_rate:.1f}%")
        logger.info(f"Improvement: +{summary['improvement']:.1f}%")
        logger.info(f"Remediation success: {summary['remediation_success']}")

        # push XCom results
        ti = context["task_instance"]
        ti.xcom_push("remediation_summary", summary)
        ti.xcom_push("initial_unsuccessful_expectations", final_fails)
        ti.xcom_push("initial_success_rate", final_rate)
        ti.xcom_push("table_name", target_table_name)
        ti.xcom_push("dataset_id", remed_dataset_id)

        return summary

    except Exception as e:
        logger.error(f"DQ Remediation failed: {e}")
        context["task_instance"].xcom_push(
            key="remediation_summary",
            value={"remediation_success": False, "error": str(e)},
        )
        raise


def evaluate_remediation_results(**context):
    """
    Decide next step based on remediation results.

    Args:
        context: Airflow execution context

    Returns:
        str: task_id of the next operator  
             ("successful_remediation" or "insufficient_remediation")
    """
    try:
        summary = context["task_instance"].xcom_pull(
            task_ids="run_dq_remediation",
            key="remediation_summary",
        )

        if not summary:
            logger.error("No remediation summary available.")
            return "failed_remediation"

        final_rate = summary.get("final_success_rate", 0)
        success = summary.get("remediation_success", False)

        logger.info(
            f"Remediation evaluated: final={final_rate:.1f}%, success={success}"
        )

        return "successful_remediation" if success else "insufficient_remediation"

    except Exception as e:
        logger.error(f"Error evaluating remediation results: {e}")
        return "failed_remediation"


# ------------------------- DAG Definition ------------------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "german_credit_dq_remediation",
    default_args=default_args,
    description="Data Quality remediation workflow for corrupted German Credit dataset",
    schedule=None,  # Triggered only when DQ fails
    catchup=False,
    tags=["data_quality", "remediation"],
) as dag:

    start = EmptyOperator(task_id="start_remediation")

    remediation_task = PythonOperator(
        task_id="run_dq_remediation",
        python_callable=run_dq_remediation,
    )

    evaluation_task = BranchPythonOperator(
        task_id="evaluate_remediation_results",
        python_callable=evaluate_remediation_results,
    )

    trigger_ml_task = TriggerDagRunOperator(
        task_id="trigger_ml_pipeline",
        trigger_dag_id="german_credit_ml_pipeline",
        conf={
            "table_name": "{{ ti.xcom_pull('run_dq_remediation', key='table_name') }}",
            "dataset_id": "{{ ti.xcom_pull('run_dq_remediation', key='dataset_id') }}",
            "source_dag": "german_credit_dq_remediation",
            "execution_date": "{{ ds }}",
        },
        wait_for_completion=False,
    )

    success_branch = EmptyOperator(task_id="successful_remediation")
    fail_branch = EmptyOperator(task_id="insufficient_remediation")
    end = EmptyOperator(task_id="end_remediation", trigger_rule="none_failed")

    # DAG flow
    start >> remediation_task >> evaluation_task
    evaluation_task >> [success_branch, fail_branch]
    success_branch >> trigger_ml_task >> end
    fail_branch >> end
