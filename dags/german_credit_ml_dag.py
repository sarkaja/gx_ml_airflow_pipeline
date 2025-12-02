# dags/german_credit_ml_dag.py
"""
Machine Learning DAG for the German Credit dataset.

This DAG:
- Waits for a trigger from the DQ pipeline
- Loads the corresponding (clean or remediated) dataset from PostgreSQL
- Runs the full ML pipeline (preprocessing → training → evaluation → saving results)

Required trigger parameters (via TriggerDagRunOperator):
    corruption_type
    corruption_scenario
    dataset_id
    table_name
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import logging

logger = logging.getLogger("airflow.task")


def run_ml_pipeline(**context):
    """
    Load corrupted/remediated data and execute full ML pipeline:
    preprocessing → training → evaluation → database saving.

    Args:
        context: Airflow runtime context containing trigger parameters

    Returns:
        ML pipeline evaluation results (best model, metrics, CV scores)
    """
    try:
        logger.info("Starting ML pipeline...")

        from ml_pipeline.ml_runner import MLRunner
        from sql.postgres_manager import PostgresManager

        dag_run = context.get("dag_run")

        corruption_type = dag_run.conf.get("corruption_type")
        dataset_id = dag_run.conf.get("dataset_id")
        corruption_scenario = dag_run.conf.get("corruption_scenario")
        table_name = dag_run.conf.get("table_name")

        db_manager = PostgresManager()

        # Load dataset selected by DQ + corruption pipeline
        df = db_manager.load_selected_columns(
            corruption_scenario=corruption_scenario,
            corruption_type=corruption_type,
            table_name=table_name,
            dataset_id=dataset_id,
        )

        # Run ML pipeline
        ml_runner = MLRunner(test_size=0.2, random_state=42, cv_folds=5)
        results = ml_runner.run_full_pipeline_with_saving(
            df=df,
            dataset_id=dataset_id,
            save_to_db=True,
        )

        # Logging best results
        best_model = results["best_model"]
        best_accuracy = results["best_accuracy"]
        logger.info(f"Best model: {best_model} (Accuracy: {best_accuracy:.3f})")

        for result in results["test_results"]:
            logger.info(
                f"Results for {result['model']}: "
                f"Accuracy={result['accuracy']:.3f}, "
                f"ROC-AUC={result['roc_auc']:.3f}"
            )

        return results

    except Exception as e:
        logger.error(f"ML pipeline failed: {e}")
        raise


# ---------------------- Airflow DAG Definition ---------------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "german_credit_ml_pipeline",
    default_args=default_args,
    description="Machine Learning pipeline for German Credit dataset",
    schedule="@daily",
    catchup=False,
    tags=["machine_learning", "credit_risk"],
) as dag:

    start = EmptyOperator(task_id="start_ml")

    ml_task = PythonOperator(
        task_id="run_ml_pipeline",
        python_callable=run_ml_pipeline,
    )

    end = EmptyOperator(task_id="end_ml")

    start >> ml_task >> end
