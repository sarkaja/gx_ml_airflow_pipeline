# dags/german_credit_initial_load.py
"""
Airflow DAG for the one-time initial load of the German Credit dataset into PostgreSQL.

This DAG:
- Reads the raw CSV from the path defined in ProjectConfig,
- Loads the data into the RAW table in PostgreSQL,
- Assigns a timestamp-based dataset_id (e.g., il_250212_1030).
"""

from datetime import datetime
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from sql.postgres_manager import PostgresManager
from dq_framework.data_quality_runner import load_csv_as_df

logger = logging.getLogger("airflow.task")


def initial_data_load(**context):
    """
    Perform the initial CSV → PostgreSQL load for the German Credit dataset.

    Steps:
        1. Resolve CSV path and RAW table name from ProjectConfig.
        2. Load CSV into a DataFrame.
        3. Save data into the RAW table with a timestamped dataset_id.

    Args:
        context: Airflow execution context (used for execution_date).

    Returns:
        dict: Summary of the load (rows loaded, dataset_version flag).
    """
    try:
        logger.info("=== INITIAL DATA LOAD TO POSTGRESQL ===")

        from scripts.get_environment_config import ProjectConfig

        cfg = ProjectConfig()
        csv_path = cfg.csv_path
        table_name = cfg.table_names["raw"]

        df = load_csv_as_df(csv_path, sep=";")
        logger.info(f"Loaded raw data: {df.shape}")

        # Use execution_date if available, otherwise current time
        execution_date = context.get("execution_date", datetime.now())
        timestamp = execution_date.strftime("%y%m%d_%H%M")
        dataset_id = f"il_{timestamp}"

        db_manager = PostgresManager()
        success = db_manager.save_clean_data(
            df=df,
            table_name=table_name,
            dataset_id=dataset_id,
            data_source="original_csv",
        )

        if success:
            logger.info(f"Successfully loaded {len(df)} rows to PostgreSQL")
            return {
                "load_success": True,
                "rows_loaded": len(df),
                "dataset_version": "initial_load_v1",
            }
        else:
            raise Exception("Failed to load data to PostgreSQL")

    except Exception as e:
        logger.error(f"Initial load failed: {e}")
        raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "retries": 1,
}

with DAG(
    "german_credit_initial_load",
    default_args=default_args,
    description="One-time initial load of German Credit data into PostgreSQL",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["data_load"],
) as dag:

    load_task = PythonOperator(
        task_id="initial_data_load",
        python_callable=initial_data_load,
    )
