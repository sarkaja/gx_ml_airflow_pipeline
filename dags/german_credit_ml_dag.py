# dags/german_credit_ml_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import os
import sys
import logging
from dags.datasets import ML_READY_DATASET

logger = logging.getLogger("airflow.task")


def check_dq_results(**context):
    """
    Check DQ results before running ML pipeline.
    """
    try:
        # Data taken from context - it was triggered so context is present 
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            logger.info(f"Trigger config: {dag_run.conf}")
        
        logger.info("DQ results verified - proceeding with ML")
        return True
        
    except Exception as e:
        logger.error(f"ERROR checking DQ results: {e}")
        raise


def run_ml_pipeline(**context):
    """
    Run ML pipeline for German Credit dataset.
    """
    try:
        logger.info("Starting ML Pipeline")
        execution_date = context.get('execution_date', datetime.now())
        shared_run_id = f"pipeline_{execution_date.strftime('%Y%m%d_%H%M%S')}"

        from ml_pipeline.ml_runner import MLRunner

        
        dag_run = context.get('dag_run')
        csv_path = dag_run.conf.get('csv_path')
        filename = os.path.basename(csv_path)

        logger.info(f"CSV path: {csv_path}")
        
        
        # Initialize and run ML pipeline
        ml_runner = MLRunner(test_size=0.2, random_state=42, cv_folds=5)
        results = ml_runner.run_full_pipeline_with_saving(
            csv_path, 
            filename,  # filename used as a dataset id 
            save_to_db=True,
            shared_run_id=shared_run_id,
        )
        
        # Post run logging the results
        shared_run_id = results["shared_run_id"]
        best_model = results["best_model"]
        best_accuracy = results["best_accuracy"]
        
        logger.info(f"Pipeline completed: {shared_run_id}")
        logger.info(f"Best Model: {best_model} with accuracy: {best_accuracy:.3f}")
        
        for result in results["test_results"]:
            logger.info(f"Resuts {result['model']}: "
                       f"Accuracy: {result['accuracy']:.3f}, "
                       f"ROC-AUC: {result['roc_auc']:.3f}")
        
        return results
        
    except Exception as e:
        logger.error(f"ML Pipeline failed: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'german_credit_ml_pipeline',
    default_args=default_args,
    description='Machine Learning pipeline for German Credit dataset',
    schedule=[ML_READY_DATASET], 
    catchup=False,
    tags=['machine_learning', 'credit_risk'],
) as dag:

    start = EmptyOperator(task_id='start_ml')

    # Check DQ results before proceeding
    check_dq_task = PythonOperator(
        task_id='check_dq_results',
        python_callable=check_dq_results,
    )

    # Main ML task
    ml_task = PythonOperator(
        task_id='run_ml_pipeline',
        python_callable=run_ml_pipeline,   
    )

    end = EmptyOperator(task_id='end_ml')

    # Define workflow
    start >> check_dq_task >> ml_task >> end