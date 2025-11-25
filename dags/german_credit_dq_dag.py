# dags/german_credit_dq_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.datasets import Dataset
from dags.datasets import DQ_RESULTS_DATASET, ML_READY_DATASET
import os
import sys
import logging

logger = logging.getLogger("airflow.task")

def run_german_credit_validation(**context):
    """DAG for DQ validation of German Credit dataset"""
    try:
        logger.info("=== STARTING DQ VALIDATION ===")
        
        from scripts.setup_environment import setup_environment
        project_path = setup_environment()
        
        from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_csv

        execution_date = context.get('execution_date', datetime.now())
        shared_run_id = f"pipeline_{execution_date.strftime('%Y%m%d_%H%M%S')}"

        yaml_path = os.path.join(project_path, 'include', 'dq_configs', 'german_credit_validation.yaml')
        csv_path = os.path.join(project_path, 'include', 'data', 'raw', 'german_credit.csv')
        
        logger.info(f"YAML path: {yaml_path}")
        logger.info(f"CSV path: {csv_path}")
        
        if not os.path.exists(yaml_path):
            available_files = os.listdir(os.path.join(project_path, 'include', 'dq_configs'))
            logger.error(f"YAML file not found. Available: {available_files}")
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")
        
        if not os.path.exists(csv_path):
            available_files = os.listdir(os.path.join(project_path, 'include', 'data', 'raw'))
            logger.error(f"CSV file not found. Available: {available_files}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info("All files found, starting validation...")
        
        # DQ validation run
        results = run_data_quality_from_yaml_and_csv(
            yaml_path,
            csv_path,
            sep=";",
            run_id=shared_run_id,
            save_to_db=True,
            dataset_id="german_credit",
        )
        
        # Post run logging
        stats = results.get('statistics', {})
        success_rate = stats.get('success_percent', 0)
        overall_success = success_rate >= 90.0  

        dq_summary = {
            'run_id': shared_run_id,
            'success_rate': success_rate,
            'overall_success': overall_success,
            'total_expectations': stats.get('evaluated_expectations', 0),
            'successful_expectations': stats.get('successful_expectations', 0),
            'execution_date': execution_date.isoformat()
        }

        logger.info(f"DQ Validation completed: {success_rate:.1f}% success")
        logger.info(f"Overall success (>=90%): {overall_success}")
        logger.info(f"Shared Run ID: {shared_run_id}")
        
        context['task_instance'].xcom_push(key='dq_summary', value=dq_summary)

        return dq_summary
        
    except Exception as e:
        logger.error(f"Data Quality Validation failed: {e}")
        # Push failure info to XCom
        context['task_instance'].xcom_push(key='dq_summary', value={
            'overall_success': False,
            'error': str(e)
        })
        raise


def evaluate_dq_results(**context):
    """
    Evaluate DQ results and decide if ML should run.
    """
    try:
        # Get DQ results from XCom
        dq_summary = context['task_instance'].xcom_pull(
            task_ids='run_data_quality_validation', 
            key='dq_summary'
        )
        
        if not dq_summary:
            logger.error("No DQ results found in XCom")
            return "fail"
        
        success_rate = dq_summary.get('success_rate', 0)
        overall_success = dq_summary.get('overall_success', False)
        
        logger.info(f"DQ Evaluation: {success_rate:.1f}% success, Overall: {overall_success}")
        
        if overall_success:
            context['task_instance'].xcom_push(key='ml_should_run', value=True)
            logger.info("Data quality meets threshold (>=90%) - ML can proceed")
            return "proceed"
        else:
            logger.warning(f"Data quality below threshold ({success_rate:.1f}%) - ML will not run")
            context['task_instance'].xcom_push(key='ml_should_run', value=False)
        
            return "skip"
            
    except Exception as e:
        logger.error(f"Error evaluating DQ results: {e}")
        return "fail"

def update_dataset_success(**context):
    """
    Update dataset to signal successful DQ validation.
    """
    try:
        logger.info("Updating dataset task start, DQ summary validation")
        
        dq_summary = context['task_instance'].xcom_pull(
            task_ids='run_data_quality_validation', 
            key='dq_summary'
        )
        
        ml_should_run = context['task_instance'].xcom_pull(
            task_ids='evaluate_dq_results', 
            key='ml_should_run'
        )
        
        if ml_should_run and dq_summary:
            logger.info("Updating dataset (DQ sufficient) - ML can be triggered")
        else:
            logger.info("Not updating dataset (DQ insufficient) - ML cannot be triggered")
            
    except Exception as e:
        logger.error(f"Error while dataset update (DQ validation run): {e}")
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
    'german_credit_data_quality',
    default_args=default_args,
    description='Data Quality validation for German Credit dataset',
    schedule='@daily',
    catchup=False,
    tags=['data_quality'],
) as dag:

    start = EmptyOperator(task_id='start_dq') 

    data_quality_task = PythonOperator(
        task_id='run_data_quality_validation',
        
        python_callable=run_german_credit_validation,
        outlets=[DQ_RESULTS_DATASET],
    )

    evaluation_task = PythonOperator(
        task_id='evaluate_dq_results',
        
        python_callable=evaluate_dq_results,
    )

    update_dataset_task = PythonOperator(
        task_id='update_dataset_on_success',
        python_callable=update_dataset_success,
        
        outlets=[ML_READY_DATASET], 
    )


    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_if_successful',
        trigger_dag_id='german_credit_ml_pipeline',
        conf={"dq_success": True},
        wait_for_completion=False,
        
    ) 
    
    success_branch = EmptyOperator(task_id='proceed')  
    fail_branch = EmptyOperator(task_id='skip') 
    end = EmptyOperator(task_id='end_dq', trigger_rule='none_failed')  

    # Workflow of tasks (2 potential branches depending on the evaluation task result)
    start >> data_quality_task >> evaluation_task
    evaluation_task >> success_branch >> update_dataset_task >> trigger_ml >> end
    evaluation_task >> fail_branch >> end