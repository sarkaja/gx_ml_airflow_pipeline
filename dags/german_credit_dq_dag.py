# dags/german_credit_dq_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import os
import logging

from scripts.get_environment_config import get_all_paths_from_config

logger = logging.getLogger("airflow.task")

def run_data_quality_validation(**context):
    """DAG for DQ validation of German Credit dataset"""
    try:
        logger.info("=== STARTING DQ VALIDATION ===")
        
        from dq_framework.data_quality_runner import run_data_quality_from_yaml_and_csv
        yaml_path, csv_path = get_all_paths_from_config()
        
        execution_date = context.get('execution_date', datetime.now())
        shared_run_id = f"pipeline_{execution_date.strftime('%Y%m%d_%H%M%S')}"
        
        
        logger.info(f"YAML path: {yaml_path}")
        logger.info(f"CSV path: {csv_path}")
        
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        logger.info("All files found, starting validation...")
        
        filename = os.path.basename(csv_path)

        # DQ validation run
        results = run_data_quality_from_yaml_and_csv(
            yaml_path,
            csv_path,
            sep=";",
            run_id=shared_run_id,
            save_to_db=True,
            dataset_id=filename, # filename as dataset id
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
        context['task_instance'].xcom_push(key='dq_csv_file_path', value=csv_path)
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
            logger.info("Data quality meets threshold (>=90%) - ML can proceed")
            return "successful_dq"
        else:
            logger.warning(f"Data quality below threshold ({success_rate:.1f}%) - ML will not run")
            return "insufficient_dq"
            
    except Exception as e:
        logger.error(f"Error evaluating DQ results: {e}")
        return "end_dq"


def trigger_ml(**context):
    """
    Tigger ml taks - signal successful dq.
    
    Args:
        context: Airflow context dictionary
    """
        
    csv_path_for_ml = context['task_instance'].xcom_pull(
        task_ids='run_data_quality_validation', 
        key='dq_csv_file_path'
    )

    context['task_instance'].xcom_push(key='dq_csv_path_for_ml', value=csv_path_for_ml)

    logger.info(f"Proceeding with ML for {csv_path_for_ml} - DQ successful.")


def trigger_remediation(**context):
    """
    Trigger remediation task - signal unsuccessful dq.
    
    Args:
        context: Airflow context dictionary
    """
        
    csv_path_for_remediation = context['task_instance'].xcom_pull(
        task_ids='run_data_quality_validation', 
        key='dq_csv_file_path'
    )

    context['task_instance'].xcom_push(key='dq_csv_path_for_remediation', value=csv_path_for_remediation)

    logger.info(f"Proceeding with remediation for {csv_path_for_remediation}. DQ unsuccessful - ML cannot be triggered")



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
        python_callable=run_data_quality_validation,
    )

    evaluation_task = BranchPythonOperator(
        task_id='evaluate_dq_results',
        python_callable=evaluate_dq_results,
    )

    trigger_ml_task = TriggerDagRunOperator(
        task_id='trigger_ml_pipeline',
        trigger_dag_id='german_credit_ml_pipeline',
        conf={
            "csv_path": "{{ ti.xcom_pull(task_ids='run_data_quality_validation', key='dq_csv_file_path') }}",
            "dq_success": True,
            "source_dag": "german_credit_data_quality",
            "execution_date": "{{ ds }}"
        },
        wait_for_completion=False,
    )

    trigger_remediation_task = TriggerDagRunOperator(
        task_id='trigger_remediation_pipeline',
        trigger_dag_id='german_credit_dq_remediation',  
        conf={
            "csv_path": "{{ ti.xcom_pull(task_ids='run_data_quality_validation', key='dq_csv_file_path') }}",
            "dq_success": False,
            "source_dag": "german_credit_data_quality", 
            "execution_date": "{{ ds }}"
        },
        wait_for_completion=False,
    )

    success_branch = EmptyOperator(task_id='successful_dq')  
    fail_branch = EmptyOperator(task_id='insufficient_dq') 
    end = EmptyOperator(task_id='end_dq', trigger_rule='none_failed')  

    # Workflow of tasks (2 potential branches depending on the evaluation task result)
    start >> data_quality_task >> evaluation_task
    evaluation_task >> [success_branch, fail_branch, end]

    success_branch >> trigger_ml_task >> end
    fail_branch >> trigger_remediation_task >> end
 
