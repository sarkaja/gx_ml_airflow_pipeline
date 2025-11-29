# dags/german_credit_dq_remediation_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import os
import sys
import logging
from pathlib import Path

from scripts.get_environment_config import get_project_path, get_yaml_path_from_config

#PROJECT_PATH = setup_environment()
#INCLUDE_PATH = os.path.join(PROJECT_PATH, 'include')
#if INCLUDE_PATH not in sys.path:
#    sys.path.append(INCLUDE_PATH)
#
#from remediation_strategies.dq_improvement_pipeline import DQImprovementPipeline

logger = logging.getLogger("airflow.task")


def run_dq_remediation_pipeline(**context):
    """
    Run data quality remediation pipeline for corrupted German Credit dataset.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with remediation results
    """
    try:
        logger.info("=== STARTING DQ REMEDIATION PIPELINE ===")
        
        project_path = get_project_path()
        sys.path.append(os.path.join(project_path, 'include', 'remediation_strategies'))
        from remediation_strategies.dq_improvement_pipeline import DQImprovementPipeline

        dag_run = context.get('dag_run')

        execution_date = context.get('execution_date', datetime.now())
        shared_run_id = f"remediation_{execution_date.strftime('%Y%m%d_%H%M%S')}"

        # Path configuration
        yaml_path = get_yaml_path_from_config()
        csv_path_for_remed = dag_run.conf.get('csv_path')

        # Creation of other files for the further remediation attempts
        original_path = Path(csv_path_for_remed)
        new_filename = f"{original_path.stem}_remed{original_path.suffix}"
        remed_output_csv_path = str(original_path.parent / new_filename)
        remed_output_csv_path = remed_output_csv_path.replace('corrupted', 'remediated')

        logger.info(f"YAML config path: {yaml_path}")
        logger.info(f"CSV path needed remediation: {csv_path_for_remed}")
        logger.info(f"Output remediated CSV path: {remed_output_csv_path}")

        # Validate file existence
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")
        
        if not os.path.exists(csv_path_for_remed):
            raise FileNotFoundError(f"Corrupted CSV file not found: {csv_path_for_remed}")
        
        logger.info("All files found, starting remediation pipeline...")
        

        attempt = 1
        if dag_run and dag_run.conf:
            attempt = dag_run.conf.get('remediation_attempt', 1)
            logger.info(f"Remediation attempt: {attempt}")

        # Initialize and run remediation pipeline
        remediation_pipeline = DQImprovementPipeline(yaml_path)
        results = remediation_pipeline.run_improvement_pipeline(
            input_csv_path=csv_path_for_remed,
            output_csv_path=remed_output_csv_path
        )
        
        # Extract improvement statistics
        improvement_stats = results['improvement_stats']
        initial_success_rate = improvement_stats['initial_success_rate']
        final_success_rate = improvement_stats['final_success_rate']
        improvement = improvement_stats['improvement']
        
        remediation_summary = {
            'run_id': shared_run_id,
            'initial_success_rate': initial_success_rate,
            'final_success_rate': final_success_rate,
            'improvement': improvement,
            'failures_fixed': improvement_stats['failures_fixed'],
            'remediated_file_path': remed_output_csv_path,
            'remediation_success': final_success_rate >= 95.0,
            'remediation_attempt': attempt,
            'execution_date': execution_date.isoformat()
        }

        logger.info(f"DQ Remediation completed:")
        logger.info(f"Initial DQ: {initial_success_rate:.1f}% → Final DQ: {final_success_rate:.1f}%")
        logger.info(f"Improvement: +{improvement:.1f}%")
        logger.info(f"Remediation successful: {remediation_summary['remediation_success']}")
        logger.info(f"Remediated file: {remed_output_csv_path}")
        
        context['task_instance'].xcom_push(key='remediation_summary', value=remediation_summary)
        context['task_instance'].xcom_push(key='remed_output_csv_path', value=remed_output_csv_path)
        return remediation_summary
        
    except Exception as e:
        logger.error(f"Data Quality Remediation failed: {e}")
        context['task_instance'].xcom_push(key='remediation_summary', value={
            'remediation_success': False,
            'error': str(e)
        })
        raise

def evaluate_remediation_results(**context):
    """
    Evaluate remediation results and decide if ML should run.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        String indicating next step: "proceed_remediaton", "skip_remediaton", or "fail_remediaton"
    """
    try:

        # Get remediation results from XCom
        remediation_summary = context['task_instance'].xcom_pull(
            task_ids='run_dq_remediation', 
            key='remediation_summary'
        )
        
        if not remediation_summary:
            logger.error("No remediation results found in XCom")
            return "fail_remediation"
        
        final_success_rate = remediation_summary.get('final_success_rate', 0)
        remediation_success = remediation_summary.get('remediation_success', False)
        attempt = remediation_summary.get('remediation_attempt', 1)
        attempt = int(attempt)
        logger.info(f"Remediation Evaluation: {final_success_rate:.1f}% success, Overall: {remediation_success}")
        
        
        if remediation_success:
            logger.info("Data quality successfully remediated (>=90%) - ML can proceed")
            return "successful_remediation"
        else:
            if attempt >= 3:  # Maximum 3 attempts of remediation
                logger.error(f"Max remediation attempts ({attempt}) reached - stopping remediation")
                return "failed_remediation"
            else:
                logger.warning(f"Data quality remediation failed ({final_success_rate:.1f}%) - retrying")
                return "insufficient_remediation"
            
    except Exception as e:
        logger.error(f"Error evaluating remediation results: {e}")
        return "failed_remediation"



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
    'german_credit_dq_remediation',
    default_args=default_args,
    description='Data Quality remediation for corrupted German Credit dataset',
    schedule='@daily',  # Can also be triggered manually when DQ fails
    catchup=False,
    tags=['data_quality', 'remediation'],
) as dag:

    start = EmptyOperator(task_id='start_remediation')

    remediation_task = PythonOperator(
        task_id='run_dq_remediation',
        python_callable=run_dq_remediation_pipeline,
    )

    evaluation_task = BranchPythonOperator(
        task_id='evaluate_remediation_results',
        python_callable=evaluate_remediation_results,
    )

    trigger_ml_task = TriggerDagRunOperator(
        task_id='trigger_ml_pipeline',
        trigger_dag_id='german_credit_ml_pipeline',
        conf={
            "csv_path": "{{ ti.xcom_pull(task_ids='run_dq_remediation', key='remed_output_csv_path') }}", 
            "dq_success": True,
            "source_dag": "german_credit_dq_remediation",
            "execution_date": "{{ ds }}"
        },
        wait_for_completion=False,
    )

    repeat_remediation_task = TriggerDagRunOperator(
        task_id='repeat_remediation_pipeline',
        trigger_dag_id='german_credit_dq_remediation',
        conf={
            "csv_path": "{{ ti.xcom_pull(task_ids='run_dq_remediation', key='remed_output_csv_path') }}", 
            "dq_success": False,
            "source_dag": "german_credit_dq_remediation", 
            "execution_date": "{{ ds }}",
            "remediation_attempt": "{{ ti.xcom_pull(task_ids='run_dq_remediation', key='remediation_summary').remediation_attempt | int + 1 }}"
        },
        wait_for_completion=False,
    )



    success_branch = EmptyOperator(task_id='successful_remediation')
    fail_branch = EmptyOperator(task_id='insufficient_remediation')
    max_attempts_branch = EmptyOperator(task_id='failed_remediation')  # Pro max attempts
    
    end = EmptyOperator(
        task_id='end_remediation', 
        trigger_rule='none_failed'
    )

    # Workflow definition
    start >> remediation_task >> evaluation_task
    evaluation_task >> [success_branch, fail_branch, max_attempts_branch]
    
    # Úspěšná remediac → ML
    success_branch >> trigger_ml_task >> end
    
    # Neúspěšná remediation (méně než 3 pokusy) → opakuj
    fail_branch >> repeat_remediation_task >> end
    
    # Max attempts reached → konec
    max_attempts_branch >> end