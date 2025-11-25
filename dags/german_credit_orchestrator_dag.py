# dags/german_credit_orchestrator.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator  
import logging

logger = logging.getLogger("airflow.task")

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
    'german_credit_orchestrator',
    default_args=default_args,
    description='Orchestrator: Trigger DQ DAG and let ML DAG auto-trigger based on results',
    schedule='@daily',
    catchup=False,
    tags=['orchestrator', 'data_quality', 'machine_learning'],
) as dag:

    start = EmptyOperator(task_id='start_orchestration')  

    # Trigger Data Quality DAG
    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='german_credit_data_quality',
        wait_for_completion=False,  # ML DAG will handle the waiting
        conf={"dq_success": True},

    )

    # ML DAG will automatically trigger based on ExternalTaskSensor
    # when DQ DAG completes and conditions are met

    end = EmptyOperator(task_id='end_orchestration') 

    # Define workflow
    start >> trigger_data_quality >> end