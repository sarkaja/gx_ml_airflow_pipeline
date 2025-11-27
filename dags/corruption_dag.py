from __future__ import annotations
import great_expectations as gx
import pandas
import logging
import os
import sys
from scripts.setup_environment import setup_environment
from datetime import datetime, timedelta
import os
import yaml
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
project_path = setup_environment()

PROJECT_PATH = os.path.join(os.path.dirname(__file__), '..')
INCLUDE_PATH = os.path.join(PROJECT_PATH, 'include')
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)


logger = logging.getLogger("airflow.test")

def dq_corruption():
    logger.info("=== STARTING DQ CORRUPTION ===")
    
    from degradation_strategies.corruption_experiment_runner import run_corruption_experiment

    csv_path = os.path.join(project_path, 'include', 'data', 'raw', 'german_credit.csv')

    yaml_path = os.path.join(project_path, 'include', 'dq_configs', 'german_credit_validation.yaml')
   

    results = run_corruption_experiment(
        original_csv_path=csv_path,
        yaml_config_path=yaml_path,
        output_dir="./include/data/corrupted"
    )
    stat_corrs = []
    print("\n=== EXPERIMENT SUMMARY ===")
    for scenario, result in results.items():
        if 'error' not in result:
            stats = result.get('statistics', {})
            success_pct = stats.get('success_percent', 0)
            stat_corrs.append(stats)
            print(f"{scenario.upper()}: {success_pct:.1f}% DQ success rate")

    #out = stat_corrs.to_json_dict()
    print(stat_corrs)
    logger.info(f"validation_results: {stat_corrs}")
    return stat_corrs



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
    'german_credit_dq_corruption',
    default_args=default_args,
    description='Data Corruption for test Credit dataset',
    schedule='@daily',
    catchup=False,
    tags=['data_quality'],
) as dag:

    run_dq_corruption = PythonOperator(
        task_id='dq_corruption',
        python_callable=dq_corruption

    )

    run_dq_corruption