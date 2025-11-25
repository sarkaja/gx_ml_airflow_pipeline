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

def dq_test():
    logger.info("=== STARTING DQ VALIDATION ===")
    csv_path = os.path.join(project_path, 'include', 'data', 'raw', 'german_credit.csv')
    dataframe = pandas.read_csv(csv_path, sep =";", encoding= "utf-8")
    logger.info(f"df: {dataframe}")


    context = gx.get_context()

    # Retrieve the dataframe Batch Definition
    data_source_name = "my_data_source"
    data_source = context.data_sources.add_pandas(name=data_source_name)
    data_asset = data_source.add_dataframe_asset(name="my_dataframe_data_asset")

    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="my_batch_definition")
    batch = batch_definition.get_batch({"dataframe": dataframe})
    
    expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="Purpose", value_set = ["A40","A41"]
    ) 
    #expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    #    column="Category", max_value=6, min_value=1
    #)
    logger.info(f"expectation: {expectation}")


    validation_results = batch.validate(expectation)
    out = validation_results.to_json_dict()
    print(validation_results)
    logger.info(f"validation_results: {out}")
    return out



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
    'dq_testik',
    default_args=default_args,
    description='Data Quality validation for test Credit dataset',
    schedule='@daily',
    catchup=False,
    tags=['data_quality'],
) as dag:

    run_dq_test_validation = PythonOperator(
        task_id='dq_testujeme3',
        python_callable=dq_test
    )

    run_dq_test_validation