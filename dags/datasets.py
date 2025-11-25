# dags/datasets.py
from airflow import Dataset

# Datasets for communication between DAGs
DQ_RESULTS_DATASET = Dataset("dq://german_credit/data_quality_results")
ML_READY_DATASET = Dataset("ml://german_credit/ready_for_training")