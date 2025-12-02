Documentation for the project

How to build the project by docker 
To build the image for the project, run:
- docker build -t dp-airflow:3.0.6-custom .

To run the following build:
- docker compose build
- docker compose up airflow-init
- docker compose up -d 

DAGs
corruption_dag and german_credit_dq_dag is currently possible to run
- specify the paths in the .\include\dq_configs\german_credit_config.yaml file to determine which csv and yaml should be used for the process of data corruption or data quality validation
- german_credit_dq_dag is followed by remediation dag (if the DQ does not meet the requirement of 90 %)
   or in the case of sufficient DQ score, airflow workflow automatically triggers the ml dag 


Config file (with the project variables)
Config is loaded by ProjectConfig in scripts/get_environment_config.py.
Pipeline notebooks, DAGs, and scripts do not hardcode paths — always read them from config.

yaml_validation_file
- Description: Name of the YAML file containing DQ validation rules.
- Location: include/dq_configs/
- Required: Yes
- Example: german_credit_validation.yaml

csv_folder
- Description: Subdirectory inside include/data/ containing the CSV file.
- Required: Yes
- Common values:
- raw → original dataset
- corrupted → corrupted dataset
- Example: raw

csv_file_for_dq
- Description: Name of the CSV file used for initial load or corruption.
- Required: Yes
- Example: german_credit.csv
- Full path: include/data/<csv_folder>/<csv_file_for_dq>

corruption_function
- Description: Name of the corruption function/strategy.
- Required: Yes (if using corruption pipeline)
- Allowed values: missing_values, categorical_errors, label_errors, relationship_violation, conjoined_corruption

corruption_scenario
- Description: Selected corruption scenario or intensity level.
- Required: Yes
- Example: severe
- dq_threshold
- Description: Minimum DQ success rate (%) required for ML pipeline to run.
- Required: Yes
- Example: 90

run_id
- Description: Identifier linking pipeline stages (initial load, corruption, DQ).
- Required: Yes
- Note: Pipelines may override this with timestamp-based IDs.
- Example: "initial_load"

database.table_name
- Description: Base name for all tables created in PostgreSQL.
- Used to generate:
- raw_<table_name>
- corrupted_<table_name>
- remediated_<table_name>
- <table_name>_metadata
- Required: Yes
- Example: german_credit_data

database.schema
- Description: Name of the PostgreSQL schema.
- Required: Yes
- Example: public



When adding new corruption strategies or DQ rules, update:
- YAML config
