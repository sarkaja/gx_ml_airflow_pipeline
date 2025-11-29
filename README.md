Documentation for the project

To build the image for the project, run:
- docker build -t dp-airflow:3.0.6-custom .

To run the following build:
- docker compose build
- docker compose up airflow-init
- docker compose up -d 


corruption_dag and german_credit_dq_dag is currently possible to run
- specify the paths in the .\include\dq_configs\german_credit_config.yaml file to determine which csv and yaml should be used for the process of data corruption or data quality validation
- german_credit_dq_dag is followed by remediation dag (if the DQ does not meet the requirement of 90 %)
   or in the case of sufficient DQ score, airflow workflow automatically triggers the ml dag 
