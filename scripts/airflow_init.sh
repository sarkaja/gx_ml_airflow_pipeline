#!/bin/bash
set -e

echo "Creating necessary directories..."
mkdir -p /opt/airflow/{logs,dags,plugins,config,scripts,include}

echo "Setting ownership..."
chown -R "${AIRFLOW_UID}:0" /opt/airflow/

echo "Initializing Airflow metadata database..."
airflow db migrate

echo "Creating admin user (if not exists)..."
airflow users create \
  --username ${_AIRFLOW_WWW_USER_USERNAME:-airflow} \
  --password ${_AIRFLOW_WWW_USER_PASSWORD:-airflow} \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || echo "✅ User already exists."

echo "✅ Initialization complete. Exiting..."
