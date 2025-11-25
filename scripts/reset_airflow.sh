#!/bin/bash
set -e

echo "WARNING: This will reset your Airflow metadata database!"
echo "   - All DAG runs, task instances, variable values, connections, and logs will be deleted."
echo "   - DAG definitions (Python files) will remain untouched."
echo ""
read -p "Are you sure you want to continue? (y/N): " confirm

if [[ $confirm != "y" && $confirm != "Y" ]]; then
  echo "Reset aborted."
  exit 1
fi

echo "Stopping all running Airflow containers..."
docker compose down

echo "Removing metadata database volume..."
docker volume ls -q | grep postgres-db-volume | xargs -r docker volume rm

echo "Clearing old logs..."
rm -rf logs/* || true

echo "Re-initializing Airflow..."
docker compose up airflow-init

echo "Restarting all services..."
docker compose up -d

echo "Airflow reset complete!"
echo "   - DAGs reloaded from scratch."
echo "   - Metadata DB recreated."
echo "   - Admin user re-created."
