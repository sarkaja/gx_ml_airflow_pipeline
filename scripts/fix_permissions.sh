#!/bin/bash
set -e

# =============================================
#  fix_permissions.sh
# Fixes ownership and permissions for Airflow project directories
# so that Docker containers (running as AIRFLOW_UID) can access them.
# =============================================

# Default UID (should match AIRFLOW_UID in .env)
AIRFLOW_UID=${AIRFLOW_UID:-50000}

# Directories to fix (adjust if needed)
DIRS=("dags" "logs" "plugins" "include" "great_expectations" "scripts")

echo "🔧 Fixing permissions for Airflow project directories..."
echo "   Using AIRFLOW_UID: $AIRFLOW_UID"
echo "   Target directories: ${DIRS[*]}"
echo ""

for dir in "${DIRS[@]}"; do
  if [ -d "$dir" ]; then
    echo "Processing $dir ..."
    sudo chown -R $AIRFLOW_UID:0 "$dir"
    sudo chmod -R 775 "$dir"
  else
    echo "Directory $dir does not exist — skipping."
  fi
done

echo ""
echo "Permissions fixed! You can now safely run:"
echo "   docker compose down -v && docker compose up airflow-init"
