#!/usr/bin/env bash

echo "⚡ Fast rebuild of Airflow stack of requirements, dags, scripts, plugins (keeping volumes, logs and DB data)..."

# 1️⃣ Stop all running containers but keep volumes
echo "🛑 Stopping running containers..."
docker compose down --remove-orphans

# 2️⃣ Rebuild Docker images using cache (much faster)
echo "🔨 Rebuilding Docker images using cache..."
docker compose build

# 3️⃣ Restart the stack
echo "🚀 Starting containers..."
docker compose up -d

# 4️⃣ Check container health status
echo "🔍 Checking container status..."
docker compose ps

echo "✅ Fast rebuild complete. Database and volumes preserved."
