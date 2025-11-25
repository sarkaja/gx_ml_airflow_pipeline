#!/usr/bin/env bash

echo "Docker Reset Script – Cleaning everything to start fresh..."

# 1️⃣ Stop and remove all running containers
echo "Stopping all containers..."
docker compose down -v --remove-orphans

# 2️⃣ Remove dangling images and build cache
echo "Removing unused images and build cache..."
docker image prune -af
docker builder prune -af

# 3️⃣ Remove all volumes (including PostgreSQL data)
echo "Removing all volumes..."
docker volume prune -f

# 4️⃣ Check disk usage (optional but useful)
echo "Current Docker disk usage:"
docker system df

# 5️⃣ Rebuild from scratch with no cache
echo "Rebuilding Docker images..."
docker compose build --no-cache

# 6️⃣ Start containers again
echo "Starting containers..."
docker compose up -d

echo "Reset complete! Your Docker environment is now clean and rebuilt."
