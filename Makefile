# Docker commands
up:
	docker compose up -d

down:
	docker compose down

rebuild:
	docker compose down -v && docker compose build --no-cache && docker compose up airflow-init && docker compose up -d

init:
	docker compose up airflow-init

reset:
	./scripts/reset_airflow.sh --force

fix-permissions:
	./scripts/fix_permissions.sh

logs:
	docker compose logs -f

# Development utilities
ps:
	docker compose ps

shell:
	docker compose exec airflow-scheduler bash
