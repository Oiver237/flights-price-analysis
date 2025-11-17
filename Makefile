postgres:
	docker compose up -d postgres

init:
	docker compose up -d airflow-init

up:
	docker compose up -d 

down:
	docker compose down -v

ps:
	docker compose ps

logs-webserver:
	docker compose logs airflow-webserver

logs-scheduler:
	docker compose logs airflow-scheduler
