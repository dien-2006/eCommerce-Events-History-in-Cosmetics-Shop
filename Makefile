SHELL := /bin/bash
COMPOSE := docker compose --env-file .env.deploy
.PHONY: bootstrap preflight up down test status trigger bi dashboard monitoring logs demo
bootstrap:
	python3 scripts/bootstrap.py
preflight:
	bash scripts/preflight.sh
up: bootstrap preflight
	$(COMPOSE) up -d --build --wait --wait-timeout 600
down:
	$(COMPOSE) --profile bi --profile observability down
test:
	bash scripts/test.sh
status:
	bash scripts/verify_services.sh
trigger:
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger cosmetics_elt
bi:
	$(COMPOSE) --profile bi up -d --build --wait --wait-timeout 600 superset
dashboard:
	$(COMPOSE) exec -T superset python /app/docker-init/provision.py
monitoring:
	$(COMPOSE) --profile observability up -d metrics prometheus grafana
logs:
	$(COMPOSE) logs --tail 100 -f airflow-scheduler spark-thrift
demo:
	python3 scripts/demo.py
