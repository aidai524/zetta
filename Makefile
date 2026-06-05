.PHONY: test compile endpoints seed-tasks up down db-ping load-gamma

test:
	PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest -q

compile:
	python -m compileall src tests

endpoints:
	PYTHONPATH=src python -m zetta.cli endpoints

seed-tasks:
	PYTHONPATH=src python -m zetta.cli tasks seed-basic --page-limit 100 --max-pages 1

up:
	docker compose up -d

down:
	docker compose down

db-ping:
	PYTHONPATH=src python -m zetta.cli db ping

load-gamma:
	PYTHONPATH=src python -m zetta.cli load gamma-raw
