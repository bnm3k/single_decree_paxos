# =============================================================================
PROJECT_NAME:=paxos

.DEFAULT_GOAL:=run
.SILENT:
SHELL:=/usr/bin/bash


# =============================================================================
# 			DEV
# =============================================================================
#
VENV_DIR:=.venv
VENV_BIN:=.venv/bin/
ACTIVATE:=source .venv/bin/activate &&

.PHONY: setup run lint format test build coverage

setup:
	test -d $(VENV_DIR) || python3 -m venv $(VENV_DIR)
	poetry install

run:
	python paxos.py

lint:
	flake8 --show-source .
	bandit -q -r -c "pyproject.toml" .

format:
	black .

test:
	pytest test_paxos.py

build:
	poetry build -q

clean:
	rm -rf $(VENV_DIR)
	find . -type d -name '__pycache__' -exec rm -rf {} +
# =============================================================================
