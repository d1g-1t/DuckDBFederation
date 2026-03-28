.PHONY: setup build up down logs test lint clean demo gpg-setup

setup: build up wait demo

gpg-setup:
	bash scripts/setup-gpg.sh

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

wait:
	@echo "⏳ waiting for services..."
	@python scripts/wait_for_services.py

test:
	docker compose exec app python -m pytest tests/ -v

lint:
	docker compose exec app python -m ruff check src/ tests/

clean:
	docker compose down -v --rmi local

demo:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║  ParquetFederation — DuckDB Data Federation Service        ║"
	@echo "╠══════════════════════════════════════════════════════════════╣"
	@echo "║  API:     http://localhost:8421                             ║"
	@echo "║  Docs:    http://localhost:8421/docs                        ║"
	@echo "║  Health:  http://localhost:8421/api/v1/federation/health    ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "→ federation query demo (Parquet ← JOIN → PostgreSQL):"
	@echo ""
	@curl -s http://localhost:8421/api/v1/federation/health | python -m json.tool 2>/dev/null || echo "(curl not available — open the URLs manually)"
	@echo ""
