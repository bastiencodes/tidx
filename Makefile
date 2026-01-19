.PHONY: help up down logs seed reset psql build test bench bench-open clean

.DEFAULT_GOAL := help

# Docker compose
COMPOSE := docker compose -f docker-compose.test.yml

# Default seed parameters
DURATION ?= 30
TPS ?= 100

# ============================================================================
# Environment
# ============================================================================

# Start all services (TimescaleDB + Tempo + Indexer)
up: build
	@$(COMPOSE) up -d
	@echo "Waiting for TimescaleDB..."
	@until $(COMPOSE) exec -T timescaledb pg_isready -U ak47 -d ak47_test > /dev/null 2>&1; do sleep 1; done
	@echo "✓ Ready. Run: ./ak47 --help"

# Stop all services
down:
	@$(COMPOSE) down

# Tail indexer logs
logs:
	@$(COMPOSE) logs -f ak47

# ============================================================================
# Data
# ============================================================================

# Seed chain with transactions (uses dev mnemonic for pre-funded accounts)
seed:
	@echo "Seeding chain with $(TPS) TPS for $(DURATION) seconds..."
	@docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
		run-max-tps --duration $(DURATION) --tps $(TPS) --accounts 10 \
		--target-urls http://localhost:8545 --disable-2d-nonces \
		--mnemonic "test test test test test test test test test test test junk"

# Heavy seed: ~1M+ txs with max variance (TIP-20, ERC-20, swaps, multicalls)
# Takes ~10 mins at 2000 TPS for 600 seconds
HEAVY_DURATION ?= 600
HEAVY_TPS ?= 2000
HEAVY_ACCOUNTS ?= 1000

seed-heavy:
	@echo "Heavy seeding: $(HEAVY_TPS) TPS for $(HEAVY_DURATION)s (~$$(($(HEAVY_TPS) * $(HEAVY_DURATION))) txs)"
	@docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
		run-max-tps \
		--duration $(HEAVY_DURATION) \
		--tps $(HEAVY_TPS) \
		--accounts $(HEAVY_ACCOUNTS) \
		--target-urls http://localhost:8545 \
		--disable-2d-nonces \
		--mnemonic "test test test test test test test test test test test junk" \
		--tip20-weight 3 \
		--erc20-weight 2 \
		--swap-weight 2

# Seed and sync: generate txs then index them
seed-and-sync: seed-heavy
	@echo "Syncing indexed data..."
	@./ak47 up --rpc http://localhost:8545 --db postgres://ak47:ak47@localhost:5433/ak47_test &
	@PID=$$!; sleep 30; kill $$PID 2>/dev/null || true
	@echo "✓ Seeded and synced"

# Reset database
reset:
	@echo "Dropping and recreating database..."
	@$(COMPOSE) exec -T timescaledb psql -U ak47 -c "DROP DATABASE IF EXISTS ak47_test" > /dev/null
	@$(COMPOSE) exec -T timescaledb psql -U ak47 -c "CREATE DATABASE ak47_test" > /dev/null
	@echo "✓ Database reset"

# Open psql shell
psql:
	@$(COMPOSE) exec timescaledb psql -U ak47 -d ak47_test

# ============================================================================
# Build & Test
# ============================================================================

# Build Docker image
build:
	@$(COMPOSE) build ak47

# Run tests (sequential execution due to shared DB)
test:
	@$(COMPOSE) up -d timescaledb tempo
	@sleep 2
	@cargo test -- --test-threads=1 --nocapture

# Run benchmarks
bench:
	@$(COMPOSE) up -d timescaledb tempo
	@sleep 2
	@DATABASE_URL=postgres://ak47:ak47@localhost:5433/ak47_test cargo bench
	@echo "Report: target/criterion/report/index.html"

# Run benchmarks and open report
bench-open: bench
	@open target/criterion/report/index.html 2>/dev/null || xdg-open target/criterion/report/index.html 2>/dev/null || echo "Open target/criterion/report/index.html"

# Clean everything
clean:
	@$(COMPOSE) down -v
	@cargo clean

# ============================================================================
# Help
# ============================================================================

help:
	@echo "ak47 Development"
	@echo ""
	@echo "  make up           Start all services"
	@echo "  make down         Stop all services"
	@echo "  make logs         Tail indexer logs"
	@echo "  make seed         Generate transactions (DURATION=30 TPS=100)"
	@echo "  make seed-heavy   Generate ~1M+ txs with max variance"
	@echo "  make seed-and-sync  Seed + index data for tests"
	@echo "  make reset        Reset database"
	@echo "  make psql         Open psql shell"
	@echo "  make build        Build Docker image"
	@echo "  make test         Run tests (auto-seeds)"
	@echo "  make bench        Run benchmarks"
	@echo "  make bench-open   Run benchmarks and open report"
	@echo "  make clean        Stop services and clean"
	@echo ""
	@echo "CLI:"
	@echo "  ./ak47 sync --rpc http://tempo:8545 --db postgres://ak47:ak47@timescaledb:5432/ak47_test forward --from 1 --to 1000"
	@echo "  ./ak47 query \"SELECT COUNT(*) FROM txs\" --db postgres://ak47:ak47@timescaledb:5432/ak47_test"
	@echo "  ./ak47 compress --db postgres://ak47:ak47@timescaledb:5432/ak47_test"
