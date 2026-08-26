SHELL := /bin/bash

# Override if needed, for example:
# make COMPOSE="docker compose" up
COMPOSE ?= podman compose

# Modular compose file paths (layered architecture)
CORE = -f data/docker-compose.core.yml       # PostgreSQL + MinIO (foundation)
META = -f data/docker-compose.metadata.yml   # Hive Metastore (requires core)
PROC = -f data/docker-compose.processing.yml # Glue + Spark jobs (requires metadata)
QUERY = -f data/docker-compose.query-ui.yml  # Trino + Superset (requires metadata)
MON = -f data/docker-compose.monitoring.yml  # DQ Monitor (requires core)

.PHONY: help jars up up-core up-meta up-full up-query up-processing up-monitoring down restart ps logs config pull clean ingest transform aggregate seed-incremental source-counts jobs

help:
	@echo "=== Data Lakehouse Stack - Modular Compose Setup ==="
	@echo ""
	@echo "Quick Start:"
	@echo "  make up-full      - Start everything (recommended)"
	@echo "  make down         - Stop all services"
	@echo ""
	@echo "Modular Composition Targets (start only what you need):"
	@echo "  make up-core      - Core only: PostgreSQL + MinIO"
	@echo "  make up-meta      - Core + Metadata: adds Hive Metastore"
	@echo "  make up-query     - Core + Metadata + Query UI: adds Trino + Superset"
	@echo "  make up-processing- Core + Metadata + Processing: adds Glue jobs"
	@echo "  make up-monitoring- Core + Monitoring: adds DQ Monitor"
	@echo "  make up-full      - Everything (all layers)"
	@echo "  make up           - Alias for up-full (legacy)"
	@echo ""
	@echo "Management:"
	@echo "  make down         - Stop and remove all services"
	@echo "  make restart      - Restart the stack"
	@echo "  make ps           - Show service status"
	@echo "  make logs         - Tail all service logs"
	@echo "  make config       - Validate and render compose config"
	@echo "  make pull         - Pull latest images"
	@echo "  make jars         - Download required Hive/Glue JARs"
	@echo "  make clean        - Wipe data and restart fresh"
	@echo ""
	@echo "ETL Job Execution:"
	@echo "  make ingest       - Run ingest job"
	@echo "  make transform    - Run transform job (requires ingest success)"
	@echo "  make aggregate    - Run aggregate job (requires transform success)"
	@echo "  make jobs         - Run full chain: ingest → transform → aggregate"
	@echo ""
	@echo "Database Utilities:"
	@echo "  make seed-incremental - Add test data to source database"
	@echo "  make source-counts    - Show row counts in source tables"
	@echo ""
	@echo "Advanced:"
	@echo "  make COMPOSE='docker compose' up-full  - Use Docker Compose instead of podman"
	@echo ""
	@echo "Network Note: Auto-created by docker-compose.core.yml (no manual setup needed)"
	@echo "See COMPOSE_STRUCTURE.md for more details"

jars:
	bash download-jars.sh

# Ensure network exists (defensive: core.yml creates it, but this is a safety net)
network:
	@podman network inspect data-lakehouse-stack_network > /dev/null 2>&1 || \
		(podman network create data-lakehouse-stack_network && echo "✓ Network created")

# ============================================================================
# MODULAR COMPOSITION TARGETS (select what you need to run)
# ============================================================================

up-core: jars network
	$(COMPOSE) $(CORE) up -d

up-meta: jars network
	$(COMPOSE) $(CORE) $(META) up -d

up-query: jars network
	$(COMPOSE) $(CORE) $(META) $(QUERY) up -d

up-processing: jars network
	$(COMPOSE) $(CORE) $(META) $(PROC) up -d

up-monitoring: jars network
	$(COMPOSE) $(CORE) $(MON) up -d

up-full: jars network
	$(COMPOSE) $(CORE) $(META) $(PROC) $(QUERY) $(MON) up -d

# Legacy: up = up-full (for backwards compatibility)
up: up-full

# ============================================================================
# SERVICE MANAGEMENT
# ============================================================================

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

config:
	$(COMPOSE) config

pull:
	$(COMPOSE) pull

clean:
	$(COMPOSE) down -v --remove-orphans
	rm -rf data/minio data/postgres/db data/glue/data data/superset/home
	mkdir -p data/minio data/postgres/db data/glue/data data/superset/home

# ============================================================================
# ETL JOB EXECUTION
# ============================================================================

ingest: network
	$(COMPOSE) $(CORE) $(META) $(PROC) up glue-ingest
 
transform: network
	$(COMPOSE) $(CORE) $(META) $(PROC) up glue-transform

aggregate: network
	$(COMPOSE) $(CORE) $(META) $(PROC) up glue-aggregate

jobs: network
	$(MAKE) ingest
	$(MAKE) transform
	$(MAKE) aggregate

# ============================================================================
# DATABASE UTILITIES
# ============================================================================

seed-incremental:
	$(COMPOSE) exec postgres psql -U postgres -d source_db -f /docker-entrypoint-initdb.d/03-seed-incremental-data.sql

source-counts:
	$(COMPOSE) exec postgres psql -U postgres -d source_db -c "select 'customers' as table_name, count(*) from customers union all select 'products', count(*) from products union all select 'orders', count(*) from orders;"
