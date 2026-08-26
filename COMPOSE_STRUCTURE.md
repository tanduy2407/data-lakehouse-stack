# Modular Compose Structure

The lakehouse stack is now organized into modular Docker Compose files for flexibility and maintainability.

## Podman-Compose Compatibility

**Your system**: `podman-compose 1.6.0` (does NOT support `include:`)

Use the **Makefile targets** for composition. The `include:` feature requires Docker Compose v2.1+ or podman-compose 2.0+.

**Network is auto-created** by the core compose file, so no manual setup needed!

## Architecture

```
Dependency Graph:
  
  Core (Foundation)
  ├─ PostgreSQL
  └─ MinIO
     
  Metadata Layer (depends on Core)
  ├─ MinIO Bucket Init
  └─ Hive Metastore
     
  Processing (depends on Metadata)
  ├─ Glue (long-running shell)
  ├─ Glue-Ingest
  ├─ Glue-Transform
  └─ Glue-Aggregate
     
  Query & UI (depends on Metadata)
  ├─ Trino
  └─ Superset
     
  Monitoring (depends on Core)
  └─ Data Quality Monitor
```

## Files

| File | Services | Purpose | Dependencies |
|------|----------|---------|--------------|
| `data/docker-compose.core.yml` | PostgreSQL, MinIO | Foundation: object storage + database | None |
| `data/docker-compose.metadata.yml` | Hive Metastore, Bucket Init | Catalog: table metadata & schema | core |
| `data/docker-compose.processing.yml` | Glue, Glue-Ingest, Glue-Transform, Glue-Aggregate | ETL: Spark jobs | metadata |
| `data/docker-compose.query-ui.yml` | Trino, Superset | Query engines & UI | metadata |
| `data/docker-compose.monitoring.yml` | DQ Monitor | Monitoring: job freshness & data quality | core |

## ✅ Recommended: Use Makefile Targets (Podman-Compatible)

The Makefile provides convenient targets that work with **any version** of podman-compose or Docker Compose:

```bash
# Infrastructure only
make up-core

# Core + Metadata (adds Hive Metastore)
make up-meta

# Full stack (all services)
make up-full

# Individual layers (requires core + metadata running)
make up-query       # Trino + Superset
make up-processing  # Glue + jobs
make up-monitoring  # DQ Monitor

# Legacy alias (same as up-full)
make up
```

### Why Use Makefile?
- ✅ Works with podman-compose 1.6.0+
- ✅ Works with Docker Compose 1.29+
- ✅ Works with Docker Compose 2.1+
- ✅ Simple, readable commands
- ✅ Consistent across platforms

## Manual Composition (Command-Line)

If you prefer not to use Make:

```bash
# Full stack
podman compose \
  -f data/docker-compose.core.yml \
  -f data/docker-compose.metadata.yml \
  -f data/docker-compose.processing.yml \
  -f data/docker-compose.query-ui.yml \
  -f data/docker-compose.monitoring.yml \
  up -d

# Core only
podman compose -f data/docker-compose.core.yml up -d

# Core + Metadata
podman compose \
  -f data/docker-compose.core.yml \
  -f data/docker-compose.metadata.yml \
  up -d

# Query layer (requires core + metadata running)
podman compose \
  -f data/docker-compose.core.yml \
  -f data/docker-compose.metadata.yml \
  -f data/docker-compose.query-ui.yml \
  up -d
```

## Docker Compose v2.1+ (Include Feature)

If you upgrade to **Docker Compose v2.1+** or **podman-compose 2.0+**, you can uncomment the `include:` section in `docker-compose.yml` for simpler usage:

```yaml
include:
  - path: ./data/docker-compose.core.yml
  - path: ./data/docker-compose.metadata.yml
  - path: ./data/docker-compose.processing.yml
  - path: ./data/docker-compose.query-ui.yml
  - path: ./data/docker-compose.monitoring.yml
```

Then use:
```bash
docker compose up -d
```

## Useful Make Targets

| Command | Purpose |
|---------|---------|
| `make up-core` | Start PostgreSQL + MinIO |
| `make up-meta` | Add Hive Metastore |
| `make up-full` | Start everything |
| `make up-query` | Add Trino + Superset |
| `make up-processing` | Add Glue jobs |
| `make up-monitoring` | Add DQ Monitor |
| `make ps` | Show service status |
| `make logs` | View all logs |
| `make down` | Stop all services |
| `make restart` | Restart services |
| `make ingest` | Run ingest job |
| `make transform` | Run transform job |
| `make aggregate` | Run aggregate job |

## Common Workflows

**Local Development (minimal footprint):**
```bash
# Start only core services
make up-core

# Later, when you need metadata:
make up-meta

# Or add query layer:
make up-query
```

**Data Engineering (full stack):**
```bash
# Start everything
make up-full

# Run ETL jobs
make ingest
make transform
make aggregate

# Query results in Trino (http://localhost:8080) and Superset (http://localhost:8088)
```

**Testing Query Layer:**
```bash
# Assuming core + metadata are already running
make up-query

# Now test Trino and Superset
```

**Testing Processing Layer:**
```bash
# Assuming core + metadata are already running
make up-processing

# Run a single job
make ingest
```

## Network & Volume Management

All services share:
- **Network**: `data-lakehouse-stack_network` (bridge)
  - Created automatically by `docker-compose.core.yml`
  - All other compose files reuse the same network
  - No manual network creation needed!
- **Volumes**:
  - `postgres_data` (PostgreSQL persistence)
  - `superset_home_data` (Superset metadata)
- **Bind mounts**: `data/minio`, `data/postgres`, `data/glue`, `data/hive`, `data/trino`, `data/monitor`

## Stopping Individual Layers

```bash
# Stop query layer (leaves core + metadata running)
podman compose -f data/docker-compose.query-ui.yml down

# Stop processing layer
podman compose -f data/docker-compose.processing.yml down

# Full cleanup with volume deletion
podman compose down -v
```

## Supported Tools

| Tool | Version | Status | Method |
|------|---------|--------|--------|
| **Podman Compose** | 1.6.0+ | ✅ Supported | Make or manual `-f` flags |
| **Docker Compose** | 1.29+ | ✅ Supported | Make or manual `-f` flags |
| **Docker Compose** | 2.1+ | ✅ Supported | Make, manual `-f` flags, OR `include:` |

## Upgrading Podman-Compose

To enable the `include:` feature and simplify to single-command startup:

```bash
# Upgrade podman-compose to 2.0+
pip install --upgrade podman-compose

# Verify version
podman-compose --version

# Then uncomment include: in docker-compose.yml
# And use: podman compose up -d
```

## Best Practices

1. **Always start `core` first** - It's the foundation for everything else
2. **Verify services are healthy** - Use `make ps` or `podman compose ps` to check
3. **Keep `.env` file in sync** - All services depend on environment variables
4. **Use Make for simplicity** - Works everywhere, version-agnostic
5. **Be careful with `down -v`** - Deletes PostgreSQL and Superset data permanently
6. **Check logs when services fail** - Use `make logs` or `podman compose logs -f <service>`
