# Local Lakehouse Stack

This repository provides a local lakehouse lab using Podman/Docker Compose with MinIO, Hive Metastore, AWS Glue (Spark), Trino, Superset, PostgreSQL, and a data-quality monitor.

The stack is intended for local development and demonstrations. It stores objects in MinIO and uses PostgreSQL for both the source database and the Hive Metastore database.

## Services

### MinIO
- **Purpose**: S3-compatible object storage
- **Console**: http://localhost:9001
- **S3 Endpoint**: http://localhost:9000
- **Default Credentials**: 
  - Username: `admin`
  - Password: `password`

### Hive Metastore
- **Purpose**: Centralized metadata catalog for Spark/Glue
- **Metastore Server**: thrift://localhost:9083
- **Backend**: PostgreSQL
- **Features**: 
  - Manages table schemas and partitions
  - S3-backed warehouse directory
  - Native integration with Spark SQL

### AWS Glue
- **Purpose**: Data integration and ETL
- **Spark UI**: http://localhost:4040
- **Glue Studio**: http://localhost:18080
- **Volumes**: 
  - Scripts mounted at `/home/glue/scripts`
  - Data mounted at `/home/glue/data`
- **Metastore**: Automatically configured to use Hive Metastore

### PostgreSQL
- **Purpose**: Relational database & Hive Metastore backend
- **Host**: localhost
- **Port**: 5432
- **Default User**: `postgres`
- **Default Password**: `postgres`
- **Default Database**: `source_db`
- **Metastore DB**: `hive_metastore`
- **Persistence**: Named volume `postgres_data` mounted at `/var/lib/postgresql/data`

### Trino
- **Purpose**: Distributed SQL query engine (Athena-like in local lab)
- **UI / API**: http://localhost:8080
- **Catalog**: `iceberg` wired to Hive Metastore + MinIO

### Superset
- **Purpose**: Browser SQL interface (SQL Lab) and optional dashboards
- **URL**: http://localhost:8088
- **Default Login**: `admin` / `admin`
- **Persistence**: Named volume `superset_home_data` mounted at `/app/superset_home`
- **Auto-registered connections on startup**:
  - `PostgreSQL` -> `source_db`
  - `Hive_Metastore_PostgreSQL` -> `hive_metastore`
  - `Trino` -> `iceberg/gold`

### Data-quality monitor
- **Purpose**: Poll PostgreSQL job-control and data-quality tables
- **Logs**: `podman compose logs -f dq-monitor`
- **Configuration**: `MONITOR_POLL_INTERVAL`, `MONITOR_ALERT_WINDOW_MINUTES`, and `MONITOR_JOB_FRESHNESS_HOURS` in `.env`


## Runtime Command Style

Use `podman compose` commands directly, or use Make targets. The Makefile defaults to Podman. If you prefer Docker Compose, set:

```bash
make COMPOSE="docker compose" up
```

## Prerequisites

Install one of the following:

- Podman and Podman Compose, or Docker and Docker Compose
- GNU Make
- `curl` for downloading the required JDBC and Hadoop JARs

The first startup requires network access to pull container images and download JARs.

## Quick Start

Create the local environment file before starting the stack:

```bash
cp .env.example .env
```

Review `.env` if you need to change credentials, ports, or Spark memory settings. The default local credentials are suitable for a development environment only.

### Start infrastructure services

The recommended startup command excludes the one-shot Glue job containers:

```bash
make jars
podman compose up -d minio minio-bucket-init postgres hive-metastore glue trino superset dq-monitor
```

The equivalent Docker Compose command is:

```bash
make COMPOSE="docker compose" jars
make COMPOSE="docker compose" up
```

`make up` currently runs `podman compose up -d` for the complete Compose file. That also starts the one-shot `glue-ingest`, `glue-transform`, and `glue-aggregate` services. Use the explicit service list above when you want infrastructure only.

### Start SQL query services only
```bash
podman compose up -d trino superset
```

This still brings up their required dependencies: PostgreSQL, MinIO, bucket init, and Hive Metastore.

### View logs
```bash
# All services
podman compose logs -f

# Specific service
podman compose logs -f minio
podman compose logs -f glue
podman compose logs -f postgres
podman compose logs -f trino
podman compose logs -f superset
podman compose logs -f dq-monitor
```

### Stop services
```bash
podman compose down
# or
make down
```

### Reset named volumes (PostgreSQL + Superset)
```bash
podman compose down -v
# If using Docker Compose:
docker compose down -v
```

PostgreSQL initialization scripts run only when the `postgres_data` volume is created. A reset is required before changes to files under `data/postgres/init` will be applied to an existing database.

### Remove bind-mounted data (MinIO objects)
```bash
podman compose down
rm -rf data/minio
mkdir -p data/minio
```

For a local reset that removes containers, named volumes, and bind-mounted runtime data:

```bash
make clean
```

## Directory Structure

Create these directories for mounting bind mounts:
```bash
mkdir -p data/minio data/glue/data
```

The remaining required directories and JAR mounts are provided by this repository and `make jars`.

## Connect to PostgreSQL

```bash
# Using psql
psql -h localhost -U postgres -d source_db

# Using Podman compose exec
podman compose exec postgres psql -U postgres -d source_db
```

For Hive metastore database:

```bash
podman compose exec postgres psql -U postgres -d hive_metastore
```

To list all databases quickly:

```bash
podman compose exec postgres psql -U postgres -lqt
```

## Upload files to MinIO

You can use the MinIO Console at http://localhost:9001 or AWS CLI:

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://warehouse
aws --endpoint-url http://localhost:9000 s3 cp local_file.csv s3://warehouse/
```

## Using Hive Metastore with Spark

Once in the Glue container, you can use Spark SQL to query tables managed by Hive Metastore:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a managed table
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_table (
        id INT,
        name STRING
    )
    USING PARQUET
""")

# Query the table
spark.sql("SELECT * FROM my_table").show()
```

## Querying with Trino

```bash
# Check Trino health
curl -f http://localhost:8080/v1/info

# The configured catalog is iceberg.
podman compose exec trino trino --execute "SHOW CATALOGS"
podman compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"

# List and query gold tables
podman compose exec trino trino --execute "SHOW TABLES FROM iceberg.gold"
podman compose exec trino trino --execute "SELECT * FROM iceberg.gold.product_metrics LIMIT 20"
```

## Querying in Browser with Superset SQL Lab

1. Open http://localhost:8088 and log in with `admin` / `admin`.
2. Open SQL Lab -> SQL Editor.
3. Select the `Trino` connection and run:

```sql
SHOW TABLES FROM iceberg.gold;
SELECT * FROM iceberg.gold.product_metrics LIMIT 20;
```

4. You can also use:
  - `PostgreSQL` to query source tables (`public.customers`, `public.products`, `public.orders`)
  - `Hive_Metastore_PostgreSQL` to inspect Hive metadata tables (`"DBS"`, `"TBLS"`, `"SDS"`, `"COLUMNS_V2"`)

## ETL Job Workflow

The available Make targets are:

```bash
make ingest
make transform
make aggregate
make jobs
```

`make ingest` runs `data/glue/scripts/ingest.py`. The Compose file currently references `data/glue/scripts/transform.py`, but that file is not present in this checkout, so `make transform` and `make jobs` cannot complete until the transform script is added. `aggregate.py` expects the resulting `s3a://warehouse/silver/events` dataset.

Run the working ingest job after infrastructure is ready:

```bash
make ingest
```

The job state is persisted in PostgreSQL and the watermark files are written under the configured Glue data mount. Inspect job status with:

```bash
podman compose exec postgres psql -U postgres -d source_db \
  -c "select run_id, job_name, status, started_at, ended_at, rows_in, rows_out from job_control_runs order by started_at desc limit 20;"
```

To validate source row counts:

```bash
make source-counts
```

The incremental seed SQL under `data/postgres/sql_query/seed-incremental-data.sql` is intended for manual ad hoc checks rather than the main pipeline flow. It is not mounted into the PostgreSQL container, so apply it from the host with:

```bash
podman compose cp data/postgres/sql_query/seed-incremental-data.sql postgres:/tmp/seed-incremental-data.sql
podman compose exec postgres psql -U postgres -d source_db -f /tmp/seed-incremental-data.sql
```

The `make seed-incremental` target currently points to a different, non-mounted path and should not be used until that Makefile target is corrected.

Incremental state is persisted in PostgreSQL control tables:

- `job_control_watermarks`
- `job_control_runs`

Inspect state quickly:

```bash
podman compose exec postgres psql -U postgres -d source_db -c "select * from job_control_watermarks order by job_name, entity_name;"
podman compose exec postgres psql -U postgres -d source_db -c "select run_id, job_name, status, started_at, ended_at, rows_in, rows_out from job_control_runs order by started_at desc limit 20;"
```

## Configuration

Edit `.env` to customize credentials and resource allocation.
Copy `.env.example` to `.env` and review the settings. Important entries include:

```env
SUPERSET_SECRET_KEY=replace-with-a-long-random-string
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_EMAIL=admin@local.lab
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_WAREHOUSE_BUCKET=warehouse
POSTGRES_DB=source_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
MINIO_ENDPOINT=http://minio:9000
MINIO_WAREHOUSE_BUCKET=warehouse
S3_PATH_STYLE_ACCESS=true
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

The Compose file uses container-side service names such as `minio`, `postgres`, and `hive-metastore`. From the host, use `localhost` with the published ports listed above.

## Notes

- MinIO data persists in bind mount `data/minio`
- PostgreSQL data persists in named volume `postgres_data`
- Superset metadata persists in named volume `superset_home_data`
- Job watermark and run state persist in PostgreSQL (`source_db.job_control_*`)
- All services connect via `data-lakehouse-stack_network` bridge network
- The long-running Glue container is a Spark shell container (`sleep infinity`); port `4040` is only useful when a Spark application exposes its UI. Port `18080` is published but no Glue Studio server is started by the current Compose command.
- `hive_metastore` is created by `data/postgres/init/01-create-hive-metastore-db.sql` on first initialization of a fresh `postgres_data` volume
- Job control tables are created by `data/postgres/init/03-create-job-control-tables.sql` on first initialization of a fresh `postgres_data` volume
- Health checks ensure services are ready before dependent services start
