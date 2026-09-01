# Apache Iceberg vs. Delta Lake vs. Apache Hudi

Apache Iceberg, Delta Lake, and Apache Hudi are open table formats for data lakes. They add database-like table features on top of columnar files, usually Parquet, in object storage such as Amazon S3 or MinIO.

All three formats can provide ACID transactions, schema evolution, time travel, and support for updates or deletes. The important difference is how each format manages metadata and which workload it optimizes.

## Quick Choice

| Primary need | Recommended format | Why |
|---|---|---|
| Multi-engine analytics with Spark, Trino, Flink, or Hive | Apache Iceberg | Engine-neutral design, efficient metadata, and hidden partitioning |
| Spark or Databricks-first lakehouse | Delta Lake | Mature Spark integration and a simple transaction-log model |
| CDC, frequent record updates, and near-real-time ingestion | Apache Hudi | Built around incremental upserts and record-level change handling |
| Read-heavy curated Silver or Gold tables | Apache Iceberg | Strong query planning and metadata scaling for analytical workloads |
| Append-only event history | Any, usually Iceberg or Delta Lake | All work well; choose based on the query engine ecosystem |
| Serving fresh updates before compaction completes | Apache Hudi with Merge-on-Read | Log files make recent updates available quickly |

## Feature Comparison

| Area | Apache Iceberg | Delta Lake | Apache Hudi |
|---|---|---|---|
| Metadata model | Snapshot metadata files, manifest lists, and manifests | Ordered transaction log in `_delta_log` | Timeline plus file groups, base files, and optional log files |
| Primary strength | Open, engine-neutral analytics | Spark and Databricks lakehouse workflows | Incremental ingestion and mutable records |
| Common engines | Spark, Trino, Flink, Hive, Dremio, Snowflake | Spark, Databricks, Flink, Trino | Spark, Flink, Trino, Presto |
| ACID transactions | Yes | Yes | Yes |
| Time travel | Snapshot-based | Version or timestamp-based | Commit timeline-based |
| Schema evolution | Yes | Yes | Yes |
| Partition evolution | Yes, without rewriting existing data | Supported, with engine/version considerations | Supported, but operationally more involved |
| Hidden partitioning | Yes | No; partition columns are explicit to users and queries | No; partition paths are typically part of table design |
| Upserts and deletes | Supported through row-level operations | Strong `MERGE INTO` support | Core capability, optimized for it |
| Streaming ingestion | Supported, especially with Flink and Spark | Strong Spark Structured Streaming support | Strong Spark and Flink streaming support |
| Operational overhead | Moderate | Low to moderate in a Spark-first stack | Moderate to high due to indexing, compaction, and cleaning |
| Best storage layout | Large analytical files with snapshot metadata | Data files with an append-only transaction log | Copy-on-Write or Merge-on-Read file groups |

## When to Use Apache Iceberg

Choose Iceberg when the lakehouse must be queried and written by more than one compute engine, or when analytical query performance across a large number of partitions matters most.

Iceberg is a strong choice for:

- A shared data platform where Spark writes data and Trino serves analysts.
- Silver and Gold tables that are queried frequently but updated in scheduled batches.
- Tables whose partition strategy may need to change as data volume or query patterns evolve.
- Object-store datasets with many files, where metadata pruning needs to avoid expensive file listings.
- Teams that want to avoid coupling table storage to a single vendor or engine.

Iceberg uses hidden partitioning. For example, a table can physically partition timestamps by day while users write ordinary predicates such as:

```sql
SELECT *
FROM gold.customer_metrics
WHERE metric_date >= DATE '2026-08-01';
```

The engine uses Iceberg metadata to prune partitions and files without requiring users to know the physical partition transform.

### Tradeoffs

- Write support and maintenance procedures vary slightly across engines and versions.
- Small files still need management through compaction or rewrite operations.
- It is optimized for table-scale metadata rather than Hudi-style record-level incremental ingestion.

## When to Use Delta Lake

Choose Delta Lake when Spark or Databricks is the main processing and query environment, especially when the team values a mature `MERGE INTO` workflow and tight Structured Streaming integration.

Delta Lake is a strong choice for:

- A Spark-first pipeline with batch and streaming transformations.
- Databricks workloads using features such as Change Data Feed, optimized writes, or platform-managed maintenance.
- ETL workflows that regularly merge source changes into analytical tables.
- Teams that prefer a direct, inspectable transaction-log model.

Delta records commits in a `_delta_log` directory. Readers reconstruct a consistent table version from this log and the referenced data files.

### Tradeoffs

- The broadest feature set is most mature in the Databricks and Spark ecosystem.
- Cross-engine support exists, but feature parity should be verified for the exact engine and version.
- Partitioning is generally explicit, so physical partition choices remain more visible in table design and query behavior.

## When to Use Apache Hudi

Choose Hudi when incoming data continuously changes existing records and the platform needs efficient upserts, deletes, and incremental downstream consumption.

Hudi is a strong choice for:

- Change Data Capture (CDC) from operational databases.
- Customer, account, inventory, or device datasets where late-arriving corrections are common.
- Pipelines that need to process only records changed since the previous checkpoint.
- Near-real-time ingestion where readers must see updates soon after arrival.

Hudi offers two principal table types:

| Table type | Write behavior | Read behavior | Best for |
|---|---|---|---|
| Copy-on-Write (COW) | Rewrites columnar base files on update | Fast analytical reads | Read-heavy data with less frequent updates |
| Merge-on-Read (MOR) | Writes updates to log files, then compacts later | May merge base and log files during reads | Low-latency writes and frequent updates |

### Tradeoffs

- Hudi requires more active operational planning: compaction, clustering, cleaning, indexing, and checkpoint management.
- Merge-on-Read improves write latency but can make queries more expensive before compaction.
- Its strengths are less important for immutable, append-only analytical facts.

## Decision Guide

Use this sequence to choose a format:

1. **Do records change frequently after they land?** Choose Hudi when record-level upserts, CDC, and low ingestion latency are the dominant concern.
2. **Is the platform primarily Spark or Databricks?** Choose Delta Lake when its Spark-native APIs and operational tooling are the natural fit.
3. **Will multiple engines query the same curated tables?** Choose Iceberg for an open analytics layer shared by Spark, Trino, Flink, and similar engines.
4. **Are the tables mostly append-only and analytical?** Prefer Iceberg for multi-engine access or Delta Lake for a Spark-first deployment.
5. **Does a required engine support the exact table operation?** Confirm compatibility for reads, writes, `MERGE`, deletes, streaming, and maintenance using the versions deployed in the environment.

## Common Selection Scenarios

| Situation | Recommended format | Reason |
|---|---|---|
| A platform must share curated analytical tables across Spark, Trino, Flink, and other engines | Apache Iceberg | Its open metadata design and hidden partitioning work well for multi-engine analytics |
| A team uses Spark or Databricks as its primary data platform | Delta Lake | It has mature Spark APIs, `MERGE INTO`, and Structured Streaming support |
| A CDC feed continuously updates customer profiles, inventory, or account records | Apache Hudi | It is designed for incremental upserts, deletes, and consumption of changed records |
| Large append-only event tables serve dashboards and ad hoc analytics | Iceberg or Delta Lake | Both are well suited; choose based on the principal compute ecosystem |
| Fresh updates must become visible quickly, before a full rewrite or compaction | Apache Hudi with Merge-on-Read | Changes can be written to logs and merged during reads until compaction |
| The partition scheme will likely change as a table grows | Apache Iceberg | Partition evolution can be performed without rewriting existing data |

## Operational Practices for Any Format

- Prefer Parquet files sized for the query engine and workload; avoid accumulating many small files.
- Run the format's supported compaction, rewrite, optimize, clustering, or vacuum procedures on an appropriate schedule.
- Retain snapshots or table versions long enough to support rollback and reproducible reporting.
- Test concurrent writes, schema changes, and recovery behavior before production use.
- Treat the catalog, table metadata, and object storage permissions as part of the same reliability boundary.

## Further Reading

- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
- [Delta Lake documentation](https://docs.delta.io/)
- [Apache Hudi documentation](https://hudi.apache.org/docs/overview/)