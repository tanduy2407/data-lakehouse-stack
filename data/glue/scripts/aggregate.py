from awsglue.context import GlueContext
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, current_timestamp, lit, max as spark_max, min as spark_min, ntile, percent_rank, sum as spark_sum, when
from pyspark.sql.window import Window
from state_store import JobStateStore
from data_quality import assert_approx_equal, assert_no_nulls, assert_positive


def main() -> None:
    hive_metastore_uris = os.getenv("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
    warehouse_bucket = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
    spark = (
        SparkSession.builder
        .appName("medallion_aggregate")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{warehouse_bucket}/")
        .config("spark.sql.warehouse.dir", f"s3a://{warehouse_bucket}/")
        .config("hive.metastore.uris", hive_metastore_uris)
        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uris)
        .config("hive.metastore.client.factory.class", "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory")
        .config("spark.hadoop.hive.metastore.client.factory.class", "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext
    glue_context = GlueContext(sc)

    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_db = os.getenv("POSTGRES_DB", "source_db")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_path_style_access = os.getenv("S3_PATH_STYLE_ACCESS", "true")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password"))
    hadoop_conf.set("fs.s3a.path.style.access", s3_path_style_access)
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    if minio_endpoint.startswith("http://"):
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    silver_base_path = f"s3a://{warehouse_bucket}/silver"

    def table_exists(table_name: str) -> bool:
        return spark.catalog.tableExists(table_name)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://{warehouse_bucket}/gold'")

    state_store = JobStateStore(spark, jdbc_url, postgres_user, postgres_password)
    run_id = state_store.start_run("aggregate")
    rows_in = 0
    rows_out = 0

    try:
        watermark = state_store.load_watermarks("aggregate", ["silver_transform_ts"])
        last_silver_transform_ts = watermark.get("silver_transform_ts")

        silver_df = spark.read.parquet(f"{silver_base_path}/events")
        if last_silver_transform_ts:
            silver_df = silver_df.filter(col("transform_ts") > lit(last_silver_transform_ts).cast("timestamp"))

        incremental_count = silver_df.count()
        rows_in = incremental_count
        if incremental_count == 0:
            state_store.finish_run(run_id, "SUCCEEDED", rows_in=rows_in, rows_out=rows_out)
            print("Aggregate job completed: no new silver events for incremental gold load")
            return

        assert_no_nulls(
            silver_df,
            ["event_id", "user_id", "product_id", "quantity", "gross_amount", "year", "month", "transform_ts"],
            "silver_events_incremental",
        )
        assert_positive(silver_df, ["quantity", "gross_amount"], "silver_events_incremental")

        impacted_partitions = silver_df.select("year", "month").distinct().cache()

        # Build incremental product metrics from new silver rows.
        gold_product_delta_df = (
            silver_df.groupBy("year", "month", "product_id", "category")
            .agg(
                count("event_id").alias("event_count"),
                spark_sum("quantity").alias("units"),
                spark_sum("gross_amount").alias("gross_revenue"),
                countDistinct("user_id").alias("active_users"),
            )
            .withColumn("aggregate_ts", current_timestamp())
        )
        
        # Enrich product metrics with performance tier based on revenue.
        revenue_window = Window.partitionBy("year", "month").orderBy(col("gross_revenue").desc())

        def enrich_product_metrics(df):
            return (
                df
                .withColumn("revenue_percentile", ntile(4).over(revenue_window))
                .withColumn(
                    "performance_tier",
                    when(col("revenue_percentile") == 1, "Top Performer")
                    .when(col("revenue_percentile") == 2, "Strong")
                    .when(col("revenue_percentile") == 3, "Average")
                    .otherwise("Underperformer"),
                )
                .withColumn("avg_quantity_per_event", (col("units") / col("event_count")).cast("decimal(10,2)"))
                .drop("revenue_percentile")
            )

        gold_product_delta_df = enrich_product_metrics(gold_product_delta_df)

        # Build incremental customer metrics from new silver rows.
        gold_customer_delta_df = (
            silver_df.groupBy(
                "year",
                "month",
                "user_id",
                "customer_name",
                "customer_email",
                "customer_city",
                "customer_is_active",
            )
            .agg(
                count("event_id").alias("event_count"),
                spark_sum("quantity").alias("units"),
                spark_sum("gross_amount").alias("gross_revenue"),
                countDistinct("product_id").alias("distinct_products"),
            )
            .withColumn("aggregate_ts", current_timestamp())
        )
        
        # Enrich customer metrics with segmentation based on spending.
        spend_window = Window.partitionBy("year", "month").orderBy(col("gross_revenue").desc())

        def enrich_customer_metrics(df):
            return (
                df
                .withColumn("spend_percentile", ntile(4).over(spend_window))
                .withColumn(
                    "customer_segment",
                    when(col("spend_percentile") == 1, "VIP")
                    .when(col("spend_percentile") == 2, "High-Value")
                    .when(col("spend_percentile") == 3, "Standard")
                    .otherwise("At-Risk"),
                )
                .withColumn("avg_order_value", (col("gross_revenue") / col("event_count")).cast("decimal(10,2)"))
                .withColumn("avg_items_per_order", (col("units") / col("event_count")).cast("decimal(10,2)"))
                .drop("spend_percentile")
            )

        gold_customer_delta_df = enrich_customer_metrics(gold_customer_delta_df)

        silver_totals = silver_df.agg(
            count("event_id").alias("event_count"),
            spark_sum("quantity").alias("units"),
            spark_sum("gross_amount").alias("gross_revenue"),
        ).collect()[0]
        product_totals = gold_product_delta_df.agg(
            spark_sum("event_count").alias("event_count"),
            spark_sum("units").alias("units"),
            spark_sum("gross_revenue").alias("gross_revenue"),
        ).collect()[0]
        customer_totals = gold_customer_delta_df.agg(
            spark_sum("event_count").alias("event_count"),
            spark_sum("units").alias("units"),
            spark_sum("gross_revenue").alias("gross_revenue"),
        ).collect()[0]

        assert_approx_equal(product_totals["event_count"], silver_totals["event_count"], "gold_product_metrics.event_count")
        assert_approx_equal(product_totals["units"], silver_totals["units"], "gold_product_metrics.units")
        assert_approx_equal(product_totals["gross_revenue"], silver_totals["gross_revenue"], "gold_product_metrics.gross_revenue")
        assert_approx_equal(customer_totals["event_count"], silver_totals["event_count"], "gold_customer_metrics.event_count")
        assert_approx_equal(customer_totals["units"], silver_totals["units"], "gold_customer_metrics.units")
        assert_approx_equal(customer_totals["gross_revenue"], silver_totals["gross_revenue"], "gold_customer_metrics.gross_revenue")

        product_table = "gold.product_metrics"
        customer_table = "gold.customer_metrics"

        if table_exists(product_table):
            existing_product_impacted_df = (
                spark.table(product_table)
                .join(impacted_partitions, on=["year", "month"], how="inner")
            )
            gold_product_upsert_df = enrich_product_metrics(
                existing_product_impacted_df
                .unionByName(gold_product_delta_df, allowMissingColumns=True)
                .groupBy("year", "month", "product_id", "category")
                .agg(
                    spark_sum("event_count").alias("event_count"),
                    spark_sum("units").alias("units"),
                    spark_sum("gross_revenue").alias("gross_revenue"),
                    spark_sum("active_users").alias("active_users"),
                )
                .withColumn("aggregate_ts", current_timestamp())
            )
            gold_product_upsert_df.writeTo(product_table).overwritePartitions()
        else:
            gold_product_delta_df.writeTo(product_table).partitionedBy("year", "month").using("iceberg").create()

        if table_exists(customer_table):
            existing_customer_impacted_df = (
                spark.table(customer_table)
                .join(impacted_partitions, on=["year", "month"], how="inner")
            )
            gold_customer_upsert_df = enrich_customer_metrics(
                existing_customer_impacted_df
                .unionByName(gold_customer_delta_df, allowMissingColumns=True)
                .groupBy(
                    "year",
                    "month",
                    "user_id",
                    "customer_name",
                    "customer_email",
                    "customer_city",
                    "customer_is_active",
                )
                .agg(
                    spark_sum("event_count").alias("event_count"),
                    spark_sum("units").alias("units"),
                    spark_sum("gross_revenue").alias("gross_revenue"),
                    spark_sum("distinct_products").alias("distinct_products"),
                )
                .withColumn("aggregate_ts", current_timestamp())
            )
            gold_customer_upsert_df.writeTo(customer_table).overwritePartitions()
        else:
            gold_customer_delta_df.writeTo(customer_table).partitionedBy("year", "month").using("iceberg").create()

        max_silver_transform_ts = silver_df.agg(spark_max("transform_ts").alias("max_value")).collect()[0]["max_value"]
        if max_silver_transform_ts is not None:
            state_store.upsert_watermark(
                "aggregate",
                "silver_transform_ts",
                max_silver_transform_ts.isoformat(sep=" ", timespec="microseconds"),
                run_id,
            )

        # Iceberg auto-maintains partition metadata — no MSCK REPAIR needed.
        rows_out = gold_product_delta_df.count() + gold_customer_delta_df.count()
        state_store.finish_run(run_id, "SUCCEEDED", rows_in=rows_in, rows_out=rows_out)

        print(
            "Aggregate job completed: incremental gold update applied "
            f"(silver_rows={incremental_count}), watermark updated in job_control_watermarks"
        )
    except Exception as exc:
        state_store.finish_run(run_id, "FAILED", rows_in=rows_in, rows_out=rows_out, error_message=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
