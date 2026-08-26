import os
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit, max as spark_max
from state_store import JobStateStore
from data_quality import assert_no_nulls, assert_unique, assert_non_negative, assert_positive


def main() -> None:
    spark = SparkSession.builder.appName("medallion_ingest").getOrCreate()

    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_db = os.getenv("POSTGRES_DB", "source_db")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    warehouse_bucket = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_path_style_access = os.getenv("S3_PATH_STYLE_ACCESS", "true")
    bronze_base_path = f"s3a://{warehouse_bucket}/bronze"
    jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"

    table_configs = {
        "customers": {
            "incremental_col": "signup_date",
            "literal_type": "DATE",
        },
        "products": {
            "incremental_col": "created_at",
            "literal_type": "TIMESTAMP",
        },
        "orders": {
            "incremental_col": "order_ts",
            "literal_type": "TIMESTAMP",
        },
    }

    def format_value(value) -> str:
        if isinstance(value, datetime):
            return value.isoformat(sep=" ", timespec="microseconds")
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    def incremental_dbtable(table_name: str, column_name: str, literal_type: str, last_value: str):
        if not last_value:
            return table_name
        escaped_value = last_value.replace("'", "''")
        return (
            f"(SELECT * FROM {table_name} "
            f"WHERE {column_name} > {literal_type} '{escaped_value}') AS {table_name}_incremental"
        )

    def path_exists(path: str) -> bool:
        jvm = spark.sparkContext._jvm
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        return bool(fs.exists(hadoop_path))

    # Force S3A to use static MinIO credentials and avoid STS-based provider chain.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password"))
    hadoop_conf.set("fs.s3a.path.style.access", s3_path_style_access)
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    if minio_endpoint.startswith("http://"):
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    print(f"jdbc_url: {jdbc_url}")
    jdbc_options = {
        "url": jdbc_url,
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    state_store = JobStateStore(spark, jdbc_url, postgres_user, postgres_password)
    run_id = state_store.start_run("ingest")
    rows_in = 0
    rows_out = 0

    try:
        watermark = state_store.load_watermarks("ingest", table_configs.keys())
        bronze_paths_exist = all(
            path_exists(f"{bronze_base_path}/{table_name}")
            for table_name in table_configs.keys()
        )
        first_run_bootstrap = not bronze_paths_exist
        effective_watermark = {} if first_run_bootstrap else watermark

        if first_run_bootstrap:
            print("Bronze paths not found. Running bootstrap full load for first ingest run.")

        customers_df = spark.read.format("jdbc").options(
            **jdbc_options,
            dbtable=incremental_dbtable(
                "customers",
                table_configs["customers"]["incremental_col"],
                table_configs["customers"]["literal_type"],
                effective_watermark.get("customers"),
            ),
        ).load()
        products_df = spark.read.format("jdbc").options(
            **jdbc_options,
            dbtable=incremental_dbtable(
                "products",
                table_configs["products"]["incremental_col"],
                table_configs["products"]["literal_type"],
                effective_watermark.get("products"),
            ),
        ).load()
        orders_df = spark.read.format("jdbc").options(
            **jdbc_options,
            dbtable=incremental_dbtable(
                "orders",
                table_configs["orders"]["incremental_col"],
                table_configs["orders"]["literal_type"],
                effective_watermark.get("orders"),
            ),
        ).load()

        if customers_df.count() > 0:
            assert_no_nulls(customers_df, ["customer_id", "email", "signup_date"], "bronze_customers")
            assert_unique(customers_df, ["customer_id"], "bronze_customers")
        if products_df.count() > 0:
            assert_no_nulls(products_df, ["product_id", "unit_price", "created_at"], "bronze_products")
            assert_unique(products_df, ["product_id"], "bronze_products")
            assert_positive(products_df, ["unit_price"], "bronze_products")
            assert_non_negative(products_df, ["in_stock_qty"], "bronze_products")
        if orders_df.count() > 0:
            assert_no_nulls(orders_df, ["order_id", "customer_id", "product_id", "quantity", "order_ts"], "bronze_orders")
            assert_unique(orders_df, ["order_id"], "bronze_orders")
            assert_positive(orders_df, ["quantity", "total_amount"], "bronze_orders")

        max_customers_ts = customers_df.agg(spark_max("signup_date").alias("max_value")).collect()[0]["max_value"]
        max_products_ts = products_df.agg(spark_max("created_at").alias("max_value")).collect()[0]["max_value"]
        max_orders_ts = orders_df.agg(spark_max("order_ts").alias("max_value")).collect()[0]["max_value"]

        # Bronze layer: replicate source tables from PostgreSQL.
        bronze_customers = customers_df.withColumn("ingest_ts", current_timestamp()).withColumn("source_system", lit("postgres"))
        bronze_products = products_df.withColumn("ingest_ts", current_timestamp()).withColumn("source_system", lit("postgres"))
        bronze_orders = orders_df.withColumn("ingest_ts", current_timestamp()).withColumn("source_system", lit("postgres"))

        # Add year/month partition columns for medallion storage layout.
        def add_year_month_partitions(df):
            return (
                df.withColumn("year", date_format(col("ingest_ts"), "yyyy"))
                .withColumn("month", date_format(col("ingest_ts"), "MM"))
            )

        bronze_customers = add_year_month_partitions(bronze_customers)
        bronze_products = add_year_month_partitions(bronze_products)
        bronze_orders = add_year_month_partitions(bronze_orders)

        customers_count = bronze_customers.count()
        products_count = bronze_products.count()
        orders_count = bronze_orders.count()
        rows_in = customers_count + products_count + orders_count
        rows_out = rows_in

        if rows_in == 0:
            state_store.finish_run(run_id, "SUCCEEDED", rows_in=rows_in, rows_out=rows_out)
            print("Ingest job completed: no new rows found for incremental load")
            return

        (
            bronze_customers.write.mode("append")
            .format("parquet")
            .partitionBy("year", "month")
            .save(f"{bronze_base_path}/customers")
        )
        (
            bronze_products.write.mode("append")
            .format("parquet")
            .partitionBy("year", "month")
            .save(f"{bronze_base_path}/products")
        )
        (
            bronze_orders.write.mode("append")
            .format("parquet")
            .partitionBy("year", "month")
            .save(f"{bronze_base_path}/orders")
        )

        if max_customers_ts is not None:
            state_store.upsert_watermark("ingest", "customers", format_value(max_customers_ts), run_id)
        if max_products_ts is not None:
            state_store.upsert_watermark("ingest", "products", format_value(max_products_ts), run_id)
        if max_orders_ts is not None:
            state_store.upsert_watermark("ingest", "orders", format_value(max_orders_ts), run_id)

        state_store.finish_run(run_id, "SUCCEEDED", rows_in=rows_in, rows_out=rows_out)
        print(
            "Ingest job completed: incremental rows written "
            f"(customers={customers_count}, products={products_count}, orders={orders_count}), "
            "watermarks updated in job_control_watermarks"
        )
    except Exception as exc:
        state_store.finish_run(run_id, "FAILED", rows_in=rows_in, rows_out=rows_out, error_message=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
