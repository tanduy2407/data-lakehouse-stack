#!/usr/bin/env python3
import os
from pathlib import Path
from pyspark.sql import SparkSession

parquet_file = "/Users/duy.pham@optum.com/Documents/DataEngineer/cds-ofg-next-gen/data-lakehouse-stack/data/fake/events_20260825_154925_bdad0163.parquet"
s3_uri = "s3://duncan-bucket-20250407/test_write_parquet/"


def load_env_file(env_file=None) -> None:
    env_path = Path(env_file) if env_file else Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip("\"'"))


load_env_file()
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]
print(aws_access_key_id)
assert False

def create_spark_session(app_name: str = "StoreParquetToS3") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def configure_spark_for_s3(
    spark: SparkSession,
    access_key_id=None,
    secret_access_key=None,
    region=None,
    endpoint_url=None,
) -> None:
    if bool(access_key_id) != bool(secret_access_key):
        raise ValueError("Both access_key_id and secret_access_key must be provided together")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    if access_key_id and secret_access_key:
        hadoop_conf.set("fs.s3a.access.key", access_key_id)
        hadoop_conf.set("fs.s3a.secret.key", secret_access_key)
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.endpoint.region", region)
    if endpoint_url:
        hadoop_conf.set("fs.s3a.endpoint", endpoint_url)
        hadoop_conf.set("fs.s3a.path.style.access", "true")


def normalize_s3a_uri(s3_uri: str) -> str:
    if s3_uri.startswith("s3://"):
        return f"s3a://{s3_uri.removeprefix('s3://')}"
    if s3_uri.startswith("s3a://"):
        return s3_uri

    raise ValueError("Target URI must start with s3:// or s3a://")


def write_parquet_to_s3(
    source_file,
    s3_uri: str,
    mode: str,
    access_key_id: str,
    secret_access_key: str,
    region: str):
    active_spark = create_spark_session()
    configure_spark_for_s3(
        spark=active_spark,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region
    )

    target_uri = normalize_s3a_uri(s3_uri)
    df = active_spark.read.parquet(str(source_file))
    df.write.mode(mode).parquet(target_uri)
    print(f"Wrote Parquet data from {source_file} to {target_uri}")


def main() -> None:
    write_parquet_to_s3(
        source_file=parquet_file,
        s3_uri=s3_uri,
        mode="append",
        access_key_id=aws_access_key_id,
        secret_access_key=aws_secret_access_key,
        region=aws_region
    )



if __name__ == "__main__":
    main()
