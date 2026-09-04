#!/usr/bin/env python3
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import yaml


def parse_s3_uri(s3_uri: str) -> tuple:
    if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")

    bucket, separator, key = s3_uri.removeprefix("s3://").partition("/")
    if not bucket or not separator or not key:
        raise ValueError("S3 URI must include both a bucket and object key")

    return bucket, key


def validate_config(config: dict) -> dict:
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")

    required_keys = {"bronze", "silver", "gold"}
    missing_keys = required_keys - config.keys()
    if missing_keys:
        raise ValueError(
            "Missing required configuration keys: " +
            ", ".join(sorted(missing_keys))
        )
    for layer in ("bronze", "silver", "gold"):
        layer_config = config[layer]
        if not isinstance(layer_config, dict):
            raise ValueError(f"{layer} must be a mapping")
        try:
            parse_s3_uri(layer_config.get("s3_uri"))
        except ValueError as error:
            raise ValueError(f"Invalid {layer}.s3_uri: {error}") from error
        if layer_config.get("write_mode") not in {"append", "overwrite", "error", "ignore"}:
            raise ValueError(
                f"{layer}.write_mode must be append, overwrite, error, or ignore"
            )
        # Validate partition_by if present
        partition_by = layer_config.get("partition_by")
        if partition_by is not None:
            if isinstance(partition_by, str):
                # Accept single string, will convert to list
                pass
            elif (
                not isinstance(partition_by, list)
                or not all(isinstance(column, str) for column in partition_by)
            ):
                raise ValueError(
                    f"{layer}.partition_by must be a string or list of strings"
                )
    return config


def load_s3_config(config_uri: str) -> dict:
    bucket, key = parse_s3_uri(config_uri)
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    yaml_contents = response["Body"].read().decode("utf-8")
    return validate_config(yaml.safe_load(yaml_contents))


def read_parquet(
    glue_context: GlueContext,
    folder_name: str,
    year: str = None,
    month: str = None,
):
    """Read Parquet data from a folder or a year/month partition.

    Args:
        glue_context: Glue context used to read the data.
        folder_name: S3 folder containing the Parquet data.
        year: Optional partition year.
        month: Optional partition month.
    """
    parse_s3_uri(folder_name)
    if (year is None) != (month is None):
        raise ValueError("year and month must be provided together")

    folder_path = folder_name.rstrip("/")
    if year is not None and month is not None:
        folder_path += f"/year={year}/month={str(month).zfill(2)}"

    return glue_context.spark_session.read.parquet(folder_path)


def write_parquet(dataframe, target_uri: str, mode, partition_by=None) -> None:
    """Write a DataFrame to S3 as Parquet, optionally partitioned.

    Args:
        dataframe: DataFrame to write.
        target_uri: S3 destination folder.
        mode: Write mode, such as append or overwrite.
        partition_by: Optional column name or list of column names.
    """
    write_op = dataframe.write.mode(mode)
    if partition_by:
        # Accept both string and list; convert string to list
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        write_op = write_op.partitionBy(*partition_by)
    write_op.parquet(target_uri)


def register_catalog_table(
    dataframe,
    target_uri: str,
    database: str,
    table_name: str,
    partition_by=None,
) -> None:
    """Create or update a Glue table definition."""
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    partition_by = partition_by or []

    fields = {field.name: field for field in dataframe.schema.fields}
    missing_partitions = set(partition_by) - fields.keys()
    if missing_partitions:
        raise ValueError(
            "Partition columns not found in DataFrame: "
            + ", ".join(sorted(missing_partitions))
        )

    columns = [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in dataframe.schema.fields
        if field.name not in partition_by
    ]
    partition_keys = [
        {"Name": name, "Type": fields[name].dataType.simpleString()}
        for name in partition_by
    ]
    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet"},
        "PartitionKeys": partition_keys,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": target_uri,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
    }

    glue_client = boto3.client("glue")
    try:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)


def register_catalog_partition(
    database: str,
    table_name: str,
    target_uri: str,
    year: str,
    month: str,
    dataframe,
    partition_by=None,
) -> None:
    """Create or update one year/month partition in Glue Catalog."""
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    partition_by = partition_by or ["year", "month"]
    partition_month = str(month).zfill(2)
    partition_uri = (
        f"{target_uri.rstrip('/')}/year={year}/month={partition_month}"
    )
    columns = [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in dataframe.schema.fields
        if field.name not in partition_by
    ]
    partition = {
        "Values": [str(year), partition_month],
        "StorageDescriptor": {
            "Columns": columns,
            "Location": partition_uri,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
    }

    glue_client = boto3.client("glue")
    try:
        glue_client.update_partition(
            DatabaseName=database,
            TableName=table_name,
            PartitionValueList=partition["Values"],
            PartitionInput=partition,
        )
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_partition(
            DatabaseName=database,
            TableName=table_name,
            PartitionInput=partition,
        )


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "CONFIG_FILE", "TARGET_LAYER", "FOLDER_NAME"],
    )
    optional_args = [name for name in ("YEAR", "MONTH") if f"--{name}" in sys.argv]
    if optional_args:
        args.update(getResolvedOptions(sys.argv, optional_args))
    config_uri = args["CONFIG_FILE"]
    target_layer = args["TARGET_LAYER"]
    folder_name = args["FOLDER_NAME"]
    partition_year = args.get("YEAR")
    partition_month = args.get("MONTH")
    parse_s3_uri(config_uri)
    valid_layers = {"bronze", "silver", "gold"}
    if target_layer not in valid_layers:
        raise ValueError("TARGET_LAYER must be bronze, silver, or gold")

    print(f"Loading configuration from {config_uri}")
    config = load_s3_config(config_uri)

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    target_config = config[target_layer]
    df = read_parquet(
        glue_context, folder_name, partition_year, partition_month
    )
    partition_by = target_config.get("partition_by")
    write_parquet(df, target_config["s3_uri"], target_config["write_mode"], partition_by)

    catalog_database = target_config.get("database")
    catalog_table = target_config.get("table")
    if catalog_database and catalog_table:
        register_catalog_table(
            df,
            target_config["s3_uri"],
            catalog_database,
            catalog_table,
            partition_by,
        )
        if (
            partition_year is not None
            and partition_by == ["year", "month"]
        ):
            register_catalog_partition(
                catalog_database,
                catalog_table,
                target_config["s3_uri"],
                partition_year,
                partition_month,
                df,
                partition_by,
            )

    source_description = folder_name
    if partition_year is not None:
        source_description += (
            f"/year={partition_year}/month={str(partition_month).zfill(2)}"
        )
    print(
        f"Wrote Parquet data from {source_description} "
        f"to {target_config['s3_uri']}"
    )

    job.commit()


if __name__ == "__main__":
    main()
