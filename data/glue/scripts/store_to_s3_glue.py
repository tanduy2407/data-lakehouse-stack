#!/usr/bin/env python3
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lpad
import yaml


def _parse_s3_uri(s3_uri: str) -> tuple:
    """Return the bucket and key from an S3 URI."""
    if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")

    bucket, separator, key = s3_uri.removeprefix("s3://").partition("/")
    if not bucket or not separator or not key:
        raise ValueError("S3 URI must include both a bucket and object key")
    return bucket, key


def _validate_config(config: dict) -> dict:
    """Validate the layer configuration."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")

    region = config.get("region")
    if region is not None and (not isinstance(region, str) or not region):
        raise ValueError("region must be a non-empty string")

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
            _parse_s3_uri(layer_config.get("s3_uri"))
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
    """Load and validate YAML configuration from S3."""
    bucket, key = _parse_s3_uri(config_uri)
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    yaml_contents = response["Body"].read().decode("utf-8")
    return _validate_config(yaml.safe_load(yaml_contents))


def _get_glue_client(region: str = None):
    """Create a Glue client using the configured or default AWS Region."""
    if region:
        return boto3.client("glue", region_name=region)
    return boto3.client("glue")


### Parquet Reading Functions
def read_parquet(glue_context: GlueContext, folder_uri: str) -> DataFrame:
    """Read all Parquet data from a folder.

    Args:
        glue_context: Glue context used to read the data.
        folder_uri: S3 folder containing the Parquet data.
    """
    _parse_s3_uri(folder_uri)
    print(f"Reading Parquet data from {folder_uri}")
    try:
        dataframe = glue_context.spark_session.read.format("parquet").load(folder_uri)
        return dataframe
    except Exception as error:
        raise RuntimeError(
            f"Failed to read Parquet data from {folder_uri}"
        ) from error


def _validate_partitions(partitions: list[dict[str, object]]) -> None:
    """Validate partition dictionaries."""
    if not isinstance(partitions, list) or not partitions:
        raise ValueError("partitions must be a non-empty list")

    for partition in partitions:
        if not isinstance(partition, dict) or not partition:
            raise ValueError("each partition must be a non-empty dictionary")


def _validate_partition_values(
    partition_by: list[str], partition_values: dict[str, object]
) -> None:
    """Validate partition values for configured keys."""
    if not isinstance(partition_by, list) or not partition_by:
        raise ValueError("partition_by must be a non-empty list")
    if not isinstance(partition_values, dict):
        raise ValueError("partition values must be a dictionary")
    if set(partition_by) != set(partition_values):
        raise ValueError("partition values must match partition_by")

    if "year" in partition_by:
        year = str(partition_values["year"])
        if len(year) != 4 or not year.isdigit():
            raise ValueError("year partition values must be four-digit values")

    if "month" in partition_by:
        month = str(partition_values["month"])
        if not month.isdigit() or not 1 <= int(month) <= 12:
            raise ValueError("month partition values must be between 1 and 12")


def _build_partitioned_folder_paths(
    base_path: str,
    partition_by: list[str],
    partitions: list[dict[str, object]],
) -> list[str]:
    """Build S3 paths from partition values and key order."""
    _validate_partitions(partitions)
    if not partition_by:
        raise ValueError("partition_by is required")

    folder_paths = []
    for partition in partitions:
        _validate_partition_values(partition_by, partition)
        path = base_path.rstrip("/")
        for column in partition_by:
            value = str(partition[column])
            if column == "month":
                value = value.zfill(2)
            path += f"/{column}={value}"
        folder_paths.append(path)
    return folder_paths


def read_partitioned_parquets(
    glue_context: GlueContext,
    folder_uri: str,
    partitions: list[dict[str, object]],
    partition_by: list[str],
) -> DataFrame:
    """Read Parquet data from specific year/month partitions.

    Args:
        glue_context: Glue context used to read the data.
        folder_uri: S3 folder containing the Parquet data.
        partitions: Partition dictionaries specifying the values to read.
        partition_by: Ordered partition column names.
    """
    _parse_s3_uri(folder_uri)
    base_path = folder_uri.rstrip("/")
    folder_paths = _build_partitioned_folder_paths(
        base_path, partition_by, partitions
    )
    print(
        f"Reading Parquet partitions {partitions} from {folder_uri}: "
        f"{folder_paths}"
    )
    try:
        dataframe = (
            glue_context.spark_session.read
            .option("basePath", base_path)
            .format("parquet")
            .load(folder_paths)
        )
        return dataframe
    except Exception as error:
        raise RuntimeError(
            f"Failed to read Parquet data from partitions in {folder_uri}"
        ) from error


### Parquet Writing Functions
def _prepare_partition_columns(dataframe: DataFrame, partition_by: list[str]) -> DataFrame:
    """Validate partition columns and normalize year/month values."""
    if not isinstance(partition_by, list) or not all(
        isinstance(column, str) and column for column in partition_by
    ):
        raise ValueError("partition_by must be a list of strings")

    if not partition_by:
        raise ValueError("partition_by must not be empty")

    missing_columns = set(partition_by) - set(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "Partition columns not found in DataFrame: "
            + ", ".join(sorted(missing_columns))
        )

    if "year" in partition_by:
        invalid_years = dataframe.filter(
            col("year").isNull()
            | ~col("year").cast("string").rlike(r"^\d{4}$")
        ).limit(1).count()

        if invalid_years:
            raise ValueError("year partition values must be four-digit values")

    if "month" in partition_by:
        invalid_months = dataframe.filter(
            col("month").isNull()
            | ~col("month").cast("string").rlike(r"^(0?[1-9]|1[0-2])$")
        ).limit(1).count()

        if invalid_months:
            raise ValueError("month partition values must be between 1 and 12")

        dataframe = dataframe.withColumn(
            "month",
            lpad(col("month").cast("string"), 2, "0"),
        )
    return dataframe


def write_parquet(dataframe: DataFrame, target_uri: str, mode: str) -> None:
    """Write a DataFrame to S3 as Parquet.

    Args:
        dataframe: DataFrame to write.
        target_uri: S3 destination folder.
        mode: Write mode, such as append or overwrite.
    """
    print(f"Writing Parquet data to {target_uri} with mode={mode}")
    try:
        dataframe.write.mode(mode).format("parquet").save(target_uri)
    except Exception as error:
        raise RuntimeError(
            f"Failed to write Parquet data to {target_uri}"
        ) from error

    
def write_partitioned_parquets(
    dataframe: DataFrame,
    target_uri: str,
    mode: str,
    partition_by: list[str],
) -> None:
    """Write a DataFrame to S3 as Parquet, partitioned by columns.

    Args:
        dataframe: DataFrame to write.
        target_uri: S3 destination folder.
        mode: Write mode, such as append or overwrite.
        partition_by: List of column names to partition by.
    """
    dataframe = _prepare_partition_columns(dataframe, partition_by)
    print(
        f"Writing partitioned Parquet data to {target_uri} "
        f"with mode={mode}, partitions={partition_by}"
    )
    try:
        dataframe.write.mode(mode).partitionBy(*partition_by).format("parquet").save(
            target_uri
        )
    except Exception as error:
        raise RuntimeError(
            f"Failed to write partitioned Parquet data to {target_uri}"
        ) from error





def register_catalog_table(
    dataframe,
    target_uri: str,
    database: str,
    table_name: str,
    partition_by: list[str],
    region: str = None,
) -> None:
    """Create or update a Glue table definition."""
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

    glue_client = _get_glue_client(region)
    try:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)


def register_catalog_partition(
    database: str,
    table_name: str,
    target_uri: str,
    dataframe,
    partition_by: list[str],
    partition_values: dict[str, object],
    region: str = None,
) -> None:
    """Create or update one partition in Glue Catalog."""
    _validate_partition_values(partition_by, partition_values)

    partition_uri = target_uri.rstrip("/")
    values = []
    for column in partition_by:
        value = str(partition_values[column])
        if column == "month":
            value = value.zfill(2)
        partition_uri += f"/{column}={value}"
        values.append(value)

    columns = [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in dataframe.schema.fields
        if field.name not in partition_by
    ]
    partition = {
        "Values": values,
        "Parameters": {"classification": "parquet"},
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

    glue_client = _get_glue_client(region)
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


def register_catalog_partitions(
    database: str,
    table_name: str,
    target_uri: str,
    dataframe: DataFrame,
    partition_by: list[str],
    region: str = None,
) -> None:
    """Register all distinct DataFrame partitions in Glue Catalog."""
    if not partition_by:
        raise ValueError("partition_by is required to register partitions")

    partitions = [
        {column: row[column] for column in partition_by}
        for row in dataframe.select(*partition_by).distinct().collect()
    ]
    for partition_values in sorted(
        partitions,
        key=lambda values: tuple(str(values[column]) for column in partition_by),
    ):
        register_catalog_partition(
            database,
            table_name,
            target_uri,
            dataframe,
            partition_by,
            partition_values,
            region,
        )


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "CONFIG_FILE", "SOURCE_LAYER", "TARGET_LAYER"],
    )
    config_uri = args["CONFIG_FILE"]
    source_layer = args["SOURCE_LAYER"]
    target_layer = args["TARGET_LAYER"]
    _parse_s3_uri(config_uri)
    valid_layers = {"bronze", "silver", "gold"}
    if source_layer not in valid_layers:
        raise ValueError("SOURCE_LAYER must be bronze, silver, or gold")
    if target_layer not in valid_layers:
        raise ValueError("TARGET_LAYER must be bronze, silver, or gold")

    print(f"Loading configuration from {config_uri}")
    config = load_s3_config(config_uri)
    catalog_region = config.get("region")
    print(f"Glue Catalog region: {catalog_region or 'job default'}")

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    target_config = config[target_layer]
    source_uri = config[source_layer]["s3_uri"]
    print(f"Source URI: {source_uri}")
    df = read_parquet(
        glue_context, source_uri
    )
    print(f"Total rows: {df.count()}")
    target_uri = target_config["s3_uri"]
    print(f"Target URI: {target_uri}")
    partition_by = target_config.get("partition_by")
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    if partition_by:
        write_partitioned_parquets(
            df, target_uri, target_config["write_mode"], partition_by
        )
    else:
        write_parquet(df, target_uri, target_config["write_mode"])

    catalog_database = target_config.get("database")
    catalog_table = target_config.get("table")
    if catalog_database and catalog_table:
        catalog_partition_by = partition_by or []
        register_catalog_table(
            df,
            target_uri,
            catalog_database,
            catalog_table,
            catalog_partition_by,
            catalog_region,
        )
        if catalog_partition_by:
            register_catalog_partitions(
                catalog_database,
                catalog_table,
                target_uri,
                df,
                catalog_partition_by,
                catalog_region,
            )
    job.commit()


if __name__ == "__main__":
    main()
