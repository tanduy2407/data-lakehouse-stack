#!/usr/bin/env python3
import logging
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lpad
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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



### Parquet Reading Functions
def read_parquet(glue_context: GlueContext, folder_uri: str) -> DataFrame:
    """Read all Parquet data from a folder.

    Args:
        glue_context: Glue context used to read the data.
        folder_uri: S3 folder containing the Parquet data.
    """
    _parse_s3_uri(folder_uri)
    logger.info("Reading Parquet data from %s", folder_uri)
    try:
        dataframe = glue_context.spark_session.read.format("parquet").load(folder_uri)
        return dataframe
    except Exception as error:
        raise RuntimeError(
            f"Failed to read Parquet data from {folder_uri}"
        ) from error


def _validate_partitions(partitions: list[dict[str, object]]) -> None:
    """Validate the partition dictionary list structure."""
    if not isinstance(partitions, list) or not partitions:
        raise ValueError("partitions must be a non-empty list")

    for partition in partitions:
        if not isinstance(partition, dict) or not partition:
            raise ValueError("each partition must be a non-empty dictionary")


def _validate_partition_values(
    partition_by: list[str], partition_values: dict[str, object]
) -> None:
    """Validate one partition against its configured keys."""
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
    """Build S3 paths from ordered partition keys and values."""
    _validate_partitions(partitions)
    if not partition_by:
        raise ValueError("partition_by is required")

    folder_paths = []
    for partition in partitions:
        _validate_partition_values(partition_by, partition)
        path = base_path.rstrip("/")
        for column in partition_by:
            value = _normalize_partition_value(column, partition[column])
            path += f"/{column}={value}"
        folder_paths.append(path)
    return folder_paths


def read_partitioned_parquets(
    glue_context: GlueContext,
    folder_uri: str,
    partitions: list[dict[str, object]],
    partition_by: list[str],
) -> DataFrame:
    """Read Parquet data from specific configured partitions.

    Args:
        glue_context: Glue context used to read the data.
        folder_uri: S3 folder containing the Parquet data.
        partitions: Partition dictionaries specifying values to read.
        partition_by: Ordered partition column names used in each path.
    """
    _parse_s3_uri(folder_uri)
    base_path = folder_uri.rstrip("/")
    folder_paths = _build_partitioned_folder_paths(
        base_path, partition_by, partitions
    )
    logger.info(
        "Reading Parquet partitions from %s with keys=%s, values=%s: %s",
        folder_uri, partition_by, partitions, folder_paths,
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
    """Validate partition columns and normalize values."""
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
    logger.info("Writing Parquet data to %s with mode=%s", target_uri, mode)
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
    logger.info(
        "Writing partitioned Parquet data to %s with mode=%s, partitions=%s",
        target_uri, mode, partition_by,
    )
    try:
        dataframe.write.mode(mode).partitionBy(*partition_by).format("parquet").save(
            target_uri
        )
    except Exception as error:
        raise RuntimeError(
            f"Failed to write partitioned Parquet data to {target_uri}"
        ) from error




### Glue Catalog Functions
class GlueCatalogManager:
    """Manage Glue Catalog tables and partitions with one client."""

    def __init__(self, region: str = None):
        self.glue_client = boto3.client("glue", region_name=region)
        self.s3_client = boto3.client("s3", region_name=region)

    @staticmethod
    def _map_pyspark_type_to_glue(pyspark_type_str: str) -> str:
        """Map PySpark types that require a Glue Catalog type name."""
        type_mapping = {
            "timestamp_ntz": "timestamp",
            "byte": "tinyint",
            "short": "smallint",
            "long": "bigint",
        }
        normalized = pyspark_type_str.lower()
        return type_mapping.get(normalized, pyspark_type_str)

    @staticmethod
    def _normalize_partition_value(column: str, value: object) -> str:
        """Return the canonical string form of a partition value."""
        normalized_value = str(value)
        if column == "month":
            return normalized_value.zfill(2)
        return normalized_value

    def _database_exists(self, database_name: str) -> bool:
        """Return whether the Glue database exists."""
        try:
            self.glue_client.get_database(Name=database_name)
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.error("Glue database does not exist: %s", database_name)
            return False

    def _table_exists(self, database_name: str, table_name: str) -> bool:
        """Return whether the Glue table exists in the specified database."""
        try:
            self.glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name,
            )
            return True
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.error(
                "Glue table does not exist: %s.%s",
                database_name,
                table_name,
            )
            return False

    def _s3_path_exists(self, s3_uri: str) -> bool:
        """Return whether an S3 path contains at least one object."""
        bucket, prefix = _parse_s3_uri(s3_uri)
        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix.rstrip("/") + "/",
            MaxKeys=1,
        )
        return bool(response.get("Contents"))

    def register_table(
        self,
        dataframe,
        target_uri: str,
        database_name: str,
        table_name: str,
        partition_by: list[str],
    ) -> None:
        """Create or update a Glue table definition."""
        if not self._database_exists(database_name):
            return
        fields = {field.name: field for field in dataframe.schema.fields}
        missing_partitions = set(partition_by) - fields.keys()
        if missing_partitions:
            raise ValueError(
                "Partition columns not found in DataFrame: "
                + ", ".join(sorted(missing_partitions))
            )

        columns = [
            {
                "Name": field.name,
                "Type": self._map_pyspark_type_to_glue(
                    field.dataType.simpleString()
                ),
            }
            for field in dataframe.schema.fields
            if field.name not in partition_by
        ]
        partition_keys = [
            {
                "Name": name,
                "Type": self._map_pyspark_type_to_glue(
                    fields[name].dataType.simpleString()
                ),
            }
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
        try:
            self.glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input,
            )
            logger.info("Updated Glue table: %s.%s", database_name, table_name)
        except self.glue_client.exceptions.EntityNotFoundException:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input,
            )
            logger.info("Created Glue table: %s.%s", database_name, table_name)

    def _register_partition(
        self,
        database_name: str,
        table_name: str,
        target_uri: str,
        dataframe,
        partition_by: list[str],
        partition_values: dict[str, object],
    ) -> None:
        """Create or update one partition."""
        partition_uri = target_uri.rstrip("/")
        for column in partition_by:
            value = GlueCatalogManager._normalize_partition_value(
                column,
                partition_values[column],
            )
            partition_uri += f"/{column}={value}"

        if not self._s3_path_exists(partition_uri):
            logger.info(
                "Skipping Glue partition registration because the S3 "
                "path is missing or empty: %s", partition_uri,
            )
            return

        _validate_partition_values(partition_by, partition_values)

        values = []
        for column in partition_by:
            value = GlueCatalogManager._normalize_partition_value(
                column,
                partition_values[column],
            )
            values.append(value)

        columns = [
            {
                "Name": field.name,
                "Type": self._map_pyspark_type_to_glue(
                    field.dataType.simpleString()
                ),
            }
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
        try:
            self.glue_client.update_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionValueList=partition["Values"],
                PartitionInput=partition,
            )
            logger.info("Updated Glue partition: %s", partition_uri)
        except self.glue_client.exceptions.EntityNotFoundException:
            self.glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput=partition,
            )
            logger.info("Created Glue partition: %s", partition_uri)

    def register_partitions(
        self,
        database_name: str,
        table_name: str,
        target_uri: str,
        dataframe: DataFrame,
        partition_by: list[str],
    ) -> None:
        """Register all distinct DataFrame partitions in Glue Catalog."""
        if not partition_by:
            raise ValueError("partition_by is required to register partitions")

        if not self._database_exists(database_name):
            return
        if not self._table_exists(database_name, table_name):
            return
        partitions = [
            {column: row[column] for column in partition_by}
            for row in dataframe.select(*partition_by).distinct().collect()
        ]
        for partition_values in sorted(
            partitions,
            key=lambda values: tuple(
                str(values[column]) for column in partition_by
            ),
        ):
            self._register_partition(
                database_name,
                table_name,
                target_uri,
                dataframe,
                partition_by,
                partition_values,
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

    logger.info("Loading configuration from %s", config_uri)
    config = load_s3_config(config_uri)
    catalog_region = config.get("region")
    logger.info("Glue Catalog region: %s", catalog_region or "job default")

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    target_config = config[target_layer]
    source_uri = config[source_layer]["s3_uri"]
    logger.info("Source URI: %s", source_uri)
    df = read_parquet(
        glue_context, source_uri
    )
    logger.info("Total rows: %s", df.count())
    target_uri = target_config["s3_uri"]
    logger.info("Target URI: %s", target_uri)
    partition_by = ["year", "month"]
    # if partition_by:
    #     write_partitioned_parquets(
    #         df, target_uri, target_config["write_mode"], partition_by
    #     )
    # else:
    #     write_parquet(df, target_uri, target_config["write_mode"])

    catalog_database_name = "test_write_cpar"
    catalog_table = "silver_test"
    if catalog_database_name and catalog_table:
        catalog_partition_by = partition_by
        catalog_manager = GlueCatalogManager(catalog_region)
        catalog_manager.register_table(
            df,
            target_uri,
            catalog_database_name,
            catalog_table,
            catalog_partition_by,
        )
        if catalog_partition_by:
            catalog_manager.register_partitions(
                catalog_database_name,
                catalog_table,
                target_uri,
                df,
                catalog_partition_by,
            )
    job.commit()


if __name__ == "__main__":
    main()
