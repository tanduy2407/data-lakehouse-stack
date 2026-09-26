#!/usr/bin/env python3
import logging
import json
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


class S3Client:
    """Provide S3 URI, listing, loading, and upload helpers."""

    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """Parse an S3 object/file URI and return its bucket and object key."""
        if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
            raise ValueError("S3 URI must start with s3://")

        bucket, separator, key = s3_uri.removeprefix("s3://").partition("/")
        if not bucket or not separator or not key:
            raise ValueError("S3 URI must include both a bucket and object key")
        return bucket, key

    def _parse_s3_prefix(self, s3_uri: str) -> tuple[str, str]:
        """Parse an S3 prefix URI and return its bucket and normalized key prefix."""
        bucket, prefix = self._parse_s3_uri(s3_uri)
        return bucket, prefix.rstrip("/") + "/"

    def read_files(self, s3_prefix: str, extension: str | None = None) -> list[str]:
        """Read object URIs under an S3 prefix, optionally filtered by extension."""
        if extension is not None and (not isinstance(extension, str) or not extension):
            raise ValueError("extension must be a non-empty string")

        bucket, prefix = self._parse_s3_prefix(s3_prefix)
        files = []
        paginator = boto3.client("s3").get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for object_info in page.get("Contents", []):
                key = object_info.get("Key", "")
                if key and (extension is None or key.endswith(extension)):
                    files.append(f"s3://{bucket}/{key}")
        logger.info("Found %d objects under S3 prefix: %s", len(files), s3_prefix)
        return files

    def path_exists(self, s3_prefix: str) -> bool:
        """Return whether an S3 prefix contains at least one object."""
        bucket, prefix = self._parse_s3_prefix(s3_prefix)
        response = boto3.client("s3").list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        return bool(response.get("Contents"))

    def upload_json(self, s3_prefix: str, filename: str, payload: dict) -> str:
        """Upload a JSON object under an S3 prefix and return its URI."""
        if not isinstance(filename, str) or not filename or "/" in filename:
            raise ValueError("filename must be a non-empty file name")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dictionary")

        bucket, prefix = self._parse_s3_prefix(s3_prefix)
        key = f"{prefix}{filename}"
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        schema_uri = f"s3://{bucket}/{key}"
        logger.info("Uploaded JSON object to %s", schema_uri)
        return schema_uri

    def load_file(self, s3_uri: str) -> str:
        """Load raw file contents from S3."""
        bucket, key = self._parse_s3_uri(s3_uri)
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def load_json(self, s3_uri: str) -> dict:
        """Load and parse JSON from an S3 object."""
        try:
            return json.loads(self.load_file(s3_uri))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSON in S3 file: {s3_uri}") from error

    def load_config(self, config_uri: str) -> dict:
        """Load and validate YAML configuration from S3."""
        config = yaml.safe_load(self.load_file(config_uri))
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a YAML mapping")
        for layer in ("bronze", "silver", "gold"):
            if layer not in config or not isinstance(config[layer], dict):
                raise ValueError(f"Missing or invalid {layer} configuration")
            self._parse_s3_uri(config[layer].get("s3_uri"))
        return config

    def read_parquet(self, glue_context: GlueContext, folder_uri: str) -> DataFrame:
        """Read a Parquet dataset from S3."""
        self._parse_s3_uri(folder_uri)
        return glue_context.spark_session.read.format("parquet").load(folder_uri)

    def read_partitioned_parquets(
        self,
        glue_context: GlueContext,
        folder_uri: str,
        partitions: list[dict[str, object]],
        partition_by: list[str],
    ) -> DataFrame:
        """Read configured Parquet partitions from S3."""
        self._parse_s3_uri(folder_uri)
        base_path = folder_uri.rstrip("/")
        folder_paths = self._build_partitioned_folder_paths(
            base_path, partition_by, partitions
        )
        return (
            glue_context.spark_session.read
            .option("basePath", base_path)
            .format("parquet")
            .load(folder_paths)
        )

    def write_parquet(self, dataframe: DataFrame, target_uri: str, mode: str) -> None:
        """Write a DataFrame to an S3 Parquet dataset."""
        self._parse_s3_uri(target_uri)
        dataframe.write.mode(mode).format("parquet").save(target_uri)

    def write_partitioned_parquets(
        self,
        dataframe: DataFrame,
        target_uri: str,
        mode: str,
        partition_by: list[str],
    ) -> None:
        """Write a partitioned DataFrame to S3 as Parquet."""
        dataframe = self._prepare_partition_columns(dataframe, partition_by)
        dataframe.write.mode(mode).partitionBy(*partition_by).format("parquet").save(
            target_uri
        )

    @staticmethod
    def _prepare_partition_columns(dataframe: DataFrame, partition_by: list[str]) -> DataFrame:
        """Validate and normalize partition columns."""
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

        if "day" in partition_by:
            invalid_days = dataframe.filter(
                col("day").isNull()
                | ~col("day").cast("string").rlike(r"^(0?[1-9]|[12][0-9]|3[01])$")
            ).limit(1).count()
            if invalid_days:
                raise ValueError("day partition values must be between 1 and 31")
            dataframe = dataframe.withColumn(
                "day",
                lpad(col("day").cast("string"), 2, "0"),
            )
        return dataframe

    @staticmethod
    def _normalize_partition_value(column: str, value: object) -> str:
        """Return the canonical string form of a partition value."""
        normalized_value = str(value)
        return normalized_value.zfill(2) if column == "month" else normalized_value

    @staticmethod
    def _build_partitioned_folder_paths(
        base_path: str,
        partition_by: list[str],
        partitions: list[dict[str, object]],
    ) -> list[str]:
        """Build S3 paths from ordered partition keys and values."""
        S3Client._validate_partitions(partitions)
        if not partition_by:
            raise ValueError("partition_by is required")

        folder_paths = []
        for partition in partitions:
            S3Client._validate_partition_values(partition_by, partition)
            path = base_path.rstrip("/")
            for column in partition_by:
                value = S3Client._normalize_partition_value(
                    column, partition[column]
                )
                path += f"/{column}={value}"
            folder_paths.append(path)
        return folder_paths

    @staticmethod
    def _validate_partitions(partitions: list[dict[str, object]]) -> None:
        """Validate the configured partition dictionaries."""
        if not isinstance(partitions, list) or not partitions:
            raise ValueError("partitions must be a non-empty list")
        for partition in partitions:
            if not isinstance(partition, dict) or not partition:
                raise ValueError("each partition must be a non-empty dictionary")

    @staticmethod
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
        if "day" in partition_by:
            day = str(partition_values["day"])
            if not day.isdigit() or not 1 <= int(day) <= 31:
                raise ValueError("day partition values must be between 1 and 31")


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
        # Build the canonical URI once; use it for both the S3 check and Glue metadata.
        partition_uri = target_uri.rstrip("/")
        for column in partition_by:
            value = GlueCatalogManager._normalize_partition_value(
                column,
                partition_values[column],
            )
            partition_uri += f"/{column}={value}"

        # Glue should only contain partitions backed by data already present in S3.
        if not self.s3_client.path_exists(partition_uri):
            logger.info(
                "Skipping Glue partition registration because the S3 "
                "path is missing or empty: %s", partition_uri,
            )
            return

        S3Client._validate_partition_values(partition_by, partition_values)

        # Glue expects partition values in the same order as PartitionKeys.
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

        # Distinct combinations avoid duplicate Glue calls when rows share a partition.
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
    s3_client = S3Client()
    valid_layers = {"bronze", "silver", "gold"}
    if source_layer not in valid_layers:
        raise ValueError("SOURCE_LAYER must be bronze, silver, or gold")
    if target_layer not in valid_layers:
        raise ValueError("TARGET_LAYER must be bronze, silver, or gold")

    logger.info("Loading configuration from %s", config_uri)
    config = s3_client.load_config(config_uri)
    catalog_region = config.get("region")
    logger.info("Glue Catalog region: %s", catalog_region or "job default")

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    target_config = config[target_layer]
    source_uri = config[source_layer]["s3_uri"]
    logger.info("Source URI: %s", source_uri)
    df = s3_client.read_parquet(
        glue_context, source_uri
    )
    logger.info("Total rows: %s", df.count())
    target_uri = target_config["s3_uri"]
    logger.info("Target URI: %s", target_uri)
    partition_by = ["year", "month"]
    # if partition_by:
    #     _s3_client.write_partitioned_parquets(
    #         df, target_uri, target_config["write_mode"], partition_by
    #     )
    # else:
    #     _s3_client.write_parquet(df, target_uri, target_config["write_mode"])

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
