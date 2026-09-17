#!/usr/bin/env python3
import logging
import os
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, year

from mssql_connector import MSSQLConnector
from schema_registry import SchemaRegistry


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def _required_environment(name: str) -> str:
	"""Return a required non-empty environment variable."""
	value = os.getenv(name)
	if not value:
		raise ValueError(f"Required environment variable is missing: {name}")
	return value


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
	"""Return the bucket and key from an S3 URI."""
	if not s3_uri.startswith("s3://"):
		raise ValueError("S3 URI must start with s3://")

	bucket, separator, key = s3_uri.removeprefix("s3://").partition("/")
	if not bucket or not separator or not key:
		raise ValueError("S3 URI must include both a bucket and object key")
	return bucket, key


def _add_partition_columns(dataframe: DataFrame) -> DataFrame:
	"""Derive year and month partition columns from event_timestamp."""
	fields = {field.name: field for field in dataframe.schema.fields}
	timestamp_field = fields.get("event_timestamp")
	if timestamp_field is None:
		raise ValueError("Source view must contain event_timestamp")
	if timestamp_field.dataType.simpleString() not in {"timestamp", "timestamp_ntz"}:
		raise ValueError("event_timestamp must be timestamp or timestamp_ntz")

	if dataframe.filter(col("event_timestamp").isNull()).limit(1).count():
		raise ValueError("event_timestamp contains null values")

	return (
		dataframe.withColumn("year", year(col("event_timestamp")))
		.withColumn("month", month(col("event_timestamp")))
	)


def main() -> None:
	"""Validate and ingest a JDBC view into partitioned Parquet on S3."""
	args = getResolvedOptions(sys.argv, ["JOB_NAME"])
	mssql_config_uri = _required_environment("MSSQL_CONFIG_URI")
	target_s3_uri = _required_environment("TARGET_S3_URI")
	schema_uri = _required_environment("EXPECTED_SCHEMA_URI")
	write_mode = os.getenv("WRITE_MODE", "append").lower()

	_parse_s3_uri(target_s3_uri)

	glue_context = GlueContext(SparkContext.getOrCreate())
	job = Job(glue_context)
	job.init(args["JOB_NAME"], args)

	schema_registry = SchemaRegistry(schema_uri)
	mssql_connector, view_name = MSSQLConnector.from_yaml(mssql_config_uri)
	source_df = mssql_connector.read_view(
		glue_context.spark_session,
		view_name,
	)
	source_schema = source_df.schema
	logger.info("Source view schema:\n%s", source_schema.treeString())
	if not schema_registry.is_match(source_df):
		raise ValueError("Source schema does not match the configured definition")

	partitioned_df = _add_partition_columns(source_df)
	row_count = partitioned_df.count()
	if row_count == 0:
		logger.info("Source view %s returned no rows; nothing to write", view_name)
		job.commit()
		return

	logger.info(
		"Writing %s rows to %s partitioned by year and month",
		row_count,
		target_s3_uri,
	)
	(
		partitioned_df.write.mode(write_mode)
		.format("parquet")
		.partitionBy("year", "month")
		.save(target_s3_uri)
	)
	logger.info("Ingestion completed successfully")
	job.commit()


if __name__ == "__main__":
	main()
