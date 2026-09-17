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
from store_to_s3 import write_partitioned_parquets


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
	"""Return the bucket and key from an S3 URI."""
	if not s3_uri.startswith("s3://"):
		raise ValueError("S3 URI must start with s3://")

	bucket, separator, key = s3_uri.removeprefix("s3://").partition("/")
	if not bucket or not separator or not key:
		raise ValueError("S3 URI must include both a bucket and object key")
	return bucket, key

class Ingestion:
	@staticmethod
	def read_watermark_value() -> str:
		"""Return a fixed watermark value for testing."""
		return "2026-01-01T00:00:00"

	def _add_partition_columns(self, dataframe: DataFrame, derived_from: str) -> DataFrame:
		"""Derive year and month partition columns from the specified timestamp column."""
		fields = {field.name: field for field in dataframe.schema.fields}
		derived_from_field = fields.get(derived_from)
		if derived_from_field is None:
			raise ValueError(f"Source view must contain {derived_from}")
		if derived_from_field.dataType.simpleString() not in {
			"date",
			"timestamp",
			"timestamp_ntz",
		}:
			raise ValueError(
				f"{derived_from} must be date, timestamp, or timestamp_ntz"
			)

		if dataframe.filter(col(derived_from).isNull()).limit(1).count():
			raise ValueError(f"{derived_from} contains null values")

		return (
			dataframe.withColumn("year", year(col(derived_from)))
			.withColumn("month", month(col(derived_from)))
		)


def main() -> None:
	"""Validate and ingest a JDBC query into partitioned Parquet on S3."""
	args = getResolvedOptions(sys.argv, ["JOB_NAME"])
	target_s3_uri = 's3://your-bucket/your-prefix'
	schema_uri = 's3://your-bucket/your-schema-file'
	write_mode = 'append'

	_parse_s3_uri(target_s3_uri)

	glue_context = GlueContext(SparkContext.getOrCreate())
	job = Job(glue_context)
	job.init(args["JOB_NAME"], args)

	ingestion = Ingestion()
	watermark_value = ingestion.read_watermark_value()
	logger.info("Loaded watermark value: %s", watermark_value)

	schema_registry = SchemaRegistry(schema_uri)
	host = 'host'
	database_name = 'database'
	user = 'user'
	password = 'password'
	query = 'SELECT * FROM dbo.events_view'
	mssql_connector = MSSQLConnector(
		host=host,
		database_name=database_name,
		user=user,
		password=password,
	)
	source_df = mssql_connector.read_query(
		glue_context.spark_session,
		query,
	)
	if not schema_registry.is_match(source_df):
		raise ValueError("Source schema does not match the configured definition")

	partitioned_df = ingestion._add_partition_columns(source_df, derived_from="event_timestamp")
	row_count = partitioned_df.count()
	if row_count == 0:
		logger.info("Source query returned no rows; nothing to write")
		job.commit()
		return
	
	write_partitioned_parquets(
		partitioned_df,
		target_s3_uri,
		write_mode=write_mode,
		partition_by=["year", "month"],
	)
	logger.info("Ingestion completed successfully")
	job.commit()


if __name__ == "__main__":
	main()
