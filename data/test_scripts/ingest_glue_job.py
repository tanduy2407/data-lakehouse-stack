#!/usr/bin/env python3
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofmonth, month, year, max as spark_max

from mssql_connector import MSSQLConnector
from schema_registry import SchemaRegistry
from store_to_s3 import write_parquet, write_partitioned_parquets


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Ingestion:
	@staticmethod
	def read_watermark() -> str | None:
		"""Return the configured watermark for incremental loads, 
		or None for a full load."""
		return "2026-01-01T00:00:00"

	@staticmethod
	def calculate_new_watermark(dataframe: DataFrame, timestamp_column: str):
		"""Return the greatest timestamp value from the ingested data."""
		if not isinstance(timestamp_column, str) or not timestamp_column.strip():
			raise ValueError("Timestamp column must be a non-empty string")
		if timestamp_column not in dataframe.columns:
			raise ValueError(f"DataFrame must contain {timestamp_column}")
		new_watermark = dataframe.agg(
			spark_max(col(timestamp_column)).alias("new_watermark")
		).first()["new_watermark"]
		return new_watermark

	@staticmethod
	def process_fail_fast(
		dataframe: DataFrame,
		schema_registry: SchemaRegistry,
		error_s3_uri: str,
	) -> None:
		"""Validate schema, save mismatched data, and fail fast on invalid data."""
		if not isinstance(error_s3_uri, str) or not error_s3_uri.strip():
			raise ValueError("Error S3 URI must be a non-empty string")

		if not schema_registry.is_match(dataframe):
			logger.error("Schema mismatch; writing rejected data to %s", error_s3_uri)
			write_parquet(dataframe, error_s3_uri, mode="append")
			raise ValueError("Source schema does not match the configured definition")
		logger.info("Source schema validated")

	@staticmethod
	def apply_watermark_filter(query: str, watermark_value: str | None, 
	                        timestamp_column: str) -> str:
		"""Apply a watermark filter, or return the query unchanged for a full load."""
		if not isinstance(query, str) or not query.strip():
			raise ValueError("Query must be a non-empty string")
		if watermark_value is not None and (not isinstance(watermark_value, str) or not watermark_value.strip()):
			raise ValueError("Watermark value must be a non-empty string")
		if not isinstance(timestamp_column, str) or not timestamp_column.strip():
			raise ValueError("Timestamp column must be a non-empty string")
		normalized_query = query.strip().rstrip(";").strip()
		if watermark_value is None:
			logger.info("No watermark found; running full load")
			return normalized_query
		# Add WHERE clause for incremental load
		filtered_query = f"{normalized_query} WHERE {timestamp_column} > '{watermark_value}'"
		logger.info(f"Applied watermark filter: {timestamp_column} > {watermark_value}")
		return filtered_query

	def add_partition_columns(
		self,
		dataframe: DataFrame,
		derived_from: str,
		partition_keys: tuple[str],
	) -> DataFrame:
		"""Derive requested date partition columns from a date or timestamp column."""
		if not partition_keys or any(
			key not in {"year", "month", "day"} for key in partition_keys
		):
			raise ValueError("partition_keys must contain only year, month, or day")
		if len(set(partition_keys)) != len(partition_keys):
			raise ValueError("partition_keys must not contain duplicates")

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

		partition_expressions = {
			"year": year(col(derived_from)),
			"month": month(col(derived_from)),
			"day": dayofmonth(col(derived_from)),
		}
		for key in partition_keys:
			dataframe = dataframe.withColumn(key, partition_expressions[key])
		return dataframe


def main() -> None:
	"""Validate and ingest a JDBC query into partitioned Parquet on S3."""
	args = getResolvedOptions(sys.argv, ["JOB_NAME"])
	target_s3_uri = 's3://your-bucket/your-prefix'
	error_s3_uri = 's3://your-bucket/error-prefix'
	schema_uri = 's3://your-bucket/your-schema-file'
	write_mode = 'append'

	glue_context = GlueContext(SparkContext.getOrCreate())
	job = Job(glue_context)
	job.init(args["JOB_NAME"], args)

	ingestion = Ingestion()
	watermark_value = ingestion.read_watermark()
	logger.info("Loaded watermark value: %s", watermark_value)

	schema_registry = SchemaRegistry(schema_uri)
	host = 'host'
	database_name = 'database'
	user = 'user'
	password = 'password'
	schema_name = 'dbo'
	table_name = 'events_view'
	timestamp_column = 'event_timestamp'
	
	mssql_connector = MSSQLConnector(
		host=host,
		database=database_name,
		user=user,
		password=password,
	)
	
	# Apply watermark filter for incremental load
	base_query = f'SELECT * FROM dbo.{table_name}'
	filtered_query = ingestion.apply_watermark_filter(
		base_query, 
		watermark_value, 
		timestamp_column
	)
	
	# Read data from source with retry logic
	source_df = mssql_connector.execute_query(
		glue_context.spark_session,
		filtered_query,
	)
	logger.info("Data read from SQL Server successfully")
	
	# Validate schema matches contract
	ingestion.process_fail_fast(
		source_df,
		schema_registry,
		error_s3_uri,
	)

	# Add partition columns
	partition_keys = ("year", "month")
	partitioned_df = ingestion.add_partition_columns(
		source_df,
		derived_from=timestamp_column,
		partition_keys=partition_keys,
	)
	row_count = partitioned_df.count()
	if row_count == 0:
		logger.info("Source query returned no rows; nothing to write")
		job.commit()
		return
	
	# Write to S3
	write_partitioned_parquets(
		partitioned_df,
		target_s3_uri,
		mode=write_mode,
		partition_by=list(partition_keys),
	)
	logger.info(f"Successfully ingested {row_count} rows")
	
	# Log new watermark (for reference, not persisted yet)
	new_watermark = ingestion.calculate_new_watermark(
		partitioned_df,
		timestamp_column,
	)
	logger.info(f"Next watermark should be: {new_watermark}")
	
	logger.info("Ingestion completed successfully")
	job.commit()


if __name__ == "__main__":
	main()
