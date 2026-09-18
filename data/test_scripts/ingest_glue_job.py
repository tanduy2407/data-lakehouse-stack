#!/usr/bin/env python3
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, year, max as spark_max

from mssql_connector import MSSQLConnector
from schema_registry import SchemaRegistry
from store_to_s3 import write_partitioned_parquets, _parse_s3_uri


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Ingestion:
	@staticmethod
	def read_watermark_value() -> str:
		"""Return a fixed watermark value for testing.
		
		Returns:
			Watermark timestamp string (ISO 8601 format).
		"""
		return "2026-01-01T00:00:00"

	@staticmethod
	def apply_watermark_filter(query: str, watermark_value: str, 
	                            timestamp_column: str) -> str:
		"""Apply watermark filter to incremental load query.
		
		Args:
			query: Base SELECT query.
			watermark_value: Watermark timestamp (ISO 8601 format).
			timestamp_column: Column name to filter on (e.g., 'event_timestamp').
		
		Returns:
			Query with watermark filter applied.
		
		Raises:
			ValueError: If query is invalid or doesn't end with proper syntax.
		"""
		if not isinstance(query, str) or not query.strip():
			raise ValueError("Query must be a non-empty string")
		if not isinstance(watermark_value, str) or not watermark_value.strip():
			raise ValueError("Watermark value must be a non-empty string")
		if not isinstance(timestamp_column, str) or not timestamp_column.strip():
			raise ValueError("Timestamp column must be a non-empty string")
		
		normalized_query = query.strip().rstrip(";").strip()
		# Add WHERE clause for incremental load
		filtered_query = f"{normalized_query} WHERE {timestamp_column} > '{watermark_value}'"
		logger.info(f"Applied watermark filter: {timestamp_column} > {watermark_value}")
		return filtered_query

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
	
	# Step 4: Read data from source with retry logic
	source_df = mssql_connector.read_query(
		glue_context.spark_session,
		filtered_query,
	)
	logger.info("Data read from SQL Server successfully")
	
	# Step 5: Validate schema matches contract
	if not schema_registry.is_match(source_df):
		raise ValueError("Source schema does not match the configured definition")
	logger.info("Source schema validated")

	# Step 6: Add partition columns
	partitioned_df = ingestion._add_partition_columns(source_df, derived_from=timestamp_column)
	row_count = partitioned_df.count()
	if row_count == 0:
		logger.info("Source query returned no rows; nothing to write")
		job.commit()
		return
	
	# Step 7: Write to S3
	write_partitioned_parquets(
		partitioned_df,
		target_s3_uri,
		write_mode=write_mode,
		partition_by=["year", "month"],
	)
	logger.info(f"Successfully ingested {row_count} rows")
	
	# Step 8: Log new watermark (for reference, not persisted yet)
	new_watermark = partitioned_df.agg(
		spark_max(col(timestamp_column))
	).collect()[0][0]
	logger.info(f"Next watermark should be: {new_watermark}")
	
	logger.info("Ingestion completed successfully")
	job.commit()


if __name__ == "__main__":
	main()
