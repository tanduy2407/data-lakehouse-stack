#!/usr/bin/env python3
from datetime import date, datetime
import logging
import sys
import time

from sqlalchemy.exc import (
	OperationalError,
	ProgrammingError,
	IntegrityError,
	DatabaseError,
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofmonth, month, year, max as spark_max

from mssql_connector import MSSQLConnector
from schema_registry import SchemaRegistry
from store_to_s3 import S3Client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FailFastIngestionError(RuntimeError):
	"""Signal a non-retryable ingestion failure."""


class Ingestion:
	@staticmethod
	def read_watermark() -> str | None:
		"""Return the configured watermark for incremental loads, 
		or None for a full load."""
		watermark = "2026-01-01T00:00:00"
		logger.info("Loaded watermark value: %s", watermark)
		return watermark

	@staticmethod
	def calculate_new_watermark(
		dataframe: DataFrame,
		watermark_column: str,
	) -> str | None:
		"""Return the greatest watermark value as a string."""
		if not isinstance(watermark_column, str) or not watermark_column.strip():
			raise ValueError("Watermark column must be a non-empty string")
		if watermark_column not in dataframe.columns:
			raise ValueError(f"DataFrame must contain {watermark_column}")
		if dataframe.limit(1).count() == 0:
			logger.info("DataFrame is empty; no new watermark was generated")
			return None
		new_watermark = dataframe.agg(
			spark_max(col(watermark_column)).alias("new_watermark")
		).first()["new_watermark"]
		if new_watermark is None:
			return None
		if isinstance(new_watermark, (date, datetime)):
			watermark = new_watermark.isoformat()
		else:
			watermark = str(new_watermark)
		logger.info("Next watermark should be: %s", watermark)
		return watermark

	@staticmethod
	def process_fail_fast(
		dataframe: DataFrame,
		schema_registry: SchemaRegistry,
		error_s3_uri: str,
	) -> None:
		"""Validate schema, save mismatched data, and fail fast on invalid data."""
		if not isinstance(error_s3_uri, str) or not error_s3_uri.strip():
			raise ValueError("Error S3 URI must be a non-empty string")

		if not schema_registry.validate_schema(dataframe):
			logger.error("Schema mismatch; writing rejected data to %s", error_s3_uri)
			S3Client().write_parquet(dataframe, error_s3_uri, mode="append")
			raise FailFastIngestionError(
				"Source schema does not match the configured definition"
			)
		logger.info("Source schema validated")

	@staticmethod
	def _apply_watermark_filter(
		query: str,
		watermark_value: str | None,
		watermark_column: str,
	) -> str:
		"""Apply a watermark filter, or return the query unchanged for a full load."""
		if not isinstance(query, str) or not query.strip():
			raise ValueError("Query must be a non-empty string")
		if watermark_value is not None and (not isinstance(watermark_value, str) or not watermark_value.strip()):
			raise ValueError("Watermark value must be a non-empty string")
		if not isinstance(watermark_column, str) or not watermark_column.strip():
			raise ValueError("Watermark column must be a non-empty string")
		normalized_query = query.strip().rstrip(";").strip()
		if watermark_value is None:
			logger.info("No watermark found; running full load")
			return normalized_query
		# Add WHERE clause for incremental load
		filtered_query = f"{normalized_query} WHERE {watermark_column} > '{watermark_value}'"
		logger.info(
			"Applied watermark filter: %s > %s",
			watermark_column,
			watermark_value,
		)
		return filtered_query

	@staticmethod
	def read_source_data(
		connector: MSSQLConnector,
		spark_session,
		query: str,
		watermark_value: str | None,
		watermark_column: str,
		max_retries: int = 3,
	) -> DataFrame:
		"""Apply the watermark and execute the source query."""
		if max_retries < 0:
			raise ValueError("max_retries must be non-negative")
		filtered_query = Ingestion._apply_watermark_filter(
			query,
			watermark_value,
			watermark_column,
		)
		for attempt in range(max_retries + 1):
			try:
				logger.info(
					"Reading SQL Server query (attempt %d/%d)",
					attempt + 1,
					max_retries + 1,
				)
				dataframe = connector.execute_query(spark_session, filtered_query)
				row_count = dataframe.count()
				logger.info(
					"Data read from SQL Server successfully: %d rows",
					row_count,
				)
				return dataframe
			except (ProgrammingError, IntegrityError) as error:
				raise RuntimeError(f"Failed to read SQL Server query: {error}") from error
			except OperationalError as error:
				retryable = True
			except DatabaseError as error:
				error_message = str(error)
				retryable = "1205" in error_message or "deadlock" in error_message.lower()
			except Exception as error:
				retryable = True

			if not retryable or attempt >= max_retries:
				raise RuntimeError(
					f"Failed to read SQL Server query after {attempt + 1} attempts: {error}"
				) from error
			wait_time = 2 ** attempt
			logger.warning(
				"Query attempt %d failed: %s. Retrying in %d seconds",
				attempt + 1,
				error,
				wait_time,
			)
			time.sleep(wait_time)

	def add_partition_columns(
		self,
		dataframe: DataFrame,
		derived_from: str,
		partition_keys: tuple[str],
	) -> DataFrame:
		"""Derive requested date partition columns from a date or timestamp column."""
		logger.info(
			"Adding partition columns %s from timestamp column %s",
			partition_keys,
			derived_from,
		)
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
		logger.info("Added partition columns: %s", partition_keys)
		return dataframe

	@staticmethod
	def write_if_not_empty(
		dataframe: DataFrame,
		s3_client: S3Client,
		target_uri: str,
		mode: str,
		partition_by: list[str],
	) -> None:
		"""Write a non-empty DataFrame when rows are available."""
		row_count = dataframe.count()
		if row_count == 0:
			logger.info("Source query returned no rows; nothing to write")
			return

		s3_client.write_partitioned_parquets(
			dataframe,
			target_uri,
			mode=mode,
			partition_by=partition_by,
		)
		logger.info("Successfully ingested %d rows", row_count)


def main() -> None:
	"""Validate and ingest a JDBC query into partitioned Parquet on S3."""
	args = getResolvedOptions(sys.argv, ["JOB_NAME"])
	target_s3_uri = 's3://your-bucket/your-prefix'
	error_s3_uri = 's3://your-bucket/error-prefix'
	write_mode = 'append'
	project_name = 'your-bucket'
	table_name = 'events_view'

	glue_context = GlueContext(SparkContext.getOrCreate())
	job = Job(glue_context)
	job.init(args["JOB_NAME"], args)

	ingestion = Ingestion()
	s3_client = S3Client()
	watermark_value = ingestion.read_watermark()

	schema_registry = SchemaRegistry(project_name, table_name)
	host = 'host'
	database_name = 'database'
	user = 'user'
	password = 'password'
	schema_name = 'dbo'
	watermark_column = 'event_timestamp'
	
	mssql_connector = MSSQLConnector(
		host=host,
		database=database_name,
		user=user,
		password=password,
	)
	
	base_query = f'SELECT * FROM {schema_name}.{table_name}'
	source_df = ingestion.read_source_data(
		mssql_connector,
		glue_context.spark_session,
		base_query,
		watermark_value,
		watermark_column,
	)
	
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
		derived_from=watermark_column,
		partition_keys=partition_keys,
	)
	ingestion.write_if_not_empty(
		partitioned_df,
		s3_client,
		target_s3_uri,
		mode=write_mode,
		partition_by=list(partition_keys),
	)
	
	new_watermark = ingestion.calculate_new_watermark(
		partitioned_df,
		watermark_column,
	)
	
	logger.info("Ingestion completed successfully")
	job.commit()


if __name__ == "__main__":
	main()
