#!/usr/bin/env python3
import json
import logging
import re
from datetime import datetime, timezone

import boto3
from pyspark.sql import DataFrame
from fastavro import parse_schema
from store_to_s3 import _parse_s3_prefix, load_s3_json

logger = logging.getLogger(__name__)


class SchemaRegistry:
	"""Manage Avro schema definitions stored in S3."""
	def __init__(self, project: str, dataset: str):
		bucket = "schema-registry"
		self.project = project
		self.dataset = dataset
		self.schema_prefix = f"s3://{bucket}/{self.project}/{self.dataset}"
		self.columns = self.load_avro_definition()

	def _read_schema_files(self) -> list[str]:
		"""Read all object paths under an S3 table schema prefix."""
		bucket, prefix = _parse_s3_prefix(self.schema_prefix)
		files = []
		paginator = boto3.client("s3").get_paginator("list_objects_v2")
		pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
		for page in pages:
			for object_info in page.get("Contents", []):
				key = object_info.get("Key", "")
				if key:
					files.append(f"s3://{bucket}/{key}")
		logger.info("Found %d objects under schema prefix: %s", len(files), self.schema_prefix)
		return files

	def _get_latest_avro_file(self) -> tuple[str, int]:
		"""Return the latest schema URI and its numeric filename version."""
		versioned_schemas = []
		schema_files = self._read_schema_files()
		for schema_file in schema_files:
			match = re.search(r"(?:^|/)v(\d+)\.avsc$", schema_file)
			if match:
				versioned_schemas.append((int(match.group(1)), schema_file))

		if not versioned_schemas:
			raise ValueError("No versioned Avro schemas found under the S3 schema path")

		latest_version, latest_schema_uri = max(
			versioned_schemas, key=lambda item: item[0]
		)
		logger.info(
			"Selected latest schema: %s (version=%d)",
			latest_schema_uri,
			latest_version,
		)
		return latest_schema_uri, latest_version
	
	def load_avro_definition(self) -> list[dict]:
		"""Load an Avro schema definition from S3."""
		self.schema_uri, self.schema_version = self._get_latest_avro_file()
		logger.info(
			"Loading schema definition: %s (version=%d)",
			self.schema_uri,
			self.schema_version,
		)
		definition = load_s3_json(self.schema_uri)
		if not isinstance(definition, dict):
			raise ValueError("Avro schema definition must be a JSON object")
		try:
			parse_schema(definition)
		except Exception as error:
			raise ValueError("Invalid Avro schema definition") from error

		# Validate schema structure: must be a record type with non-empty fields list
		if definition.get("type") != "record":
			raise ValueError("Avro schema definition must be a record")

		fields = definition.get("fields")
		if not isinstance(fields, list) or not fields:
			raise ValueError("Avro schema must contain a non-empty fields list")

		columns = []
		for field in fields:
			if not isinstance(field, dict):
				raise ValueError("Each schema field must be a mapping")

			name = field.get("name")
			if not isinstance(name, str) or not name:
				raise ValueError("Each schema field requires a non-empty name")
			# Convert Avro type to Spark type for schema validation
			data_type = SchemaRegistry._avro_to_spark_type(field.get("type"))

			columns.append(
				{
					"name": name,
					"type": data_type,
				}
			)
		return columns

	@staticmethod
	def _avro_to_spark_type(avro_type) -> str:
		"""Convert an Avro type definition to Spark simpleString format."""
		# Handle Avro union types (e.g., ["null", "string"]) by extracting the non-null type
		if isinstance(avro_type, list):
			non_null_types = [item for item in avro_type if item != "null"]
			if len(non_null_types) != 1:
				raise ValueError(
					"Avro unions must contain exactly one non-null data type"
				)
			return SchemaRegistry._avro_to_spark_type(non_null_types[0])

		if isinstance(avro_type, dict):
			# Handle Avro logical types (special types with additional metadata)
			logical_type = avro_type.get("logicalType")
			if logical_type in {"timestamp-millis", "timestamp-micros"}:
				return "timestamp"
			if logical_type == "date":
				return "date"
			if logical_type == "decimal":
				precision = avro_type.get("precision")
				scale = avro_type.get("scale", 0)
				if not isinstance(precision, int) or not isinstance(scale, int):
					raise ValueError("Avro decimal requires integer precision and scale")
				return f"decimal({precision},{scale})"
			# Extract the underlying type for logical types
			avro_type = avro_type.get("type")

		# Map basic Avro types to Spark SQL types
		type_mapping = {
			"boolean": "boolean",
			"int": "int",
			"long": "bigint",
			"float": "float",
			"double": "double",
			"bytes": "binary",
			"string": "string",
		}
		if avro_type not in type_mapping:
			raise ValueError(f"Unsupported Avro field type: {avro_type}")
		return type_mapping[avro_type]

	def _find_differences(self, dataframe) -> list[str]:
		"""Return differences between a DataFrame and the schema contract."""
		differences = []
		# Dictionaries make schema comparison independent of column order.
		expected_schema = {
			column["name"]: column["type"] for column in self.columns
		}
		actual_schema = {
			field.name: field.dataType.simpleString().lower()
			for field in dataframe.schema.fields
		}

		# Check for columns present in expected but missing in actual
		missing_columns = set(expected_schema) - set(actual_schema)
		if missing_columns:
			differences.append(
				"missing columns: " + ", ".join(sorted(missing_columns))
			)

		# Check for columns present in actual but not in expected
		extra_columns = set(actual_schema) - set(expected_schema)
		if extra_columns:
			differences.append(
				"extra columns: " + ", ".join(sorted(extra_columns))
			)

		# Compare data types for columns present in both schemas
		for column_name in set(expected_schema) & set(actual_schema):
			if actual_schema[column_name] != expected_schema[column_name]:
				differences.append(
					f"{column_name} type: expected={expected_schema[column_name]}, "
					f"actual={actual_schema[column_name]}"
				)
		return differences

	def is_match(self, dataframe) -> bool:
		"""Return whether the DataFrame schema matches the contract."""
		differences = self._find_differences(dataframe)
		if differences:
			logger.error("Source schema mismatch: %s", "; ".join(differences))
			return False
		logger.info("Source view schema matches the configured definition")
		return True

	def _spark_dataframe_to_avro_schema(
		self,
		dataframe: DataFrame
	) -> dict:
		"""Convert a Spark DataFrame schema to an Avro record definition."""
		converter = dataframe.sparkSession._jvm.org.apache.spark.sql.avro.SchemaConverters
		avro_schema = converter.toAvroType(
			dataframe._jdf.schema(),
			False,
			self.dataset,
			self.project.lower(),
		)
		definition = json.loads(avro_schema.toString())
		definition["version"] = self.schema_version + 1
		definition["insert_timestamp"] = datetime.now(timezone.utc).strftime(
			"%Y-%m-%d %H:%M:%S"
		)
		logger.info(
			"Generated schema definition for %s version=%d at %s",
			self.dataset,
			definition["version"],
			definition["insert_timestamp"],
		)
		return definition

	def register_schema(
		self,
		dataframe: DataFrame,
	) -> dict:
		"""Build and upload the next Avro schema version to S3."""
		try:
			avro_schema = self._spark_dataframe_to_avro_schema(
				dataframe
			)
			version = avro_schema["version"]
			bucket, prefix = _parse_s3_prefix(self.schema_prefix)
			key = f"{prefix}v{version}.avsc"
			schema_uri = f"s3://{bucket}/{key}"
			boto3.client("s3").put_object(
				Bucket=bucket,
				Key=key,
				Body=json.dumps(avro_schema, indent=2).encode("utf-8"),
				ContentType="application/json",
			)
			logger.info("Uploaded schema version %d to %s", version, schema_uri)
			return avro_schema
		except Exception as e:
			logger.error("Failed to register schema: %s", str(e))
			raise