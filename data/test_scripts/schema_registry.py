#!/usr/bin/env python3
import json
import logging

import boto3


logger = logging.getLogger(__name__)


class SchemaRegistry:
	"""Load an ordered schema contract and compare it with a DataFrame schema."""

	def __init__(self, schema_uri: str):
		self._columns = self.load_avro_definition(schema_uri)

	@staticmethod
	def load_avro_definition(schema_uri: str) -> list[dict]:
		"""Load an Avro schema definition from local disk or S3."""
		if schema_uri.startswith("s3://"):
			bucket, separator, key = schema_uri.removeprefix("s3://").partition("/")
			if not bucket or not separator or not key:
				raise ValueError("Schema S3 URI must include bucket and key")
			response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
			contents = response["Body"].read().decode("utf-8")
		else:
			with open(schema_uri, encoding="utf-8") as schema_file:
				contents = schema_file.read()

		definition = json.loads(contents)
		if not isinstance(definition, dict) or definition.get("type") != "record":
			raise ValueError("Avro schema definition must be a record")

		fields = definition.get("fields")
		if not isinstance(fields, list) or not fields:
			raise ValueError("Avro schema must contain a non-empty fields list")

		columns = []
		for field in fields:
			if not isinstance(field, dict):
				raise ValueError("Each schema field must be a mapping")

			name = field.get("name")
			data_type = field.get("type")
			if not isinstance(name, str) or not name:
				raise ValueError("Each schema field requires a non-empty name")
			if not isinstance(data_type, str) or not data_type:
				raise ValueError(
					f"Schema field {name} requires a Spark simple type string"
				)

			columns.append(
				{
					"name": name,
					"type": data_type.lower(),
				}
			)
		return columns

	def _find_differences(self, dataframe) -> list[str]:
		"""Return differences between a DataFrame and the schema contract."""
		differences = []
		# Dictionaries make schema comparison independent of column order.
		expected_schema = {
			column["name"]: column["type"] for column in self._columns
		}
		actual_schema = {
			field.name: field.dataType.simpleString().lower()
			for field in dataframe.schema.fields
		}

		# Set differences identify columns present on only one side.
		missing_columns = set(expected_schema) - set(actual_schema)
		if missing_columns:
			differences.append(
				"missing columns: " + ", ".join(sorted(missing_columns))
			)

		extra_columns = set(actual_schema) - set(expected_schema)
		if extra_columns:
			differences.append(
				"extra columns: " + ", ".join(sorted(extra_columns))
			)

		# Compare types only for columns that exist in both schemas.
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
