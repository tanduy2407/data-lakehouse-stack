#!/usr/bin/env python3
import logging

from fastavro import parse_schema
from store_to_s3 import load_s3_json


logger = logging.getLogger(__name__)


class SchemaRegistry:
	"""Load an ordered schema contract and compare it with a DataFrame schema."""

	def __init__(self, schema_uri: str):
		self._columns = self.load_avro_definition(schema_uri)

	@staticmethod
	def load_avro_definition(schema_uri: str) -> list[dict]:
		"""Load an Avro schema definition from S3."""
		definition = load_s3_json(schema_uri)
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
			column["name"]: column["type"] for column in self._columns
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
