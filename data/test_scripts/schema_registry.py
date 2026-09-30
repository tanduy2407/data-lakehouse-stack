#!/usr/bin/env python3
import json
import logging
import re
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from fastavro import parse_schema
from store_to_s3 import S3Client

logger = logging.getLogger(__name__)


class SchemaRegistry:
	"""Manage Avro schema definitions stored in S3."""
	def __init__(self, project: str, dataset: str, bucket = "schema-registry"):
		self.project = project
		self.dataset = dataset
		self.s3_client = S3Client()
		self.schema_prefix = f"s3://{bucket}/{self.project}/{self.dataset}"
		self.columns = self._load_avro_definition()

	def _get_latest_avro_file(self) -> tuple[str, int]:
		"""Return the latest schema URI and its numeric filename version."""
		versioned_schemas = []
		schema_files = self.s3_client.read_files(
			self.schema_prefix,
			extension=".avsc",
		)
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

	def _fields_to_columns(self, fields: list[dict]) -> list[dict]:
		columns = []
		for field in fields:
			if not isinstance(field, dict):
				raise ValueError("Each schema field must be a mapping")
			name = field.get("name")
			if not isinstance(name, str) or not name:
				raise ValueError("Each schema field requires a non-empty name")
			success, data_type, error = self._avro_to_spark_type(field.get("type"))
			if not success:
				raise ValueError(error)
			columns.append({"name": name, "type": data_type})
		return columns
	
	def _load_avro_definition(self) -> list[dict]:
		"""Load an Avro schema definition from S3."""
		self.schema_uri, self.schema_version = self._get_latest_avro_file()
		logger.info(
			"Loading schema definition: %s (version=%d)",
			self.schema_uri,
			self.schema_version,
		)
		definition = self.s3_client.load_json(self.schema_uri)

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
		
		self.definition = definition
		return self._fields_to_columns(fields)
	
	def _has_schema_changes(self, dataframe) -> bool:
		"""Return whether a DataFrame schema differs from the contract."""
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

		# Check for columns present in actual but not in expected
		extra_columns = set(actual_schema) - set(expected_schema)

		# Compare data types for columns present in both schemas
		change_columns = []
		for column_name in set(expected_schema) & set(actual_schema):
			if actual_schema[column_name] != expected_schema[column_name]:
				change_columns.append(column_name)
		if any((missing_columns, extra_columns, change_columns)):
			return True
		return False

	def validate_schema(self, dataframe):
		"""Return whether the DataFrame schema matches the contract."""
		has_changes = self._has_schema_changes(dataframe)
		if has_changes:
			logger.error("Source schema mismatch")
		logger.info("Source view schema matches the configured definition")
	
	@staticmethod
	def _avro_to_spark_type(avro_type) -> tuple[bool, str | None, str | None]:
		"""Convert an Avro type definition to Spark simpleString format."""
		if isinstance(avro_type, list):
			non_null_types = [item for item in avro_type if item not in {"null", None}]
			if len(non_null_types) != 1:
				return False, None,  "Avro unions must contain exactly one non-null data type"
			return SchemaRegistry._avro_to_spark_type(non_null_types[0])

		if isinstance(avro_type, dict):
			logical_type = avro_type.get("logicalType")
			if logical_type in {"timestamp-millis", "timestamp-micros"}:
				return True, "timestamp", None
			if logical_type == "date":
				return True, "date", None
			if logical_type == "decimal":
				precision = avro_type.get("precision")
				scale = avro_type.get("scale", 0)
				if not isinstance(precision, int) or not isinstance(scale, int):
					return False, None, "Avro decimal requires integer precision and scale"
				return True, f"decimal({precision},{scale})", None

			avro_kind = avro_type.get("type")
			if avro_kind == "array":
				success, item_type, error = SchemaRegistry._avro_to_spark_type(
					avro_type.get("items")
				)
				if not success:
					return False, None, error
				return True, f"array<{item_type}>", None

			if avro_kind == "map":
				success, value_type, error = SchemaRegistry._avro_to_spark_type(
					avro_type.get("values")
				)
				if not success:
					return False, None, error
				return True, f"map<string,{value_type}>", None

			if avro_kind == "record":
				fields = avro_type.get("fields")
				if not isinstance(fields, list):
					return False, None, "Avro record must contain a fields list"
				field_types = []
				for field in fields:
					if not isinstance(field, dict) or not field.get("name"):
						return False, None, "Each nested Avro field requires a name"
					success, field_type, error = SchemaRegistry._avro_to_spark_type(
						field.get("type")
					)
					if not success:
						return False, None, error
					field_types.append(f"{field['name']}:{field_type}")
				return True, "struct<" + ",".join(field_types) + ">", None

			if avro_kind == "enum":
				return True, "string", None
			if avro_kind == "fixed":
				return True, "binary", None
			avro_type = avro_kind

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
			return False, None,  f"Unsupported Avro field type: {avro_type}"
		return True, type_mapping[avro_type], None
	
	def _fields_to_type_map(self, fields: list[dict]) -> dict[str, str]:
		"""Convert Avro fields to a field-name-to-Spark-type mapping."""
		result = {}
		for field in fields:
			name = field.get("name")
			if not isinstance(name, str) or not name:
				raise ValueError("Each schema field requires a non-empty name")
			success, data_type, error = self._avro_to_spark_type(field.get("type"))
			if not success:
				raise ValueError(error)
			result[name] = data_type
		return result

	@staticmethod
	def _compare_field_types(
		old_field_types: dict[str, str],
		new_field_types: dict[str, str],
	) -> dict:
		"""Compare two field/type mappings."""
		added = [
			{"name": name, "type": new_field_types[name]}
			for name in sorted(set(new_field_types) - set(old_field_types))
		]
		removed = [
			{"name": name, "type": old_field_types[name]}
			for name in sorted(set(old_field_types) - set(new_field_types))
		]
		modified = [
			{
				"name": name,
				"from": old_field_types[name],
				"to": new_field_types[name],
			}
			for name in sorted(set(old_field_types) & set(new_field_types))
			if old_field_types[name] != new_field_types[name]
		]
		return {"added": added, 
		        "removed": removed, 
				"modified": modified}

	def _get_schema_changes(self, new_fields: list[dict]) -> dict:
		"""Describe field additions, removals, and type changes."""
		old_field_types = self._fields_to_type_map(self.definition["fields"])
		new_field_types = self._fields_to_type_map(new_fields)
		return self._compare_field_types(old_field_types, new_field_types)
	
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
		definition["changes"] = self._get_schema_changes(definition["fields"])
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
	):
		"""Build and upload the next Avro schema version when fields changed."""
		try:
			avro_schema = self._spark_dataframe_to_avro_schema(
				dataframe
			)
			changes = avro_schema["changes"]
			if not any(changes.values()):
				logger.info("Schema unchanged; skipping registration")
				return
			version = avro_schema["version"]
			schema_uri = self.s3_client.upload_json(
				self.schema_prefix,
				f"v{version}.avsc",
				avro_schema,
			)
			logger.info("Registered schema version %d at %s", version, schema_uri)
		except Exception as e:
			logger.error("Failed to register schema: %s", str(e))
			raise