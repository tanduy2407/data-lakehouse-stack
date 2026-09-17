#!/usr/bin/env python3
import logging
import re

import boto3
import yaml


logger = logging.getLogger(__name__)

VIEW_NAME_PATTERN = re.compile(
	r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$"
)


def _load_yaml(config_uri: str) -> dict:
	"""Load a YAML mapping from local disk or S3."""
	if config_uri.startswith("s3://"):
		bucket, separator, key = config_uri.removeprefix("s3://").partition("/")
		if not bucket or not separator or not key:
			raise ValueError("MSSQL config S3 URI must include bucket and key")
		response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
		contents = response["Body"].read().decode("utf-8")
	else:
		with open(config_uri, encoding="utf-8") as config_file:
			contents = config_file.read()

	config = yaml.safe_load(contents)
	if not isinstance(config, dict):
		raise ValueError("MSSQL configuration must be a YAML mapping")
	return config


class MSSQLConnector:
	"""Read SQL Server views through Spark JDBC."""

	def __init__(
		self,
		host: str,
		database_name: str,
		user: str,
		password: str,
		port: int = 1433,
		encrypt: bool = True,
		trust_server_certificate: bool = False,
		driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
	):
		if not all((host, database_name, user, password)):
			raise ValueError(
				"MSSQL host, database, user, and password are required"
			)

		self.jdbc_url = (
			f"jdbc:sqlserver://{host}:{port};"
			f"databaseName={database_name};"
			f"encrypt={str(encrypt).lower()};"
			f"trustServerCertificate={str(trust_server_certificate).lower()}"
		)
		self.user = user
		self.password = password
		self.driver = driver

	@classmethod
	def from_yaml(cls, config_uri: str) -> tuple["MSSQLConnector", str]:
		"""Create a connector and view name from a YAML configuration."""
		config = _load_yaml(config_uri)
		required_keys = {"host", "database", "user", "password", "view"}
		missing_keys = required_keys - config.keys()
		if missing_keys:
			raise ValueError(
				"Missing MSSQL configuration keys: "
				+ ", ".join(sorted(missing_keys))
			)

		view_name = config["view"]
		if not isinstance(view_name, str) or not VIEW_NAME_PATTERN.fullmatch(view_name):
			raise ValueError(
				"MSSQL view must be a view name or schema-qualified view name"
			)

		connector = cls(
			host=config["host"],
			database_name=config["database"],
			user=config["user"],
			password=config["password"],
			port=config.get("port", 1433),
			encrypt=config.get("encrypt", True),
			trust_server_certificate=config.get(
				"trust_server_certificate",
				False,
			),
			driver=config.get(
				"driver",
				"com.microsoft.sqlserver.jdbc.SQLServerDriver",
			),
		)
		return connector, view_name

	def read_view(self, spark_session, view_name: str):
		"""Read a view and return its Spark DataFrame."""
		if not VIEW_NAME_PATTERN.fullmatch(view_name):
			raise ValueError(
				"MSSQL view must be a view name or schema-qualified view name"
			)

		logger.info("Reading SQL Server view %s", view_name)
		return (
			spark_session.read.format("jdbc")
			.option("url", self.jdbc_url)
			.option("dbtable", view_name)
			.option("user", self.user)
			.option("password", self.password)
			.option("driver", self.driver)
			.load()
		)
