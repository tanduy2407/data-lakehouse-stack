#!/usr/bin/env python3
import logging

logger = logging.getLogger(__name__)


class MSSQLConnector:
	def __init__(
		self,
		host: str,
		database: str,
		user: str,
		password: str,
		port: int = 1433,
		encrypt: bool = True,
		trust_server_certificate: bool = False,
		driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
		login_timeout_seconds: int = 30,
		query_timeout_seconds: int = 300,
		fetch_size: int = 10000,
		num_partitions: int = 4,
		connection_timeout_ms: int = 30000,
		idle_timeout_ms: int = 600000
	) -> None:
		if not all((host, database, user, password)):
			raise ValueError(
				"MSSQL host, database, user, and password are required"
			)
		if login_timeout_seconds <= 0 or query_timeout_seconds <= 0:
			raise ValueError("MSSQL timeouts must be greater than zero")
		if fetch_size <= 0:
			raise ValueError("MSSQL fetch size must be greater than zero")
		if num_partitions <= 0:
			raise ValueError("MSSQL num_partitions must be greater than zero")
		if connection_timeout_ms <= 0 or idle_timeout_ms <= 0:
			raise ValueError("MSSQL connection pool timeouts must be greater than zero")

		self._jdbc_url = (
			f"jdbc:sqlserver://{host}:{port};"
			f"databaseName={database};"
			f"encrypt={str(encrypt).lower()};"
			f"trustServerCertificate={str(trust_server_certificate).lower()};"
			f"loginTimeout={login_timeout_seconds};"
			"applicationIntent=ReadOnly"
		)
		self._user = user
		self._password = password
		self._driver = driver
		self._query_timeout_seconds = query_timeout_seconds
		self._fetch_size = fetch_size
		self._num_partitions = num_partitions
		self._connection_timeout_ms = connection_timeout_ms
		self._idle_timeout_ms = idle_timeout_ms

	@staticmethod
	def _prepare_query(query: str) -> str:
		"""Validate and wrap one read-only query for Spark JDBC."""
		if not isinstance(query, str) or not query.strip():
			raise ValueError("MSSQL query must be a non-empty string")

		normalized_query = query.strip().rstrip(";").strip()
		if ";" in normalized_query:
			raise ValueError("MSSQL query must contain only one statement")
		if normalized_query.split(None, 1)[0].upper() != "SELECT":
			raise ValueError("MSSQL query must be a SELECT statement")
		return f"({normalized_query}) AS source_query"

	def execute_query(self, spark_session, query: str):
		"""Execute one read-only query through Spark JDBC."""
		jdbc_query = self._prepare_query(query)
		return (
			spark_session.read.format("jdbc")
			.option("url", self._jdbc_url)
			.option("dbtable", jdbc_query)
			.option("user", self._user)
			.option("password", self._password)
			.option("driver", self._driver)
			.option("queryTimeout", self._query_timeout_seconds)
			.option("fetchsize", self._fetch_size)
			.option("numPartitions", self._num_partitions)
			.option("connectionTimeout", self._connection_timeout_ms)
			.option("idleTimeout", self._idle_timeout_ms)
			.load()
		)
				
