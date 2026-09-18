#!/usr/bin/env python3
import logging


logger = logging.getLogger(__name__)


class MSSQLConnector:
	"""Read SQL Server data through Spark JDBC."""

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
	) -> None:
		if not all((host, database, user, password)):
			raise ValueError(
				"MSSQL host, database, user, and password are required"
			)
		if ";" in host or ";" in database:
			raise ValueError("MSSQL host and database must not contain semicolons")
		if login_timeout_seconds <= 0 or query_timeout_seconds <= 0:
			raise ValueError("MSSQL timeouts must be greater than zero")
		if fetch_size <= 0:
			raise ValueError("MSSQL fetch size must be greater than zero")

		self._jdbc_url = (
			f"jdbc:sqlserver://{host}:{port};"
			f"databaseName={database};"
			f"encrypt={str(encrypt).lower()};"
			f"trustServerCertificate={str(trust_server_certificate).lower()};"
			f"loginTimeout={login_timeout_seconds};"
			"applicationIntent=ReadOnly"
		)
		self.user = user
		self.password = password
		self.driver = driver
		self.query_timeout_seconds = query_timeout_seconds
		self.fetch_size = fetch_size

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

	def read_query(self, spark_session, query: str):
		"""Execute a query and return its Spark DataFrame."""
		jdbc_query = self._prepare_query(query)
		logger.info("Reading SQL Server query")
		return (
			spark_session.read.format("jdbc")
			.option("url", self._jdbc_url)
			.option("dbtable", jdbc_query)
			.option("user", self.user)
			.option("password", self.password)
			.option("driver", self.driver)
			.option("queryTimeout", self.query_timeout_seconds)
			.option("fetchsize", self.fetch_size)
			.load()
		)
