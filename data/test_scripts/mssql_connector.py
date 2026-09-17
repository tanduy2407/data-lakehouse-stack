#!/usr/bin/env python3
import logging

logger = logging.getLogger(__name__)

class MSSQLConnector:
	"""Read SQL Server queries through Spark JDBC."""

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

	def read_query(self, spark_session, query: str):
		"""Execute a query and return its Spark DataFrame."""
		if not isinstance(query, str) or not query.strip():
			raise ValueError("MSSQL query must be a non-empty string")

		jdbc_query = f"({query.strip().rstrip(';')}) AS source_query"
		logger.info("Reading SQL Server query")
		return (
			spark_session.read.format("jdbc")
			.option("url", self.jdbc_url)
			.option("dbtable", jdbc_query)
			.option("user", self.user)
			.option("password", self.password)
			.option("driver", self.driver)
			.load()
		)
