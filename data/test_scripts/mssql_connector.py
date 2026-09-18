#!/usr/bin/env python3
import logging
import time

from sqlalchemy import create_engine, text
from sqlalchemy.exc import (
	OperationalError,
	ProgrammingError,
	IntegrityError,
	DatabaseError,
)

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
		num_partitions: int = 4,
		connection_timeout_ms: int = 30000,
		idle_timeout_ms: int = 600000,
		max_retries: int = 3,
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
		if num_partitions <= 0:
			raise ValueError("MSSQL num_partitions must be greater than zero")
		if connection_timeout_ms <= 0 or idle_timeout_ms <= 0:
			raise ValueError("MSSQL connection pool timeouts must be greater than zero")
		if max_retries < 0:
			raise ValueError("MSSQL max_retries must be non-negative")

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
		self.num_partitions = num_partitions
		self.connection_timeout_ms = connection_timeout_ms
		self.idle_timeout_ms = idle_timeout_ms
		self.max_retries = max_retries

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
		"""Execute a query with connection pooling and smart retry logic.
		
		Retries only on transient errors (timeouts, connection issues).
		Fails immediately on permanent errors (not found, permission denied, etc).
		"""
		jdbc_query = self._prepare_query(query)
		
		for attempt in range(self.max_retries + 1):
			try:
				logger.info(
					f"Reading SQL Server query (attempt {attempt + 1}/{self.max_retries + 1})"
				)
				return (
					spark_session.read.format("jdbc")
					.option("url", self._jdbc_url)
					.option("dbtable", jdbc_query)
					.option("user", self.user)
					.option("password", self.password)
					.option("driver", self.driver)
					.option("queryTimeout", self.query_timeout_seconds)
					.option("fetchsize", self.fetch_size)
					# Connection pool configuration
					.option("numPartitions", self.num_partitions)
					.option("connectionTimeout", self.connection_timeout_ms)
					.option("idleTimeout", self.idle_timeout_ms)
					.load()
				)
			# Permanent errors - fail immediately
			except (ProgrammingError, IntegrityError) as error:
				logger.error(f"Query failed with permanent error after {attempt + 1} attempt(s): {error}")
				raise RuntimeError(f"Failed to read SQL Server query: {error}") from error
			
			# Transient error - connection/timeout issues
			except OperationalError as error:
				if attempt < self.max_retries:
					wait_time = 2 ** attempt
					logger.warning(
						f"Query attempt {attempt + 1} failed with transient error (OperationalError): {error}. "
						f"Retrying in {wait_time}s... (attempt {attempt + 2}/{self.max_retries + 1})"
					)
					time.sleep(wait_time)
				else:
					logger.error(f"Query failed with transient error after {self.max_retries + 1} attempts: {error}")
					raise RuntimeError(f"Failed to read SQL Server query after max retries: {error}") from error
			
			# Database error - check if transient (deadlock)
			except DatabaseError as error:
				error_msg = str(error)
				is_deadlock = "1205" in error_msg or "deadlock" in error_msg.lower()
				
				if is_deadlock and attempt < self.max_retries:
					wait_time = 2 ** attempt
					logger.warning(
						f"Query attempt {attempt + 1} failed with deadlock: {error}. "
						f"Retrying in {wait_time}s... (attempt {attempt + 2}/{self.max_retries + 1})"
					)
					time.sleep(wait_time)
				else:
					logger.error(f"Query failed with permanent database error after {attempt + 1} attempt(s): {error}")
					raise RuntimeError(f"Failed to read SQL Server query: {error}") from error
			
			# Unknown exceptions - retry as default safe behavior
			except Exception as error:
				if attempt < self.max_retries:
					wait_time = 2 ** attempt
					logger.warning(
						f"Query attempt {attempt + 1} failed with unknown error: {error}. "
						f"Retrying in {wait_time}s... (attempt {attempt + 2}/{self.max_retries + 1})"
					)
					time.sleep(wait_time)
				else:
					logger.error(f"Query failed with unknown error after {self.max_retries + 1} attempts: {error}")
					raise RuntimeError(f"Failed to read SQL Server query: {error}") from error
				
