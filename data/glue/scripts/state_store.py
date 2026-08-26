import uuid
from typing import Dict, Iterable, Optional


class JobStateStore:
    def __init__(self, spark, jdbc_url: str, postgres_user: str, postgres_password: str) -> None:
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_options = {
            "url": jdbc_url,
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver",
        }

    def _escape(self, value: str) -> str:
        return value.replace("'", "''")

    def _execute(self, sql: str) -> None:
        jvm = self.spark.sparkContext._jvm
        conn = None
        stmt = None
        try:
            jvm.java.lang.Class.forName("org.postgresql.Driver")
            conn = jvm.java.sql.DriverManager.getConnection(
                self.jdbc_url,
                self.jdbc_options["user"],
                self.jdbc_options["password"],
            )
            stmt = conn.createStatement()
            stmt.execute(sql)
        finally:
            if stmt is not None:
                stmt.close()
            if conn is not None:
                conn.close()

    def _query(self, sql: str):
        return (
            self.spark.read.format("jdbc")
            .options(**self.jdbc_options, dbtable=f"({sql}) AS state_query")
            .load()
        )

    def start_run(self, job_name: str) -> str:
        run_id = str(uuid.uuid4())
        safe_job_name = self._escape(job_name)
        self._execute(
            f"""
            INSERT INTO job_control_runs (run_id, job_name, status)
            VALUES ('{run_id}', '{safe_job_name}', 'STARTED')
            """
        )
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_in: Optional[int] = None,
        rows_out: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        safe_status = self._escape(status)
        rows_in_sql = "NULL" if rows_in is None else str(int(rows_in))
        rows_out_sql = "NULL" if rows_out is None else str(int(rows_out))
        error_sql = "NULL"
        if error_message:
            safe_error = self._escape(error_message[:4000])
            error_sql = f"'{safe_error}'"

        self._execute(
            f"""
            UPDATE job_control_runs
            SET ended_at = NOW(),
                status = '{safe_status}',
                rows_in = {rows_in_sql},
                rows_out = {rows_out_sql},
                error_message = {error_sql}
            WHERE run_id = '{run_id}'
            """
        )

    def load_watermarks(self, job_name: str, entities: Iterable[str]) -> Dict[str, str]:
        entities = list(entities)
        if not entities:
            return {}

        safe_job_name = self._escape(job_name)
        safe_entities = ", ".join([f"'{self._escape(entity)}'" for entity in entities])
        df = self._query(
            f"""
            SELECT entity_name, watermark_value
            FROM job_control_watermarks
            WHERE job_name = '{safe_job_name}'
              AND entity_name IN ({safe_entities})
            """
        )
        return {row["entity_name"]: row["watermark_value"] for row in df.collect()}

    def upsert_watermark(self, job_name: str, entity_name: str, watermark_value: str, run_id: str) -> None:
        safe_job_name = self._escape(job_name)
        safe_entity_name = self._escape(entity_name)
        safe_watermark = self._escape(watermark_value)
        safe_run_id = self._escape(run_id)

        self._execute(
            f"""
            INSERT INTO job_control_watermarks (job_name, entity_name, watermark_value, updated_by_run_id)
            VALUES ('{safe_job_name}', '{safe_entity_name}', '{safe_watermark}', '{safe_run_id}')
            ON CONFLICT (job_name, entity_name)
            DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value,
                updated_at = NOW(),
                updated_by_run_id = EXCLUDED.updated_by_run_id
            """
        )

    def log_dq_metric(
        self,
        run_id: str,
        job_name: str,
        check_name: str,
        metric_name: str,
        metric_value: float,
        passed: bool,
    ) -> None:
        safe_run_id = self._escape(run_id)
        safe_job_name = self._escape(job_name)
        safe_check_name = self._escape(check_name)
        safe_metric_name = self._escape(metric_name)
        passed_sql = "TRUE" if passed else "FALSE"
        self._execute(
            f"""
            INSERT INTO job_control_dq_metrics
                (run_id, job_name, check_name, metric_name, metric_value, passed)
            VALUES
                ('{safe_run_id}', '{safe_job_name}', '{safe_check_name}',
                 '{safe_metric_name}', {float(metric_value)}, {passed_sql})
            """
        )
