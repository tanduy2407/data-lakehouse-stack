#!/usr/bin/env python3
"""DQ monitoring daemon — polls job control tables and emits structured alerts."""
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

POLL_INTERVAL = int(os.getenv("MONITOR_POLL_INTERVAL", "60"))
ALERT_WINDOW_MINUTES = int(os.getenv("MONITOR_ALERT_WINDOW_MINUTES", "60"))
JOB_FRESHNESS_HOURS = int(os.getenv("MONITOR_JOB_FRESHNESS_HOURS", "24"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("dq-monitor")


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "source_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        connect_timeout=10,
    )


def emit_alert(level: str, title: str, details: dict) -> None:
    payload = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": level, "title": title, **details}
    log.warning("ALERT %s", json.dumps(payload))


def check_failed_runs(cur) -> None:
    cur.execute(
        """
        SELECT run_id, job_name, started_at, ended_at, error_message
        FROM job_control_runs
        WHERE status = 'FAILED'
          AND ended_at >= NOW() - (%(w)s || ' minutes')::INTERVAL
        ORDER BY ended_at DESC
        """,
        {"w": ALERT_WINDOW_MINUTES},
    )
    for row in cur.fetchall():
        emit_alert(
            "ERROR",
            f"Job failed: {row['job_name']}",
            {
                "run_id": row["run_id"],
                "started_at": str(row["started_at"]),
                "ended_at": str(row["ended_at"]),
                "error": (row["error_message"] or "")[:500],
            },
        )


def check_job_freshness(cur) -> None:
    for job_name in ("ingest", "transform", "aggregate"):
        cur.execute(
            """
            SELECT MAX(ended_at) AS last_success
            FROM job_control_runs
            WHERE job_name = %s AND status = 'SUCCEEDED'
            """,
            (job_name,),
        )
        row = cur.fetchone()
        last_success: Optional[datetime] = row["last_success"] if row else None
        if last_success is None:
            emit_alert("WARNING", f"Job has never succeeded: {job_name}", {"job_name": job_name})
        else:
            age_hours = (datetime.now(timezone.utc) - last_success).total_seconds() / 3600
            if age_hours > JOB_FRESHNESS_HOURS:
                emit_alert(
                    "WARNING",
                    f"Stale data: {job_name} last succeeded {age_hours:.1f}h ago",
                    {"job_name": job_name, "last_success": str(last_success), "age_hours": round(age_hours, 1)},
                )


def check_dq_metric_failures(cur) -> None:
    cur.execute(
        """
        SELECT run_id, job_name, check_name, metric_name, metric_value, recorded_at
        FROM job_control_dq_metrics
        WHERE passed = FALSE
          AND recorded_at >= NOW() - (%(w)s || ' minutes')::INTERVAL
        ORDER BY recorded_at DESC
        """,
        {"w": ALERT_WINDOW_MINUTES},
    )
    for row in cur.fetchall():
        emit_alert(
            "ERROR",
            f"DQ check failed: {row['check_name']} ({row['job_name']})",
            {
                "run_id": row["run_id"],
                "job_name": row["job_name"],
                "check": row["check_name"],
                "metric": row["metric_name"],
                "value": str(row["metric_value"]),
                "recorded_at": str(row["recorded_at"]),
            },
        )


def dq_metrics_table_exists(cur) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'job_control_dq_metrics')"
    )
    return cur.fetchone()["exists"]


def poll_once() -> None:
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            check_failed_runs(cur)
            check_job_freshness(cur)
            if dq_metrics_table_exists(cur):
                check_dq_metric_failures(cur)
        conn.close()
    except psycopg2.OperationalError as exc:
        log.error("DB connection failed (will retry next poll): %s", exc)


def main() -> None:
    log.info(
        "DQ monitor started — poll=%ds window=%dmin freshness=%dh",
        POLL_INTERVAL,
        ALERT_WINDOW_MINUTES,
        JOB_FRESHNESS_HOURS,
    )
    while True:
        log.info("Polling job control tables ...")
        poll_once()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
