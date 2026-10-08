#!/usr/bin/env python3
"""AWS Lambda that deletes S3 year/month partitions outside a rolling retention range."""
import logging
import os
from datetime import datetime, timezone

import boto3
from store_to_s3 import S3Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

YEAR_PREFIX = "year="
MONTH_PREFIX = "month="
DELETE_BATCH_SIZE = 1000
s3_client = boto3.client("s3")
s3_uri_parser = S3Client()


class RetentionPolicyError(ValueError):
    """Raised when the retention policy input is invalid."""


def _retention_range(
    retention_years: int,
) -> tuple[tuple[int, int], tuple[int, int]]:
    """Return the inclusive month bounds from the exact retention cutoff to now."""
    if not isinstance(retention_years, int) or isinstance(retention_years, bool):
        raise RetentionPolicyError("retention_years must be an integer")
    if retention_years < 1:
        raise RetentionPolicyError("retention_years must be at least 1")

    reference = datetime.now(timezone.utc)
    cutoff = reference.replace(year=reference.year - retention_years)
    return (cutoff.year, cutoff.month), (reference.year, reference.month)


def _format_month(year_month: tuple[int, int]) -> str:
    """Render a (year, month) pair as year=YYYY/month=MM."""
    year, month = year_month
    return f"{YEAR_PREFIX}{year}/{MONTH_PREFIX}{month:02d}"


def _list_child_prefixes(bucket: str, prefix: str) -> list[str]:
    """List the immediate child prefixes of an S3 prefix."""
    children = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        children.extend(entry["Prefix"] for entry in page.get("CommonPrefixes", []))
    return children


def _partition_value(child_prefix: str, key_prefix: str) -> str | None:
    """Return the value of a key=value prefix, or None when it does not match."""
    name = child_prefix.rstrip("/").rsplit("/", 1)[-1]
    if not name.startswith(key_prefix):
        return None
    value = name.removeprefix(key_prefix)
    return value if value.isdigit() else None


def _list_month_partitions(bucket: str, table_prefix: str) -> tuple[list[dict], list[str]]:
    """List year=YYYY/month=MM partitions under an S3 table prefix."""
    partitions = []
    skipped = []

    for year_prefix in _list_child_prefixes(bucket, table_prefix):
        year_value = _partition_value(year_prefix, YEAR_PREFIX)
        if year_value is None:
            skipped.append(f"s3://{bucket}/{year_prefix}")
            continue

        for month_prefix in _list_child_prefixes(bucket, year_prefix):
            month_value = _partition_value(month_prefix, MONTH_PREFIX)
            location = f"s3://{bucket}/{month_prefix}"
            if month_value is None or not 1 <= int(month_value) <= 12:
                skipped.append(location)
                continue

            partitions.append(
                {
                    "partition": f"{YEAR_PREFIX}{year_value}/{MONTH_PREFIX}{month_value}",
                    "location": location.rstrip("/"),
                    "year_month": (int(year_value), int(month_value)),
                    "key_prefix": month_prefix,
                }
            )

    partitions.sort(key=lambda item: item["year_month"])
    return partitions, skipped

def _delete_batch(bucket: str, batch: list[dict]) -> int:
    """Delete one batch of object keys and return how many were removed."""
    response = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    for error in response.get("Errors", []):
        logger.error(
            "Failed to delete s3://%s/%s: %s", bucket, error["Key"], error.get("Message")
        )
    return len(response.get("Deleted", []))

def _delete_prefix(bucket: str, key_prefix: str) -> int:
    """Delete every object under an S3 prefix and return how many were removed."""
    deleted = 0
    batch = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for entry in page.get("Contents", []):
            batch.append({"Key": entry["Key"]})
            if len(batch) == DELETE_BATCH_SIZE:
                deleted += _delete_batch(bucket, batch)
                batch = []
    if batch:
        deleted += _delete_batch(bucket, batch)
    return deleted

def apply_retention(s3_uri: str, retention_years: int) -> dict:
    """Delete partitions older than the retention range declared by one config entry."""
    bucket, table_prefix = s3_uri_parser._parse_s3_prefix(s3_uri)
    range_start, range_end = _retention_range(retention_years)
    partitions, skipped = _list_month_partitions(bucket, table_prefix)
    retained = []
    expired = []
    for partition in partitions:
        entry = {"partition": partition["partition"], "location": partition["location"]}
        if partition["year_month"] >= range_start:
            retained.append(entry)
            continue

        entry["deleted_objects"] = _delete_prefix(bucket, partition["key_prefix"])
        logger.info(
            "Deleted %s (%d object(s))", partition["location"], entry["deleted_objects"]
        )
        expired.append(entry)

    if skipped:
        logger.warning(
            "Skipped %d prefix(es) under %s that are not year/month partitions",
            len(skipped),
            s3_uri,
        )

    return {
        "s3_uri": s3_uri,
        "retention_years": retention_years,
        "retention_range": {
            "start": _format_month(range_start),
            "end": _format_month(range_end),
        },
        "partition_count": len(partitions),
        "retained_partitions": retained,
        "deleted_count": len(expired),
        "deleted_partitions": expired,
        "skipped_prefixes": skipped,
    }


def lambda_handler(event, context):
    """Entry point that applies every retention config to its S3 URI."""
    event = event or {}
    s3_uri = event.get("s3_uri", "")
    retention_years = event.get("retention_years", 5)
    results = [apply_retention(s3_uri, retention_years)]
    total = sum(result["deleted_count"] for result in results)
    logger.info(
        "Deleted %d partition(s) across %d prefix(es)",
        total,
        len(results),
    )
    return {
        "reference_date": datetime.now(timezone.utc).date().isoformat(),
        "total_deleted": total,
        "results": results,
    }
