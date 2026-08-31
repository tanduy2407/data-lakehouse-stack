#!/usr/bin/env python3
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import yaml


def validate_config(config: dict) -> dict:
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")

    required_keys = {"bronze", "silver", "gold"}
    missing_keys = required_keys - config.keys()
    if missing_keys:
        raise ValueError(
            "Missing required configuration keys: " +
            ", ".join(sorted(missing_keys))
        )
    for layer in ("bronze", "silver", "gold"):
        layer_config = config[layer]
        if not isinstance(layer_config, dict):
            raise ValueError(f"{layer} must be a mapping")
        if not layer_config.get("s3_uri", "").startswith("s3://"):
            raise ValueError(f"{layer}.s3_uri must start with s3://")
        if layer_config.get("write_mode") not in {"append", "overwrite", "error", "ignore"}:
            raise ValueError(
                f"{layer}.write_mode must be append, overwrite, error, or ignore"
            )
    return config


def load_s3_config(config_uri: str) -> dict:
    bucket, key = config_uri.removeprefix("s3://").split("/", 1)
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    yaml_contents = response["Body"].read().decode("utf-8")
    return validate_config(yaml.safe_load(yaml_contents))


def read_parquet(glue_context: GlueContext, source_file: str):
    return glue_context.spark_session.read.parquet(source_file)


def write_parquet(dataframe, target_uri: str, mode) -> None:
    dataframe.write.mode(mode).parquet(target_uri)


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "CONFIG_FILE", "SOURCE_LAYER", "TARGET_LAYER"],
    )
    config_uri = args["CONFIG_FILE"]
    source_layer = args["SOURCE_LAYER"]
    target_layer = args["TARGET_LAYER"]
    if not config_uri.startswith("s3://"):
        raise ValueError("CONFIG_FILE must start with s3://")
    valid_layers = {"bronze", "silver", "gold"}
    if source_layer not in valid_layers or target_layer not in valid_layers:
        raise ValueError("SOURCE_LAYER and TARGET_LAYER must be bronze, silver, or gold")

    print(f"Loading configuration from {config_uri}")
    config = load_s3_config(config_uri)

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_uri = config[source_layer]["s3_uri"]
    target_config = config[target_layer]
    df = read_parquet(glue_context, source_uri)
    write_parquet(
        df, target_config["s3_uri"], target_config["write_mode"])
    print(
        f"Wrote Parquet data from {source_uri} "
        f"to {target_config['s3_uri']}"
    )

    job.commit()


if __name__ == "__main__":
    main()
