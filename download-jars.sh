#!/usr/bin/env bash
set -euo pipefail

JARS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/data/hive/jars" && pwd)"
mkdir -p "$JARS_DIR"

ensure_jar() {
  local file_name="$1"
  local url="$2"
  local target="$JARS_DIR/$file_name"

  if [[ -s "$target" ]]; then
    echo "Jar exists, skip: $file_name"
    return 0
  fi

  echo "Downloading: $file_name"
  curl -fL --retry 3 --retry-delay 2 -o "$target" "$url"
}

ensure_jar "postgresql-42.7.4.jar" "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
ensure_jar "hadoop-aws-3.3.6.jar" "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"
ensure_jar "aws-java-sdk-bundle-1.12.367.jar" "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar"

echo "All required jars are ready in $JARS_DIR"
