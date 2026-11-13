#!/usr/bin/env bash
# Submit the user behavior marketing jar to a Flink cluster.
# Usage: ./submit-job.sh [--<config.key>=<value> ...]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${SCRIPT_DIR}/../target/behavior-app-1.0.0-SNAPSHOT.jar"
MAIN_CLASS="org.example.behavior.app.UserBehaviorMarketingJob"

if [[ ! -f "${JAR}" ]]; then
  echo "Jar not found: ${JAR}. Run 'mvn clean package -DskipTests' first." >&2
  exit 1
fi

exec flink run -c "${MAIN_CLASS}" "${JAR}" "$@"
