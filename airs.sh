#!/usr/bin/env bash
# AIRS-Lite helper. Run from Git Bash / WSL / macOS / Linux.
#   ./airs.sh up [core|batch|bi ...]   start profiles (default: all)
#   ./airs.sh down                     stop everything (keeps data volumes)
#   ./airs.sh reset                    stop everything AND delete all data volumes
#   ./airs.sh ps | logs <service>      status / logs
#   ./airs.sh wait                     wait until the running services answer
#   ./airs.sh smoke                    Phase 0 smoke test (Spark + Flink + Airflow + Trino on one Iceberg table)
#   ./airs.sh test                     unit tests (pytest inside the Spark container)
#   ./airs.sh catalog                  run the catalog_daily DAG once, now (Phase 1 risk catalog)
#   ./airs.sh init                     Phase 2: create topics, register schemas, create stream tables, publish policies
#   ./airs.sh policies                 publish policies/*.yaml to Flink (applies within seconds)
#   ./airs.sh build-flink              build + unit-test the Flink job jar (Maven in Docker)
#   ./airs.sh submit-flink [--fresh]   (re)submit airs-stream, resuming from its latest checkpoint unless --fresh
#   ./airs.sh simulate [args]          run the fleet simulator, e.g. --rate 50 --duration 300 --scenarios rogue_mcp
#   ./airs.sh stream-check             Phase 2 acceptance checks (dedupe, no raw PII, verdicts, latency)
set -euo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting /opt/... container paths into Windows paths

ALL_PROFILES=(core batch bi stream)
compose_all() { docker compose $(printf -- '--profile %s ' "${ALL_PROFILES[@]}") "$@"; }

[[ -f .env ]] || { echo "Missing .env: copy .env.example to .env and set the secrets."; exit 1; }

wait_http() {  # name url [tries]
  local name=$1 url=$2 tries=${3:-60}
  printf '  waiting for %-18s' "$name"
  for _ in $(seq 1 "$tries"); do
    if curl -fsS "$url" >/dev/null 2>&1; then echo " up"; return 0; fi
    sleep 3; printf '.'
  done
  echo " NOT RESPONDING ($url)"; return 1
}

# Newest completed checkpoint (one with a _metadata file) of any airs-stream run, as s3://... path.
latest_checkpoint() {
  docker run --rm --network airs_default -e AWS_ACCESS_KEY_ID="$(grep ^S3_ACCESS_KEY .env | cut -d= -f2)"     -e AWS_SECRET_ACCESS_KEY="$(grep ^S3_SECRET_KEY .env | cut -d= -f2)" -e AWS_DEFAULT_REGION=us-east-1     amazon/aws-cli:2.38.4 --endpoint-url http://seaweedfs:8333 s3 ls --recursive s3://checkpoints/flink/ 2>/dev/null     | grep '/chk-[0-9]*/_metadata$' | sort -k1,2 | tail -1 | awk '{print $4}' | sed 's|/_metadata$||; s|^|s3://checkpoints/|'
}

running() { compose_all ps --status running --services 2>/dev/null | grep -qx "$1"; }

cmd_wait() {
  running iceberg-rest      && wait_http iceberg-rest  http://localhost:8181/v1/config
  running flink-jobmanager  && wait_http flink         http://localhost:8081/overview
  running redpanda-console  && wait_http redpanda-console http://localhost:8080
  running spark-master      && wait_http spark-master  http://localhost:8082
  running airflow           && wait_http airflow       http://localhost:8088/api/v2/monitor/health 100
  running trino             && wait_http trino         http://localhost:8085/v1/info
  running superset          && wait_http superset      http://localhost:8089/health 100
  return 0
}

step() { echo; echo "── $* ─────────────────────────────────────────────"; }

cmd_smoke() {
  for s in iceberg-rest flink-jobmanager flink-taskmanager spark-master spark-worker airflow trino; do
    running "$s" || { echo "Service '$s' is not running. Start it with: ./airs.sh up core batch bi"; exit 1; }
  done
  cmd_wait

  step "1/5 Spark creates the Iceberg table and writes 2 rows"
  compose_all exec -T spark-master spark-submit --master spark://spark-master:7077 \
    /opt/airs/spark-jobs/smoke/smoke_spark.py init

  step "2/5 Flink SQL appends 2 rows to the same table and reads it back"
  compose_all exec -T flink-jobmanager /opt/flink/bin/sql-client.sh \
    -f /opt/airs/flink-jobs/smoke/smoke_flink.sql

  step "3/5 Airflow runs the hello_spark_iceberg DAG (SparkSubmitOperator)"
  compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
  compose_all exec -T airflow airflow dags test hello_spark_iceberg

  step "4/5 Trino reads every engine's rows"
  local out
  out=$(compose_all exec -T trino trino --output-format=ALIGNED \
    --execute "SELECT engine, count(*) AS n FROM airs.smoke.engine_writes GROUP BY engine ORDER BY engine")
  echo "$out"
  for e in airflow flink spark; do
    grep -q "$e" <<<"$out" || { echo "[smoke] FAILED: Trino does not see rows from $e"; exit 1; }
  done

  step "5/5 Spark verifies rows and Iceberg snapshots"
  compose_all exec -T spark-master spark-submit --master spark://spark-master:7077 \
    /opt/airs/spark-jobs/smoke/smoke_spark.py verify

  echo; echo "✅ Phase 0 smoke test PASSED"
}

case "${1:-help}" in
  up)
    shift
    profiles=("$@"); [[ ${#profiles[@]} -eq 0 ]] && profiles=("${ALL_PROFILES[@]}")
    docker compose $(printf -- '--profile %s ' "${profiles[@]}") up -d --build
    cmd_wait ;;
  down)   compose_all down ;;
  reset)  compose_all down -v ;;
  ps)     compose_all ps ;;
  logs)   shift; compose_all logs -f --tail=200 "$@" ;;
  wait)   cmd_wait ;;
  smoke)  cmd_smoke ;;
  init)
    compose_all run --rm simulator python init_topics.py
    compose_all exec -T trino trino -f /dev/stdin < sql/ddl/stream_tables.sql
    compose_all run --rm simulator python publish_policies.py ;;
  policies) compose_all run --rm simulator python publish_policies.py ;;
  build-flink)
    docker run --rm -v "$PWD/flink-jobs/airs-stream:/src" -v airs_maven-cache:/root/.m2 -w /src       maven:3.9.13-eclipse-temurin-17 mvn -q -B package ;;
  submit-flink)
    for id in $(compose_all exec -T flink-jobmanager flink list -r 2>/dev/null | grep -oE '[0-9a-f]{32} : airs-stream' | cut -d' ' -f1); do
      echo "cancelling running airs-stream job $id"; compose_all exec -T flink-jobmanager flink cancel "$id" >/dev/null
    done
    resume=()
    if [[ "${2:-}" != "--fresh" ]]; then
      cp=$(latest_checkpoint)
      [[ -n "$cp" ]] && { echo "resuming from $cp"; resume=(-s "$cp"); }
    fi
    compose_all exec -T flink-jobmanager flink run -d "${resume[@]}" /opt/airs/flink-jobs/airs-stream/target/airs-stream.jar ;;
  simulate) shift; compose_all run --rm simulator python simulator.py "$@" ;;
  stream-check) compose_all exec -T trino trino --output-format=ALIGNED -f /dev/stdin < sql/checks/stream_checks.sql ;;
  test)   compose_all exec -T -w /opt/airs spark-master python -m pytest -q -p no:cacheprovider tests ;;
  catalog)
    compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
    compose_all exec -T airflow airflow dags test catalog_daily ;;
  *)      sed -n '2,17p' "$0" ;;
esac
