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
set -euo pipefail
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting /opt/... container paths into Windows paths

ALL_PROFILES=(core batch bi)
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
  test)   compose_all exec -T -w /opt/airs spark-master python -m pytest -q -p no:cacheprovider tests ;;
  catalog)
    compose_all exec -T airflow airflow dags reserialize >/dev/null 2>&1 || true
    compose_all exec -T airflow airflow dags test catalog_daily ;;
  *)      sed -n '2,11p' "$0" ;;
esac
