# 06 – Setup & Operations

## 1. Prerequisites

| Requirement | Recommended |
|---|---|
| OS | Windows 11 with WSL2, macOS or Linux |
| Docker Desktop | Latest, with **≥ 8 GB RAM** (10–12 GB recommended), 6 CPUs and 40 GB disk allocated |
| Shell | Git Bash, WSL, macOS or Linux terminal (for `airs.sh`) |
| Java | JDK 17 (only needed on the host to build the Phase 2 Flink job JAR) |
| Python | 3.11 (simulator; Spark and Airflow run in containers) |
| Build tool | Maven or Gradle (Flink job JAR) |
| API tokens | GitHub personal access token (read-only), Hugging Face token (optional; raises rate limits) |

## 2. Version matrix (pinned in Phase 0)

The **same Iceberg version (1.12.0)** is used by Spark and Flink. The REST catalog server speaks the same REST protocol.

| Component | Version | Image / key artefacts |
|---|---|---|
| Apache Flink | 1.20.3 (Java 17) | `airs/flink` = `flink:1.20.3-scala_2.12-java17` + `iceberg-flink-runtime-1.20`, `iceberg-aws-bundle`, `hadoop-client-api/runtime 3.3.6`, `flink-sql-connector-kafka 3.4.0-1.20`, S3 plugin |
| Apache Spark | 3.5.8 (Scala 2.12, Python 3.11) | `airs/spark` = Python 3.11 + Temurin 17 + `pyspark==3.5.8` + `iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle` |
| Apache Iceberg | 1.12.0 | REST catalog: `apache/iceberg-rest-fixture:1.10.1` (SQLite-backed, persisted in a volume) |
| Apache Airflow | 3.3.2 (Python 3.11) | `airs/airflow` = `apache/airflow:3.3.2-python3.11` + Java 17 + `pyspark` + `apache-airflow-providers-apache-spark` |
| Redpanda | v25.3.18 | `redpandadata/redpanda`, Console `redpandadata/console:v3.12.0` |
| SeaweedFS | 4.47 | `chrislusf/seaweedfs`: S3-compatible object storage |
| Trino | 483 | `trinodb/trino`, Iceberg connector against the REST catalog |
| Superset | 6.1.0 | `airs/superset` = `apache/superset:6.1.0` + `trino` driver |
| PostgreSQL | 16.15 | Airflow metadata DB |

> **Why SeaweedFS instead of MinIO?** MinIO's public Docker Hub images were withdrawn, so a fresh `docker pull minio/minio` no longer works. SeaweedFS is actively maintained, implements the S3 API and works with Iceberg's `S3FileIO` (path-style access).
>
> **Why Airflow 3?** Airflow 2.x reached end of life in 2026. Airflow 3 runs here in `standalone` mode (scheduler, DAG processor, API server and triggerer in one container) with the LocalExecutor on Postgres.

## 3. Services (`docker-compose.yml`)

| Service | Profile | Host port(s) | Purpose |
|---|---|---|---|
| `redpanda` | core | 19092 (Kafka), 18081 (Schema Registry), 9644 (admin) | Event bus |
| `redpanda-console` | core | 8080 | Topic, schema and consumer-lag UI |
| `seaweedfs` | core | 8333 (S3 API), 8888 (filer UI) | Object storage: buckets `warehouse`, `checkpoints` |
| `s3-init` | core | – | One-shot job that creates the buckets |
| `iceberg-rest` | core | 8181 | Iceberg REST catalog (catalog name `airs`) |
| `flink-jobmanager` | core | 8081 | Flink UI / REST |
| `flink-taskmanager` | core | – | 1 instance, 4 slots, RocksDB state, checkpoints to `s3://checkpoints/flink` |
| `spark-master` | batch | 7077, 8082 (UI) | Spark standalone master |
| `spark-worker` | batch | 8083 (UI) | 2 cores, 1 GB |
| `postgres` | batch | – | Airflow metadata |
| `airflow` | batch | 8088 | Airflow 3 UI + API (no login in local dev) |
| `trino` | bi | 8085 | SQL engine (catalog `airs`) |
| `superset` | bi | 8089 | Dashboards (user `admin`, password in `.env`) |

Memory limits are set on every container. With Docker Desktop's default WSL2 share (about 8 GB on a 16 GB machine) the whole stack runs (about 4.2 GB used when idle). Later phases run streaming and batch jobs at the same time, so 10 GB+ is more comfortable (see section 9).

## 4. Repository layout

```
airs-lite/
├── docker-compose.yml
├── airs.sh                   # helper: up / down / reset / ps / logs / wait / smoke
├── .env.example              # passwords & tokens (copy to .env, never commit .env)
├── infra/                    # Dockerfiles and configs per service
│   ├── flink/  spark/  airflow/  superset/
│   ├── trino/                # catalog/airs.properties, jvm.config
│   └── redpanda-console/
├── simulator/                # Python fleet generator                         (Phase 2)
├── flink-jobs/               # smoke/ now; airs-stream job (Java/Maven)        (Phase 2)
├── spark-jobs/
│   ├── smoke/                # Phase 0 smoke test
│   └── catalog/              # Phase 1: ingest_*, resolve_entities, score_assets, publish, dq
│       └── seed/             # curated AI services/agents, allow-list, trusted publishers, seed packages
├── airflow/dags/             # hello_spark_iceberg, catalog_daily
├── schemas/                  # Avro schemas (asset_risk_update.avsc)
├── policies/                 # *.yaml policy rules                             (Phase 2)
├── sql/                      # DDL, investigation queries
├── dashboards/               # Superset exports                                (Phase 3)
├── tests/catalog/            # pytest unit tests for normalisation and scoring
└── docs/
```

## 5. Getting started

Run these from **Git Bash** (Windows), WSL, macOS or Linux, in the project folder.

```bash
cp .env.example .env
```
Edit `.env` and replace every `change-me`.

```bash
./airs.sh up core batch bi
```
This builds the custom images (the first run takes several minutes and downloads about 15 GB of images), starts everything and waits until each service answers. All 13 containers use about 4.2 GB of RAM when idle, so an 8 GB Docker allocation is enough for Phase 0.

```bash
./airs.sh smoke
```
This runs the Phase 0 smoke test (section 5.1).

```bash
./airs.sh down
```
This stops everything but keeps the data. `./airs.sh reset` also deletes all volumes.

### 5.1 What the smoke test proves
1. **Spark** creates `airs.smoke.engine_writes` (an Iceberg v2 table, partitioned by `engine`) through the REST catalog and writes 2 rows to SeaweedFS.
2. **Flink SQL** connects to the same catalog, appends 2 rows and reads back all rows.
3. **Airflow** runs the DAG `hello_spark_iceberg`. Its `SparkSubmitOperator` submits a job to the Spark cluster, which appends 2 more rows.
4. **Trino** reads rows from all three writers.
5. **Spark** checks that all three engines wrote and that there is one Iceberg snapshot per commit.

### 5.2 Phase 1: build the risk catalog

```bash
./airs.sh catalog
```
This runs the `catalog_daily` DAG once, immediately (`airflow dags test`). The first run takes about 20–25 minutes on a slow connection; later runs are incremental. In the Airflow UI the DAG is paused by default; unpause it to run nightly at 02:00.

```bash
./airs.sh test
```
This runs the unit tests (pytest inside the Spark container).

Optional: put a GitHub personal access token (no scopes needed) in `.env` as `GITHUB_TOKEN=...`. That raises GitHub's limit from 60 to 5,000 requests per hour, so all linked repos are enriched in one run instead of 50 per run. Other caps (`CATALOG_MCP_MAX_PAGES`, `CATALOG_HF_MODELS`, `CATALOG_HF_DATASETS`) can also be set in `.env`.

Look at the result in Trino (http://localhost:8085 or `docker compose --profile bi exec trino trino`):
```sql
SELECT band, count(*) FROM airs.silver.dim_asset_risk WHERE is_current GROUP BY 1;
```

### 5.3 Commands planned for later phases
`./airs.sh init` (topics, schemas) · `seed` · `submit-flink` · `simulate` · `demo-crash` · `demo-tamper`

Then open Superset (http://localhost:8089), the Flink UI (http://localhost:8081), the Airflow UI (http://localhost:8088) and Redpanda Console (http://localhost:8080).

## 6. Monitoring

| What | Where | Healthy when |
|---|---|---|
| Flink job status and checkpoints | Flink UI / REST `/jobs/:id/checkpoints` | RUNNING; last checkpoint < 2 min old; duration < 10 s |
| Consumer lag | Redpanda Console | Lag on `ai-events-raw` < 10k and not growing |
| Verdict latency | `gold.pipeline_health` | p95 < 1 s |
| Late / DLQ / unknown-asset % | `gold.pipeline_health` | late < 5%, DLQ < 0.1%, unknown < 2% |
| Batch runs | Airflow UI | All DAGs green; `audit_verify` never red |
| Table health | Spark `…files` / `…snapshots` metadata tables | Average file size > 64 MB after compaction; snapshots < 100 |

## 7. Testing strategy

| Level | What | Tools |
|---|---|---|
| Unit | Redaction detectors (true and false positives), Luhn, scoring rules, entity-resolution normalisation | JUnit (Flink), pytest (Spark/Python) |
| Operator | Dedupe, watermarks, broadcast enrichment, session windows | Flink test harness (`KeyedOneInputStreamOperatorTestHarness`) |
| Integration | Simulator → Redpanda → Flink → Iceberg row counts | Testcontainers or the compose stack in CI |
| Data quality | The checks in [04 – Pipelines, Part E](04-pipelines.md#part-e-data-quality-checks) | Airflow tasks |
| Chaos demos | TaskManager kill, offline-device burst, schema evolution, audit tampering | `airs.sh` commands |

CI (GitHub Actions): lint, run unit tests, build the Flink JAR, run Spark job tests with local mode and the Iceberg Hadoop catalog.

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Flink commits nothing to Iceberg | Checkpointing disabled (the Iceberg sink commits on checkpoint) | Enable checkpointing |
| `ClassNotFoundException` for Iceberg/S3 | Missing runtime or AWS bundle JARs | Add `iceberg-flink-runtime-1.20` + `iceberg-aws-bundle` to `/opt/flink/lib` |
| Spark and Flink see different tables | Different catalog configs | Point both at `http://iceberg-rest:8181`, same warehouse |
| Watermark doesn't advance | Idle Redpanda partitions | `withIdleness(Duration.ofMinutes(1))` |
| Thousands of tiny Parquet files | Frequent checkpoints | Run the `iceberg_maintenance` DAG; raise the checkpoint interval |
| Docker becomes very slow | Not enough RAM | Start only the profiles you need; 1 Spark worker with 2 GB |
| GitHub API 403 | Rate limit | Use a token; cache responses; incremental ingestion |
| `pip install pyspark` times out during `docker build` | Slow connection; the wheel is about 300 MB | The Dockerfiles already use `--default-timeout=600 --retries 10`; just re-run the build |
| `curl: (23) ... ERROR on write` in Git Bash | `MSYS_NO_PATHCONV=1` stops Windows curl understanding `-o /dev/null` | Redirect with `>/dev/null` instead (as `airs.sh` does) |
| `Invalid call to dataType on unresolved object` | `F.nullif` bug in PySpark 3.5 | Use `F.when(col != value, col)` (see `resolve_entities.nullif`) |
| `ClassCastException: Integer cannot be cast to Short` in py4j | py4j turns a Java `Short` into a Python int | Avoid passing Java `Short` through Python (replication factor left to the broker) |
| Catalog jobs slow on tiny tables | 200 default shuffle partitions | `spark.sql.shuffle.partitions 8` in `spark-defaults.conf` |
| `docker pull minio/minio` fails | MinIO's Docker Hub images were withdrawn | Use SeaweedFS (already configured) |
| `/opt/...` paths become `C:/Program Files/Git/opt/...` | Git Bash path conversion | `export MSYS_NO_PATHCONV=1` before `docker compose exec` (as `airs.sh` does) |

## 9. Giving Docker more memory (Windows / WSL2)

By default WSL2 gives Docker about half of the machine's RAM. To raise it, create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=11GB
processors=8
swap=4GB
```

Then restart WSL from PowerShell and start Docker Desktop again:

```bash
wsl --shutdown
```
