# 07 – Roadmap

Each phase ends with something that **runs end to end** and a checklist that proves it.

```mermaid
gantt
    title AIRS-Lite build plan (indicative)
    dateFormat  YYYY-MM-DD
    axisFormat  %d %b
    section Foundation
    Phase 0 · Environment            :p0, 2026-10-27, 3d
    section Core
    Phase 1 · Risk catalog            :p1, after p0, 5d
    Phase 2 · Streaming pipeline      :p2, after p1, 6d
    Phase 3 · Gold marts + dashboards :p3, after p2, 4d
    section Depth
    Phase 4 · Sessions + investigation:p4, after p3, 4d
    Phase 5 · Hardening & demos       :p5, after p4, 5d
```

## Phase 0: Environment
**Build:** Docker Compose with Redpanda, SeaweedFS (S3), the Iceberg REST catalog, Flink, Spark, Airflow, Trino and Superset. Add the `airs.sh` helper and `.env.example`.
**Status: ✅ done (25 Oct 2026)**
- [x] All services are healthy with one command (`./airs.sh up`)
- [x] Spark creates an Iceberg table, Flink appends to it, and Trino reads both writes
- [x] Airflow runs a hello-world `SparkSubmitOperator` DAG (`hello_spark_iceberg`)
- [x] Extra checks: Superset → Trino query, Redpanda produce/consume, catalog survives a restart, smoke test can be re-run

**Measured:** all 13 containers together use about 4.2 GB of RAM (Docker limit 8 GB). A full smoke run takes about 4 minutes.

## Phase 1: Risk catalog (Spark + Iceberg + Airflow)
**Build:** the 5 ingestion jobs, entity resolution, AIRS-Lite scoring, the SCD2 merge, and the `catalog_daily` DAG.
**Status: ✅ done (25 Oct 2026)**
- [x] ≥ 5,000 assets in `silver.dim_asset_risk`, each with a score, band and reasons: **22,063 assets** from 5 public sources + 23 curated
- [x] A second run with changed data creates new SCD2 versions (old version closed, one current row); an unchanged run creates none; retired assets are closed and tombstoned
- [x] Entity-resolution precision and recall reported every run (against declared registry links instead of a hand-labelled sample): **precision 0.999, recall 0.77**
- [x] Data-quality checks pass in Airflow: 16 checks, all fatal ones green
- [x] Extra: scores published to the compacted Redpanda topic `asset-risk-updates` (Avro, Schema Registry), ready for Flink in Phase 2; 31 unit tests

**Measured:** a full DAG run takes about 27 min on a slow home connection (most of it is the package API calls). The MCP Registry backfill resumes 6,000 servers per run (18,000 so far).
**Known limits:** without a `GITHUB_TOKEN` only 50 repos are enriched per run; registry descriptions are short, so keyword-based capability signals are rough; exposure is static until Phase 2 adds fleet telemetry.

## Phase 2: Streaming pipeline (Redpanda + Flink + Iceberg)
**Build:** the simulator, Avro schemas, and the Flink job (redact → dedupe → enrich with broadcast state → policy → sinks). Spark publishes scores to `asset-risk-updates`.
**Done when:**
- [ ] Events flow into `silver.fct_ai_events` with scores and verdicts
- [ ] 0 raw PII found by the no-PII check
- [ ] Duplicates are removed (count verified)
- [ ] Changing a score in the catalog changes verdicts within seconds, without restarting Flink
- [ ] p95 verdict latency is measured and recorded

## Phase 3: Gold marts + dashboards (Spark + Airflow + Trino + Superset)
**Build:** the `gold_marts` DAG and 4 Superset dashboards (Shadow AI, Risk Posture, AI ROI, Pipeline Health).
**Done when:**
- [ ] Dashboards update hourly from Airflow
- [ ] The scripted scenarios (rogue MCP, finance upload) are visible on the dashboards

## Phase 4: Sessions + investigation (Flink + Spark + Iceberg time travel)
**Build:** session windows, the late-event side output and backfill, `investigate.py`, and an Investigation dashboard.
**Done when:**
- [ ] An offline device flushing 12 h of events produces correct sessions
- [ ] An investigation shows *score at event time* vs *score today*, plus blast radius
- [ ] Time-travel results agree with the SCD2 as-of join

## Phase 5: Hardening & demos
**Build:** the hash-chained audit log + `audit_verify`, the `iceberg_maintenance` DAG, the `flink_job_health` DAG, OCSF export, the schema-evolution demo, CI, and a short benchmark report.
**Done when:**
- [ ] `./airs.sh demo-crash`: row counts are identical after the TaskManager is killed (exactly-once)
- [ ] `./airs.sh demo-tamper`: `audit_verify` detects the edited row
- [ ] Compaction cuts the file count by more than 80% on streaming tables
- [ ] Adding a new event field doesn't break the running Flink job
- [ ] `docs/benchmarks.md` records events/s, p95 latency, late %, catalog size

## Later ideas (optional)
- An ML-based prompt classifier (prompt-injection detection) as a Flink async operator
- Multimodal events (image uploads)
- Kubernetes deployment (Flink Kubernetes Operator, Spark on K8s) and Terraform for cloud object storage
- A managed Iceberg catalog (Polaris / Glue) and a cloud deployment
