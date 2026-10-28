# SuperAlign: Company Research and a Data Engineering Portfolio Project

*Researched from superalign.ai, airiskdb.com and their blog on 25 Oct 2026.*

---

## 1. What SuperAlign does

**One line:** an AI security platform for enterprises. It finds every AI tool, agent, model, MCP server, skill and browser extension running across a company's devices, gives each one a risk score, and blocks or warns on risky actions before they finish.

Their tagline is "Your control plane for AI."

### The problem they go after
- Employees and their agents bring AI into a company through tools IT never approved (Cursor, Claude, ChatGPT, local Llama, random MCP servers, browser extensions). This is **"Shadow AI."**
- By their claim, **63% of AI usage never crosses the network**, so gateways and SASE tools can't see it. Local models and MCP servers run on the laptop itself.
- SIEM and EDR tools don't log "which AI was active, what data it touched, what tool it called," which leaves a forensic gap.

### Product components (their names)
| Component | What it does | The data side of it |
|---|---|---|
| **Surface** ("north-star") | A lightweight agent on macOS/Win/Linux, a browser extension and a network sensor, pushed out through MDM (Jamf/Intune). Captures each AI event at prompt level. | High-volume event telemetry from endpoints, correlated across device + browser + network into "one correlated stream" |
| **AIRiskDB** (airiskdb.com) | A catalog of **1M+ AI assets in 8 categories** (models, agents, MCP servers, tools/skills, datasets, extensions, AI SaaS, identities). Each one gets an **AIRS score** on 3 axes: **Capability, Exposure, Governance**, scored *statically and dynamically*. Bands: Critical 9.0+, High 7–8.9, Medium 5–6.9, Low <5. Mapped to EU AI Act, NIST AI RMF, ISO 42001, MITRE ATLAS, OWASP LLM Top 10. | Large-scale ingestion from many sources, entity resolution, a scoring pipeline, and a low-latency lookup ("detection is a lookup, not a research project", <25 ms) |
| **Horizon** (Halo + Prism) | Small on-device models. **Halo** classifies threat/intent (jailbreaks, prompt injection). **Prism** detects PII/PHI/PCI and redacts dynamically. Currently a research preview. | Labeled evaluation data, human-in-the-loop voting ("Horizon Line"), an open safety benchmark |
| **Radar** | Network timeline of what data left and where it went | Network flow logs joined with endpoint data |
| **Sentinel** | Detection on the Surface stream | Streaming detection rules |
| **Signal** | An **immutable, sealed record** of every incident, with the asset's risk profile attached | Append-only, tamper-evident audit storage |
| **Console** | Fleet risk rollups, board reporting, policy, AI ROI (idle licences, tool overlap, adoption by department) | Gold-layer analytics / BI |

### Use cases they sell (9)
Shadow AI discovery · AI ROI · AI agent & MCP security · Data leakage via AI · Prompt injection / agentic attacks · AI policy enforcement · Compliance & audit readiness · Third-party AI vendor risk · **AI incident investigation**.

### Integrations
SIEM (Splunk, MS Sentinel, Elastic, Chronicle, Datadog) · SOAR (Tines, Torq, ServiceNow, Jira, PagerDuty) · MDM (Jamf, Intune, Workspace ONE, Kandji) · IdP (Okta, Entra ID, Google Workspace, SCIM).

### Go-to-market
A free two-week proof of value on 25–100 users. Day 1 the agent is installed, Week 1 brings the full inventory plus one session rebuilt end to end, Week 2 is the findings review.

### What they are planning (stated direction)
1. **Surface is the north star**: become *the* endpoint telemetry layer for AI ("the endpoint telemetry for AI that no one else has").
2. **AIRiskDB as a moat**: keep profiling new assets continuously (target: a median of 24h to profile a new asset, 95% of fleet assets already scored).
3. **Project Horizon roadmap**: open-weight releases of Halo and Prism, an open-source safety benchmark, enterprise policy engines with real-time customization, and **multimodal + agentic** safety layers.
4. **Compliance as a product**: turn risk scores into audit evidence (EU AI Act, ISO 42001).
5. Their research blog (16 posts) puts heavy weight on **MCP security, AI supply-chain attacks** (the LiteLLM PyPI poisoning, malicious agent markdown/skills files), **prompt injection**, and **autonomous AI pentest agents** (the Oct 2026 Korean banks post).

> Note: many numbers on their site are explicitly tagged "illustrative." The product looks early-stage and is moving fast. I couldn't find a careers page (`/careers` returns 404) or founder/funding information on the site. Worth checking on LinkedIn.

---

## 2. Where a data engineer fits at SuperAlign

Underneath, SuperAlign is a **data company**. Every product claim depends on a pipeline:

- **Telemetry ingestion at scale**: events from thousands of endpoints, which may be **offline and sync later**. That means late-arriving data, event time vs. processing time, and deduplication.
- **Correlation**: joining device, browser and network events into one session per identity ("cross-layer reconstruction without manual correlation").
- **AIRiskDB**: crawling and merging data about 1M+ assets from many public sources, then keeping the scores fresh (that's what "dynamic scoring" means).
- **Point-in-time correctness**: an investigation has to show "authorization status *at the time of the event*", which is a classic SCD Type 2 / as-of join problem.
- **Immutable records** for audit, and **redaction at the source** for privacy.
- **Analytics** for the Console (ROI, posture) and **exports** to SIEMs.

The project below copies this architecture end to end, at small scale.

---

## 3. The project: **"Tracewell: an AI Asset Lineage & Risk Lakehouse"**

> *A streaming + batch data platform that ingests simulated AI endpoint telemetry, enriches every event with a self-built AI asset risk catalog (built from real public data), rebuilds sessions, redacts PII at ingest, and serves fleet risk, shadow-AI, ROI and incident-investigation analytics, with point-in-time correct history and a tamper-evident audit log.*


### Tech stack and what each tool does

| Tool | Role in the project | Why it's the right tool here |
|---|---|---|
| **Redpanda** (the Kafka alternative) | Event bus: `ai-events-raw`, `asset-risk-updates` (compacted), `verdicts`, `late-events` | Speaks the Kafka API, so Flink's Kafka connector works unchanged. Single binary with no ZooKeeper or KRaft. Schema Registry and a web console are built in. Light enough for a laptop. |
| **Apache Flink** | **Real-time path**: redaction, dedupe, enrichment, policy verdicts, sessionization, streaming writes into Iceberg | True event-time processing with watermarks, keyed state, broadcast state and exactly-once checkpoints. This is the "<25 ms verdict" path. |
| **Apache Spark** | **Batch path**: building the risk catalog, entity resolution, risk scoring, SCD2 `MERGE INTO`, gold marts, incident investigation, Iceberg maintenance | Large joins, fuzzy matching and SQL on the lakehouse. Spark has the most complete Iceberg procedure support. |
| **Apache Iceberg** (on SeaweedFS S3 storage, with a REST catalog or Nessie) | The single table format both engines write to and read from | ACID tables shared by Flink and Spark, plus `MERGE INTO`, **time travel**, hidden partitioning, schema evolution and compaction. |
| **Apache Airflow** | Orchestrates every batch job, data-quality check and maintenance task | Production-standard scheduler. Runs jobs through `SparkSubmitOperator`, with sensors and retries. |
| Trino + Superset *(or Streamlit)* | Querying and dashboards over Iceberg | The "Console-lite" view |
| SeaweedFS (S3), Docker Compose | S3-compatible storage and the local environment | Runs everything on one machine |


### Architecture

```
 ┌──────────────── BATCH  (Airflow DAG: catalog_daily → SparkSubmit) ─────────────────┐
 │ Hugging Face Hub API ─┐                                                             │
 │ Official MCP Registry ┼─► Iceberg bronze ─► Spark: entity ─► Spark: risk scoring  ─► │
 │ GitHub API (MCP repos)┤   (raw JSON)        resolution       (static + dynamic      │
 │ OSV.dev advisories    ┤                     → dim_asset       exposure from silver) │
 │ PyPI / npm metadata  ─┘                                              │              │
 │                         Iceberg: dim_asset_risk (SCD Type 2 via MERGE INTO)         │
 └──────────────────────────────────────────────────────────────┬──────────────────────┘
                     Spark publishes changed scores ───────────┐ │
                                                               ▼ │
 ┌──────────────────────────── STREAMING (Apache Flink) ─────────┼──────────────────────┐
 │ Fleet simulator ──► Redpanda topic      ┌─ Redpanda compacted topic                  │
 │ (500 devices,       ai-events-raw       │  asset-risk-updates → BROADCAST STATE      │
 │  device/browser/    (Avro, Schema       ▼                                            │
 │  network sensors,   Registry) ──► Flink job:                                         │
 │  offline bursts,                  1. PII/PCI redaction (regex + Luhn)                │
 │  dupes, late data)                2. event-time watermarks + dedupe (keyed state+TTL)│
 │                                   3. enrich with asset risk (broadcast join)         │
 │                                   4. policy verdict allow / warn / block             │
 │                                   5. session windows per identity+device             │
 │                                   6. very-late events → side output topic            │
 └───────────────┬──────────────────────────────┬────────────────────────┬──────────────┘
                 ▼                              ▼                        ▼
   Iceberg silver (Flink Iceberg sink,   Iceberg audit_log          Redpanda `verdicts`
   exactly-once via checkpoints):        (hash-chained,             → OCSF export for SIEM
   fct_ai_events, fct_sessions           "Signal-lite")
                 │
                 ▼
   Airflow DAGs → Spark SQL gold marts, data-quality checks, Iceberg maintenance
   (rewrite_data_files compaction, expire_snapshots, remove_orphan_files)
                 │
                 ▼
   Trino + Superset dashboards (shadow AI, fleet risk, AI ROI, incident investigation)
```

### Airflow DAGs
| DAG | Schedule | Tasks |
|---|---|---|
| `catalog_daily` | daily | Ingest each source (in parallel) → Spark entity resolution → Spark risk scoring + SCD2 `MERGE INTO` → publish changed scores to `asset-risk-updates` → data-quality checks |
| `new_asset_profiling` | hourly | Read unknown assets that Flink flagged → look them up in the sources → score → publish (target: profiled within 24h, like AIRiskDB) |
| `gold_marts` | hourly | Spark SQL: shadow AI, fleet risk posture, AI ROI, compliance evidence |
| `iceberg_maintenance` | nightly | Compact the small files Flink writes, expire snapshots, remove orphan files |
| `audit_verify` | nightly | Recompute the hash chain and alert if any record was changed |
| `flink_job_health` | every 15 min | Check the Flink REST API for job status, checkpoint age and consumer lag in Redpanda |

### Components in detail

**A. Risk catalog ("AIRiskDB-lite"): real public data**
- **Sources:** Hugging Face Hub API (models/datasets: licence, gated flag, downloads, tags, author), the official **MCP Registry** (`registry.modelcontextprotocol.io`) plus GitHub repos for MCP servers (stars, last commit, licence, declared tools), **OSV.dev** for known vulnerabilities in AI packages (e.g. the LiteLLM 1.82.7/1.82.8 compromise from their blog), PyPI/npm metadata.
- **Entity resolution:** the same asset shows up under different names (`mcp-server-postgres`, `@modelcontextprotocol/server-postgres`, GitHub URL). Build a canonical `asset_id` using normalization and fuzzy matching.
- **AIRS-style scoring (your own transparent version):**
  - *Capability:* declared tools (filesystem / shell / network / DB write = higher), code execution, autonomy.
  - *Exposure:* how often it's seen in the fleet, how many identities use it, sensitivity of the data it touches (from telemetry, so this is the **dynamic** part).
  - *Governance:* licence, verified publisher, maintenance recency, open CVEs, sanctioned or not in your policy.
  - Band it as Critical/High/Medium/Low, and map each factor to a framework control (OWASP LLM Top 10 / NIST AI RMF) in a small crosswalk table.
- **SCD Type 2 history:** every score change is a new version with `valid_from` / `valid_to`, written with Spark `MERGE INTO` on Iceberg.
- **Publishing to the stream:** after scoring, Spark writes only the *changed* scores to the compacted Redpanda topic `asset-risk-updates`. Flink picks them up within seconds, with no restart.

**B. Fleet telemetry simulator**
- Python/Faker generator for ~500 devices and ~300 identities across departments. It emits events that follow the anatomy shown on their Surface page: `event_id, device_id, identity, sensor (device|browser|network), event_type (asset_discovered | prompt | file_upload | paste | mcp_tool_call | egress), asset_ref, prompt_text, file_name, bytes, ts_event`.
- **Realistic dirt on purpose:** devices that go offline for hours and then flush a batch (late data), duplicate sends, clock skew, unknown/new assets that aren't in the catalog yet, and prompts that contain fake PII/card numbers (Faker + Luhn-valid *test* card numbers).
- A few scripted "attack stories": a developer installs an unknown MCP server and it reads `customers.csv`; an analyst uploads `finance_q3.xlsx` to an unsanctioned chatbot; an agent calls a poisoned package version.

**C. Streaming pipeline (Apache Flink: Java, or PyFlink if you're more comfortable in Python)**
1. **Redaction at ingest** (regex + Luhn check for cards, email, phone, Aadhaar/PAN, API keys): raw prompt text never lands in the lake. Store the redacted text plus entity counts. *(This mirrors their "redaction-aware capture" and Prism.)*
2. **Event-time watermarks + dedupe:** bounded out-of-orderness watermarks on `ts_event`. Dedupe on `event_id` using keyed `ValueState` with a state TTL. Events that arrive after the allowed lateness go to a **side output** (the `late-events` topic) instead of being dropped silently.
3. **Enrichment with broadcast state:** the risk catalog stream from `asset-risk-updates` is broadcast to every parallel task and joined with events in a `KeyedBroadcastProcessFunction`. Unknown assets get a provisional "Unknown / High" score and go to a `new_asset_queue` for the hourly Airflow profiling DAG.
4. **Policy engine:** rules in YAML (e.g. `PCI data + asset governance < 5 → block`), also loaded through broadcast state so policies change without a redeploy. Output an `allow/warn/block` verdict and measure p95 latency from event to verdict.
5. **Sessionization:** event-time **session windows** per identity+device (e.g. a 30-minute gap). These link device, browser and network events into one session (the "session reconstructed end to end" from their PoV).
6. **Sinks:** Iceberg silver tables through the Flink Iceberg sink (exactly-once, committed on each checkpoint), plus the `verdicts` topic and the hash-chained audit table.
7. **Fault tolerance demo:** kill the TaskManager mid-stream, restart from the last checkpoint, and show no lost or duplicated rows in Iceberg.

**D. Lakehouse + Spark SQL gold models (Console-lite)**
- Medallion layers in Iceberg: `bronze` (raw source JSON and raw events), `silver` (cleaned, redacted, enriched), `gold` (marts).
- Iceberg features to show off: **hidden partitioning** (`days(ts_event)`, `bucket(16, device_id)`), **schema evolution** (add a column while Flink keeps writing), **time travel** (`FOR TIMESTAMP AS OF`), and **small-file compaction** for streaming writes.
- `fct_ai_events`, `fct_sessions`, `dim_asset` (SCD2), `dim_identity`, `dim_device`
- `mart_shadow_ai`: assets seen but unsanctioned, first-seen date, and how many were *never visible on the network sensor* (device-only).
- `mart_fleet_risk_posture`: risk score distribution by department, trend over time.
- `mart_ai_roi`: active users per tool per department, idle licences, overlapping tools (ChatGPT + Claude + Copilot in the same team).
- `mart_compliance_evidence`: per-asset framework mapping export.
- **Incident investigation (Spark job):** "For incident X, show what was installed on the device, its risk score **as of the event time** (an as-of join on SCD2, *not* today's score), the session timeline, and the blast radius (other devices running the same asset)." For a second proof, use Iceberg time travel to show the catalog exactly as it looked on the incident date.

**E. Signal-lite: tamper-evident audit log**
- Every block/warn verdict is written to an append-only table where `hash = sha256(prev_hash + record)`.
- A verification job recomputes the chain and flags any edited row. This is a small feature, but it gives you a strong talking point.

**F. Quality, observability, export**
- Avro schemas in the Redpanda Schema Registry, with a schema evolution demo (add a field without breaking the Flink consumer).
- Data-quality checks as Airflow tasks (Spark-based, or Great Expectations / Soda): no raw PII in silver, catalog freshness, referential integrity, row counts between Redpanda and Iceberg.
- Pipeline metrics: events/sec, Redpanda consumer lag, Flink checkpoint duration and age, late-event %, unknown-asset %.
- Export alerts in **OCSF** format (the open security schema SIEMs use) to show you understand how the data plugs into Splunk/Sentinel.

### Docker Compose services (local)
`redpanda` + `redpanda-console` · `flink-jobmanager` + `flink-taskmanager` · `spark-master` + `spark-worker` · `seaweedfs` · `iceberg-rest` (or `nessie`) · `airflow` (webserver, scheduler, Postgres metadata DB) · `trino` · `superset` · `simulator`

> **Laptop note (Windows + Docker Desktop):** give Docker at least 12–16 GB RAM. Use 1 Flink TaskManager and 1 Spark worker, and run the heavy Spark batch jobs while the stream is at low volume. Pin matching versions: Flink 1.20.x + `iceberg-flink-runtime-1.20`, Spark 3.5.x + `iceberg-spark-runtime-3.5_2.12`, and the same Iceberg version for both.

### Suggested repo layout
```
tracewell/
├── docker-compose.yml
├── simulator/            # fleet telemetry generator (Python → Redpanda)
├── flink-jobs/           # redaction, enrichment, sessionization, Iceberg sink
├── spark-jobs/           # ingestion, entity resolution, scoring, marts, maintenance
├── airflow/dags/         # catalog_daily, gold_marts, iceberg_maintenance, ...
├── schemas/              # Avro schemas
├── policies/             # YAML policy rules
├── dashboards/           # Superset exports
└── docs/                 # architecture diagram, design decisions, benchmarks
```

### Build plan
| Phase | Deliverable | Tools | Must-have? |
|---|---|---|---|
| 0 | Docker Compose environment up; Spark and Flink both read and write the same Iceberg table | All | **Yes** |
| 1 | Risk catalog: HF + MCP Registry + OSV ingestion → scored `dim_asset_risk` with SCD2, run by Airflow | Spark, Iceberg, Airflow | **Yes** (most original part) |
| 2 | Simulator → Redpanda → Flink redaction + dedupe + enrichment → Iceberg silver | Redpanda, Flink, Iceberg | **Yes** |
| 3 | Spark gold marts + Superset dashboard (shadow AI, risk posture, ROI) | Spark, Airflow, Trino, Superset | **Yes** |
| 4 | Flink session windows, late-event side output, incident investigation with as-of joins and time travel | Flink, Spark, Iceberg | Strongly recommended |
| 5 | Score updates flowing live into Flink broadcast state, compaction DAG, hash-chained audit log, OCSF export, checkpoint-recovery demo | All | Nice to have |

**If you're short on time:** get phases 0–3 working end to end first, with simple versions of each step, then deepen them. A finished, clean, documented project beats a half-built "big" one.

### README must include
- An architecture diagram and a "why this design" section (trade-offs).
- A **"Mapping to SuperAlign"** table: my component → their component (Surface / AIRiskDB / Signal / Console / Radar).
- Numbers you actually measured (events/sec, p95 enrichment latency, % late events handled, catalog size).
- A "What I'd do at production scale" section (below).

---

Full project documentation lives in [README.md](README.md) and the [docs/](docs/) folder.
