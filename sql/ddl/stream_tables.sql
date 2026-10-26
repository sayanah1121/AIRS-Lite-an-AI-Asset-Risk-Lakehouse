-- Phase 2 streaming tables (Trino DDL: Flink SQL cannot declare hidden partition transforms).
-- Run with: ./airs.sh init

CREATE SCHEMA IF NOT EXISTS airs.bronze;
CREATE SCHEMA IF NOT EXISTS airs.silver;

-- Every event as received, after redaction, before dedupe (so duplicates are visible here).
CREATE TABLE IF NOT EXISTS airs.bronze.ai_events (
  event_id         varchar,
  ts_event         timestamp(6) with time zone,
  ts_sent          timestamp(6) with time zone,
  device_id        varchar,
  os               varchar,
  identity         varchar,
  department       varchar,
  sensor           varchar,
  event_type       varchar,
  asset_ref        varchar,
  asset_version    varchar,
  tool_name        varchar,
  prompt_text      varchar,        -- redacted
  file_name        varchar,
  file_size_bytes  bigint,
  dest_host        varchar,
  bytes_out        bigint,
  sensitive_types  array(varchar),
  sensitive_count  integer,
  ingested_at      timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(ts_event)']);

-- Deduplicated, enriched events with the risk profile that was valid when the event was processed.
CREATE TABLE IF NOT EXISTS airs.silver.fct_ai_events (
  event_id               varchar,
  ts_event               timestamp(6) with time zone,
  ts_sent                timestamp(6) with time zone,
  device_id              varchar,
  os                     varchar,
  identity               varchar,
  department             varchar,
  sensor                 varchar,
  event_type             varchar,
  asset_ref              varchar,
  asset_id               varchar,
  asset_known            boolean,
  asset_category         varchar,
  airs_score_at_event    double,
  band_at_event          varchar,
  sanctioned_at_event    boolean,
  catalog_valid_from     timestamp(6) with time zone,   -- which SCD2 version of the risk profile was used
  asset_version          varchar,
  tool_name              varchar,
  prompt_text            varchar,                        -- redacted
  file_name              varchar,
  dest_host              varchar,
  bytes_out              bigint,
  sensitive_types        array(varchar),
  sensitive_count        integer,
  policy_id              varchar,
  action                 varchar,                        -- allow | warn | block
  latency_ms             bigint,                         -- ts_sent -> verdict
  processed_at           timestamp(6) with time zone
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(ts_event)', 'bucket(device_id, 16)']);
