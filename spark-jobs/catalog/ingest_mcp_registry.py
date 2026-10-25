"""Ingest the official MCP Registry into bronze.mcp_registry_raw.

The registry holds tens of thousands of servers and serves about 100 per request, so ingestion is
resumable:
  * backfill  - walk the full list with ?cursor=..., at most --max-pages per run, saving the cursor
                in ops.pipeline_state so the next run continues where this one stopped;
  * incremental - once the backfill is complete, only fetch servers with ?updated_since=<last run>.
Only the latest version of each server is requested (?version=latest).
"""
import argparse
import urllib.parse
from datetime import timedelta

from common import (env_int, get_state, http_json, log, new_run_id, set_state, spark_session,
                    utcnow, write_bronze)

API = "https://registry.modelcontextprotocol.io/v0/servers"
COMPONENT = "ingest_mcp_registry"
PAGE_SIZE = 100
FLUSH_EVERY = 20  # pages per bronze commit, so a crash loses little work


def page_url(cursor: str | None, updated_since: str | None) -> str:
    params = {"limit": PAGE_SIZE, "version": "latest"}
    if cursor:
        params["cursor"] = cursor
    if updated_since:
        params["updated_since"] = updated_since
    return f"{API}?{urllib.parse.urlencode(params)}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=env_int("CATALOG_MCP_MAX_PAGES", 60))
    args = ap.parse_args()

    spark = spark_session("ingest-mcp-registry")
    run_id = new_run_id()
    run_started = utcnow()

    backfill_done = get_state(spark, COMPONENT, "backfill_done") == "true"
    if backfill_done:
        mode, cursor = "incremental", None
        updated_since = get_state(spark, COMPONENT, "updated_since")
    else:
        mode, cursor, updated_since = "backfill", get_state(spark, COMPONENT, "backfill_cursor"), None
        if not cursor:
            set_state(spark, COMPONENT, "backfill_started_at", run_started.isoformat() + "Z")
    log(f"mode={mode} cursor={cursor} updated_since={updated_since} max_pages={args.max_pages}")

    buffer, pages, total, finished = [], 0, 0, False
    while pages < args.max_pages:
        data, _ = http_json(page_url(cursor, updated_since))
        pages += 1
        for item in data.get("servers", []):
            buffer.append((item["server"]["name"], item))
        cursor = data.get("metadata", {}).get("nextCursor")
        if not cursor:
            finished = True
        if len(buffer) and (pages % FLUSH_EVERY == 0 or finished):
            total += write_bronze(spark, "airs.bronze.mcp_registry_raw", run_id, "mcp_registry", buffer)
            buffer = []
            if mode == "backfill":
                set_state(spark, COMPONENT, "backfill_cursor", cursor)
        if finished:
            break
    if buffer:
        total += write_bronze(spark, "airs.bronze.mcp_registry_raw", run_id, "mcp_registry", buffer)
        if mode == "backfill":
            set_state(spark, COMPONENT, "backfill_cursor", cursor)

    if mode == "backfill" and finished:
        set_state(spark, COMPONENT, "backfill_done", "true")
        set_state(spark, COMPONENT, "updated_since", get_state(spark, COMPONENT, "backfill_started_at"))
        log("backfill complete; next runs are incremental")
    elif mode == "incremental":
        # One hour of overlap guards against clock skew; duplicates are removed on read.
        set_state(spark, COMPONENT, "updated_since", (run_started - timedelta(hours=1)).isoformat() + "Z")
    log(f"{mode}: {pages} pages, {total} servers written, finished={finished}")
    spark.stop()


if __name__ == "__main__":
    main()
