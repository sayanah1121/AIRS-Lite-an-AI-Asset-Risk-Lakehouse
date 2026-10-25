"""Ingest PyPI and npm metadata into bronze.pkg_metadata_raw.

Which packages: the curated seed list (AI SDKs, agent frameworks) plus every npm/PyPI package that an
MCP server in the registry declares. Packages fetched in the last --refresh-days are skipped, so
daily runs only top up new or stale packages.
"""
import argparse
import csv
import os
import re
import urllib.parse

from pyspark.sql import functions as F

from common import (HttpError, env_int, http_json, latest_bronze, log, new_run_id, parallel_map,
                    spark_session, write_bronze)

SEED = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seed", "seed_packages.csv")
MCP_PACKAGES_SCHEMA = "array<struct<registryType:string,identifier:string,version:string>>"


def pypi_norm(name: str) -> str:
    """PEP 503 normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def wanted_packages(spark) -> list[tuple[str, str]]:
    with open(SEED, newline="", encoding="utf-8") as f:
        seeds = [(r["ecosystem"], r["name"]) for r in csv.DictReader(f)]
    declared = (latest_bronze(spark, "airs.bronze.mcp_registry_raw")
                .select(F.explode(F.from_json(F.get_json_object("payload", "$.server.packages"),
                                              MCP_PACKAGES_SCHEMA)).alias("p"))
                .where(F.col("p.registryType").isin("npm", "pypi"))
                .select(F.col("p.registryType").alias("eco"), F.col("p.identifier").alias("name"))
                .distinct().collect())
    out, seen = [], set()
    for eco, name in seeds + [(r.eco, r.name) for r in declared]:
        key = (eco, pypi_norm(name) if eco == "pypi" else name.lower().strip())
        if key[1] and key not in seen:
            seen.add(key)
            out.append(key)
    log(f"packages wanted: {len(seeds)} seeds + {len(declared)} declared by MCP servers -> {len(out)} unique")
    return out


def fetch_pypi(name: str) -> dict | None:
    try:
        data, _ = http_json(f"https://pypi.org/pypi/{urllib.parse.quote(name)}/json")
    except HttpError as e:
        if e.status == 404:
            return None
        raise
    info = {k: v for k, v in data["info"].items() if k != "description"}  # drop README body
    uploads = [u.get("upload_time_iso_8601") for u in data.get("urls", []) if u.get("upload_time_iso_8601")]
    return {"info": info, "latest_upload": max(uploads) if uploads else None,
            "vulnerabilities": [{"id": v.get("id"), "aliases": v.get("aliases")} for v in data.get("vulnerabilities", [])]}


def fetch_npm(name: str) -> dict | None:
    quoted = urllib.parse.quote(name, safe="@")
    try:
        latest, _ = http_json(f"https://registry.npmjs.org/{quoted.replace('/', '%2f')}/latest")
    except HttpError as e:
        if e.status == 404:
            return None
        raise
    keep = ("name", "version", "description", "license", "repository", "homepage", "keywords",
            "deprecated", "author", "bin")
    out = {k: latest.get(k) for k in keep}
    out["maintainers"] = len(latest.get("maintainers") or [])
    # The search API carries the publish date and monthly downloads in one call.
    try:
        search, _ = http_json(f"https://registry.npmjs.org/-/v1/search?{urllib.parse.urlencode({'text': name, 'size': 5})}")
        hit = next((o for o in search.get("objects", []) if o["package"]["name"] == name), None)
        if hit:
            out["date"] = hit["package"].get("date")
            out["downloads_monthly"] = (hit.get("downloads") or {}).get("monthly")
    except HttpError:
        pass
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-packages", type=int, default=env_int("CATALOG_MAX_PACKAGES", 3000))
    ap.add_argument("--refresh-days", type=int, default=3)
    args = ap.parse_args()

    spark = spark_session("ingest-packages")
    run_id = new_run_id()
    wanted = wanted_packages(spark)

    fresh = {(r.source, r.record_key) for r in
             spark.table("airs.bronze.pkg_metadata_raw")
             .where(F.col("fetched_at") > F.expr(f"current_timestamp() - INTERVAL {args.refresh_days} DAYS"))
             .select("source", "record_key").distinct().collect()}
    todo = [p for p in wanted if p not in fresh][: args.max_packages]
    log(f"{len(fresh)} fetched recently, fetching {len(todo)} now")

    fetchers = {"pypi": fetch_pypi, "npm": fetch_npm}
    results = parallel_map(lambda p: fetchers[p[0]](p[1]), todo, workers=12)
    by_eco: dict[str, list] = {"pypi": [], "npm": []}
    errors = missing = 0
    for (eco, name), res in zip(todo, results):
        if isinstance(res, Exception):
            errors += 1
        elif res is None:
            missing += 1
            by_eco[eco].append((name, {"not_found": True}))
        else:
            by_eco[eco].append((name, res))
    for eco, records in by_eco.items():
        write_bronze(spark, "airs.bronze.pkg_metadata_raw", run_id, eco, records)
    log(f"done: {sum(map(len, by_eco.values()))} written, {missing} not found, {errors} errors")
    spark.stop()


if __name__ == "__main__":
    main()
