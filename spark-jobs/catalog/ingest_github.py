"""Ingest GitHub repository metadata into bronze.github_repos_raw.

Repos come from the MCP Registry (server.repository.url) and from package metadata (npm repository,
PyPI project_urls). Unauthenticated GitHub allows 60 requests/hour, so without GITHUB_TOKEN this job
fetches a small batch per run (repos linked to MCP servers first) and fills in over time.
Repos fetched in the last --refresh-days are skipped.
"""
import argparse
import os

from pyspark.sql import functions as F

from common import (HttpError, env_int, http_json, latest_bronze, log, new_run_id, parallel_map,
                    spark_session, write_bronze)
from transforms import github_repo

KEEP = ("full_name", "description", "stargazers_count", "forks_count", "open_issues_count", "archived",
        "disabled", "fork", "pushed_at", "created_at", "topics", "homepage")


def candidate_repos(spark):
    mcp = (latest_bronze(spark, "airs.bronze.mcp_registry_raw")
           .select(github_repo(F.get_json_object("payload", "$.server.repository.url")).alias("repo"))
           .withColumn("priority", F.lit(1)))
    pkgs = latest_bronze(spark, "airs.bronze.pkg_metadata_raw")
    npm = (pkgs.where("source = 'npm'")
           .select(github_repo(F.coalesce(F.get_json_object("payload", "$.repository.url"),
                                          F.get_json_object("payload", "$.repository"))).alias("repo"))
           .withColumn("priority", F.lit(2)))
    pypi = (pkgs.where("source = 'pypi'")
            .select(F.explode(F.from_json(F.get_json_object("payload", "$.info.project_urls"),
                                          "map<string,string>")).alias("k", "url"))
            .select(github_repo(F.col("url")).alias("repo"))
            .withColumn("priority", F.lit(2)))
    return (mcp.unionByName(npm).unionByName(pypi).where("repo IS NOT NULL")
            .groupBy("repo").agg(F.min("priority").alias("priority")))


def fetch_repo(repo: str, token: str | None) -> dict | None:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        data, _ = http_json(f"https://api.github.com/repos/{repo}", headers=headers, retries=1)
    except HttpError as e:
        if e.status in (404, 451):
            return {"not_found": True}
        raise
    out = {k: data.get(k) for k in KEEP}
    out["license"] = (data.get("license") or {}).get("spdx_id")
    out["owner_login"] = (data.get("owner") or {}).get("login")
    out["owner_type"] = (data.get("owner") or {}).get("type")
    return out


def main() -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip() or None
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-repos", type=int,
                    default=env_int("CATALOG_GITHUB_MAX_REPOS", 3000 if token else 50))
    ap.add_argument("--refresh-days", type=int, default=7)
    args = ap.parse_args()

    spark = spark_session("ingest-github")
    run_id = new_run_id()
    fresh = (spark.table("airs.bronze.github_repos_raw")
             .where(F.col("fetched_at") > F.expr(f"current_timestamp() - INTERVAL {args.refresh_days} DAYS"))
             .select(F.col("record_key").alias("repo")).distinct())
    todo = [r.repo for r in candidate_repos(spark).join(fresh, "repo", "left_anti")
            .orderBy("priority", "repo").limit(args.max_repos).collect()]
    log(f"token={'yes' if token else 'no'}; fetching {len(todo)} repos")

    results = parallel_map(lambda r: fetch_repo(r, token), todo, workers=8 if token else 2)
    records, errors = [], 0
    for repo, res in zip(todo, results):
        if isinstance(res, Exception):
            errors += 1
            if isinstance(res, HttpError) and res.status == 403:
                log("rate limit reached; remaining repos are fetched on the next run")
        else:
            records.append((repo, res))
    write_bronze(spark, "airs.bronze.github_repos_raw", run_id, "github", records)
    log(f"done: {len(records)} written, {errors} errors")
    spark.stop()


if __name__ == "__main__":
    main()
