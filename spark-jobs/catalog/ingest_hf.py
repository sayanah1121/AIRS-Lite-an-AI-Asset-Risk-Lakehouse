"""Ingest the most-downloaded Hugging Face models and datasets into bronze.hf_models_raw.

The Hub API is paginated with a cursor in the Link header. Each run takes a fresh snapshot of the
top-N by downloads; bronze keeps every snapshot, silver reads the latest per id.
"""
import argparse
import re
import urllib.parse

from common import env_int, http_json, log, new_run_id, spark_session, write_bronze

API = "https://huggingface.co/api"
FIELDS = {
    "models": ["author", "downloads", "likes", "tags", "pipeline_tag", "library_name",
               "gated", "lastModified", "createdAt"],
    "datasets": ["author", "downloads", "likes", "tags", "gated", "lastModified", "createdAt"],
}


def next_link(headers: dict) -> str | None:
    link = headers.get("Link") or headers.get("link") or ""
    m = re.search(r'<([^>]+)>;\s*rel="next"', link)
    return m.group(1) if m else None


def fetch_top(kind: str, limit: int) -> list[dict]:
    params = [("sort", "downloads"), ("direction", "-1"), ("limit", str(min(limit, 1000)))]
    params += [("expand[]", f) for f in FIELDS[kind]]
    url = f"{API}/{kind}?{urllib.parse.urlencode(params)}"
    items: list[dict] = []
    while url and len(items) < limit:
        page, headers = http_json(url)
        items.extend(page)
        url = next_link(headers)
        log(f"hf {kind}: {len(items)} fetched")
    return items[:limit]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", type=int, default=env_int("CATALOG_HF_MODELS", 3000))
    ap.add_argument("--datasets", type=int, default=env_int("CATALOG_HF_DATASETS", 1000))
    args = ap.parse_args()

    spark = spark_session("ingest-hf")
    run_id = new_run_id()
    for kind, source, limit in (("models", "hf_model", args.models), ("datasets", "hf_dataset", args.datasets)):
        items = fetch_top(kind, limit)
        write_bronze(spark, "airs.bronze.hf_models_raw", run_id, source, [(i["id"], i) for i in items])
    spark.stop()


if __name__ == "__main__":
    main()
