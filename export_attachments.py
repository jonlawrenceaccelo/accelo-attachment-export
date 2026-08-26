#!/usr/bin/env python3
"""
export_attachments.py

Downloads every attachment (Resource) in an Accelo deployment, across a
configurable set of "against" object types, and produces a manifest
mapping each downloaded file back to the object it belongs to.

--- Why this looks the way it does (read this before changing TARGET_TYPES) ---

Accelo's API has NO way to go from a bare collection_id (which is all a
Resource gives you) back to the object it belongs to. There is no
`GET /collections/{id}`. The only collections endpoint is the reverse
direction -- `GET /{object}/{object_id}/collections` -- which means you
have to already know the object to find its collections, not the other
way around. (Confirmed both against a live deployment, which 400s on
`GET /collections`, and against Accelo's own doc source.)

So the pipeline has to run object-first, not attachment-first:

  1. List every object of each target type (companies, contacts, jobs,
     issues, prospects, contracts, ...) -- id + display name.
  2. For every single one of those objects, call its `/collections`
     sub-endpoint to find which collection_id(s) it owns. This is the
     one stage that's unavoidably O(number of objects) -- there is no
     batch/filter endpoint for it. It's run with a thread pool
     (--concurrency) to keep wall-clock time down, and its results are
     cached to disk so a long run can be safely interrupted and resumed.
  3. Now that every relevant collection_id is known, batch-fetch the
     actual resources with `_filters=collection_id(...)`, which *is* a
     documented, batchable filter on /resources -- chunked so this part
     stays cheap regardless of how many attachments there are.
  4. Download each file and write manifest.csv / manifest.json.

Usage:
    python export_attachments.py                        # full run, all 6 types
    python export_attachments.py --dry-run                # build manifest only, no downloads
    python export_attachments.py --limit-per-type 25       # test run: only 25 objects per type
    python export_attachments.py --types job,company        # only these object types
    python export_attachments.py --since 2026-01-01          # only attachments created on/after this date
    python export_attachments.py --refresh-cache               # ignore cached stage 1/2 results

Configuration is read from a .env file (see .env.example) or real
environment variables:
    ACCELO_DEPLOYMENT, ACCELO_CLIENT_ID, ACCELO_CLIENT_SECRET, [ACCELO_SCOPE]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from accelo_client import AcceloClient, AcceloAPIError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("export_attachments")


# --------------------------------------------------------------------------
# Object types to scan for attachments. Confirmed against Accelo's own doc
# source (endpoint path, display-name field, and the existence of a
# `/collections` sub-endpoint) for all six of these. Add more rows here
# (same shape) if attachments can also hang off other module types in your
# account -- anything not listed here simply won't be scanned.
# --------------------------------------------------------------------------
TARGET_TYPES = {
    "company": {
        "endpoint": "/companies",
        "fields": ["name"],
        "name": lambda o: (o.get("name") or "").strip(),
    },
    "contact": {
        "endpoint": "/contacts",
        "fields": ["firstname", "surname"],
        "name": lambda o: f"{o.get('firstname', '').strip()} {o.get('surname', '').strip()}".strip(),
    },
    "job": {
        "endpoint": "/jobs",
        "fields": ["title"],
        "name": lambda o: (o.get("title") or "").strip(),
    },
    "issue": {
        "endpoint": "/issues",
        "fields": ["title"],
        "name": lambda o: (o.get("title") or "").strip(),
    },
    "prospect": {
        "endpoint": "/prospects",
        "fields": ["title"],
        "name": lambda o: (o.get("title") or "").strip(),
    },
    "contract": {
        "endpoint": "/contracts",
        "fields": ["title"],
        "name": lambda o: (o.get("title") or "").strip(),
    },
}


def load_cache(path: Path, expected_meta: dict):
    """
    Load a cache file only if it was built with the same relevant args
    (e.g. --types, --limit-per-type). Returns None on any mismatch, missing
    file, or unreadable content -- callers treat that as "no cache" and
    re-fetch. This is what stops a stale cache from a differently-scoped
    run (different --types, different --limit-per-type, or just older)
    from silently being reused.
    """
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        meta, data = payload["_meta"], payload["data"]
    except (json.JSONDecodeError, KeyError, TypeError):
        logger.warning("Cache file %s is unreadable, ignoring it and re-fetching", path)
        return None
    if meta.get("args") != expected_meta:
        logger.info(
            "Cache %s was built with different args (%s vs current %s) -- ignoring it and re-fetching",
            path, meta.get("args"), expected_meta,
        )
        return None
    age_min = (time.time() - meta.get("fetched_at", 0)) / 60
    logger.info("Using cached %s from %.0f minute(s) ago. Pass --refresh-cache to force a redo.", path.name, age_min)
    return data


def save_cache(path: Path, args_meta: dict, data) -> None:
    path.write_text(json.dumps({"_meta": {"args": args_meta, "fetched_at": time.time()}, "data": data}, indent=2))


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]+", "_", name or "").strip()
    return name or "untitled"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", default="output", help="Output directory (default: ./output)")
    p.add_argument("--dry-run", action="store_true", help="Build the manifest only; skip file downloads")
    p.add_argument("--limit-per-type", type=int, default=None, help="Only scan the first N objects per type (for testing)")
    p.add_argument("--types", default=",".join(TARGET_TYPES), help=f"Comma-separated object types to scan (default: all -- {','.join(TARGET_TYPES)})")
    p.add_argument("--since", default=None, help="Only include attachments created on/after this date (YYYY-MM-DD)")
    p.add_argument("--overwrite", action="store_true", help="Re-download files that already exist on disk")
    p.add_argument("--concurrency", type=int, default=8, help="Parallel requests when resolving each object's collections (default: 8)")
    p.add_argument("--collection-chunk", type=int, default=50, help="Batch size for resources _filters=collection_id(...) lookups")
    p.add_argument("--refresh-cache", action="store_true", help="Ignore cached stage 1/2 results from a previous run and re-fetch everything")
    return p.parse_args()


# ---------------------------------------------------------------------- #
# Stage 1: list every object of each target type
# ---------------------------------------------------------------------- #
def discover_objects(client: AcceloClient, types: list[str], limit_per_type: int | None) -> dict:
    objects_by_type = {}
    for type_key in types:
        cfg = TARGET_TYPES[type_key]
        fields = "id," + ",".join(cfg["fields"])
        objs = []
        for item in client.paginate(cfg["endpoint"], params={"_fields": fields}):
            objs.append({"id": str(item["id"]), "name": cfg["name"](item)})
            if limit_per_type and len(objs) >= limit_per_type:
                break
        objects_by_type[type_key] = objs
        logger.info("Stage 1/4: found %d %s(s)", len(objs), type_key)
    return objects_by_type


# ---------------------------------------------------------------------- #
# Stage 2: for every object, resolve which collection(s) it owns
# ---------------------------------------------------------------------- #
def resolve_collections(client: AcceloClient, objects_by_type: dict, concurrency: int) -> dict:
    tasks = [
        (type_key, obj)
        for type_key, objs in objects_by_type.items()
        for obj in objs
    ]
    total = len(tasks)
    collection_map: dict[str, dict] = {}
    done = 0
    print_lock = threading.Lock()

    def worker(type_key: str, obj: dict) -> list[tuple[str, str, str, str]]:
        endpoint = TARGET_TYPES[type_key]["endpoint"]
        try:
            # This endpoint wraps its results as {"collections": [...]}
            # rather than a bare array -- confirmed against a live account
            # (GET /jobs/{id}/collections?_fields=_ALL). paginate_nested
            # unwraps that; using plain paginate() here previously caused
            # silent wrong data (see accelo_client.paginate's docstring).
            cols = list(client.paginate_nested(f"{endpoint}/{obj['id']}/collections", "collections"))
        except AcceloAPIError as e:
            logger.warning("Couldn't list collections for %s %s (%s): %s", type_key, obj["id"], obj["name"], e)
            return []
        ids = [c["id"] if isinstance(c, dict) else c for c in cols]
        return [(str(cid), type_key, obj["id"], obj["name"]) for cid in ids]

    logger.info("Stage 2/4: resolving collections for %d object(s) (concurrency=%d)...", total, concurrency)
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(worker, t, o): (t, o) for t, o in tasks}
        for fut in as_completed(futures):
            for collection_id, type_key, against_id, against_name in fut.result():
                collection_map[collection_id] = {
                    "against_type": type_key,
                    "against_id": against_id,
                    "against_name": against_name,
                }
            done += 1
            if done % 100 == 0 or done == total:
                with print_lock:
                    logger.info("  stage 2/4: %d/%d objects checked, %d collection(s) found so far", done, total, len(collection_map))

    return collection_map


# ---------------------------------------------------------------------- #
# Stage 3: batch-fetch the actual resources for every known collection
# ---------------------------------------------------------------------- #
def fetch_resources(client: AcceloClient, collection_ids, since_ts: int | None, chunk_size: int) -> list[dict]:
    # NOTE: --since is applied client-side below, not as a server-side
    # `date_created_after(...)` filter. That filter is documented as valid
    # on /resources, but confirmed against a live account to silently
    # match zero rows instead of erroring or being ignored -- so applying
    # it server-side was quietly discarding every result. Fetching
    # everything for the known collections and filtering by date_created
    # here in Python is slightly more data over the wire, but actually
    # correct.
    collection_ids = list(dict.fromkeys(collection_ids))
    resources = []
    skipped_by_date = 0
    for i in range(0, len(collection_ids), chunk_size):
        chunk = collection_ids[i : i + chunk_size]
        params = {"_filters": f"collection_id({','.join(chunk)})", "_fields": "id,title,collection_id,date_created"}
        for r in client.paginate("/resources", params=params):
            if since_ts is not None and int(r.get("date_created") or 0) < since_ts:
                skipped_by_date += 1
                continue
            resources.append(r)
        logger.info("Stage 3/4: fetched resources for %d/%d collection(s) so far (%d resource(s) total)",
                    min(i + chunk_size, len(collection_ids)), len(collection_ids), len(resources))
    if since_ts is not None and skipped_by_date:
        logger.info("Stage 3/4: %d resource(s) excluded by --since", skipped_by_date)
    return resources


def main() -> int:
    load_dotenv()
    args = parse_args()

    deployment = os.environ.get("ACCELO_DEPLOYMENT")
    client_id = os.environ.get("ACCELO_CLIENT_ID")
    client_secret = os.environ.get("ACCELO_CLIENT_SECRET")
    scope = os.environ.get("ACCELO_SCOPE", "read(all)")

    missing = [n for n, v in [
        ("ACCELO_DEPLOYMENT", deployment),
        ("ACCELO_CLIENT_ID", client_id),
        ("ACCELO_CLIENT_SECRET", client_secret),
    ] if not v]
    if missing:
        logger.error("Missing required config: %s (see .env.example)", ", ".join(missing))
        return 1

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    unknown = [t for t in types if t not in TARGET_TYPES]
    if unknown:
        logger.error("Unknown --types value(s) %s. Valid types: %s", unknown, list(TARGET_TYPES))
        return 1

    client = AcceloClient(deployment, client_id, client_secret, scope=scope)

    output_dir = Path(args.output)
    files_dir = output_dir / "files"
    output_dir.mkdir(parents=True, exist_ok=True)
    files_dir.mkdir(parents=True, exist_ok=True)

    since_ts = None
    if args.since:
        since_ts = int(
            datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        )

    cache_objects_path = output_dir / "_cache_objects.json"
    cache_collections_path = output_dir / "_cache_collections.json"

    # Cache validity is tied to exactly the args that change what stage 1/2
    # produce. Anything else (--since, --dry-run, --overwrite, ...) doesn't
    # invalidate the cache since it doesn't affect which objects/collections
    # get discovered -- only which resources/files get pulled out of them.
    objects_cache_meta = {"types": sorted(types), "limit_per_type": args.limit_per_type}

    # ------------------------------------------------------------------ #
    # Stage 1 (with disk cache -- this + stage 2 can be the slow part)
    # ------------------------------------------------------------------ #
    objects_by_type = None if args.refresh_cache else load_cache(cache_objects_path, objects_cache_meta)
    if objects_by_type is None:
        objects_by_type = discover_objects(client, types, args.limit_per_type)
        save_cache(cache_objects_path, objects_cache_meta, objects_by_type)

    total_objects = sum(len(v) for v in objects_by_type.values())
    if total_objects == 0:
        logger.info("No objects found for the selected types -- nothing to do.")
        return 0

    # ------------------------------------------------------------------ #
    # Stage 2 (with disk cache -- keyed on the same meta as stage 1, since
    # stage 2's input *is* stage 1's output)
    # ------------------------------------------------------------------ #
    collection_map = None if args.refresh_cache else load_cache(cache_collections_path, objects_cache_meta)
    if collection_map is None:
        collection_map = resolve_collections(client, objects_by_type, args.concurrency)
        save_cache(cache_collections_path, objects_cache_meta, collection_map)

    logger.info("Stage 2/4: %d collection(s) resolved across %d object(s)", len(collection_map), total_objects)
    if not collection_map:
        logger.info("No collections found -- nothing to do.")
        return 0

    # ------------------------------------------------------------------ #
    # Stage 3
    # ------------------------------------------------------------------ #
    resources = fetch_resources(client, collection_map.keys(), since_ts, args.collection_chunk)
    logger.info("Stage 3/4: %d resource(s) total", len(resources))
    if not resources:
        logger.info("No resources found -- nothing to do.")
        return 0

    # ------------------------------------------------------------------ #
    # Stage 4: build manifest rows (+ optionally download files)
    # ------------------------------------------------------------------ #
    logger.info("Stage 4/4: %s...", "building manifest" if args.dry_run else "downloading files + building manifest")
    manifest = []
    errors = []

    for idx, r in enumerate(resources, start=1):
        collection = collection_map.get(str(r.get("collection_id")), {})
        against_type = collection.get("against_type", "")
        against_id = collection.get("against_id", "")
        against_name = collection.get("against_name", "")

        title = r.get("title") or f"resource_{r['id']}"
        safe_type = sanitize_filename(against_type or "unknown")
        safe_against = sanitize_filename(against_name or against_id or "unknown")
        safe_title = sanitize_filename(title)

        rel_path = Path(safe_type) / safe_against / f"{r['id']}_{safe_title}"
        dest_path = files_dir / rel_path

        row = {
            "resource_id": r["id"],
            "resource_title": title,
            "date_created": r.get("date_created", ""),
            "collection_id": r.get("collection_id", ""),
            "against_type": against_type,
            "against_id": against_id,
            "against_name": against_name,
            "local_path": str(rel_path) if not args.dry_run else "",
            "status": "",
        }

        if args.dry_run:
            row["status"] = "manifest-only"
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if dest_path.exists() and not args.overwrite:
                row["status"] = "already-downloaded"
            else:
                try:
                    n_bytes = client.download(r["id"], dest_path)
                    row["status"] = f"downloaded ({n_bytes} bytes)"
                except (AcceloAPIError, OSError) as e:
                    row["status"] = "ERROR"
                    errors.append({"resource_id": r["id"], "title": title, "error": str(e)})
                    logger.error("Resource %s (%s): %s", r["id"], title, e)

        manifest.append(row)

        if idx % 50 == 0 or idx == len(resources):
            logger.info("  processed %d/%d resources", idx, len(resources))

    # ------------------------------------------------------------------ #
    # Write manifest + error log
    # ------------------------------------------------------------------ #
    manifest_csv = output_dir / "manifest.csv"
    manifest_json = output_dir / "manifest.json"

    with open(manifest_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(manifest[0].keys()) if manifest else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest)

    with open(manifest_json, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    if errors:
        errors_csv = output_dir / "errors.csv"
        with open(errors_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["resource_id", "title", "error"])
            writer.writeheader()
            writer.writerows(errors)
        logger.warning("%d error(s) written to %s", len(errors), errors_csv)

    logger.info("Done. %d resource(s) in manifest. Manifest: %s", len(manifest), manifest_csv)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
