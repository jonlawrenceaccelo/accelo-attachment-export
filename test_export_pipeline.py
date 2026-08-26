"""
Offline smoke test for the rewritten (object-first) export_attachments.main()
pipeline: mocks AcceloClient entirely (no HTTP) with synthetic companies/
jobs/contacts and their collections + resources, then checks the manifest
came out right, the disk cache works, and re-runs skip existing downloads.
"""
import csv
import json
import shutil
import sys
from pathlib import Path
from unittest import mock

import export_attachments as ea

OUT = Path("/tmp/export_pipeline_test_out")


class FakeClient:
    """
    Synthetic account:
      - company 5 "Acme Co" -> collection 200 -> resources 20, 21
      - job 77 "Website Rebuild" -> collection 106 -> resources 15, 16
      - job 78 "Empty Job" -> no collections at all
      - contact 9 "Jane Doe" -> collection 300 -> resource 22
    """

    def __init__(self, *a, **kw):
        pass

    def paginate(self, path, params=None, limit=100):
        if path == "/companies":
            yield {"id": 5, "name": "Acme Co"}
        elif path == "/jobs":
            yield {"id": 77, "title": "Website Rebuild"}
            yield {"id": 78, "title": "Empty Job"}
        elif path == "/contacts":
            yield {"id": 9, "firstname": "Jane", "surname": "Doe"}
        elif path == "/issues":
            return
            yield
        elif path == "/prospects":
            return
            yield
        elif path == "/contracts":
            return
            yield
        elif path == "/resources":
            filt = params["_filters"]
            # crude parse of collection_id(a,b,c) out of the filter string
            ids_part = filt.split("collection_id(")[1].split(")")[0]
            ids = set(ids_part.split(","))
            all_resources = {
                "200": [{"id": 20, "title": "logo.png", "collection_id": "200", "date_created": 1002}],
                "106": [
                    {"id": 15, "title": "invoice.pdf", "collection_id": "106", "date_created": 1000},
                    {"id": 16, "title": "invoice-1.pdf", "collection_id": "106", "date_created": 1001},
                ],
                "300": [{"id": 22, "title": "signed_contact.pdf", "collection_id": "300", "date_created": 1003}],
            }
            for cid, items in all_resources.items():
                if cid in ids:
                    for it in items:
                        yield it
        else:
            raise AssertionError(f"unexpected path {path}")

    def paginate_nested(self, path, envelope_key, params=None, limit=100):
        # mirrors the real API: GET /{type}/{id}/collections responses are
        # wrapped as {"collections": [...]}, not a bare list
        assert envelope_key == "collections"
        if path == "/companies/5/collections":
            yield {"id": 200}  # normal shape: dict with "id"
        elif path == "/jobs/77/collections":
            yield "106"  # tolerate a bare scalar id too, belt-and-suspenders
        elif path == "/jobs/78/collections":
            return
        elif path == "/contacts/9/collections":
            yield {"id": 300}
        else:
            raise AssertionError(f"unexpected collections path {path}")

    def download(self, resource_id, dest_path):
        data = ("fake-bytes-%s" % resource_id).encode()
        Path(dest_path).write_bytes(data)
        return len(data)


def run(argv):
    old_argv = sys.argv
    sys.argv = ["export_attachments.py"] + argv
    try:
        with mock.patch.object(ea, "AcceloClient", FakeClient), \
             mock.patch.dict("os.environ", {
                 "ACCELO_DEPLOYMENT": "demo",
                 "ACCELO_CLIENT_ID": "x",
                 "ACCELO_CLIENT_SECRET": "y",
             }):
            return ea.main()
    finally:
        sys.argv = old_argv


def test_full_pipeline():
    shutil.rmtree(OUT, ignore_errors=True)
    rc = run(["--output", str(OUT), "--concurrency", "2"])
    assert rc == 0, f"main() returned {rc}"

    rows = {r["resource_id"]: r for r in csv.DictReader(open(OUT / "manifest.csv"))}
    assert len(rows) == 4, rows.keys()

    assert rows["20"]["against_type"] == "company"
    assert rows["20"]["against_name"] == "Acme Co"

    assert rows["15"]["against_type"] == "job"
    assert rows["15"]["against_name"] == "Website Rebuild"
    assert rows["16"]["collection_id"] == rows["15"]["collection_id"] == "106"

    assert rows["22"]["against_type"] == "contact"
    assert rows["22"]["against_name"] == "Jane Doe"

    # the empty job (78) contributed no collections/resources, and shouldn't error
    for rid, row in rows.items():
        f = OUT / "files" / row["local_path"]
        assert f.exists(), f"missing file for resource {rid}"
        assert "downloaded" in row["status"]

    # disk cache was written, in the {_meta, data} shape
    assert (OUT / "_cache_objects.json").exists()
    assert (OUT / "_cache_collections.json").exists()
    cached = json.loads((OUT / "_cache_objects.json").read_text())
    assert "types" in cached["_meta"]["args"] and "limit_per_type" in cached["_meta"]["args"]
    assert len(cached["data"]["job"]) == 2  # includes the empty job

    print("OK: full object-first pipeline resolves company/job/contact attachments correctly")


def test_cache_is_reused():
    # second run with a client that would blow up if stage 1/2 were re-run
    class ExplodingClient(FakeClient):
        def paginate(self, path, params=None, limit=100):
            if path in ("/companies", "/jobs", "/contacts", "/issues", "/prospects", "/contracts"):
                raise AssertionError(f"stage 1 should not re-run for cached path {path}")
            return super().paginate(path, params=params, limit=limit)

        def paginate_nested(self, path, envelope_key, params=None, limit=100):
            raise AssertionError(f"stage 2 should not re-run for cached path {path}")

    with mock.patch.object(ea, "AcceloClient", ExplodingClient), \
         mock.patch.dict("os.environ", {
             "ACCELO_DEPLOYMENT": "demo", "ACCELO_CLIENT_ID": "x", "ACCELO_CLIENT_SECRET": "y",
         }):
        sys.argv = ["export_attachments.py", "--output", str(OUT), "--concurrency", "2"]
        rc = ea.main()
    assert rc == 0
    rows = list(csv.DictReader(open(OUT / "manifest.csv")))
    assert all(r["status"] == "already-downloaded" for r in rows), [r["status"] for r in rows]
    print("OK: cached stage 1/2 results are reused, re-run skips already-downloaded files")


def test_cache_invalidated_by_different_types():
    # OUT currently holds a cache built from a run over ALL types (from
    # test_full_pipeline / test_cache_is_reused, run just before this one).
    # Regression test for the exact bug reported live: switching --types
    # must NOT silently reuse that broader cache.
    rc = run(["--output", str(OUT), "--types", "job", "--concurrency", "2"])
    assert rc == 0
    rows = list(csv.DictReader(open(OUT / "manifest.csv")))
    assert len(rows) == 2, rows
    assert all(r["against_type"] == "job" for r in rows)
    cached = json.loads((OUT / "_cache_objects.json").read_text())
    assert cached["_meta"]["args"]["types"] == ["job"]
    print("OK: switching --types invalidates a cache built with different args, instead of silently reusing it")


class ResourceOnlyFakeClient:
    """Minimal client stub for testing fetch_resources() in isolation."""

    def paginate(self, path, params=None, limit=100):
        assert path == "/resources"
        assert "date_created_after" not in params["_filters"], (
            "date filter must never be sent to the API -- confirmed live "
            "that Accelo silently matches zero rows for it"
        )
        items = [
            {"id": 1, "title": "old.pdf", "collection_id": "10", "date_created": "1000"},  # string, like the live API
            {"id": 2, "title": "new.pdf", "collection_id": "10", "date_created": 2000},  # int, just in case
        ]
        yield from items


def test_fetch_resources_applies_since_client_side():
    client = ResourceOnlyFakeClient()

    all_r = ea.fetch_resources(client, ["10"], since_ts=None, chunk_size=50)
    assert [r["id"] for r in all_r] == [1, 2]

    filtered = ea.fetch_resources(client, ["10"], since_ts=1500, chunk_size=50)
    assert [r["id"] for r in filtered] == [2], filtered

    print("OK: --since is applied client-side (never sent as the broken server-side filter), handles string/int date_created")


def test_dry_run_and_types_filter():
    out2 = Path("/tmp/export_pipeline_test_out2")
    shutil.rmtree(out2, ignore_errors=True)
    rc = run(["--output", str(out2), "--dry-run", "--types", "job", "--concurrency", "2"])
    assert rc == 0
    rows = list(csv.DictReader(open(out2 / "manifest.csv")))
    assert len(rows) == 2, rows  # only the job's 2 resources
    assert all(r["against_type"] == "job" for r in rows)
    assert all(r["status"] == "manifest-only" for r in rows)
    assert not any((out2 / "files").rglob("*.pdf")), "dry-run should not download files"
    shutil.rmtree(out2, ignore_errors=True)
    print("OK: --dry-run + --types filter work together")


if __name__ == "__main__":
    test_full_pipeline()
    test_cache_is_reused()
    test_cache_invalidated_by_different_types()
    test_fetch_resources_applies_since_client_side()
    test_dry_run_and_types_filter()
    shutil.rmtree(OUT, ignore_errors=True)
    print("\nAll pipeline smoke tests passed.")
