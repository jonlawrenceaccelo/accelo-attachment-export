"""
Offline smoke test: mocks the HTTP layer so we can exercise
- OAuth token fetch + caching
- pagination (_page walking until a short page)
- batch id-filter chunking (get_by_ids)
- 429 retry/backoff path
- download() writing bytes to disk
without hitting the real Accelo API. Not a substitute for a real
--dry-run --limit 5 against a live deployment, but catches logic bugs.
"""
import io
import time
import types
from pathlib import Path
from unittest import mock

from accelo_client import AcceloClient, AcceloAPIError


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, content=b""):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}
        self._content = content
        self.text = str(json_data)

    def json(self):
        return self._json

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i : i + chunk_size]


def test_token_fetch_and_cache():
    client = AcceloClient("demo", "id", "secret")
    calls = {"n": 0}

    def fake_post(url, headers=None, data=None, timeout=None):
        calls["n"] += 1
        assert url == "https://demo.api.accelo.com/oauth2/v0/token"
        assert data["grant_type"] == "client_credentials"
        return FakeResponse(200, {"access_token": "tok123", "expires_in": 3600})

    client.session.post = fake_post
    tok = client._ensure_token()
    assert tok == "tok123"
    tok2 = client._ensure_token()
    assert calls["n"] == 1, "token should be cached, not refetched"
    print("OK: token fetch + cache")


def test_pagination_walks_pages():
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    pages = {
        0: [{"id": i} for i in range(100)],
        1: [{"id": i} for i in range(100, 150)],  # short page -> stop
    }
    seen_params = []

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        seen_params.append(dict(params))
        page = params["_page"]
        return FakeResponse(200, {"response": pages.get(page, [])})

    client.session.request = fake_request
    items = list(client.paginate("/resources", params={"_fields": "id"}))
    assert len(items) == 150, f"expected 150, got {len(items)}"
    assert seen_params[0]["_limit"] == 100
    assert [p["_page"] for p in seen_params] == [0, 1]
    print("OK: pagination walks pages and stops on short page")


def test_paginate_rejects_wrapped_response():
    # Regression test: GET /{object}/{id}/collections wraps its results as
    # {"collections": [...]} instead of a bare list. plain paginate() must
    # raise on that instead of silently iterating the dict's keys as fake
    # items (which is what actually happened before this fix -- a dict IS
    # iterable in Python, over its keys, so `for item in {"collections": [...]}`
    # silently yields the string "collections" as one bogus item).
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        return FakeResponse(200, {"response": {"collections": [{"id": "2318", "against_id": "535"}]}})

    client.session.request = fake_request
    try:
        list(client.paginate("/jobs/535/collections"))
        raise AssertionError("expected paginate() to raise on a wrapped/dict response")
    except AcceloAPIError as e:
        assert "collections" in str(e)
    print("OK: paginate() raises instead of silently misreading a wrapped response")


def test_paginate_nested_unwraps_envelope():
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    # exact shape confirmed against a live Accelo account via Postman
    live_shape = {
        "collections": [
            {"against_id": "535", "id": "2318", "against_type": "job", "title": "Bell Cornwell Testing"}
        ]
    }

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        page = params["_page"]
        return FakeResponse(200, {"response": live_shape if page == 0 else {"collections": []}})

    client.session.request = fake_request
    items = list(client.paginate_nested("/jobs/535/collections", "collections"))
    assert items == [{"against_id": "535", "id": "2318", "against_type": "job", "title": "Bell Cornwell Testing"}]
    print("OK: paginate_nested unwraps the envelope key, matches live API shape")


def test_get_by_ids_chunks():
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    seen_filters = []

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        seen_filters.append(params["_filters"])
        # echo back fake objects for whatever ids were requested
        ids_str = params["_filters"][len("id(") : -1]
        ids = ids_str.split(",")
        return FakeResponse(200, {"response": [{"id": i} for i in ids]})

    client.session.request = fake_request
    ids = list(range(1, 126))  # 125 distinct ids, chunk_size 50 -> 3 chunks
    results = client.get_by_ids("/collections", ids, chunk_size=50)
    assert len(results) == 125, f"expected 125, got {len(results)}"
    assert len(seen_filters) == 3, f"expected 3 batched calls, got {len(seen_filters)}"
    # dedup check
    results2 = client.get_by_ids("/collections", [1, 1, 2, 2, 3], chunk_size=50)
    assert len(results2) == 3, "duplicate ids should be deduped before batching"
    print("OK: get_by_ids batches into chunks and dedupes")


def test_429_then_success():
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    calls = {"n": 0}

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(429, headers={"X-RateLimit-Reset": str(time.time() + 0.05)})
        return FakeResponse(200, {"response": [{"id": 1}]})

    client.session.request = fake_request
    result = client.get("/resources")
    assert result == [{"id": 1}]
    assert calls["n"] == 2
    print("OK: 429 triggers backoff then succeeds")


def test_download_writes_bytes(tmp_path=Path("/tmp/accelo_client_test_dl")):
    tmp_path.mkdir(exist_ok=True)
    client = AcceloClient("demo", "id", "secret")
    client._access_token = "tok"
    client._token_expires_at = time.time() + 3600

    def fake_request(method, url, params=None, headers=None, stream=None, timeout=None):
        assert url.endswith("/resources/42/download")
        return FakeResponse(200, content=b"hello world" * 10000)

    client.session.request = fake_request
    dest = tmp_path / "file.bin"
    n = client.download(42, dest)
    assert n == len(b"hello world" * 10000)
    assert dest.read_bytes() == b"hello world" * 10000
    print("OK: download writes correct bytes")


if __name__ == "__main__":
    test_token_fetch_and_cache()
    test_pagination_walks_pages()
    test_paginate_rejects_wrapped_response()
    test_paginate_nested_unwraps_envelope()
    test_get_by_ids_chunks()
    test_429_then_success()
    test_download_writes_bytes()
    print("\nAll smoke tests passed.")
