"""
accelo_client.py

Thin HTTP client for the Accelo API (https://api.accelo.com/docs/) handling:
  - OAuth2 "client credentials" authentication (service application flow)
  - Automatic token refresh
  - Pagination (_page / _limit)
  - Rate-limit backoff (5000 req/hour per deployment; retries on 429)
  - Batch-by-id filtering helper (_filters=id(1,2,3,...))
  - File download (GET /resources/{id}/download)

This module has no side effects of its own beyond making HTTP calls -- it's
meant to be imported by export_attachments.py (or any other script).
"""

from __future__ import annotations

import base64
import threading
import time
import logging
from typing import Any, Iterable, Iterator

import requests

logger = logging.getLogger("accelo_client")

DEFAULT_LIMIT = 100  # Accelo's documented max page size
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2


class AcceloAPIError(RuntimeError):
    """Raised for non-recoverable API errors (4xx other than 429, 5xx after retries)."""


class AcceloClient:
    def __init__(
        self,
        deployment: str,
        client_id: str,
        client_secret: str,
        scope: str = "read(all)",
    ):
        self.deployment = deployment
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

        self.token_url = f"https://{deployment}.api.accelo.com/oauth2/v0/token"
        self.base_url = f"https://{deployment}.api.accelo.com/api/v0"

        self._access_token: str | None = None
        self._token_expires_at: float = 0.0
        self._token_lock = threading.Lock()

        # requests.Session is safe enough for concurrent use for our purposes
        # (connection pooling is thread-safe); the token lock above is what
        # actually protects against a thundering herd of refreshes when
        # export_attachments.py runs stage 2 with a thread pool.
        self.session = requests.Session()

    # ------------------------------------------------------------------ #
    # Auth
    # ------------------------------------------------------------------ #
    def _fetch_token(self) -> None:
        creds = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        basic = base64.b64encode(creds).decode("ascii")

        resp = self.session.post(
            self.token_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic}",
            },
            data={
                "grant_type": "client_credentials",
                "scope": self.scope,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise AcceloAPIError(
                f"OAuth token request failed ({resp.status_code}): {resp.text[:500]}"
            )
        payload = resp.json()
        self._access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        # refresh a little early to avoid edge-of-expiry failures mid-run
        self._token_expires_at = time.time() + max(expires_in - 60, 30)
        logger.info("Fetched Accelo access token (expires in %ss)", expires_in)

    def _ensure_token(self) -> str:
        # fast path without the lock for the common case (token still valid)
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token
        with self._token_lock:
            # re-check: another thread may have refreshed while we waited
            if not self._access_token or time.time() >= self._token_expires_at:
                self._fetch_token()
        return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------ #
    # Low-level request with retry/backoff
    # ------------------------------------------------------------------ #
    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        stream: bool = False,
    ) -> requests.Response:
        url = path if path.startswith("http") else f"{self.base_url}{path}"

        for attempt in range(1, MAX_RETRIES + 1):
            token = self._ensure_token()
            resp = self.session.request(
                method,
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                stream=stream,
                timeout=60,
            )

            if resp.status_code == 401 and attempt == 1:
                # token may have been invalidated server-side; force refresh once
                logger.warning("Got 401, forcing token refresh and retrying")
                self._access_token = None
                continue

            if resp.status_code == 429:
                reset_at = resp.headers.get("X-RateLimit-Reset")
                if reset_at:
                    wait = max(float(reset_at) - time.time(), 1)
                else:
                    wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                wait = min(wait, 300)
                logger.warning(
                    "Rate limited (429). Waiting %.1fs (attempt %d/%d)",
                    wait, attempt, MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Server error %d on %s. Retrying in %.1fs (attempt %d/%d)",
                    resp.status_code, url, wait, attempt, MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                raise AcceloAPIError(
                    f"{method} {url} failed ({resp.status_code}): {resp.text[:500]}"
                )

            return resp

        raise AcceloAPIError(f"{method} {url} failed after {MAX_RETRIES} retries")

    # ------------------------------------------------------------------ #
    # Public helpers
    # ------------------------------------------------------------------ #
    def get(self, path: str, params: dict | None = None) -> Any:
        """Single GET, returns the parsed `response` payload."""
        resp = self._request("GET", path, params=params)
        return resp.json().get("response")

    def paginate(
        self,
        path: str,
        params: dict | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> Iterator[dict]:
        """
        Yield every item from a list endpoint whose `response` is a bare
        array (true for e.g. /resources, /companies, /jobs...), walking
        _page until a short page (or empty page) signals the end.

        If `response` turns out to be a dict instead of a list, this
        raises rather than silently iterating the dict's keys as fake
        items (a dict IS iterable in Python, over its keys, which is a
        real bug this guards against -- happened with the /collections
        sub-endpoint, whose response is wrapped as {"collections": [...]}
        instead of a bare list; that endpoint should use
        paginate_nested() instead).
        """
        params = dict(params or {})
        page = 0
        while True:
            page_params = dict(params, _page=page, _limit=limit)
            items = self.get(path, params=page_params) or []
            if isinstance(items, dict):
                raise AcceloAPIError(
                    f"GET {path}: expected a list in `response`, got an object with "
                    f"keys {list(items.keys())} -- this endpoint likely wraps its "
                    f"results (use paginate_nested() with the right key instead)"
                )
            if not items:
                return
            for item in items:
                yield item
            if len(items) < limit:
                return
            page += 1

    def paginate_nested(
        self,
        path: str,
        envelope_key: str,
        params: dict | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> Iterator[dict]:
        """
        Like paginate(), but for endpoints whose per-page `response` is
        wrapped as {envelope_key: [...]} instead of a bare array -- e.g.
        GET /{object}/{object_id}/collections returns
        {"collections": [...]}, confirmed against a live account.
        """
        params = dict(params or {})
        page = 0
        while True:
            page_params = dict(params, _page=page, _limit=limit)
            payload = self.get(path, params=page_params)
            if isinstance(payload, dict):
                items = payload.get(envelope_key) or []
            elif isinstance(payload, list):
                # tolerate an account/endpoint that isn't wrapped after all
                items = payload
            else:
                items = []
            if not items:
                return
            for item in items:
                yield item
            if len(items) < limit:
                return
            page += 1

    def get_by_ids(
        self,
        path: str,
        ids: Iterable[int | str],
        fields: str | None = None,
        chunk_size: int = 50,
    ) -> list[dict]:
        """
        Batch-fetch objects from a list endpoint using _filters=id(...),
        chunking the id list so URLs stay a safe length. `chunk_size` is
        capped at DEFAULT_LIMIT since each chunk is fetched with a single
        page (raise if you ever expect >100 rows per id, which shouldn't
        happen since ids are unique).
        """
        ids = [str(i) for i in dict.fromkeys(ids) if i not in (None, "")]
        if not ids:
            return []

        chunk_size = min(chunk_size, DEFAULT_LIMIT)
        results: list[dict] = []
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            params = {
                "_filters": f"id({','.join(chunk)})",
                "_limit": DEFAULT_LIMIT,
            }
            if fields:
                params["_fields"] = fields
            batch = self.get(path, params=params) or []
            results.extend(batch)
        return results

    def download(self, resource_id: int | str, dest_path) -> int:
        """
        Stream a resource's file content to `dest_path`. Returns bytes written.
        Handles both a direct binary response and a redirect to a signed URL
        (requests drops the Authorization header automatically on cross-host
        redirects, which is what a signed S3-style URL needs).
        """
        resp = self._request("GET", f"/resources/{resource_id}/download", stream=True)
        total = 0
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        return total
