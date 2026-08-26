# Accelo attachment export

Downloads every attachment in an Accelo deployment and writes a manifest
mapping each file back to what it belongs to (company, contact, job,
issue, prospect, contract), without a naive one-call-per-attachment
approach.

## Why this is object-first, not attachment-first (read this before touching TARGET_TYPES)

Accelo's API has no way to go from a bare `collection_id` (which is all a
Resource/attachment gives you) back to the object it belongs to. There is
no `GET /collections/{id}`. The only collections endpoint runs the other
direction -- `GET /{object}/{object_id}/collections` -- meaning you need
to already know the object to find its collections, not the reverse.
(This was confirmed two ways: a live call to `GET /collections` 400s with
"resource endpoint not recognized", and Accelo's own doc source has no
flat collections endpoint at all -- only the per-object one.)

So the pipeline runs top-down:

1. **List every object** of each target type -- companies, contacts,
   jobs, issues, prospects, contracts -- collecting id + display name.
2. **For every single object, look up its collection(s)** via
   `/{type}/{id}/collections`. This is the one stage that's unavoidably
   one API call per object -- there's no batch endpoint for it. It runs
   with a thread pool (`--concurrency`) to keep wall-clock time down, and
   the result is cached to disk (`output/_cache_collections.json`) so a
   long run can be safely interrupted and picked back up.
3. **Batch-fetch the actual resources** now that every relevant
   `collection_id` is known, using `_filters=collection_id(...)` --
   this *is* a documented, batchable filter on `/resources`, so this
   stage stays cheap regardless of attachment count.
4. **Download each file** and write `manifest.csv` / `manifest.json`.

An earlier version of this script tried to go attachment-first (list
resources, then resolve each one's collection, then resolve each
collection's parent) -- that doesn't work because step 2 of that chain
doesn't exist as a real endpoint. This version is the one that actually
runs against the live API.

## Setup

```bash
cd accelo-attachment-export
python3 -m venv .venv && source .venv/bin/activate   # Windows: source .venv/Scripts/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with your deployment + service application client id/secret
```

You'll need an Accelo **Service Application** (client-credentials OAuth,
no browser login step) -- create one under your Accelo admin's API
applications settings, with read access to Resources and to whichever
object types you're scanning (Companies, Contacts, Jobs, Issues,
Prospects, Contracts).

## Run it

```bash
# Always start small and check the output before a full run:
python export_attachments.py --dry-run --limit-per-type 10

# Full manifest-only run (no downloads), to see total scope first:
python export_attachments.py --dry-run

# Full run, all 6 object types:
python export_attachments.py

# Only jobs and companies, attachments since a given date:
python export_attachments.py --types job,company --since 2026-01-01
```

Re-running is safe: already-downloaded files are skipped by default
(`--overwrite` to force), and stage 1/2 results are cached to disk so a
second run doesn't repeat the expensive per-object collection lookups.
The cache is tied to `--types` and `--limit-per-type` specifically --
change either and it's automatically ignored and re-fetched, so you can't
accidentally see stale results from a differently-scoped run. It's *not*
tied to time, though: if you re-run with the exact same `--types`/
`--limit-per-type` after adding a new job/company/etc. in Accelo, you'll
still get the old cached list -- pass `--refresh-cache` to force a full
redo whenever you want to pick up new objects. If some downloads fail
they're logged to `output/errors.csv` and the run continues; re-run
afterward and only the failed ones get retried.

## Output layout

```
output/
  _cache_objects.json       <- stage 1 result (safe to delete to force a redo)
  _cache_collections.json    <- stage 2 result (safe to delete to force a redo)
  manifest.csv                 <- one row per attachment: source, name, local path, status
  manifest.json                <- same data, JSON
  errors.csv                     <- only present if something failed
  files/
    company/
      <company name>/
        <resource_id>_<filename>
    job/
      <job name>/
        ...
    contact/ ...
    issue/ ...
    prospect/ ...
    contract/ ...
```

## About stage 2's cost, honestly

Stage 2 is one API call per object across all six types -- companies +
contacts + jobs + issues + prospects + contracts. There's no way around
this; it's a real limitation of the API, not something a cleverer client
can optimize past. For a large account this can be a lot of calls, and
Accelo enforces **5000 requests/hour per deployment**. The client backs
off automatically on `429`s, but for a big account expect this stage to
take a while -- use `--types` to scope to just the object types you
actually need, and `--limit-per-type` to test the full pipeline cheaply
before committing to a full run. The disk cache means you only pay this
cost once even if the run gets interrupted or you want to re-run stage
3/4 with different filters.

## A note on `--since`

`--since` is applied client-side (in Python, after fetching), not as a
server-side `date_created_after(...)` query filter. That filter is
documented as valid on `/resources`, but confirmed against a live account
to silently match zero rows instead of erroring -- so filtering by date
this way was quietly discarding every result rather than narrowing them.
Fetching everything for the known collections and filtering by
`date_created` here is a bit more data over the wire, but is actually
correct.

## A note on the `/collections` response shape

`GET /{object}/{object_id}/collections` wraps its results as
`{"collections": [...]}` rather than a bare array -- unlike every other
list endpoint this script calls (`/resources`, `/companies`, `/jobs`,
...), which return a bare array directly. This was confirmed against a
live account and is handled by a dedicated `paginate_nested()` helper in
`accelo_client.py`; `paginate()` (used everywhere else) now raises loudly
instead of silently misreading a wrapped response, in case another
endpoint turns out to have the same quirk.

## Things to verify before trusting a full run

- **Display-name fields** for each type (`name` for companies, `title`
  for jobs/issues/prospects/contracts, firstname+surname for contacts)
  were confirmed against Accelo's published API doc source, not just the
  rendered docs site (which turned out to have some stale/misleading
  examples -- that's what caused the first version of this script to
  call a `/collections` endpoint that doesn't exist). Still worth a
  glance at a `--dry-run --limit-per-type 5` manifest to confirm names
  look right for your data before a full run.
- **More object types**: if attachments in your account also hang off
  something not in this list (e.g. a custom module), add a row to
  `TARGET_TYPES` in `export_attachments.py` with its endpoint, the field(s)
  needed for a display name, and a `name` lambda -- same shape as the six
  already there.

## Turning this into a client-facing tool later

The pipeline logic here (stages 1-4 in `export_attachments.py`) is the
part that would carry over into a hosted app -- it doesn't care who's
running it. What a real client-facing version would need on top:

- Per-client OAuth (each client authorizes *their* Accelo account --
  that's the standard "authorization code" grant, not the
  client-credentials flow this script uses for your own account).
- A job queue instead of a blocking script, since stage 2 alone could
  take a while for a large client account.
- Somewhere to land the files (e.g. zipped and handed back as a
  download, or streamed straight to S3/Google Drive) rather than a
  local `files/` folder.
- Basic multi-tenant plumbing: storing each client's token, isolating
  their output, not sharing rate-limit budget across clients.

None of that needs to be decided now -- it's just what's different
between "a script I run" and "a tool clients run themselves."
