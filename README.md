# jsd-memory-etl
Memory ETL is a functional module of the Agent Memory Service which facilitate offline memory extractions into standard long term memories.

`load_calls.py` loads call transcripts from a JSON dataset into the Redis Agent Memory ETL API, one document per call, and then optionally tags the extracted memories in Redis with metadata.

## Requirements

- Python 3.10 or later
- Python packages (see `requirements.txt`):
  - `requests`
  - `python-dotenv`
  - `redis`
- A Redis Agent Memory service (Data Plane URL, agent API key and Store ID)
- At least one extractor already created in the store, with its name and ID
- A Redis database holding the long-term memories, reachable from this machine (needed only for the metadata step)
- A call transcript dataset in the format below

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env` with your values.

## Configuration (`.env`)

Every value is read from an environment variable first. If the value itself is the name of an environment variable that is set, that variable's value is used; otherwise the literal value from `.env` is used.

| Variable | Purpose |
|:--|:--|
| `IRIS_SERVICE_URL` | Data Plane base URL, including `https://` |
| `IRIS_API_KEY` | Agent API key, sent as a Bearer token |
| `IRIS_STORE_ID` | Store the documents are ingested into |
| `EXTRACTOR_NAMES` | Comma-delimited extractor names |
| `EXTRACTOR_IDS` | Comma-delimited extractor IDs, in the same order and count as the names |
| `REDIS_DB_HOST`, `REDIS_DB_PORT` | Redis database holding the memories |
| `REDIS_DB_USER`, `REDIS_DB_PASSWORD` | Database credentials (user defaults to `default`) |
| `REDIS_DB_SSL` | `true` if the database requires TLS |

Keep `.env` out of version control, since it holds secrets.

## Input format

The dataset is a JSON file with a `calls` array, defined by `schema-dataset.v2.json`. Each call follows `schema-call.v2.json`. A sample is in `slang-synthetic-v2.json`.

## Usage

```powershell
python load_calls.py [dataset.json] [options]
```

The dataset defaults to `slang-synthetic-v2.json`. When run, the script prompts for:

1. **Extractor name**, listing the names from `EXTRACTOR_NAMES`. The matching ID is sent as `extractorId`.
2. **Version/revision** (default `1`).

| Option | Effect |
|:--|:--|
| `--extractor NAME` | Skip the extractor prompt |
| `--revision VALUE` | Skip the version prompt |
| `--only SESSION_ID [...]` | Process only these calls |
| `--limit N` | Process only the first N calls |
| `--poll-seconds`, `--max-polls` | Polling interval (default 10 s) and number of polls (default 90) |

## What it does

1. **Render:** each call is turned into a `text/plain` transcript with its location, caller, time, language and outcome, followed by the dialogue as `[role] text` lines.
2. **Submit:** each transcript is posted to `/iris/data/v1/agent-memory/stores/<store>/document-ingestions`. Retries with backoff cover connection errors and 502/503/504 responses.

   | API field | Value |
   |:--|:--|
   | `documentId` | the call's `session_id` |
   | `ownerId` | the call's `caller_key` |
   | `revision` | the version you entered |
   | `extractorId` | the ID mapped from the chosen name |
   | `mediaType` | `text/plain` |
   | `idempotencyKey` | `<session_id>-revision-<revision>-<extractor_id>` |

3. **Wait:** extraction is asynchronous. The script polls each ingestion until it reports `SUCCEEDED`, `PARTIAL` or `FAILED`.
4. **Report:** it prints the memory keys for each call, or `No Memory Extracted`. A call that failed shows its error instead.
5. **Add metadata (optional):** the script keeps a map of document ID to memory keys. After reporting, it asks whether to add metadata. On `y` it connects to the Redis database and, for each memory HASH at `memory:<IRIS_STORE_ID>:ltm:<memory key>`, sets:

   | HASH field | Value |
   |:--|:--|
   | `namespace` | `<business_id>,<location_id>` |
   | `session_id` | the call's `session_id` |

   It prints `Updated N keys with meta data`. A key that does not exist as a HASH is skipped with a warning.

## Notes

- A document and revision can be submitted only once. To re-extract a call, use a new version.
- To retry a failed call, run `python load_calls.py --only <session_id>`.
- The metadata step overwrites any existing `namespace` and `session_id` fields on those HASHes.
