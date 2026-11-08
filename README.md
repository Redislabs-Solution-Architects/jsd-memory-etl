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
- One or more call transcript dataset files in the `data/` folder, in the format below

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

Dataset files go in the `data/` folder. A sample is `data/agent-turns.json`.

- Each file is JSON with a `calls` array, defined by `schema-dataset.v2.json`.
- Each call follows `schema-call.v2.json`.
- All `*.json` files in `data/` are processed, so they must all share this schema.

## Usage

```powershell
python load_calls.py [data_folder_or_file] [options]
```

By default the script loads every `.json` file in the `data/` folder, in name order, and prints how many calls each file contributed. You can pass a different folder or a single file instead.

- A file that is not a valid dataset is skipped with a warning.
- A `session_id` that appears more than once is processed once, because it is the `documentId`.
- An empty folder stops the run with an error.

When run, the script prompts for:

1. **Extractor name**, listing the names from `EXTRACTOR_NAMES`. The matching ID is sent as `extractorId`.
2. **Mode**, with option 1 as the default:
   - `[1] Single Extraction`: submits every call once. It then asks for the **version/revision** (default `1`) and prints each ingestion ID and the memory keys per call.
   - `[2] Simulate Bulk Load`: asks for the **number of batches**. Each batch submits every call again with the version `batch-<batch number>` (`batch-1`, `batch-2`, ...). No per-ingestion IDs or per-call keys are printed. Once every document is submitted it prints `All N documents submitted in MM:SS (min:sec)`. After every result is received it prints one summary line, such as `Submitted 3 batches (36 documents); all callbacks received: 36 memory keys extracted, 0 errors.` It then prints `Total time from first submission to last callback: MM:SS (min:sec)`. The `--revision` option does not apply in this mode.

In both modes the script then asks whether to add metadata. In bulk mode, a document's keys from all batches are updated together.

| Option | Effect |
|:--|:--|
| `--extractor NAME` | Skip the extractor prompt |
| `--revision VALUE` | Skip the version prompt |
| `--only SESSION_ID [...]` | Process only these calls |
| `--limit N` | Process only the first N calls |
| `--log-file PATH` | Error log file (default `load_calls.log` next to the script) |
| `--poll-seconds`, `--max-polls` | Polling interval (default 10 s) and number of polls (default 90) |

## What it does

1. **Render:** each call from every file is turned into a `text/plain` transcript with its location, caller, time, language and outcome, followed by the dialogue as `[role] text` lines.
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
- Errors are written to the log file: failed submissions, ingestions that finish as `FAILED` (with the service's status details) and errors while collecting results. Each entry is timestamped and names the `session_id`.
