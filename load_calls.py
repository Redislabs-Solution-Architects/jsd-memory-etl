#!/usr/bin/env python3
"""Load call transcripts into Redis Agent Memory ETL, one document per call.

For every call in every dataset file (*.json) in the data folder this script:
  1. renders the transcript as a text/plain document,
  2. submits it to POST {service}/iris/data/v1/agent-memory/stores/{store}/document-ingestions
     with documentId = the call's session_id,
  3. waits for the (asynchronous) extraction to finish,
  4. collects the resulting memory keys and prints them
     (or "No Memory Extracted").

Usage:  python load_calls.py [data_folder_or_file] [--extractor NAME] [--limit N]
"""

import argparse
import base64
import json
import logging
import os
import sys
import time
from pathlib import Path

import redis
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_PATH = Path(__file__).with_name("data")
DONE_STATES = {"SUCCEEDED", "PARTIAL", "FAILED"}
DEFAULT_LOG = Path(__file__).with_name("load_calls.log")
log = logging.getLogger("load_calls")


def mmss(seconds: float) -> str:
    """Format a duration as MM:SS (minutes may exceed 59)."""
    m, s = divmod(int(round(seconds)), 60)
    return f"{m:02d}:{s:02d}"


def resolve_env(name: str, default: str = "") -> str:
    """Environment first; if the value itself names a set env var, use that var's value;
    otherwise fall back to the literal value (from .env)."""
    raw = os.getenv(name, default)
    if raw and raw in os.environ:
        return os.environ[raw]
    return raw


class Config:
    def __init__(self) -> None:
        load_dotenv(Path(__file__).with_name(".env"), override=False)
        self.url = resolve_env("IRIS_SERVICE_URL").rstrip("/")
        self.api_key = resolve_env("IRIS_API_KEY")
        self.store_id = resolve_env("IRIS_STORE_ID")
        # Not used by the ETL calls themselves; loaded so the agent can reuse them.
        self.redis_host = resolve_env("REDIS_DB_HOST")
        self.redis_port = resolve_env("REDIS_DB_PORT")
        self.redis_user = resolve_env("REDIS_DB_USER", "default")
        self.redis_password = resolve_env("REDIS_DB_PASSWORD")
        self.redis_ssl = resolve_env("REDIS_DB_SSL", "false").lower() in ("1", "true", "yes")
        names = [n.strip() for n in resolve_env("EXTRACTOR_NAMES").split(",") if n.strip()]
        ids = [i.strip() for i in resolve_env("EXTRACTOR_IDS").split(",") if i.strip()]
        if len(names) != len(ids):
            sys.exit(f"EXTRACTOR_NAMES ({len(names)}) and EXTRACTOR_IDS ({len(ids)}) "
                     "must have the same number of comma-delimited values.")
        self.extractors = dict(zip(names, ids))
        missing = [n for n, v in (("IRIS_SERVICE_URL", self.url), ("IRIS_API_KEY", self.api_key),
                                  ("IRIS_STORE_ID", self.store_id)) if not v]
        if missing:
            sys.exit(f"Missing configuration: {', '.join(missing)} (set in .env or environment)")

    @property
    def base(self) -> str:
        return f"{self.url}/iris/data/v1/agent-memory/stores/{self.store_id}"


def call_to_document(call: dict) -> str:
    """Render one call as plain text, keeping the metadata useful for extraction."""
    lines = [
        f"Call transcript {call['session_id']}",
        f"Location: {call['location_name']} ({call['location_id']})",
        f"Business: {call['business_id']}",
        f"Caller: {call['caller_key']}",
        f"Started (local): {call['started_at_local']} [{call['location_timezone']}]",
        f"Started (UTC): {call['started_at']}",
        f"Language: {call['primary_language']}",
        f"Outcome: {call['outcome']}",
        "",
    ]
    for leg in call["legs"]:
        lines.append(f"--- Leg {leg['leg_id']} ({leg['leg_type']}) ---")
        for t in leg["turns"]:
            lines.append(f"[{t['role']}] {t['text']}")
        lines.append("")
    return "\n".join(lines)


def check(resp: requests.Response) -> dict:
    if not resp.ok:
        raise RuntimeError(f"{resp.status_code} {resp.request.method} {resp.url}: {resp.text[:500]}")
    return resp.json() if resp.content else {}


def choose_extractor(cfg: Config, entered: str | None) -> str:
    """Map an extractor name (from EXTRACTOR_NAMES) to its ID (EXTRACTOR_IDS).
    With no mapping configured, the entered value is used as the ID."""
    names = list(cfg.extractors)
    if not entered:
        shown = f" ({', '.join(names)})" if names else ""
        entered = input(f"Extractor name{shown}: ").strip()
    if not entered:
        sys.exit("An extractor name is required.")
    if not names:
        return entered
    for name, ext_id in cfg.extractors.items():
        if entered.lower() == name.lower():
            return ext_id
    if entered in cfg.extractors.values():  # an ID was typed directly
        return entered
    sys.exit(f"Unknown extractor '{entered}'. Choose one of: {', '.join(names)}")


def submit(cfg, session, call, extractor_id, revision) -> dict:
    session_id = call["session_id"]
    text = call_to_document(call)
    body = {
        "documentId": session_id,
        "revision": revision,
        "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
        "mediaType": "text/plain",
        "ownerId": call["caller_key"],
        "extractorId": extractor_id,
        "idempotencyKey": f"{session_id}-revision-{revision}-{extractor_id}",
    }
    return check(session.post(f"{cfg.base}/document-ingestions", json=body, timeout=60))


def wait_and_collect(cfg, session, ingestion_id, poll_s, max_polls,
                     label: str = "") -> tuple[str, list[str]]:
    state = "UNKNOWN"
    for _ in range(max_polls):
        rec = check(session.get(f"{cfg.base}/document-ingestions/{ingestion_id}", timeout=30))
        state = rec.get("ingestion", {}).get("status", {}).get("processing", "UNKNOWN")
        if state in DONE_STATES:
            if state == "FAILED":
                log.error("%s ingestion %s FAILED: %s", label, ingestion_id,
                          json.dumps(rec.get("ingestion", {}).get("status", {})))
            break
        time.sleep(poll_s)
    else:
        return state, []
    keys: list[str] = []
    cursor = None
    while True:
        params = {"limit": 100, **({"cursor": cursor} if cursor else {})}
        page = check(session.get(f"{cfg.base}/document-ingestions/{ingestion_id}/outputs",
                                 params=params, timeout=30))
        keys.extend(page.get("items", []))
        cursor = page.get("nextCursor")
        if not cursor:
            break
    return state, keys


def add_metadata(cfg: Config, doc_meta: dict[str, dict[str, str]],
                 doc_keys: dict[str, list[str]]) -> None:
    """Set the pre-computed meta-data (`namespace` = <business_id>,<location_id> and
    `session_id`) on each extracted memory HASH at memory:<IRIS_STORE_ID>:ltm:<memory key>.

    doc_meta is built once while the calls are processed, so the JSON is not re-read here.
    Commands are pipelined: one batch of TYPE checks, then one batch of HSETs."""
    if not (cfg.redis_host and cfg.redis_port):
        print("REDIS_DB_HOST / REDIS_DB_PORT are not set; cannot add meta-data.", file=sys.stderr)
        return
    r = redis.Redis(host=cfg.redis_host, port=int(cfg.redis_port), username=cfg.redis_user,
                    password=cfg.redis_password or None, ssl=cfg.redis_ssl,
                    decode_responses=True, socket_timeout=15, socket_connect_timeout=15)
    r.ping()
    prefix = f"memory:{cfg.store_id}:ltm:"
    targets = [(doc_id, prefix + k) for doc_id, keys in doc_keys.items() for k in keys]

    # Batch 1: confirm every key is an existing HASH (never create stray keys).
    pipe = r.pipeline(transaction=False)
    for _, full in targets:
        pipe.type(full)
    types = pipe.execute()

    # Batch 2: write the meta-data to the HASHes that exist.
    pipe = r.pipeline(transaction=False)
    updated = 0
    for (doc_id, full), t in zip(targets, types):
        if t != "hash":
            print(f"  {doc_id}: {full} not found as a HASH, skipped", file=sys.stderr)
            continue
        pipe.hset(full, mapping=doc_meta[doc_id])
        updated += 1
    if updated:
        pipe.execute()
    print(f"Updated {updated} keys with meta data")


def load_calls(path: Path) -> list[dict]:
    """Collect calls from one dataset file, or from every *.json file in a folder
    (all files share the same schema). Duplicate session_ids are skipped."""
    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    if not files:
        sys.exit(f"No .json files found in {path}")
    calls: list[dict] = []
    seen: set[str] = set()
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            found = data["calls"]
        except (OSError, ValueError, KeyError, TypeError) as e:
            print(f"  skipping {f.name}: not a valid dataset ({e})", file=sys.stderr)
            continue
        added = 0
        for c in found:
            if c["session_id"] in seen:
                print(f"  duplicate session_id {c['session_id']} in {f.name}, skipped", file=sys.stderr)
                continue
            seen.add(c["session_id"])
            calls.append(c)
            added += 1
        print(f"  {f.name}: {added} call(s)")
    return calls


def choose_mode() -> str:
    """Prompt for the execution mode. Option 1 (default) is the single extraction path."""
    print("\n[1] Single Extraction\n[2] Simulate Bulk Load")
    while True:
        ans = input("Select an option [1]: ").strip() or "1"
        if ans in ("1", "2"):
            return "single" if ans == "1" else "bulk"
        print("Please enter 1 or 2.")


def ask_positive(prompt: str) -> int:
    while True:
        ans = input(prompt).strip()
        if ans.isdigit() and int(ans) > 0:
            return int(ans)
        print("Please enter a whole number greater than 0.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", nargs="?", default=str(DEFAULT_PATH),
                    help="folder of dataset JSON files, or a single file (default: %(default)s)")
    ap.add_argument("--extractor", help="extractor name from EXTRACTOR_NAMES (skips the prompt)")
    ap.add_argument("--revision", help="document version (skips the prompt)")
    ap.add_argument("--only", nargs="+", metavar="SESSION_ID", help="process only these session_ids")
    ap.add_argument("--limit", type=int, help="only process the first N calls")
    ap.add_argument("--log-file", default=str(DEFAULT_LOG), help="error log file (default: %(default)s)")
    ap.add_argument("--poll-seconds", type=float, default=10)
    ap.add_argument("--max-polls", type=int, default=90)
    args = ap.parse_args()

    handler = logging.FileHandler(args.log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.ERROR)

    cfg = Config()
    calls = load_calls(Path(args.path))
    if args.only:
        calls = [c for c in calls if c["session_id"] in set(args.only)]
    calls = calls[: args.limit]
    print(f"Loaded {len(calls)} call(s) from {args.path}")

    extractor_id = choose_extractor(cfg, args.extractor)
    print(f"Using extractor ID: {extractor_id}")

    if not calls:
        sys.exit("No calls to process.")

    bulk = choose_mode() == "bulk"
    if bulk:
        total_docs = ask_positive("Number of Documents: ")
        batch_size = min(ask_positive("Batch Size: "), total_docs)
        run_ts = int(time.time())  # Unix timestamp at the start of the run, unique across runs
        # Submission i uses call (i mod number of calls). Each pass over the calls gets its own
        # version, so a document id is never submitted twice with the same version.
        plan = [(calls[i % len(calls)], f"batch-{run_ts}-{i // len(calls) + 1}")
                for i in range(total_docs)]
    else:
        revision = args.revision
        if revision is None:
            revision = input("Version/revision [1]: ").strip() or "1"
        plan = [(call, revision) for call in calls]
        batch_size = len(plan)

    http = requests.Session()
    # Retry transient network/DNS failures and 502/503/504. Safe for POST because
    # every submission carries an idempotencyKey.
    retry = Retry(total=5, connect=5, read=3, backoff_factor=2,
                  status_forcelist=(502, 503, 504), allowed_methods=None)
    http.mount("https://", HTTPAdapter(max_retries=retry))
    http.mount("http://", HTTPAdapter(max_retries=retry))
    http.headers["Authorization"] = f"Bearer {cfg.api_key}"

    # documentId -> HASH meta-data, built once for the metadata step
    doc_meta = {c["session_id"]: {"namespace": f"{c['business_id']},{c['location_id']}",
                                  "session_id": c["session_id"]} for c in calls}

    results: list[tuple[str, str, list[str]]] = []  # (session_id, state, keys)
    extracted = 0
    live = sys.stdout.isatty()  # in-place counter only on a real terminal

    def show_count() -> None:
        if live:
            print(f"\rMemories Extracted : {extracted}", end="", flush=True)

    def say(msg: str, err: bool = False) -> None:
        """Print a line without leaving the in-place counter garbled."""
        if live:
            print("\r" + " " * 40 + "\r", end="")
        print(msg, file=sys.stderr if err else sys.stdout)
        show_count()

    # Work through the plan in batches. Extraction is asynchronous, so each batch is
    # submitted, then every submission's status is polled to a final state before the
    # next batch is sent.
    t_start = time.monotonic()
    print()
    show_count()
    for start in range(0, len(plan), batch_size):
        jobs: list[list] = []  # [session_id, ingestion_id or None, error or None]
        for call, revision in plan[start:start + batch_size]:
            sid = call["session_id"]
            try:
                acc = submit(cfg, http, call, extractor_id, revision)
                jobs.append([sid, acc["ingestionId"], None])
                if not bulk:
                    say(f"  submitted {sid} -> ingestion {acc['ingestionId']}"
                        + (f" (callback: {acc['callback']})" if acc.get("callback") else ""))
            except Exception as e:  # keep the run going
                jobs.append([sid, None, f"ERROR: {e}"])
                say(f"  FAILED to submit {sid} ({revision}): {e}", err=True)
                log.error("%s (%s) submit failed: %s", sid, revision, e)

        if bulk and start + batch_size >= len(plan):
            say(f"All {len(plan)} documents submitted in {mmss(time.monotonic() - t_start)} (min:sec)")

        for sid, ing, err in jobs:
            if err:
                results.append((sid, err, []))
                continue
            try:
                state, keys = wait_and_collect(cfg, http, ing, args.poll_seconds, args.max_polls,
                                               label=sid)
            except Exception as e:
                state, keys = f"ERROR: {e}", []
                log.error("%s ingestion %s error: %s", sid, ing, e)
            results.append((sid, state, keys))
            extracted += len(keys)
            show_count()
    if live:
        print()  # finish the in-place line
    else:
        print(f"Memories Extracted : {extracted}")

    # Map of documentId (session_id) -> extracted memory keys
    doc_keys: dict[str, list[str]] = {}
    for sid, _, keys in results:
        doc_keys.setdefault(sid, []).extend(k for k in keys if k not in doc_keys[sid])
    doc_keys = {sid: keys for sid, keys in doc_keys.items() if keys}

    if bulk:
        elapsed = time.monotonic() - t_start
        errors = sum(1 for _, state, _ in results if state.startswith("ERROR") or state == "FAILED")
        n_batches = -(-len(plan) // batch_size)
        print(f"\nSubmitted {len(plan)} documents in {n_batches} batches of up to {batch_size}; "
              f"all callbacks received: "
              f"{sum(len(k) for k in doc_keys.values())} memory keys extracted, {errors} errors.")
        print(f"Total time from first submission to last callback: {mmss(elapsed)} (min:sec)")
        if errors:
            print(f"Errors were written to {args.log_file}")
    else:
        print("\n=== Memory keys per call ===")
        for sid, state, keys in results:
            print(f"{sid} [{state}]")
            if state.startswith("ERROR"):
                continue  # failure, not an empty extraction
            if keys:
                for k in keys:
                    print(f"    {k}")
            else:
                print("    No Memory Extracted")

    if not doc_keys:
        return
    total = sum(len(k) for k in doc_keys.values())
    ans = input(f"\nAdd meta-data (namespace, session_id) to {total} extracted memories in Redis "
                f"({cfg.redis_host}:{cfg.redis_port})? [y/N]: ").strip().lower()
    if ans in ("y", "yes"):
        try:
            add_metadata(cfg, doc_meta, doc_keys)
        except redis.RedisError as e:
            sys.exit(f"Redis error: {e}")


if __name__ == "__main__":
    main()
