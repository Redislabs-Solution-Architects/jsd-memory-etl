#!/usr/bin/env python3
"""Load call transcripts into Redis Agent Memory ETL, one document per call.

For every call in the dataset file this script:
  1. renders the transcript as a text/plain document,
  2. submits it to POST {service}/iris/data/v1/agent-memory/stores/{store}/document-ingestions
     with documentId = the call's session_id,
  3. waits for the (asynchronous) extraction to finish,
  4. collects the resulting memory keys and prints them
     (or "No Memory Extracted").

Usage:  python load_calls.py [dataset.json] [--extractor NAME] [--limit N]
"""

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path

import redis
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_FILE = "slang-synthetic-v2.json"
DONE_STATES = {"SUCCEEDED", "PARTIAL", "FAILED"}


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
        "idempotencyKey": f"{session_id}-revision-{revision}",
    }
    return check(session.post(f"{cfg.base}/document-ingestions", json=body, timeout=60))


def wait_and_collect(cfg, session, ingestion_id, poll_s, max_polls) -> tuple[str, list[str]]:
    state = "UNKNOWN"
    for _ in range(max_polls):
        rec = check(session.get(f"{cfg.base}/document-ingestions/{ingestion_id}", timeout=30))
        state = rec.get("ingestion", {}).get("status", {}).get("processing", "UNKNOWN")
        if state in DONE_STATES:
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


def add_metadata(cfg: Config, calls: list[dict], doc_keys: dict[str, list[str]]) -> None:
    """Set the `namespace` (<business_id>,<location_id>) and `session_id` attributes on
    each extracted memory HASH, stored at memory:<IRIS_STORE_ID>:ltm:<memory key>."""
    if not (cfg.redis_host and cfg.redis_port):
        print("REDIS_DB_HOST / REDIS_DB_PORT are not set; cannot add meta-data.", file=sys.stderr)
        return
    by_id = {c["session_id"]: c for c in calls}
    r = redis.Redis(host=cfg.redis_host, port=int(cfg.redis_port), username=cfg.redis_user,
                    password=cfg.redis_password or None, ssl=cfg.redis_ssl,
                    decode_responses=True, socket_timeout=15, socket_connect_timeout=15)
    r.ping()
    prefix = f"memory:{cfg.store_id}:ltm:"
    updated = missing = 0
    for doc_id, keys in doc_keys.items():
        call = by_id[doc_id]
        namespace = f"{call['business_id']},{call['location_id']}"
        for k in keys:
            full = prefix + k
            if r.type(full) != "hash":
                print(f"  {doc_id}: {full} not found as a HASH, skipped", file=sys.stderr)
                missing += 1
                continue
            r.hset(full, mapping={"namespace": namespace, "session_id": call["session_id"]})
            updated += 1
    print(f"Updated {updated} keys with meta data")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file", nargs="?", default=DEFAULT_FILE, help="dataset JSON (default: %(default)s)")
    ap.add_argument("--extractor", help="extractor name from EXTRACTOR_NAMES (skips the prompt)")
    ap.add_argument("--revision", help="document version (skips the prompt)")
    ap.add_argument("--only", nargs="+", metavar="SESSION_ID", help="process only these session_ids")
    ap.add_argument("--limit", type=int, help="only process the first N calls")
    ap.add_argument("--poll-seconds", type=float, default=10)
    ap.add_argument("--max-polls", type=int, default=90)
    args = ap.parse_args()

    cfg = Config()
    dataset = json.loads(Path(args.file).read_text(encoding="utf-8"))
    calls = dataset["calls"]
    if args.only:
        calls = [c for c in calls if c["session_id"] in set(args.only)]
    calls = calls[: args.limit]
    print(f"Loaded {len(calls)} call(s) from {args.file}")

    extractor_id = choose_extractor(cfg, args.extractor)
    print(f"Using extractor ID: {extractor_id}")

    revision = args.revision
    if revision is None:
        revision = input("Version/revision [1]: ").strip() or "1"

    http = requests.Session()
    # Retry transient network/DNS failures and 502/503/504. Safe for POST because
    # every submission carries an idempotencyKey.
    retry = Retry(total=5, connect=5, read=3, backoff_factor=2,
                  status_forcelist=(502, 503, 504), allowed_methods=None)
    http.mount("https://", HTTPAdapter(max_retries=retry))
    http.mount("http://", HTTPAdapter(max_retries=retry))
    http.headers["Authorization"] = f"Bearer {cfg.api_key}"

    # Submit everything first (extraction is async), then collect results.
    pending: dict[str, str] = {}
    results: dict[str, tuple[str, list[str]]] = {}
    for call in calls:
        sid = call["session_id"]
        try:
            acc = submit(cfg, http, call, extractor_id, revision)
            pending[sid] = acc["ingestionId"]
            print(f"  submitted {sid} -> ingestion {acc['ingestionId']}"
                  + (f" (callback: {acc['callback']})" if acc.get("callback") else ""))
        except Exception as e:  # keep the batch going
            results[sid] = (f"ERROR: {e}", [])
            print(f"  FAILED to submit {sid}: {e}", file=sys.stderr)

    for sid, ing in pending.items():
        try:
            results[sid] = wait_and_collect(cfg, http, ing, args.poll_seconds, args.max_polls)
        except Exception as e:
            results[sid] = (f"ERROR: {e}", [])

    print("\n=== Memory keys per call ===")
    for call in calls:
        sid = call["session_id"]
        state, keys = results[sid]
        print(f"{sid} [{state}]")
        if state.startswith("ERROR"):
            continue  # failure, not an empty extraction
        if keys:
            for k in keys:
                print(f"    {k}")
        else:
            print("    No Memory Extracted")

    # Map of documentId (session_id) -> extracted memory keys
    doc_keys = {sid: keys for sid, (state, keys) in results.items() if keys}
    if not doc_keys:
        return
    total = sum(len(k) for k in doc_keys.values())
    ans = input(f"\nAdd meta-data (namespace, session_id) to {total} extracted memories in Redis "
                f"({cfg.redis_host}:{cfg.redis_port})? [y/N]: ").strip().lower()
    if ans in ("y", "yes"):
        try:
            add_metadata(cfg, calls, doc_keys)
        except redis.RedisError as e:
            sys.exit(f"Redis error: {e}")


if __name__ == "__main__":
    main()
