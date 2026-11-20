#!/usr/bin/env python3
"""AER-1 independent, durable test-effect provider for APC evaluation.

This is a synthetic test ledger, not a real payment, commerce, CRM, or
customer provider. It implements the minimal generic HTTPS contract consumed
by APC's HttpsProviderAdapter:

  POST /v1/execute                 provider effect (Idempotency-Key required)
  GET  /v1/lookup/{idempotency_key} authoritative lookup (404 means UNKNOWN)
  GET  /v1/records?operation_key=...&session_id=...  independent counter
  GET  /v1/records/{record_id}     public record for third-party recount
  GET  /healthz

The provider keeps one SQLite row per provider idempotency key. Its public
record exposes the exact APC outcome object and a SHA-256 commitment over
canonical JSON, allowing the adapter to compare the outcome recovered from
APC with the independently retained provider result.

Set APC_TEST_SINK_TOKEN to a random test-only bearer token. By default all
non-health routes require it. Set APC_TEST_SINK_PUBLIC_READS=true only when
the database contains synthetic test data and you intend third parties to
recount published results. POST and /lookup remain authenticated regardless.
Run behind a customer-controlled HTTPS reverse proxy or ingress; this simple
reference server itself speaks HTTP only.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from contextlib import contextmanager
from urllib.parse import parse_qs, urlparse

MAX_BODY = 1_000_000


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def db_path() -> str:
    value = os.environ.get("APC_TEST_SINK_DB", "./var/apc-test-provider.sqlite3")
    Path(value).parent.mkdir(parents=True, exist_ok=True)
    return value


@contextmanager
def connect():
    conn = sqlite3.connect(db_path(), timeout=20, isolation_level="IMMEDIATE")
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db():
    with connect() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("""CREATE TABLE IF NOT EXISTS effects (
            record_id TEXT PRIMARY KEY,
            provider_idempotency_key TEXT NOT NULL UNIQUE,
            request_digest TEXT NOT NULL,
            operation TEXT NOT NULL,
            resource TEXT NOT NULL,
            test_operation_key TEXT NOT NULL,
            test_session_id TEXT NOT NULL,
            status TEXT NOT NULL,
            provider_reference TEXT NOT NULL UNIQUE,
            apc_outcome_json TEXT NOT NULL,
            outcome_digest TEXT NOT NULL,
            created_at REAL NOT NULL
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_effects_test_key ON effects(test_operation_key, test_session_id)")


def record_dict(row):
    return {
        "record_id": row["record_id"],
        "test_operation_key": row["test_operation_key"],
        "test_session_id": row["test_session_id"],
        "operation": row["operation"],
        "resource": row["resource"],
        "status": row["status"],
        "provider_reference": row["provider_reference"],
        "apc_outcome": json.loads(row["apc_outcome_json"]),
        "outcome_digest": row["outcome_digest"],
        "created_at": row["created_at"],
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "APC-AER1-Test-Provider/1.0"
    sys_version = ""

    def log_message(self, fmt, *args):
        # Exclude query values and never log request bodies or authorization.
        path = urlparse(self.path).path
        code = args[1] if len(args) > 1 else "-"
        print(f"{self.command} {path} {code}")

    def _send(self, code, obj):
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self):
        expected = os.environ.get("APC_TEST_SINK_TOKEN", "")
        supplied = self.headers.get("Authorization", "")
        if not expected:
            return False
        return hmac.compare_digest(supplied, "Bearer " + expected)

    def _authorized_read(self):
        public_reads = os.environ.get("APC_TEST_SINK_PUBLIC_READS", "false").lower() == "true"
        return public_reads or self._authorized()

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return None, (400, {"error": "invalid_content_length"})
        if n <= 0 or n > MAX_BODY:
            return None, (413, {"error": "body_size_invalid"})
        try:
            obj = json.loads(self.rfile.read(n))
        except Exception:
            return None, (400, {"error": "invalid_json"})
        if not isinstance(obj, dict):
            return None, (400, {"error": "json_object_required"})
        return obj, None

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/healthz":
            return self._send(200, {"status": "OK", "service": "apc-aer1-test-provider"})
        if path.startswith("/v1/lookup/"):
            if not self._authorized():
                return self._send(401, {"error": "unauthorized"})
            key = path.rsplit("/", 1)[-1]
            with connect() as c:
                row = c.execute("SELECT * FROM effects WHERE provider_idempotency_key=?", (key,)).fetchone()
            if not row:
                # APC's provider adapter conservatively maps 404 to UNKNOWN.
                return self._send(404, {"error": "not_found_is_not_proof_of_no_effect"})
            rec = record_dict(row)
            return self._send(200, {
                "status": rec["status"],
                "provider_reference": rec["provider_reference"],
            })
        if path == "/v1/records":
            if not self._authorized_read():
                return self._send(401, {"error": "unauthorized"})
            query = parse_qs(parsed.query)
            key = (query.get("operation_key") or [None])[0]
            session_id = (query.get("session_id") or [None])[0]
            if not key:
                return self._send(400, {"error": "operation_key_required"})
            sql = "SELECT * FROM effects WHERE test_operation_key=?"
            args = [key]
            if session_id is not None:
                sql += " AND test_session_id=?"
                args.append(session_id)
            sql += " ORDER BY created_at, record_id"
            with connect() as c:
                rows = c.execute(sql, tuple(args)).fetchall()
            return self._send(200, {"items": [record_dict(r) for r in rows], "count": len(rows)})
        if path.startswith("/v1/records/"):
            if not self._authorized_read():
                return self._send(401, {"error": "unauthorized"})
            rid = path.rsplit("/", 1)[-1]
            with connect() as c:
                row = c.execute("SELECT * FROM effects WHERE record_id=?", (rid,)).fetchone()
            if not row:
                return self._send(404, {"error": "record_not_found"})
            return self._send(200, record_dict(row))
        return self._send(404, {"error": "not_found"})

    def do_POST(self):
        if urlparse(self.path).path != "/v1/execute":
            return self._send(404, {"error": "not_found"})
        if not self._authorized():
            return self._send(401, {"error": "unauthorized"})
        request_key = self.headers.get("Idempotency-Key", "")
        if not request_key or len(request_key) > 200:
            return self._send(400, {"error": "idempotency_key_required"})
        body, error = self._body()
        if error:
            return self._send(*error)
        operation = body.get("operation")
        resource = body.get("resource")
        payload = body.get("payload")
        if not isinstance(operation, str) or not operation.strip() or not isinstance(resource, str) or not resource.strip() or not isinstance(payload, dict):
            return self._send(400, {"error": "operation_resource_payload_required"})
        op_key = payload.get("test_operation_key")
        session_id = payload.get("test_session_id")
        if not isinstance(op_key, str) or not op_key or not isinstance(session_id, str) or not session_id:
            return self._send(400, {"error": "test_operation_key_and_test_session_id_required"})
        request_digest = digest(body)
        with connect() as c:
            existing = c.execute("SELECT * FROM effects WHERE provider_idempotency_key=?", (request_key,)).fetchone()
            if existing:
                if existing["request_digest"] != request_digest:
                    return self._send(409, {"error": "idempotency_conflict"})
                rec = record_dict(existing)
                return self._send(200, {"status": "SUCCESS", "provider_reference": rec["provider_reference"], "replayed": True})
            rid = str(uuid.uuid4())
            provider_reference = "aer1-test-" + rid
            outcome = {"status": "SUCCESS", "provider_reference": provider_reference}
            outcome_digest = hashlib.sha256(canonical(outcome)).hexdigest()
            c.execute(
                "INSERT INTO effects VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    rid, request_key, request_digest, operation, resource,
                    op_key, session_id, "SUCCESS", provider_reference,
                    json.dumps(outcome, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                    outcome_digest, time.time(),
                ),
            )
        return self._send(200, {"status": "SUCCESS", "provider_reference": provider_reference, "replayed": False})


def main():
    init_db()
    host = os.environ.get("APC_TEST_SINK_HOST", "127.0.0.1")
    port = int(os.environ.get("APC_TEST_SINK_PORT", "8790"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"APC AER-1 test provider listening on {host}:{port}; TLS must terminate upstream")
    server.serve_forever()


if __name__ == "__main__":
    main()
