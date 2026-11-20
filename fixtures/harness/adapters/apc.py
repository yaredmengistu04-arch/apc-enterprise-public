"""APC Enterprise production-API adapter for the lost-response harness.

Target API (APC 7.1.8-dev production integration):
  GET  {base_url}/healthz
  GET  {base_url}/readyz
  POST {base_url}/v1/actions
  POST {base_url}/v1/actions/recover   (Idempotency-Key only + fresh bearer auth)

The adapter deliberately does NOT infer an external effect from APC's HTTP
response or APC's own action record. It reads a separately operated,
durable test-provider ledger (the companion apc_test_provider.py fixture)
for the independently countable external effect. The provider fixture is a
non-payment, synthetic evaluation sink; do not point this adapter at a real
provider or customer endpoint.

adapter_config keys:
  base_url: APC API base URL, e.g. https://apc-eval.example (required).
  token_env: environment variable containing a short-lived APC bearer token
             with permission to execute and recover the evaluation workload.
  expected_version: exact /healthz version; defaults to 7.1.8-dev.
  strict_version: fail initialization if version differs; defaults true.
  timeout_seconds: per-request timeout; defaults 20.
  tls_verify: bool or CA-bundle path; defaults true.
  action_template: request object for POST /v1/actions. String values may
                   contain {K} and {SESSION}. Both values must be projected
                   through the provider allowlist as test_operation_key and
                   test_session_id (or the configured field names).
  mismatch: {field: dotted.path, altered_value: ...}; exactly this one field
            is changed for A2. It must be part of action_template.
  effect_counter: independent test-provider ledger settings:
      base_url, list_path (default /v1/records),
      record_path_template (default /v1/records/{record_id}),
      token_env (optional for public read endpoints), items_path (default
      items), id_field (default record_id), session_field (default
      test_session_id), operation_key_field (default test_operation_key),
      status_field (default status), success_values (default [SUCCESS]),
      outcome_field (default apc_outcome), digest_field (default outcome_digest).

No bearer tokens are read from config files or written to evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import requests

from .base import SystemAdapter


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _path_get(obj, dotted, default=None):
    cur = obj
    if not dotted:
        return obj
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _path_set(obj, dotted, value):
    parts = dotted.split(".")
    if not dotted or any(not p for p in parts):
        raise ValueError("mismatch.field must be a non-empty dotted path")
    cur = obj
    for part in parts[:-1]:
        if not isinstance(cur, dict) or part not in cur or not isinstance(cur[part], dict):
            raise ValueError(f"mismatch path does not exist: {dotted}")
        cur = cur[part]
    if not isinstance(cur, dict) or parts[-1] not in cur:
        raise ValueError(f"mismatch path does not exist: {dotted}")
    cur[parts[-1]] = value


def _substitute(value, mapping):
    if isinstance(value, str):
        out = value
        for key, item in mapping.items():
            out = out.replace("{" + key + "}", str(item))
        return out
    if isinstance(value, dict):
        return {k: _substitute(v, mapping) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, mapping) for v in value]
    return value


def _response_json(response):
    try:
        value = response.json()
        return value if isinstance(value, dict) else {"_non_object_json": value}
    except Exception:
        return {"_body_text": response.text[:4000]}


class Adapter(SystemAdapter):
    """Production REST adapter. The external effect counter is independent."""

    name = "apc"

    def __init__(self, config):
        super().__init__(config)
        self.base_url = str(self.config.get("base_url", "")).rstrip("/")
        if not self.base_url.startswith("https://"):
            raise ValueError("APC base_url must be HTTPS")
        self.token_env = self.config.get("token_env", "APC_EVAL_BEARER_TOKEN")
        self.expected_version = self.config.get("expected_version", "7.1.8-dev")
        self.strict_version = bool(self.config.get("strict_version", True))
        self.timeout = float(self.config.get("timeout_seconds", 20))
        self.verify = self.config.get("tls_verify", True)
        self.action_template = self.config.get("action_template")
        self.mismatch = self.config.get("mismatch") or {}
        self.counter = self.config.get("effect_counter") or {}
        if not isinstance(self.action_template, dict):
            raise ValueError("action_template must be a JSON object")
        if not isinstance(self.mismatch.get("field"), str) or "altered_value" not in self.mismatch:
            raise ValueError("mismatch must define field and altered_value")
        if not self.counter.get("base_url"):
            raise ValueError("effect_counter.base_url is required; no APC-response-only counter is accepted")
        if not str(self.counter["base_url"]).startswith("https://"):
            raise ValueError("effect_counter.base_url must be HTTPS")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _apc_token(self):
        token = os.environ.get(self.token_env, "").strip()
        if not token:
            raise RuntimeError(f"required APC token environment variable is missing: {self.token_env}")
        return token

    def _counter_token(self):
        env_name = self.counter.get("token_env")
        return os.environ.get(env_name, "").strip() if env_name else ""

    def _apc_headers(self, *, operation_key=None):
        headers = {
            "Authorization": "Bearer " + self._apc_token(),
            "Content-Type": "application/json",
            "X-Request-ID": "apc-aer1-" + uuid.uuid4().hex,
        }
        if operation_key is not None:
            headers["Idempotency-Key"] = operation_key
        return headers

    def _counter_headers(self):
        headers = {"Accept": "application/json"}
        token = self._counter_token()
        if token:
            headers["Authorization"] = "Bearer " + token
        return headers

    def _request(self, method, url, *, headers=None, json_body=None, params=None):
        started = time.monotonic()
        started_at = _utcnow()
        try:
            response = self.session.request(
                method, url, headers=headers, json=json_body, params=params,
                timeout=self.timeout, verify=self.verify,
            )
            elapsed_ms = round((time.monotonic() - started) * 1000, 3)
            return response, {
                "request_started_at": started_at,
                "response_received_at": _utcnow(),
                "http_status": response.status_code,
                "response_bytes": len(response.content),
                "elapsed_ms": elapsed_ms,
                "request_id": response.headers.get("X-Request-ID"),
                "transport_error": None,
            }
        except requests.RequestException as exc:
            elapsed_ms = round((time.monotonic() - started) * 1000, 3)
            return None, {
                "request_started_at": started_at,
                "response_received_at": _utcnow(),
                "http_status": None,
                "response_bytes": None,
                "elapsed_ms": elapsed_ms,
                "request_id": None,
                "transport_error": type(exc).__name__,
            }

    def initialize(self):
        health, health_meta = self._request("GET", self.base_url + "/healthz")
        ready, ready_meta = self._request("GET", self.base_url + "/readyz")
        health_body = _response_json(health) if health is not None else None
        ready_body = _response_json(ready) if ready is not None else None
        version = (health_body or {}).get("version")
        if health is None or health.status_code != 200:
            raise RuntimeError(f"APC /healthz failed: {health_meta}")
        if ready is None or ready.status_code != 200:
            raise RuntimeError(f"APC /readyz failed: {ready_meta}")
        if self.strict_version and self.expected_version and version != self.expected_version:
            raise RuntimeError(
                f"APC version pin mismatch: expected {self.expected_version!r}, observed {version!r}; "
                "stop rather than silently test another candidate"
            )
        evidence = {
            "apc_base_url": self.base_url,
            "health": {"transport": health_meta, "body": health_body},
            "readiness": {"transport": ready_meta, "body": ready_body},
            "expected_version": self.expected_version,
            "observed_version": version,
            "strict_version": self.strict_version,
        }
        summary = {
            "health_http_status": health.status_code,
            "ready_http_status": ready.status_code,
            "version": version,
            "version_pin_matched": (not self.expected_version or version == self.expected_version),
        }
        return evidence, summary

    def _action_request(self, session_id, operation_key, mismatched):
        request = _substitute(
            self.action_template,
            {"K": operation_key, "SESSION": session_id},
        )
        if mismatched:
            _path_set(request, self.mismatch["field"], self.mismatch["altered_value"])
        # The values used for attribution must actually be in the action's
        # projected provider payload; otherwise the independent counter cannot
        # safely attribute a record to this harness operation.
        params = request.get("parameters")
        if not isinstance(params, dict):
            raise ValueError("action_template.parameters must be an object")
        if operation_key not in json.dumps(params, ensure_ascii=False):
            raise ValueError("action_template.parameters must include the substituted operation key {K}")
        if session_id not in json.dumps(params, ensure_ascii=False):
            raise ValueError("action_template.parameters must include the substituted session id {SESSION}")
        return request

    def _classify(self, response, body):
        if response is None:
            return "transport_error"
        status = response.status_code
        err = str((body or {}).get("error", ""))
        state = str((body or {}).get("status", "")).upper()
        if status == 409 and err == "idempotency_binding_mismatch":
            return "tool_error:idempotency_binding_mismatch"
        if status == 404 and err == "operation_not_found":
            return "not_found"
        if status == 403 and err == "recovery_not_authorized":
            return "tool_error:recovery_not_authorized"
        if status == 401:
            return "tool_error:unauthorized"
        if status == 200 and state == "SUCCESS":
            return "success"
        # Only the exact APC binding-mismatch response is an A2 rejection.
        # Other errors remain unclassified so the frozen harness cannot
        # mistakenly score an infrastructure/auth/provider error as proof
        # that APC rejected a changed action under the same K.
        return None

    def execute(self, *, session_id, operation_key, mismatched=False, drop_response=False):
        request_body = self._action_request(session_id, operation_key, mismatched)
        response, transport = self._request(
            "POST", self.base_url + "/v1/actions",
            headers=self._apc_headers(operation_key=operation_key),
            json_body=request_body,
        )
        if drop_response:
            # The request is allowed to finish and commit. The client then
            # discards every response byte except transport metadata. No body,
            # invocation ID, provider reference, receipt ID or digest escapes.
            return {
                "arguments_sent": request_body,
                "call": {
                    "dropped": True,
                    "transport": transport,
                    "response_class": "dropped_response" if response is not None else "transport_error",
                    "receipt_ids": [],
                },
            }
        body = _response_json(response) if response is not None else None
        # The harness's receipt_ids must resolve through fetch_record().
        # APC invocation IDs are not records in the external-effect ledger,
        # so use identifiers read independently from the provider ledger.
        external_record_ids = []
        if response is not None and response.status_code == 200 and str((body or {}).get("status", "")).upper() in {"SUCCESS", "FAILURE"}:
            try:
                records, _, _ = self._verified_effect_records(
                    operation_key=operation_key, session_id=session_id,
                    label="post_execute_receipt_resolution",
                )
                external_record_ids = sorted({str(x[self.counter.get("id_field", "record_id")]) for x in records})
            except Exception:
                # Do not substitute an APC invocation ID or an inferred ID.
                # A missing external record leaves the run-id leg unproven.
                external_record_ids = []
        call = {
            "dropped": False,
            "transport": transport,
            "body": body,
            "response_class": self._classify(response, body),
            "receipt_ids": external_record_ids,
            "apc_invocation_id": body.get("invocation_id") if isinstance(body, dict) else None,
            "receipt_id_source": "independent_test_provider_ledger" if external_record_ids else "none",
        }
        return {"arguments_sent": request_body, "call": call}

    def _counter_list(self, *, operation_key, session_id=None):
        cfg = self.counter
        params = {"operation_key": operation_key}
        if session_id is not None:
            params["session_id"] = session_id
        path = cfg.get("list_path", "/v1/records")
        url = str(cfg["base_url"]).rstrip("/") + "/" + path.lstrip("/")
        response, meta = self._request("GET", url, headers=self._counter_headers(), params=params)
        body = _response_json(response) if response is not None else None
        if response is None or response.status_code != 200:
            raise RuntimeError(f"independent effect-counter listing failed ({meta})")
        items_path = cfg.get("items_path", "items")
        items = _path_get(body, items_path, body if isinstance(body, list) else None)
        if not isinstance(items, list):
            raise ValueError(f"counter listing did not contain a list at {items_path!r}")
        return body, items, meta

    def _counter_record(self, record_id):
        cfg = self.counter
        template = cfg.get("record_path_template", "/v1/records/{record_id}")
        path = template.replace("{record_id}", quote(str(record_id), safe=""))
        url = str(cfg["base_url"]).rstrip("/") + "/" + path.lstrip("/")
        response, meta = self._request("GET", url, headers=self._counter_headers())
        body = _response_json(response) if response is not None else None
        return response, body, meta

    def _verified_effect_records(self, *, operation_key, session_id=None, label="probe"):
        listing, items, meta = self._counter_list(operation_key=operation_key, session_id=session_id)
        cfg = self.counter
        id_field = cfg.get("id_field", "record_id")
        session_field = cfg.get("session_field", "test_session_id")
        key_field = cfg.get("operation_key_field", "test_operation_key")
        status_field = cfg.get("status_field", "status")
        success_values = {str(x).upper() for x in cfg.get("success_values", ["SUCCESS"])}
        files = {f"counter_listing_{label}.json": {"transport": meta, "body": listing}}
        found, non_execution = [], []
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            rid = item.get(id_field)
            if not rid or str(rid) in seen:
                continue
            if item.get(key_field) != operation_key:
                continue
            if session_id is not None and item.get(session_field) != session_id:
                continue
            response, record, rec_meta = self._counter_record(rid)
            files[f"counter_record_{label}_{str(rid)[:48]}.json"] = {
                "transport": rec_meta, "body": record,
            }
            if response is None or response.status_code != 200 or not isinstance(record, dict):
                continue
            if record.get(key_field) != operation_key:
                continue
            if session_id is not None and record.get(session_field) != session_id:
                continue
            seen.add(str(rid))
            if str(record.get(status_field, "")).upper() in success_values:
                found.append(record)
            else:
                non_execution.append(record)
        return found, non_execution, files

    def count_effects(self, *, session_id, operation_key, label):
        found, non_execution, files = self._verified_effect_records(
            operation_key=operation_key, session_id=session_id, label=label,
        )
        ids = [str(x[self.counter.get("id_field", "record_id")]) for x in found]
        evidence = {
            "label": label,
            "session_id": session_id,
            "operation_key": operation_key,
            "at": _utcnow(),
            "method": "independent durable test-provider ledger; list then fetch and verify each matching record",
            "external_effect_count": len(set(ids)),
            "execution_record_ids": sorted(set(ids)),
            "non_execution_record_ids": [str(x.get(self.counter.get("id_field", "record_id"))) for x in non_execution],
            "counter_record_count": len(found) + len(non_execution),
            "third_party_recount": "GET list_path filtered by operation_key/session_id, then GET each record_id; no APC response is used as the counter",
        }
        files[f"count_{label}_enumeration.json"] = {
            "external_effect_count": evidence["external_effect_count"],
            "execution_record_ids": evidence["execution_record_ids"],
            "non_execution_record_ids": evidence["non_execution_record_ids"],
        }
        return {"evidence": evidence, "files": files}

    def _digest_record(self, record):
        cfg = self.counter
        outcome_field = cfg.get("outcome_field", "apc_outcome")
        digest_field = cfg.get("digest_field", "outcome_digest")
        outcome = _path_get(record, outcome_field)
        served_digest = _path_get(record, digest_field)
        if not isinstance(outcome, dict) or not isinstance(served_digest, str):
            return {}, {"status": "not_computable", "reason": "record does not expose the configured APC outcome object and digest"}
        computed = _sha256(outcome)
        return {"result_digest": served_digest.lower()}, {
            "status": "matched" if computed == served_digest.lower() else "mismatched",
            "computed_sha256": computed,
            "served_digest": served_digest.lower(),
            "canonicalization": "UTF-8 JSON; sorted keys; compact separators; ensure_ascii=false; NaN disallowed",
        }

    def fetch_record(self, record_id):
        # The harness calls this with a sink-issued record ID for the run-id
        # leg, and with K for the delayed A5 K-only recovery leg.
        if str(record_id).startswith("lr-op-"):
            operation_key = str(record_id)
            response, body, meta = self._recover(operation_key=operation_key)
            if response is None:
                return {"record_id": record_id, "http_status": 0, "digests": {},
                        "recomputation": {"status": "not_computable", "reason": "transport failure"},
                        "record_json": body, "transport": meta}
            digests, recomp = self._digest_recovery(body or {})
            # A5 is not satisfied by a healthy APC response alone: the K-only
            # recovery must still match the independently stored provider
            # outcome after the full delay.
            try:
                records, _, counter_files = self._verified_effect_records(
                    operation_key=operation_key, session_id=None, label="a5_konly",
                )
                counter_digests = [
                    self._digest_record(item)[0].get("result_digest")
                    for item in records
                    if self._digest_record(item)[1].get("status") == "matched"
                ]
                recomp["independent_counter_match"] = bool(
                    digests.get("result_digest") and counter_digests
                    and all(x == digests["result_digest"] for x in counter_digests)
                )
                recomp["independent_counter_record_count"] = len(records)
            except Exception as exc:
                counter_files = {}
                recomp["independent_counter_match"] = False
                recomp["independent_counter_error"] = type(exc).__name__
            if response.status_code == 200 and recomp.get("status") == "matched" and recomp.get("independent_counter_match"):
                status = 200
            else:
                status = response.status_code
            return {"record_id": record_id, "http_status": status, "digests": digests,
                    "recomputation": recomp, "record_json": body, "transport": meta,
                    "counter_evidence_files": counter_files,
                    "recovery_method": "POST /v1/actions/recover with Idempotency-Key only; digest cross-checked against independent provider ledger"}

        response, body, meta = self._counter_record(str(record_id))
        if response is None:
            return {"record_id": record_id, "http_status": 0, "digests": {},
                    "recomputation": {"status": "not_computable", "reason": "transport failure"},
                    "record_json": body, "transport": meta}
        digests, recomp = self._digest_record(body or {}) if response.status_code == 200 else ({}, {"status": "not_computable", "reason": "record not found"})
        return {"record_id": record_id, "http_status": response.status_code,
                "digests": digests, "recomputation": recomp,
                "record_json": body, "transport": meta,
                "record_source": "independent durable test-provider ledger"}

    def _recover(self, *, operation_key):
        response, meta = self._request(
            "POST", self.base_url + "/v1/actions/recover",
            headers=self._apc_headers(operation_key=operation_key),
            json_body=None,
        )
        body = _response_json(response) if response is not None else None
        return response, body, meta

    def _digest_recovery(self, body):
        outcome = body.get("outcome") if isinstance(body, dict) else None
        served_digest = body.get("outcome_digest") if isinstance(body, dict) else None
        if not isinstance(outcome, dict) or not isinstance(served_digest, str):
            return {}, {"status": "not_computable", "reason": "APC recovery did not expose a terminal outcome and outcome_digest"}
        computed = _sha256(outcome)
        return {"result_digest": served_digest.lower()}, {
            "status": "matched" if computed == served_digest.lower() else "mismatched",
            "computed_sha256": computed,
            "served_digest": served_digest.lower(),
            "canonicalization": "UTF-8 JSON; sorted keys; compact separators; ensure_ascii=false; NaN disallowed",
        }

    def recover_by_key(self, *, operation_key):
        response, body, transport = self._recover(operation_key=operation_key)
        files = {"a3_konly_apc_recover.json": {
            "transport": transport,
            "http_status": response.status_code if response is not None else None,
            "body": body,
            "request_used": "POST /v1/actions/recover; Idempotency-Key only; fresh bearer token; no request body",
        }}
        attempts = []
        recovered_digests, recovered_recomp = self._digest_recovery(body or {})
        # Independent verification is a second, read-only evidence path. It
        # does not pass an invocation ID/result/digest into APC recovery.
        try:
            records, non_execution, counter_files = self._verified_effect_records(
                operation_key=operation_key, session_id=None, label="a3_konly",
            )
            files.update(counter_files)
            independent_digests = []
            for rec in records:
                d, r = self._digest_record(rec)
                if r.get("status") == "matched" and d.get("result_digest"):
                    independent_digests.append(d["result_digest"])
            files["a3_konly_counter_confirmation.json"] = {
                "matching_success_records": len(records),
                "record_ids": [x.get(self.counter.get("id_field", "record_id")) for x in records],
                "independent_outcome_digests": independent_digests,
                "non_execution_records": len(non_execution),
            }
        except Exception as exc:
            independent_digests = []
            files["a3_konly_counter_error.json"] = {"error_type": type(exc).__name__}

        returned_digest = recovered_digests.get("result_digest")
        digest_matches_counter = bool(
            returned_digest and recovered_recomp.get("status") == "matched"
            and independent_digests and all(x == returned_digest for x in independent_digests)
        )
        status = response.status_code if response is not None else None
        state = str((body or {}).get("status", "")).upper()
        if status == 200 and state in {"SUCCESS", "FAILURE", "DENIED"} and digest_matches_counter:
            response_class = "found"
        elif status == 404:
            response_class = "not_found"
        elif status in (200, 202) and state in {"IN_FLIGHT", "UNKNOWN", "RECEIVED"}:
            response_class = "in_progress" if state != "UNKNOWN" else "outcome_unknown"
        elif returned_digest and not digest_matches_counter:
            response_class = "digest_mismatch"
        elif response is None:
            response_class = "transport_error"
        else:
            response_class = f"http_error:{status}"

        attempts.append({
            "path": "POST /v1/actions/recover using only K in Idempotency-Key plus fresh bearer authentication",
            "http_status": status,
            "response_class": response_class,
            "apc_status": state or None,
            "returned_digest": returned_digest,
            "recomputed_digest_status": recovered_recomp.get("status"),
            "independent_counter_digest_match": digest_matches_counter,
            "effect_reissued": (body or {}).get("effect_reissued"),
            "recovery_state": (body or {}).get("recovery_state"),
        })
        return {"attempts": attempts, "files": files}
