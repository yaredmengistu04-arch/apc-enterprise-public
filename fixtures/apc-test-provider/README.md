# APC AER-1 independent test provider

This is a neutral, synthetic provider and durable ledger for a bounded APC evaluation. It exists so the contract's external-effect count is read from an independently operated record store, rather than inferred from APC's responses or database.

**It is not a real commerce, payment, CRM, procurement, or customer provider.** Use synthetic test values only. Do not point APC at a live customer or financial provider for this contract run.

## Protocol

- `POST /v1/execute` — authenticated provider operation. Requires `Idempotency-Key` and body `{ "operation": "...", "resource": "...", "payload": { "test_operation_key": "K", "test_session_id": "SESSION", ... } }`. Commits one SQLite record per provider key.
- `GET /v1/lookup/{idempotency_key}` — authenticated authoritative lookup for APC reconciliation. Unknown key returns 404; APC conservatively treats that as `UNKNOWN`.
- `GET /v1/records?operation_key=K&session_id=SESSION` — independently countable record listing. When public reads are enabled, this read is unauthenticated so a third party can recount.
- `GET /v1/records/{record_id}` — public/restricted record read, depending on configuration.
- `GET /healthz` — health endpoint.

Every effect record stores the exact provider outcome that APC normalizes and persists:

```json
{
  "status": "SUCCESS",
  "provider_reference": "aer1-test-..."
}
```

`outcome_digest` is SHA-256 of canonical UTF-8 JSON (sorted keys, compact separators, `ensure_ascii=false`). The APC adapter recomputes that digest and compares it with the digest returned by `/v1/actions/recover`.

## Run locally

```bash
export APC_TEST_SINK_TOKEN="replace-with-a-long-random-test-only-token"
export APC_TEST_SINK_DB="./var/apc-test-provider.sqlite3"
export APC_TEST_SINK_HOST="127.0.0.1"
export APC_TEST_SINK_PORT="8790"
# Enable only for a dedicated synthetic-only evaluation instance whose
# records are safe to expose publicly for third-party recount:
export APC_TEST_SINK_PUBLIC_READS="true"
python3 apc_test_provider.py
```

The reference server speaks HTTP. Put it behind an HTTPS reverse proxy or ingress before connecting APC; the APC production provider client requires HTTPS. Do not expose its write endpoint directly to the public internet without TLS, network restrictions, and a random test-only token.

The same test-only token is used by APC's provider credential (stored in the customer's secret manager) and by the adapter's counter read when public reads are disabled. Never put the token in `config.apc.example.json`, Git, notebook outputs, or published run evidence.

## APC provider configuration

The customer-controlled APC deployment must point its generic provider adapter at this service:

```text
APC_PROVIDER_ID=aer1-test-provider
APC_PROVIDER_API_VERSION=v1
APC_PROVIDER_BASE_URL=https://APC-TEST-PROVIDER.example
APC_PROVIDER_EXECUTE_PATH=/v1/execute
APC_PROVIDER_LOOKUP_PATH=/v1/lookup/{idempotency_key}
APC_PROVIDER_IDEMPOTENCY_HEADER=Idempotency-Key
APC_PROVIDER_SUPPORTED_OPERATIONS=aer1_test_action
APC_PROVIDER_ALLOWED_FIELDS=test_operation_key,test_session_id,test_action_value
```

The runtime also requires customer-owned OIDC, PDP, PostgreSQL, AWS KMS, Secrets Manager and a valid provider qualification evidence manifest before effects can be enabled. Do not disable those safeguards to make the test pass. The qualification manifest must truthfully describe the synthetic provider and evidence that has actually been run; this repository does not fabricate approval or qualification evidence.

## Public record semantics

Set `APC_TEST_SINK_PUBLIC_READS=true` only on an isolated test instance whose records contain synthetic evaluation data. The counter reports one durable record per committed execution and can be independently recounted from the record IDs and public record endpoints. Keep the sink and its database separate from APC's own operational database.
