# APC adapter notes

The `apc.py` adapter targets APC Enterprise's production REST interface, not the legacy/demo API:

- `GET /healthz` and `GET /readyz` pin the server and readiness.
- `POST /v1/actions` uses a stable `Idempotency-Key` and fresh `X-Request-ID` for every attempt.
- `POST /v1/actions/recover` performs K-only recovery with fresh bearer authentication and no action body.
- The independent effect counter is the separately operated AER-1 synthetic test-provider ledger under `fixtures/apc-test-provider/`.

The adapter deliberately requires a separate effect counter. APC's own API responses, `invocation_id`, PostgreSQL action row, or event chain are not substituted for an external effect count.

## Pinning

The shared baseline expected by the adapter config is **7.1.8-dev**. Do not change `expected_version` to `7.1.8-dev.1` for the first pinned joint run. The separately versioned 7.1.8-dev.1 derivative must be a different run and a different version identifier. Do not mix its outputs with the frozen 7.1.8-dev baseline.

## Configuration

Copy `fixtures/harness/config.apc.example.json` to a private local config file and replace only the endpoint names and the action parameter values that the synthetic provider allows. Configure `APC_EVAL_BEARER_TOKEN` in the runner environment and, when counter reads are not public, `APC_EFFECT_COUNTER_TOKEN` as well. The two token values are never stored in the config or returned in raw evidence.

The `action_template.parameters` must include `{K}` and `{SESSION}` as `test_operation_key` and `test_session_id`, and both fields must be included in APC's provider projection allowlist. The mismatch changes one field only (`parameters.test_action_value`).

## Important scoring behavior

The adapter marks an A3 K-only attempt as `found` only after the APC-recovered `outcome_digest` recomputes correctly and matches the independently fetched provider outcome digest. It marks A5's K-only record lookup as HTTP 200 only after the same independent comparison.

The currently supplied harness driver uses a `found` marker to infer K-only digest equality rather than comparing the adapter-returned digest itself. The adapter therefore enforces that comparison before returning `found`; raw digests and both source records remain in the evidence files. This preserves the driver version without silently editing the shared contract harness.

Only the exact HTTP 409 `idempotency_binding_mismatch` response is classified as a binding rejection. Other errors are left unclassified so an authentication, network, policy, or provider failure is not misrepresented as proof of A2 binding enforcement.

## Scope and safety

This is evaluation glue only. It contains no APC implementation modules, customer configuration, API token, AWS credential, signing key, provider secret, or internal production endpoint. Do not copy the private APC release archive into Brennan's shared repository. The shared branch should contain this adapter, its example config, and the synthetic test-provider fixture only.
