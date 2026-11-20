# APC adapter contribution — review branch

This contribution adds an APC Enterprise adapter and a synthetic test-provider fixture to the lost-response harness.

## Scope

- `fixtures/harness/adapters/apc.py` maps the harness to APC's production REST API.
- `fixtures/harness/config.apc.example.json` contains placeholders only.
- `fixtures/apc-test-provider/` provides an independently countable synthetic effect ledger for isolated tests.

The adapter requires a separate durable effect counter. It does not count APC response bodies, APC invocation IDs, APC database rows, or APC event-chain records as external effects. The provider fixture is synthetic only and must never be connected to a live payment, commerce, customer, or other real provider.

## Test status

The adapter passed a local **mock-interface software check** against a simulated APC REST endpoint and the synthetic provider. That is not an APC product result. A1–A4 have not been run against the deployed APC product; A5 requires a real 24-hour delayed run. No certification or independent assessment is claimed.

## Baseline

The first joint APC run must remain pinned to **7.1.8-dev**. The separately versioned 7.1.8-dev.1 FIX1 derivative is not a substitute for the frozen baseline and must only be tested in a separately identified follow-up run.

## Publication boundary

No APC product source archive, deployment image, private architecture, private endpoint, credential, key, or customer configuration is included in this contribution. The adapter and example files are evaluation glue only. Please agree contribution licensing and merge terms with the repository maintainer before merging this branch.
