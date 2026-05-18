# sipmesh-mock

Minimal in-memory OperatorAPI gRPC server for sipmesh-frontend test
stands (Playwright, contract tests, local dev without a full engine).

## Why this exists

Frontend wants to test against the same OperatorAPI surface the real
sipmesh engine exposes — every form save, every dropdown of live
trunks/pipelines, every dry-run diagnostic. Shipping the full engine
to a CI container (with carrier creds, ai-worker GPU images,
operator state) is impractical and would still drift from production
behaviour over time.

This binary is the deliberate alternative:

- Implements `OperatorAPIServer` from `sipmesh-common` directly. A
  compile-time `var _ OperatorAPIServer = (*server)(nil)` assertion
  fails to build the next time the proto adds a new RPC without a
  handler here, so wire-shape drift is impossible.

- `WriteConfig` validation comes from `sipmesh-common/validate` — the
  same function the real engine uses. New rules land in both places
  in lock-step.

- In-memory only: no Redis, no edge, no proxy, no audio. Side effects
  end at process exit (or `/__reset`).

## Running

```bash
go run ./cmd/sipmesh-mock --addr :50051 --seed-addr :50052
```

Or pre-built image (per release tag):

```bash
docker run --rm -p 50051:50051 -p 50052:50052 \
  ghcr.io/rromenskyi/sipmesh-mock:v0.x.y
```

Pin the image tag to the sipmesh-common version your frontend repo
pins — they share the proto contract.

## Ports

| Port | Purpose | Auth |
|---|---|---|
| 50051 | gRPC OperatorAPI surface — `WriteConfig`, `GetOperatorConfig`, `ListPipelines`, etc. | none |
| 50052 | HTTP test seed endpoint — `/__reset`, `/__seed/operator-config`, `/__seed/dry-run-validate` | none |

The seed port is **test-only**. Disable it with `--seed-addr=""`
when running in any environment where untrusted callers might
reach it.

## Playwright wiring (sketch)

```ts
import { test, beforeEach } from '@playwright/test';

const MOCK_HTTP = 'http://localhost:50052';

beforeEach(async () => {
  await fetch(`${MOCK_HTTP}/__reset`, { method: 'POST' });
});

test('renders empty trunks tab on fresh load', async ({ page }) => {
  // Optionally pre-seed a known fixture before navigating.
  await fetch(`${MOCK_HTTP}/__seed/operator-config`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pipelines: [{ name: 'demo', steps: [{ hangup: {} }] }] }),
  });
  await page.goto('/');
  // ... assertions ...
});
```

## Seed endpoint reference

### `POST /__reset`

Wipes all in-memory state. Returns `204 No Content`. Run at the top
of every scenario to guarantee a known-empty starting point.

### `POST /__seed/operator-config`

Wholesale-replaces the default group's `OperatorConfig` with a
protojson-encoded body. Bumps version, returns:

```json
{ "version": 1, "etag": "<sha256-hex>" }
```

**Skips validation** — tests CAN seed deliberately-invalid state to
exercise downstream error paths. To validate the same payload first,
POST to `/__seed/dry-run-validate`.

### `POST /__seed/calls`

Replaces the live-calls fixture. Body envelope:

```json
{
  "calls": [
    { "internal_call_id": "abc", "worker": "edge-a", "trunk": "t-1", "state": "answered", "flow": "voicebot" },
    ...
  ]
}
```

Each entry is a `CallSummary` protojson object; the `calls` field
itself is a JSON-array envelope so the body parses with stdlib
`json.Unmarshal` before per-entry protojson decode. Returns
`{"seeded": N}`. Entries with empty `internal_call_id` are dropped.

After seeding, gRPC `ListCalls` / `GetCall` / `HangupCall` see the
seeded state. `HangupCall` mutates the fixture in place
(subsequent `ListCalls` no longer returns the hung-up call), so
frontend's end-to-end hangup flow can be tested against the mock.

### `POST /__seed/workers`

Same envelope shape, with `WorkerSummaryV2` entries. Drives
`ListWorkers` / `GetWorker` / `DrainWorker`. `DrainWorker` removes
the worker from the fixture (requires `confirm=true`).

### `POST /__seed/ai-workers`

Same envelope shape, with `AIWorkerCapability` entries. Drives
`ListAIWorkers`. Use this to populate the voice / model dropdowns
the pipeline-edit form depends on without bringing up a real
ai-worker pool.

### `POST /__seed/dry-run-validate`

Runs `validate.OperatorConfig` against a protojson body without
storing. Returns:

```json
{
  "diagnostics": [
    { "severity": "SEVERITY_ERROR", "message": "...", "field_path": "..." }
  ]
}
```

## What this mock doesn't do (intentionally)

| Surface | Status | Why |
|---|---|---|
| `ListCalls` / `GetCall` / `HangupCall` | seeded via `POST /__seed/calls` | Real call runtime not simulated; seed fixtures drive read RPCs and `HangupCall` mutates the fixture |
| `ListWorkers` / `GetWorker` / `DrainWorker` | seeded via `POST /__seed/workers` | Same shape — fixture-driven; `DrainWorker` removes from fixture |
| `ListAIWorkers` | seeded via `POST /__seed/ai-workers` | Fixture-driven for voice/model dropdowns |
| `SubscribeEvents` / `StreamSipTrace` | returns `Unimplemented` | Streaming events are per-test fixtures; expand seed endpoint when needed |
| `ListCallArchive` / `GetCallArtifactURL` | returns `Unimplemented` | No S3 |

When a frontend test needs one of these, extend the seed endpoint to
inject the canned data and update the RPC handler to read from the
seed store.

## Wire-protocol sync

This package's only job is "produce the same OperatorAPI responses
the real engine would produce, for the inputs the frontend cares
about". The drift guards:

1. **Compile time** — `var _ sipmeshapiv1.OperatorAPIServer = (*server)(nil)` at the bottom of `server.go` fails when proto adds an RPC.
2. **Run time** — `WriteConfig` calls `validate.OperatorConfig` directly. If engine validation rules tighten in a later sipmesh-common bump, this mock tightens in lock-step.
3. **Release coupling** — pin frontend's mock image tag to the same sipmesh-common version it depends on. Updating one updates both.

When a test fails because the mock disagrees with reality, that's a
signal to look here first.
