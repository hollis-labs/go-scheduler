# Migrating to v0.2.0

`v0.2.0` replaces v0.1's schedule-row rollback loop with durable fire records.
It is a breaking pre-1.0 release: `New(store, runner)` keeps the same call
shape, but `Store` does not.

Do not adopt the remote `v0.1.1` tag. It contains an intermediate version of
the breaking durable-fire API without restart recovery. Pin the supported
release explicitly:

```go
require github.com/hollis-labs/go-scheduler v0.2.0
```

## Ownership boundary

The library owns:

- cron and one-time occurrence calculation;
- stable fire identity and durable materialization;
- claim and transition compare-and-swap contracts;
- claim-lease recovery after a process exit;
- generic retry delay and exhaustion state;
- lifecycle observer ordering; and
- clock, cadence, and batch configuration.

The application owns:

- schedule and fire table shapes;
- job payload schemas and execution;
- idempotent dispatch keyed by `Job.FireID`;
- authorization and activation policy;
- product event names and telemetry storage; and
- actions after exhaustion, such as disable, notify, or escalate.

## Store migration

Remove the v0.1 methods:

```go
ClaimAndUpdateScheduleRun(ctx, id, expectedNext, lastRun, nextRun)
SetScheduleNextRun(ctx, id, nextRun)
```

Implement `CreateFire`, `ListDueFires`, `ClaimFire`, and `TransitionFire` in
addition to the retained `ListDueSchedules` and `DisableSchedule` methods.

A fire table needs durable equivalents of every `Fire` field. In particular,
persist the retry policy and opaque job descriptor as a snapshot at
materialization time; a retry must not change behavior because its schedule was
later edited. Persist terminal records permanently enough to preserve stable-ID
deduplication.

The required atomic operations are:

1. `CreateFire`: in one transaction, compare the schedule's current next-run
   with `ExpectedNext`, reject an existing `Fire.ID`, insert the pending fire,
   and advance the schedule to `NextRun`.
2. `ClaimFire` from `pending`/`retrying`: compare ID, status, attempt, and
   `ExpectedFiredAt`; increment attempt; write `claimed`, `ClaimedAt`, and
   `ClaimExpiresAt`; clear next-attempt time; return the stored row.
3. `ClaimFire` from expired `claimed`: apply the same comparison, but preserve
   attempt and replace `FiredAt`/`ClaimExpiresAt`. This is recovery of an
   ambiguous delivery, not a new application-level attempt.
4. `TransitionFire`: compare ID, `claimed` status, attempt, and `ClaimedAt`;
   write the requested result and clear the claim expiry. Comparing
   `ClaimedAt` is mandatory: it fences the prior owner after lease recovery.

`ListDueFires` must return both:

- `pending`/`retrying` rows with `NextAttemptAt <= now`; and
- `claimed` rows with a zero or elapsed `ClaimExpiresAt`.

It must not return terminal fires or unexpired claims. An existing claimed row
with no lease value is treated as expired so an intermediate-v0.1.1 deployment
can recover it.

Store timestamp precision must round-trip `ClaimedAt` and `ClaimExpiresAt`
exactly because they participate in compare-and-swap validation. UTC
RFC3339Nano strings or integer nanoseconds are suitable representations.

## Runner migration

Use `Job.FireID` as the dispatch/idempotency key. `Job.RunID` remains an exact,
deprecated alias during the v0.1 migration window. `Job.ScheduledAt` is the
occurrence time, `Job.FiredAt` is the current claim time, and `Job.Attempt` is
the one-based application-level attempt.

Recovery is at-least-once: if a process exits after enqueue succeeds but before
the success transition commits, the same `FireID` and `Attempt` are delivered
again after the lease. A runner that finds the job already accepted must return
an error wrapping `scheduler.ErrDuplicateJob`; the engine then persists
terminal `skipped`.

Choose `WithClaimLease` longer than the maximum expected `Runner.Enqueue`
latency. A lease is not extended while `Enqueue` runs. The default is five
minutes.

## Mapping Nanite's current scheduler

Nanite's generic behavior maps without moving product policy into this module:

| Nanite behavior | v0.2.0 owner |
| --- | --- |
| `agent_schedules.next_run` due query and schedule advance | Nanite `Store`; library `CreateFire` CAS contract |
| `schedule_runs` identity and attempt bookkeeping | Nanite durable fire table/adapter; library `Fire` lifecycle |
| `fired_count` and operator-facing `last_fired_at` | Nanite store transaction or observer projection |
| Exponential 30s backoff capped at 5m | `RetryPolicy{Backoff: BackoffPolicy{Strategy: BackoffExponential, InitialDelay: 30*time.Second, MaxDelay: 5*time.Minute}}` |
| `max_retries` (currently treated as total real attempts) | `RetryPolicy.MaxAttempts` |
| Backoff-window short circuit | Library `NextAttemptAt` due filtering |
| Retry/exhaustion persistence across restart | Library fire state plus Nanite store implementation |
| Duplicate dispatch handling | Nanite runner dedupe on `FireID`; library terminal `skipped` transition |
| `on_fail=disable/notify/retry` terminal action | Nanite observer on `exhaustion` |
| `schedule_fire` telemetry | Nanite observer, preserving library event order |
| Job taxonomy and JSON payload decoding | Nanite runner |

Nanite should remove retry/backoff accounting from its `RetryingRunner` when it
adopts v0.2.0; keeping both layers would double-count attempts and backoff. Its
store adapter should snapshot each schedule's `MaxRetries`, the 30s/5m
exponential policy, job type, and payload into the materialized fire. Its
observer should translate `success`, `retry`, `skip`, and `exhaustion` into
Nanite telemetry, update any product counters, and apply `on_fail` after the
durable exhaustion transition.

Like Nanite's v0.1 integration, the engine coalesces missed cron occurrences:
when an overdue occurrence is materialized, the next cron time is calculated
after the observed tick time rather than replaying every missed interval.

Existing Nanite `schedule_runs` statuses can be migrated as follows:

| Existing status | Fire status |
| --- | --- |
| `pending` | `pending` |
| `failed` | `retrying` |
| `succeeded` | `succeeded` |
| `exhausted` | `exhausted` |

Add representations for `claimed` and `skipped`, plus scheduled time, claim
expiry, retry snapshot, job type, and payload. Existing open rows need a stable
scheduled occurrence time from which to derive `FireID`; if that value cannot
be reconstructed safely, preserve the row's existing unique `run_id` as its
fire ID rather than manufacturing a possibly colliding identity.

## Verification checklist

- Run `go test ./...` and `go test -race ./...` in this module.
- Exercise the application store with two engine instances listing and
  claiming the same due fire.
- Restart with persisted pending, retrying, unexpired-claimed, and
  expired-claimed rows.
- Verify a recovered already-enqueued job becomes terminal `skipped` through
  `ErrDuplicateJob`.
- Verify observer sequences: `claim`, `fire`, then `success`; `skip`; or
  `engine_error` followed by `retry`/`exhaustion`.
- Verify one-time schedules remain disabled while their materialized fire is
  still dispatchable.
