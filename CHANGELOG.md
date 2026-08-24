# Changelog

All notable changes to `go-scheduler` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Stable `Fire` identity derived by `DeriveFireID(scheduleID, scheduledAt)`;
  scheduled time is now distinct from observed `FiredAt` on every attempt.
- Durable `FireStatus`, `FireCreation`, `FireClaim`, and `FireTransition`
  contracts for per-fire, per-attempt compare-and-swap state.
- Application-neutral `RetryPolicy` and `BackoffPolicy` contracts with
  constant, linear, and exponential delay strategies, positive maximum-attempt
  exhaustion, and an unbounded compatibility default.
- Observer events for claim, fire, retry, skip, success, exhaustion, disable,
  and engine errors. Observer callback errors and panics are isolated and
  counted in `Status.ObserverErrors`.
- Configurable clocks, tick cadence, and due-batch limits through `WithClock`,
  `WithTickCadence`, and `WithDueBatchLimit`.
- Concurrent CAS contract coverage proving two engines cannot dispatch the
  same fire attempt twice.

### Changed

- `Store` now materializes durable fire records, lists due attempts, claims
  `(fire ID, status, attempt)` atomically, and records result transitions.
- `Job` adds canonical `FireID`, `ScheduledAt`, and `Attempt`. Deprecated
  `RunID` remains populated as the exact same value as `FireID`.
- An error wrapping `ErrDuplicateJob` now transitions a stable fire to
  terminal `skipped`; it no longer requeues the same occurrence indefinitely.
- `Status` adds retry, skip, exhaustion, and observer-error counters.
- `New(store, runner)` retains its call shape and defaults; options are
  variadic and additive.

### Pre-1.0 Migration Notes

- This is a breaking `Store` interface migration. Replace
  `ClaimAndUpdateScheduleRun` and `SetScheduleNextRun` with `CreateFire`,
  `ListDueFires`, `ClaimFire`, and `TransitionFire`. `ListDueSchedules` and
  `DisableSchedule` remain.
- Persist fire IDs permanently (including terminal fires) so an exhausted or
  skipped occurrence cannot be recreated if a schedule row is presented again.
- Implement `CreateFire` as one transaction or equivalent atomic operation:
  compare the observed schedule next-run, reject an existing fire ID, insert
  the pending fire, and advance the schedule.
- Implement `ClaimFire` and `TransitionFire` as compare-and-swap operations,
  not read-then-write updates.
- Update runner idempotency keys from `Job.RunID` to `Job.FireID`. During
  migration the two values are identical.
- Set a positive `RetryPolicy.MaxAttempts` to enable exhaustion. Zero preserves
  v0.1's unbounded retry behavior.

## v0.1.0 — 2026-05-15

First public release. The cron scheduler engine was extracted from
`apps/hadron` (`internal/scheduler`) and decoupled from Hadron's persistence
and execution types so it can be reused by other services.

### Added

- `Engine` — polls a `Store` on a one-second tick and dispatches due
  schedules to a `Runner`, with `Start` / `Stop` / `TickNow` / `Status`.
  Dispatch uses an atomic compare-and-set claim (`Store.ClaimAndUpdateScheduleRun`)
  so a schedule is never dispatched twice across concurrent ticks.
- `Schedule` — the neutral, application-agnostic schedule descriptor: `ID`,
  `CronExpr`, `LastRun`, `NextRun`, `Enabled`, and an opaque job descriptor
  (`JobType` string + `Payload` `[]byte`, mirroring `go-queue`'s payload
  convention). An empty `CronExpr` marks a one-time schedule, disabled after
  it fires.
- `Job` — the neutral dispatch descriptor handed to a `Runner`: `ScheduleID`,
  engine-generated `RunID`, `JobType`, `Payload`, and `FiredAt`.
- `Store` and `Runner` interfaces — the persistence and dispatch seams.
  Consumers implement them over their own record and job types.
- `ValidateCron` and `NextRun` — standard-cron helpers built on
  `github.com/robfig/cron/v3`.
- `Status` — point-in-time snapshot: `Running`, `LastTickAt`, `Dispatches`,
  `WorkerErrors`.
- Sentinel error `ErrDuplicateJob` — a `Runner` wraps it to signal a benign
  already-enqueued race; the engine then requeues the schedule without
  counting a worker error.
- `LICENSE` (MIT, Hollis Labs), `README.md`, `CHANGELOG.md`, root `doc.go`.

### Notes

- The engine depends only on this package and `github.com/robfig/cron/v3`.
  All application coupling lives behind the `Store` and `Runner` interfaces.
