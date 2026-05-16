# Changelog

All notable changes to `go-scheduler` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
