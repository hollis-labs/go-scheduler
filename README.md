# go-scheduler

A small cron-driven schedule engine for Go. `go-scheduler` defines an `Engine`
that polls a `Store` on a fixed tick, claims due schedules atomically, and
dispatches them to a `Runner`. It is decoupled from any application's
persistence layer and job types through two neutral descriptors — `Schedule`
and `Job` — so the same engine can drive scheduled work in any service.

## Status

Pre-1.0 (`v0.1.x`). The public API is stable in shape — `Engine`, `Schedule`,
`Job`, the `Store` and `Runner` interfaces — but minor breaks may still happen
between `v0.x` releases. See [`CHANGELOG.md`](CHANGELOG.md) for per-release
detail and pin a version in your `go.mod`.

Documentation: [pkg.go.dev/github.com/hollis-labs/go-scheduler](https://pkg.go.dev/github.com/hollis-labs/go-scheduler).

## Install

```bash
go get github.com/hollis-labs/go-scheduler
```

## Usage

You provide two things: a `Store` over your schedule records and a `Runner`
that dispatches a fired schedule's job. The engine owns the tick loop, the
atomic claim, and one-time-schedule disabling.

```go
package main

import (
    "context"
    "time"

    scheduler "github.com/hollis-labs/go-scheduler"
)

func main() {
    eng := scheduler.New(myStore, myRunner)
    eng.Start()
    defer eng.Stop()

    select {} // run until shutdown
}
```

Implement the two seams over your own types, converting to and from the
neutral `Schedule` and `Job` at the boundary:

```go
type store struct{ db *sql.DB }

func (s store) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]scheduler.Schedule, error) {
    // load your own records, then map each into scheduler.Schedule —
    // packing app-specific fields into JobType + Payload.
}

// ClaimAndUpdateScheduleRun, SetScheduleNextRun, DisableSchedule ...

type runner struct{ exec *Executor }

func (r runner) Enqueue(ctx context.Context, job scheduler.Job) error {
    // decode job.Payload, hand off to your executor; if the job is already
    // enqueued, return an error wrapping scheduler.ErrDuplicateJob.
}
```

The cron helpers are usable on their own — for validating user input before
a schedule is stored:

```go
if err := scheduler.ValidateCron(expr); err != nil { /* reject */ }
next, _ := scheduler.NextRun(expr, time.Now())
```

## API Overview

Package `github.com/hollis-labs/go-scheduler`:

- `Engine` / `New(store, runner)` — polls the `Store` every second and
  dispatches due schedules. `Start` / `Stop` are idempotent; `Stop` blocks
  until the loop exits. `TickNow` runs a single tick synchronously.
- `Schedule` — neutral schedule descriptor: `ID`, `CronExpr`, `LastRun`,
  `NextRun`, `Enabled`, and an opaque job descriptor (`JobType` + `Payload`).
  An empty `CronExpr` marks a one-time schedule.
- `Job` — neutral dispatch descriptor handed to a `Runner`: `ScheduleID`,
  engine-generated `RunID`, `JobType`, `Payload`, `FiredAt`.
- `Store` — persistence seam: `ListDueSchedules`, `ClaimAndUpdateScheduleRun`,
  `SetScheduleNextRun`, `DisableSchedule`.
- `Runner` — dispatch seam: `Enqueue(ctx, Job)`.
- `Status` — engine snapshot: `Running`, `LastTickAt`, `Dispatches`,
  `WorkerErrors`.
- `ValidateCron(expr)` / `NextRun(expr, from)` — standard-cron helpers.
- Sentinel: `ErrDuplicateJob`.

## Architecture Notes

The engine never sees an application's own types. `Store` returns neutral
`Schedule` values and `Runner` consumes neutral `Job` values; an application
maps its records and job payloads at the interface boundary. This is the same
split `go-queue` makes between its `Queue` driver contract and its `Worker`.

Double-dispatch is prevented by `Store.ClaimAndUpdateScheduleRun`: it advances
a schedule's run state only if the stored next-run still matches the value the
tick observed. Two concurrent ticks (or processes) race on that compare-and-set
and only one wins the claim. A failed dispatch rolls the schedule's next-run
back so the following tick retries it; if the failure wraps `ErrDuplicateJob`
the retry is silent, otherwise it is counted in `Status.WorkerErrors`.

A one-time schedule is a `Schedule` with an empty `CronExpr`. It fires once
and the engine calls `Store.DisableSchedule` after a successful dispatch.

## Dependencies

Direct:

- [`github.com/robfig/cron/v3`](https://pkg.go.dev/github.com/robfig/cron/v3) —
  standard-cron parsing for `ValidateCron` and `NextRun`. Pinned in `go.mod`.

The package has no other external dependencies.

## Testing

```bash
go test ./...
```

The tests use in-memory fake `Store` and `Runner` implementations — no
environment variables, fixtures, or external services are needed.

## License

[MIT](LICENSE) © Hollis Labs.
