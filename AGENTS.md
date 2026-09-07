# go-scheduler

An application-neutral timed activation engine: it materializes due schedules
into durable fires, claims each attempt with compare-and-swap semantics, and
dispatches opaque jobs to a `Runner`. It owns time, fire identity, generic
retry state and lifecycle observation. Applications keep job types, event
names, activation policy, persistence schema and execution.

## Start Here

- `README.md` covers configuration and the stable-fire-identity rules.
- `MIGRATION.md` is the exact `Store` contract and the v0.1 → v0.2 checklist.
- `scheduler.go` declares `Store`, `Runner`, `Observer` and the options.
- `engine.go` owns the tick, materialization, claim and dispatch path.
- `errors.go` holds the sentinels.

## Commands

```bash
gofmt -l .
go vet ./...
go test -race -count=1 ./...
```

Tests inject a clock rather than sleeping, so the suite is fast and
wall-time-independent. There is no CI workflow in this repo.

## Boundaries

Fire identity is derived only from schedule ID and the scheduled time
normalized to UTC — never from observed tick time or attempt number
(`DeriveFireID`, `TestDeriveFireIDUsesScheduleAndScheduledTime`). That is what
lets one occurrence keep a single ID across ticks, processes and retries, and
it is what a store's uniqueness constraint depends on.

At-most-once dispatch per attempt is the core guarantee, and it must hold with
several engines running. `TestConcurrentEnginesCannotDispatchSameAttemptTwice`,
`TestDuplicateDispatchIsTerminalSkip` and
`TestCrashAfterDispatchRecoversAsDuplicateSkip` cover the racing, repeating and
crash-recovery paths. Claim recovery is fenced against a stale owner
(`TestRecoveredClaimFencesStaleOwnerTransition`), an unexpired claim is not
stolen on restart, and recovering an expired claim must not consume a retry
(`TestRestartRecoversExpiredClaimWithoutConsumingRetry`).

Retry budgets are one-way. An exhausted fire cannot reset on a later tick
(`TestExhaustedFireCannotResetOnLaterTick`), and a failed durable transition
does not count as success (`TestTransitionFailureDoesNotCountDurableSuccess`).

Observers are strictly passive: they cannot mutate the dispatch payload and
their failures cannot change the dispatch outcome
(`TestObserverCannotMutateDispatchPayload`,
`TestObserverFailuresDoNotChangeDispatchOutcome`). Observation must never be
able to break scheduling.

Outcome transitions survive caller cancellation
(`TestOutcomeTransitionSurvivesCallerCancellation`) — a cancelled context must
not leave a claimed fire stranded.
