package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// tickInterval is how often the background loop polls the Store.
const tickInterval = 1 * time.Second

// dueBatchLimit caps how many due schedules a single tick claims.
const dueBatchLimit = 100

// oneTimeHorizon is the placeholder next-run handed to the Store when a
// one-time schedule fires. The schedule is disabled immediately after a
// successful dispatch, so the value only has to be far enough out that it
// cannot re-fire in the window between the claim and the disable.
const oneTimeHorizon = 100 * 365 * 24 * time.Hour

// Status is a point-in-time snapshot of engine activity.
type Status struct {
	Running      bool      `json:"running"`
	LastTickAt   time.Time `json:"last_tick_at"`
	Dispatches   int64     `json:"dispatches"`
	WorkerErrors int64     `json:"worker_errors"`
}

// Engine polls a Store on a fixed tick and dispatches due schedules to a
// Runner. The zero value is not usable; construct one with New. An Engine is
// safe for concurrent use.
type Engine struct {
	store   Store
	runner  Runner
	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	status  Status
}

// New returns an Engine backed by the given Store and Runner.
func New(store Store, runner Runner) *Engine {
	return &Engine{store: store, runner: runner}
}

// Start launches the background tick loop. It is idempotent: calling Start on
// an already-running Engine is a no-op.
func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	e.stopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	go e.loop()
}

// Stop halts the background tick loop and blocks until it has exited. It is
// idempotent: calling Stop on a stopped Engine is a no-op.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	stopCh := e.stopCh
	doneCh := e.doneCh
	e.running = false
	e.mu.Unlock()

	close(stopCh)
	<-doneCh
}

// TickNow runs a single tick synchronously against the current time. It is
// independent of the background loop and is primarily useful in tests.
func (e *Engine) TickNow(ctx context.Context) error {
	return e.tick(ctx, time.Now().UTC())
}

// Status returns a snapshot of engine activity.
func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.status
	st.Running = e.running
	return st
}

func (e *Engine) loop() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	defer close(e.doneCh)

	for {
		select {
		case <-ticker.C:
			_ = e.tick(context.Background(), time.Now().UTC())
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) tick(ctx context.Context, now time.Time) error {
	e.mu.Lock()
	e.status.LastTickAt = now
	e.mu.Unlock()

	due, err := e.store.ListDueSchedules(ctx, now, dueBatchLimit)
	if err != nil {
		e.bumpWorkerErrors()
		return err
	}

	for _, sch := range due {
		if sch.NextRun.IsZero() {
			continue
		}
		expectedNext := sch.NextRun

		// One-time schedule: empty cron expression. It is disabled after
		// firing, so its "next run" is only a far-future placeholder.
		isOneTime := strings.TrimSpace(sch.CronExpr) == ""
		var next time.Time
		if isOneTime {
			next = now.Add(oneTimeHorizon)
		} else {
			next, err = NextRun(sch.CronExpr, now)
			if err != nil {
				e.bumpWorkerErrors()
				continue
			}
		}

		// Atomic claim: only one tick can advance the schedule from
		// expectedNext, so a schedule is never dispatched twice.
		claimed, err := e.store.ClaimAndUpdateScheduleRun(ctx, sch.ID, expectedNext, now, next)
		if err != nil {
			e.bumpWorkerErrors()
			continue
		}
		if !claimed {
			continue
		}

		job := Job{
			ScheduleID: sch.ID,
			RunID:      fmt.Sprintf("sched-%s-%d", sch.ID, now.Unix()),
			JobType:    sch.JobType,
			Payload:    sch.Payload,
			FiredAt:    now,
		}
		if enqErr := e.runner.Enqueue(ctx, job); enqErr != nil {
			// Dispatch failed: roll the schedule back to expectedNext so the
			// next tick retries it.
			_ = e.store.SetScheduleNextRun(ctx, sch.ID, expectedNext)
			// A duplicate is a benign race, not a worker fault.
			if errors.Is(enqErr, ErrDuplicateJob) {
				continue
			}
			e.bumpWorkerErrors()
			continue
		}
		e.bumpDispatches()

		// One-time schedules fire exactly once.
		if isOneTime {
			_ = e.store.DisableSchedule(ctx, sch.ID)
		}
	}
	return nil
}

func (e *Engine) bumpDispatches() {
	e.mu.Lock()
	e.status.Dispatches++
	e.mu.Unlock()
}

func (e *Engine) bumpWorkerErrors() {
	e.mu.Lock()
	e.status.WorkerErrors++
	e.mu.Unlock()
}
