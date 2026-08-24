package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTickCadence is how often the background loop polls the Store.
	DefaultTickCadence = time.Second
	// DefaultDueBatchLimit caps schedule materializations and fire attempts
	// independently during one tick.
	DefaultDueBatchLimit = 100
)

// oneTimeHorizon is the placeholder next-run persisted atomically when a
// one-time schedule is materialized. The schedule is then disabled; the value
// only prevents rematerialization if disabling temporarily fails.
const oneTimeHorizon = 100 * 365 * 24 * time.Hour

// Clock supplies deterministic time and tickers to an Engine.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

// Ticker is the minimal ticker contract required by the background loop.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTicker(cadence time.Duration) Ticker {
	return systemTicker{Ticker: time.NewTicker(cadence)}
}

type systemTicker struct {
	*time.Ticker
}

func (t systemTicker) C() <-chan time.Time { return t.Ticker.C }

// Option configures an Engine while allowing New(store, runner) to retain its
// v0.1 call shape and defaults.
type Option func(*Engine)

// WithClock configures the clock used by TickNow and the background loop. A
// nil clock is ignored and leaves the default system clock in place.
func WithClock(clock Clock) Option {
	return func(engine *Engine) {
		if clock != nil {
			engine.clock = clock
		}
	}
}

// WithTickCadence configures the background polling cadence. Non-positive
// values are ignored and leave the default in place.
func WithTickCadence(cadence time.Duration) Option {
	return func(engine *Engine) {
		if cadence > 0 {
			engine.tickCadence = cadence
		}
	}
}

// WithDueBatchLimit configures the maximum number of due schedules and due
// fires each tick requests from the Store. Non-positive values are ignored.
func WithDueBatchLimit(limit int) Option {
	return func(engine *Engine) {
		if limit > 0 {
			engine.dueBatchLimit = limit
		}
	}
}

// WithObserver registers an application-neutral lifecycle observer. Observer
// errors and panics are counted but never change scheduler state or outcomes.
func WithObserver(observer Observer) Option {
	return func(engine *Engine) {
		engine.observer = observer
	}
}

// Status is a point-in-time snapshot of engine activity.
type Status struct {
	Running        bool      `json:"running"`
	LastTickAt     time.Time `json:"last_tick_at"`
	Dispatches     int64     `json:"dispatches"`
	Retries        int64     `json:"retries"`
	Skips          int64     `json:"skips"`
	Exhaustions    int64     `json:"exhaustions"`
	WorkerErrors   int64     `json:"worker_errors"`
	ObserverErrors int64     `json:"observer_errors"`
}

// Engine materializes due schedules into durable fires, claims due attempts,
// and dispatches them to a Runner. The zero value is not usable; construct one
// with New. An Engine is safe for concurrent use.
type Engine struct {
	store         Store
	runner        Runner
	clock         Clock
	tickCadence   time.Duration
	dueBatchLimit int
	observer      Observer

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	status  Status
}

// New returns an Engine backed by store and runner. Calling New with only
// those two arguments preserves the v0.1 one-second cadence, batch limit of
// 100, system clock, and no-op observation behavior.
func New(store Store, runner Runner, options ...Option) *Engine {
	engine := &Engine{
		store:         store,
		runner:        runner,
		clock:         systemClock{},
		tickCadence:   DefaultTickCadence,
		dueBatchLimit: DefaultDueBatchLimit,
	}
	for _, option := range options {
		if option != nil {
			option(engine)
		}
	}
	return engine
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
	go e.loop(e.stopCh, e.doneCh)
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

// TickNow runs a single tick synchronously using the configured Clock. It is
// independent of the background loop and is useful for deterministic tests.
func (e *Engine) TickNow(ctx context.Context) error {
	return e.tick(ctx, e.clock.Now().UTC())
}

// Status returns a snapshot of engine activity.
func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	status := e.status
	status.Running = e.running
	return status
}

func (e *Engine) loop(stopCh, doneCh chan struct{}) {
	ticker := e.clock.NewTicker(e.tickCadence)
	defer ticker.Stop()
	defer close(doneCh)

	for {
		select {
		case <-ticker.C():
			_ = e.tick(context.Background(), e.clock.Now().UTC())
		case <-stopCh:
			return
		}
	}
}

func (e *Engine) tick(ctx context.Context, now time.Time) error {
	e.mu.Lock()
	e.status.LastTickAt = now
	e.mu.Unlock()

	dueSchedules, err := e.store.ListDueSchedules(ctx, now, e.dueBatchLimit)
	if err != nil {
		e.engineError(ctx, Fire{}, now, "list_due_schedules", err)
		return err
	}
	for index, schedule := range dueSchedules {
		if index >= e.dueBatchLimit {
			break
		}
		e.materializeSchedule(ctx, schedule, now)
	}

	dueFires, err := e.store.ListDueFires(ctx, now, e.dueBatchLimit)
	if err != nil {
		e.engineError(ctx, Fire{}, now, "list_due_fires", err)
		return err
	}
	for index, fire := range dueFires {
		if index >= e.dueBatchLimit {
			break
		}
		e.dispatchFire(ctx, fire, now)
	}
	return nil
}

func (e *Engine) materializeSchedule(ctx context.Context, schedule Schedule, now time.Time) {
	if !schedule.Enabled {
		e.skip(ctx, Fire{ScheduleID: schedule.ID}, now, "schedule_disabled")
		return
	}
	if schedule.NextRun.IsZero() {
		e.skip(ctx, Fire{ScheduleID: schedule.ID}, now, "schedule_unscheduled")
		return
	}
	if err := schedule.Retry.Validate(); err != nil {
		e.engineError(ctx, Fire{ScheduleID: schedule.ID}, now, "validate_retry_policy", err)
		return
	}

	scheduledAt := schedule.NextRun.UTC()
	oneTime := strings.TrimSpace(schedule.CronExpr) == ""
	nextRun := now.Add(oneTimeHorizon)
	if !oneTime {
		var err error
		nextRun, err = NextRun(schedule.CronExpr, now)
		if err != nil {
			e.engineError(ctx, Fire{ScheduleID: schedule.ID, ScheduledAt: scheduledAt}, now, "calculate_next_run", err)
			return
		}
	}

	fire := Fire{
		ID:            DeriveFireID(schedule.ID, scheduledAt),
		ScheduleID:    schedule.ID,
		ScheduledAt:   scheduledAt,
		Status:        FirePending,
		NextAttemptAt: scheduledAt,
		Retry:         schedule.Retry,
		JobType:       schedule.JobType,
		Payload:       append([]byte(nil), schedule.Payload...),
	}
	created, err := e.store.CreateFire(ctx, FireCreation{
		ScheduleID:   schedule.ID,
		ExpectedNext: scheduledAt,
		NextRun:      nextRun.UTC(),
		Fire:         fire,
	})
	if err != nil {
		e.engineError(ctx, fire, now, "create_fire", err)
		return
	}
	if !created {
		e.skip(ctx, fire, now, "materialization_conflict")
		return
	}

	if oneTime {
		if err := e.store.DisableSchedule(ctx, schedule.ID); err != nil {
			e.engineError(ctx, fire, now, "disable_schedule", err)
			return
		}
		e.observe(ctx, ObserverEvent{Kind: ObserverDisable, At: now, Fire: fire})
	}
}

func (e *Engine) dispatchFire(ctx context.Context, fire Fire, now time.Time) {
	if fire.Status != FirePending && fire.Status != FireRetrying {
		e.skip(ctx, fire, now, "fire_not_claimable")
		return
	}
	if fire.NextAttemptAt.After(now) {
		e.skip(ctx, fire, now, "fire_not_due")
		return
	}
	if fire.Retry.Exhausted(fire.Attempt) {
		e.transitionExhausted(ctx, fire, now, nil, "maximum_attempts_reached")
		return
	}

	claimed, won, err := e.store.ClaimFire(ctx, FireClaim{
		FireID:          fire.ID,
		ExpectedStatus:  fire.Status,
		ExpectedAttempt: fire.Attempt,
		ClaimedAt:       now,
	})
	if err != nil {
		e.engineError(ctx, fire, now, "claim_fire", err)
		return
	}
	if !won {
		e.skip(ctx, fire, now, "claim_conflict")
		return
	}
	if err := validateClaim(fire, claimed, now); err != nil {
		e.engineError(ctx, claimed, now, "validate_claim", err)
		return
	}

	e.observe(ctx, ObserverEvent{Kind: ObserverClaim, At: now, Fire: claimed})
	e.observe(ctx, ObserverEvent{Kind: ObserverFire, At: now, Fire: claimed})

	job := Job{
		ScheduleID:  claimed.ScheduleID,
		FireID:      claimed.ID,
		RunID:       claimed.ID,
		JobType:     claimed.JobType,
		Payload:     append([]byte(nil), claimed.Payload...),
		ScheduledAt: claimed.ScheduledAt,
		FiredAt:     claimed.FiredAt,
		Attempt:     claimed.Attempt,
	}
	enqueueErr := e.runner.Enqueue(ctx, job)
	if enqueueErr == nil {
		e.transitionSuccess(ctx, claimed, now)
		return
	}

	if errors.Is(enqueueErr, ErrDuplicateJob) {
		e.transitionSkipped(ctx, claimed, now, enqueueErr, "duplicate_dispatch")
		return
	}

	e.engineError(ctx, claimed, now, "enqueue", enqueueErr)
	if claimed.Retry.Exhausted(claimed.Attempt) {
		e.transitionExhausted(ctx, claimed, now, enqueueErr, "maximum_attempts_reached")
		return
	}
	e.transitionRetry(ctx, claimed, now, enqueueErr)
}

func validateClaim(before, claimed Fire, claimedAt time.Time) error {
	if claimed.ID != before.ID || claimed.ScheduleID != before.ScheduleID {
		return fmt.Errorf("%w: claimed fire identity changed", ErrInvalidClaim)
	}
	if claimed.Status != FireClaimed {
		return fmt.Errorf("%w: status is %q", ErrInvalidClaim, claimed.Status)
	}
	if claimed.Attempt != before.Attempt+1 {
		return fmt.Errorf("%w: attempt is %d, want %d", ErrInvalidClaim, claimed.Attempt, before.Attempt+1)
	}
	if !claimed.FiredAt.Equal(claimedAt) {
		return fmt.Errorf("%w: fired-at is %s, want %s", ErrInvalidClaim, claimed.FiredAt, claimedAt)
	}
	if !claimed.ScheduledAt.Equal(before.ScheduledAt) {
		return fmt.Errorf("%w: scheduled-at changed", ErrInvalidClaim)
	}
	return nil
}

func (e *Engine) transitionSuccess(ctx context.Context, fire Fire, now time.Time) {
	if !e.transition(ctx, fire, now, FireSucceeded, time.Time{}, nil, "") {
		return
	}
	fire.Status = FireSucceeded
	fire.NextAttemptAt = time.Time{}
	fire.LastError = ""
	e.bumpDispatches()
	e.observe(ctx, ObserverEvent{Kind: ObserverSuccess, At: now, Fire: fire})
}

func (e *Engine) transitionRetry(ctx context.Context, fire Fire, now time.Time, attemptErr error) {
	retryAt := now.Add(fire.Retry.Backoff.DelayAfter(fire.Attempt))
	if !e.transition(ctx, fire, now, FireRetrying, retryAt, attemptErr, "dispatch_failed") {
		return
	}
	fire.Status = FireRetrying
	fire.NextAttemptAt = retryAt
	fire.LastError = attemptErr.Error()
	e.bumpRetries()
	e.observe(ctx, ObserverEvent{
		Kind: ObserverRetry, At: now, Fire: fire, RetryAt: retryAt,
		Reason: "dispatch_failed", Err: attemptErr,
	})
}

func (e *Engine) transitionSkipped(ctx context.Context, fire Fire, now time.Time, attemptErr error, reason string) {
	if !e.transition(ctx, fire, now, FireSkipped, time.Time{}, attemptErr, reason) {
		return
	}
	fire.Status = FireSkipped
	fire.NextAttemptAt = time.Time{}
	if attemptErr != nil {
		fire.LastError = attemptErr.Error()
	}
	e.skip(ctx, fire, now, reason)
}

func (e *Engine) transitionExhausted(ctx context.Context, fire Fire, now time.Time, attemptErr error, reason string) {
	if !e.transition(ctx, fire, now, FireExhausted, time.Time{}, attemptErr, reason) {
		return
	}
	fire.Status = FireExhausted
	fire.NextAttemptAt = time.Time{}
	if attemptErr != nil {
		fire.LastError = attemptErr.Error()
	}
	e.bumpExhaustions()
	e.observe(ctx, ObserverEvent{
		Kind: ObserverExhaustion, At: now, Fire: fire, Reason: reason, Err: attemptErr,
	})
}

func (e *Engine) transition(
	ctx context.Context,
	fire Fire,
	now time.Time,
	to FireStatus,
	nextAttemptAt time.Time,
	transitionErr error,
	reason string,
) bool {
	errorText := ""
	if transitionErr != nil {
		errorText = transitionErr.Error()
	}
	transitioned, err := e.store.TransitionFire(ctx, FireTransition{
		FireID:        fire.ID,
		Attempt:       fire.Attempt,
		From:          fire.Status,
		To:            to,
		At:            now,
		NextAttemptAt: nextAttemptAt,
		Error:         errorText,
		Reason:        reason,
	})
	if err != nil {
		e.engineError(ctx, fire, now, "transition_fire", err)
		return false
	}
	if !transitioned {
		e.engineError(ctx, fire, now, "transition_fire", ErrTransitionConflict)
		return false
	}
	return true
}

func (e *Engine) skip(ctx context.Context, fire Fire, now time.Time, reason string) {
	e.bumpSkips()
	e.observe(ctx, ObserverEvent{Kind: ObserverSkip, At: now, Fire: fire, Reason: reason})
}

func (e *Engine) engineError(ctx context.Context, fire Fire, now time.Time, operation string, err error) {
	e.bumpWorkerErrors()
	e.observe(ctx, ObserverEvent{
		Kind: ObserverEngineError, At: now, Fire: fire, Operation: operation, Err: err,
	})
}

func (e *Engine) observe(ctx context.Context, event ObserverEvent) {
	if e.observer == nil {
		return
	}
	// ObserverEvent is isolated from the engine's dispatch copy. An observer
	// may annotate or redact its view without mutating the persisted/queued
	// opaque payload.
	event.Fire.Payload = append([]byte(nil), event.Fire.Payload...)
	var observerErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				observerErr = fmt.Errorf("observer panic: %v", recovered)
			}
		}()
		observerErr = e.observer.Observe(ctx, event)
	}()
	if observerErr != nil {
		e.mu.Lock()
		e.status.ObserverErrors++
		e.mu.Unlock()
	}
}

func (e *Engine) bumpDispatches() {
	e.mu.Lock()
	e.status.Dispatches++
	e.mu.Unlock()
}

func (e *Engine) bumpRetries() {
	e.mu.Lock()
	e.status.Retries++
	e.mu.Unlock()
}

func (e *Engine) bumpSkips() {
	e.mu.Lock()
	e.status.Skips++
	e.mu.Unlock()
}

func (e *Engine) bumpExhaustions() {
	e.mu.Lock()
	e.status.Exhaustions++
	e.mu.Unlock()
}

func (e *Engine) bumpWorkerErrors() {
	e.mu.Lock()
	e.status.WorkerErrors++
	e.mu.Unlock()
}
