package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule is the neutral, application-agnostic description of a timed
// schedule. The engine reads Schedules from a Store; it never sees an
// application's own record type.
//
// A Schedule with an empty CronExpr is a one-time schedule. JobType and
// Payload are opaque to the engine and are copied to each materialized Fire.
type Schedule struct {
	ID       string    // stable schedule identifier
	CronExpr string    // standard 5-field cron expression; empty means one-time
	LastRun  time.Time // last scheduled fire time; zero means never run
	NextRun  time.Time // next scheduled fire time; zero means unscheduled
	Enabled  bool      // disabled schedules should be excluded by the Store
	JobType  string    // opaque job-type tag, copied to Fire and Job
	Payload  []byte    // opaque job payload, copied to Fire and Job
	Retry    RetryPolicy
}

// BackoffStrategy identifies an application-neutral retry delay algorithm.
type BackoffStrategy string

const (
	// BackoffNone makes a retry due immediately. The engine's tick cadence
	// still bounds how quickly the retry is observed.
	BackoffNone BackoffStrategy = "none"
	// BackoffConstant always uses InitialDelay.
	BackoffConstant BackoffStrategy = "constant"
	// BackoffLinear multiplies InitialDelay by the completed attempt number.
	BackoffLinear BackoffStrategy = "linear"
	// BackoffExponential doubles InitialDelay after each failed attempt.
	BackoffExponential BackoffStrategy = "exponential"
)

// BackoffPolicy describes how the engine delays a retry after a failed fire
// attempt. MaxDelay, when positive, caps the computed delay.
type BackoffPolicy struct {
	Strategy     BackoffStrategy `json:"strategy"`
	InitialDelay time.Duration   `json:"initial_delay"`
	MaxDelay     time.Duration   `json:"max_delay"`
}

// Validate reports invalid delay values or an unknown strategy.
func (p BackoffPolicy) Validate() error {
	if p.InitialDelay < 0 {
		return fmt.Errorf("%w: initial delay must not be negative", ErrInvalidRetryPolicy)
	}
	if p.MaxDelay < 0 {
		return fmt.Errorf("%w: maximum delay must not be negative", ErrInvalidRetryPolicy)
	}
	switch p.Strategy {
	case "", BackoffNone, BackoffConstant, BackoffLinear, BackoffExponential:
		return nil
	default:
		return fmt.Errorf("%w: unknown backoff strategy %q", ErrInvalidRetryPolicy, p.Strategy)
	}
}

// DelayAfter returns the delay following completedAttempt. Attempts are
// one-based. Invalid or non-positive inputs produce no delay.
func (p BackoffPolicy) DelayAfter(completedAttempt int) time.Duration {
	if completedAttempt < 1 || p.InitialDelay <= 0 {
		return 0
	}

	delay := p.InitialDelay
	switch p.Strategy {
	case "", BackoffNone:
		return 0
	case BackoffConstant:
	case BackoffLinear:
		delay = saturatingDurationMultiply(delay, int64(completedAttempt))
	case BackoffExponential:
		for i := 1; i < completedAttempt; i++ {
			delay = saturatingDurationMultiply(delay, 2)
			if delay == time.Duration(math.MaxInt64) {
				break
			}
		}
	default:
		return 0
	}

	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func saturatingDurationMultiply(value time.Duration, multiplier int64) time.Duration {
	if multiplier <= 0 || value <= 0 {
		return 0
	}
	if int64(value) > math.MaxInt64/multiplier {
		return time.Duration(math.MaxInt64)
	}
	return value * time.Duration(multiplier)
}

// RetryPolicy controls dispatch retries for one Fire. MaxAttempts includes the
// initial attempt. A zero MaxAttempts preserves the v0.1 behavior of retrying
// without a library-imposed limit; applications that require exhaustion must
// set a positive value.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Backoff     BackoffPolicy `json:"backoff"`
}

// Validate reports invalid maximum-attempt or backoff configuration.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 0 {
		return fmt.Errorf("%w: maximum attempts must not be negative", ErrInvalidRetryPolicy)
	}
	return p.Backoff.Validate()
}

// Exhausted reports whether completedAttempts has reached the configured
// positive maximum. A zero maximum is unbounded.
func (p RetryPolicy) Exhausted(completedAttempts int) bool {
	return p.MaxAttempts > 0 && completedAttempts >= p.MaxAttempts
}

// FireStatus is the durable lifecycle status of one scheduled fire.
type FireStatus string

const (
	// FirePending is materialized and has not yet been claimed.
	FirePending FireStatus = "pending"
	// FireClaimed is held by exactly one engine for its current attempt.
	FireClaimed FireStatus = "claimed"
	// FireRetrying is waiting for NextAttemptAt after a dispatch failure.
	FireRetrying FireStatus = "retrying"
	// FireSucceeded means Runner.Enqueue accepted the dispatch.
	FireSucceeded FireStatus = "succeeded"
	// FireSkipped is terminal because dispatch was unnecessary, such as an
	// ErrDuplicateJob idempotency hit.
	FireSkipped FireStatus = "skipped"
	// FireExhausted is terminal after the configured maximum attempts.
	FireExhausted FireStatus = "exhausted"
)

// Fire is a durable scheduled-fire record. ID and ScheduledAt never change
// across attempts. FiredAt identifies the current/most recent claim epoch and
// ClaimExpiresAt bounds that claim's lease. Attempt is one-based after a
// successful pending/retrying claim. Recovering an expired claim keeps the
// same Attempt but replaces FiredAt, fencing the stale owner. Pending fires
// therefore begin at attempt zero.
type Fire struct {
	ID             string      `json:"id"`
	ScheduleID     string      `json:"schedule_id"`
	ScheduledAt    time.Time   `json:"scheduled_at"`
	FiredAt        time.Time   `json:"fired_at"`
	ClaimExpiresAt time.Time   `json:"claim_expires_at"`
	Attempt        int         `json:"attempt"`
	Status         FireStatus  `json:"status"`
	NextAttemptAt  time.Time   `json:"next_attempt_at"`
	LastError      string      `json:"last_error,omitempty"`
	Retry          RetryPolicy `json:"retry"`
	JobType        string      `json:"job_type"`
	Payload        []byte      `json:"payload"`
}

// DeriveFireID returns the stable identity for a schedule occurrence. Only
// the schedule identity and normalized scheduled time participate; observed
// tick time and attempt number deliberately do not.
func DeriveFireID(scheduleID string, scheduledAt time.Time) string {
	canonical := scheduleID + "\x00" + scheduledAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(canonical))
	return "fire-" + hex.EncodeToString(sum[:])
}

// FireCreation asks a Store to materialize a Fire and advance its Schedule in
// one atomic compare-and-swap operation. ExpectedNext is the schedule value
// observed by the engine. A Store must return false, without changing either
// record, if ExpectedNext no longer matches or Fire.ID already exists. Existing
// terminal fires must never be recreated.
type FireCreation struct {
	ScheduleID   string
	ExpectedNext time.Time
	NextRun      time.Time
	Fire         Fire
}

// FireClaim asks a Store to atomically claim exactly one attempt. On success,
// ClaimFire sets Status to FireClaimed, records ClaimedAt as FiredAt, records
// ClaimExpiresAt, and returns that updated Fire. It increments Attempt when
// ExpectedStatus is pending or retrying and preserves Attempt when recovering
// an expired claimed fire. ExpectedStatus, ExpectedAttempt, and
// ExpectedFiredAt are the compare-and-swap preconditions. A Store must reject
// recovery when the stored ClaimExpiresAt is after ClaimedAt.
type FireClaim struct {
	FireID          string
	ExpectedStatus  FireStatus
	ExpectedAttempt int
	ExpectedFiredAt time.Time
	ClaimedAt       time.Time
	ClaimExpiresAt  time.Time
}

// FireTransition is a compare-and-swap lifecycle update after an attempt.
// Attempt, From, and ClaimedAt must still match the stored fire. Retry
// transitions set NextAttemptAt; terminal transitions leave it zero. Every
// successful transition clears ClaimExpiresAt in the stored fire.
type FireTransition struct {
	FireID        string
	Attempt       int
	From          FireStatus
	ClaimedAt     time.Time
	To            FireStatus
	At            time.Time
	NextAttemptAt time.Time
	Error         string
	Reason        string
}

// Store is the durable persistence seam. Applications implement it over their
// own schedule and fire records while preserving the documented atomicity.
// Persistence schemas remain application-owned.
type Store interface {
	// ListDueSchedules returns up to limit enabled schedules whose NextRun is
	// at or before now.
	ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]Schedule, error)

	// CreateFire atomically materializes a unique Fire and advances its
	// schedule if the schedule's next-run still equals ExpectedNext.
	CreateFire(ctx context.Context, creation FireCreation) (bool, error)

	// ListDueFires returns up to limit pending or retrying fires whose
	// NextAttemptAt is at or before now, plus claimed fires whose claim lease
	// has expired. Terminal fires and unexpired claims are not due.
	ListDueFires(ctx context.Context, now time.Time, limit int) ([]Fire, error)

	// ClaimFire performs the compare-and-swap described by FireClaim. For a
	// pending or retrying fire it increments Attempt. For an expired claimed
	// fire it keeps the same Attempt and replaces FiredAt and ClaimExpiresAt;
	// this redelivers the ambiguous attempt after a process crash without
	// consuming another application-level retry. It must also verify that the
	// stored lease is expired at claim. ExpectedFiredAt prevents a stale owner
	// from winning after the lease has been replaced.
	ClaimFire(ctx context.Context, claim FireClaim) (Fire, bool, error)

	// TransitionFire atomically applies an attempt result if Attempt, From, and
	// ClaimedAt still match. Including the claim timestamp fences a stale owner
	// after an expired claim has been recovered. It returns false on a lost
	// compare-and-swap race.
	TransitionFire(ctx context.Context, transition FireTransition) (bool, error)

	// DisableSchedule marks a schedule disabled. The engine calls it after it
	// materializes a one-time schedule; the durable Fire remains dispatchable.
	DisableSchedule(ctx context.Context, id string) error
}

// Job is the neutral dispatch descriptor handed to a Runner for one claimed
// fire attempt.
type Job struct {
	ScheduleID  string    // ID of the schedule that fired
	FireID      string    // stable across every attempt of the scheduled fire
	RunID       string    // Deprecated: exact alias of FireID, retained for v0.1 consumers
	JobType     string    // copied from Fire.JobType
	Payload     []byte    // copied from Fire.Payload
	ScheduledAt time.Time // schedule occurrence time used to derive FireID
	FiredAt     time.Time // observed time of this attempt
	Attempt     int       // one-based attempt number
}

// Runner dispatches a claimed fire attempt. Applications decode the opaque
// Job payload and apply their own execution and idempotency policy.
//
// A Runner that detects that the same FireID was already dispatched should
// return an error wrapping ErrDuplicateJob. The engine records the fire as
// skipped instead of retrying it.
type Runner interface {
	Enqueue(ctx context.Context, job Job) error
}

// ObserverEventKind identifies an application-neutral engine lifecycle hook.
type ObserverEventKind string

const (
	ObserverClaim       ObserverEventKind = "claim"
	ObserverFire        ObserverEventKind = "fire"
	ObserverRetry       ObserverEventKind = "retry"
	ObserverSkip        ObserverEventKind = "skip"
	ObserverSuccess     ObserverEventKind = "success"
	ObserverExhaustion  ObserverEventKind = "exhaustion"
	ObserverDisable     ObserverEventKind = "disable"
	ObserverEngineError ObserverEventKind = "engine_error"
)

// ObserverEvent is emitted after durable state changes, except ObserverFire,
// which is emitted immediately before Runner.Enqueue. Err is provided for
// in-process telemetry only and is not a persistence schema.
type ObserverEvent struct {
	Kind      ObserverEventKind
	At        time.Time
	Fire      Fire
	RetryAt   time.Time
	Reason    string
	Operation string
	Err       error
}

// Observer receives application-neutral lifecycle events. Observer failures
// and panics are isolated from scheduler state and dispatch outcomes.
type Observer interface {
	Observe(ctx context.Context, event ObserverEvent) error
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, ObserverEvent) error

// Observe calls f(ctx, event).
func (f ObserverFunc) Observe(ctx context.Context, event ObserverEvent) error {
	return f(ctx, event)
}

// ValidateCron reports whether expr is a valid standard cron expression.
func ValidateCron(expr string) error {
	if _, err := cron.ParseStandard(strings.TrimSpace(expr)); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}

// NextRun returns the next activation time for expr after from.
func NextRun(expr string, from time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(strings.TrimSpace(expr))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}
	return sched.Next(from), nil
}
