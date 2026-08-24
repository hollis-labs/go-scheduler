package scheduler

import "errors"

// ErrDuplicateJob signals that a job for a stable FireID was already enqueued
// — a benign race or idempotency hit, not a worker fault.
//
// A Runner that detects a duplicate (for example a unique-constraint
// violation from its backing store) should return an error that wraps this
// sentinel, e.g. fmt.Errorf("enqueue run: %w", scheduler.ErrDuplicateJob).
// The engine records the fire as skipped without counting a worker error.
var ErrDuplicateJob = errors.New("scheduler: job already enqueued")

// ErrInvalidRetryPolicy reports a schedule retry policy that cannot be
// interpreted safely. MaxAttempts must be non-negative.
var ErrInvalidRetryPolicy = errors.New("scheduler: invalid retry policy")

// ErrInvalidClaim reports that a Store returned a successful claim without
// applying the documented identity, status, attempt, or observed-time update.
var ErrInvalidClaim = errors.New("scheduler: invalid fire claim")

// ErrTransitionConflict reports that an attempt result could not be persisted
// because its compare-and-swap preconditions no longer matched.
var ErrTransitionConflict = errors.New("scheduler: fire transition conflict")
