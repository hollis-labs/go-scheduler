package scheduler

import "errors"

// ErrDuplicateJob signals that a job for a fired schedule was already
// enqueued — a benign race between concurrent ticks or processes, not a
// worker fault.
//
// A Runner that detects a duplicate (for example a unique-constraint
// violation from its backing store) should return an error that wraps this
// sentinel, e.g. fmt.Errorf("enqueue run: %w", scheduler.ErrDuplicateJob).
// The engine then requeues the schedule without counting a worker error.
var ErrDuplicateJob = errors.New("scheduler: job already enqueued")
