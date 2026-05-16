package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeStore is an in-memory Store for tests. It records claims and next-run
// resets so assertions can inspect dispatch behaviour.
type fakeStore struct {
	mu       sync.Mutex
	due      []Schedule
	claimed  map[string]bool
	claimCnt int
	setNext  map[string]time.Time
	disabled map[string]bool
}

func (f *fakeStore) ListDueSchedules(_ context.Context, _ time.Time, _ int) ([]Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Schedule, len(f.due))
	copy(out, f.due)
	return out, nil
}

func (f *fakeStore) ClaimAndUpdateScheduleRun(_ context.Context, id string, _, _, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed == nil {
		f.claimed = map[string]bool{}
	}
	if f.claimed[id] {
		return false, nil
	}
	f.claimed[id] = true
	f.claimCnt++
	return true, nil
}

func (f *fakeStore) SetScheduleNextRun(_ context.Context, id string, nextRun time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setNext == nil {
		f.setNext = map[string]time.Time{}
	}
	f.setNext[id] = nextRun
	return nil
}

func (f *fakeStore) DisableSchedule(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.disabled == nil {
		f.disabled = map[string]bool{}
	}
	f.disabled[id] = true
	return nil
}

// fakeRunner records every Job it is handed.
type fakeRunner struct {
	mu   sync.Mutex
	jobs []Job
}

func (f *fakeRunner) Enqueue(_ context.Context, job Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = append(f.jobs, job)
	return nil
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

func TestValidateCron(t *testing.T) {
	if err := ValidateCron("*/5 * * * *"); err != nil {
		t.Fatalf("expected valid cron: %v", err)
	}
	if err := ValidateCron("bad"); err == nil {
		t.Fatalf("expected invalid cron error")
	}
}

func TestNextRun(t *testing.T) {
	from := time.Date(2026, 5, 15, 12, 0, 30, 0, time.UTC)
	next, err := NextRun("*/5 * * * *", from)
	if err != nil {
		t.Fatalf("NextRun failed: %v", err)
	}
	want := time.Date(2026, 5, 15, 12, 5, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("expected next run %s, got %s", want, next)
	}
	if _, err := NextRun("nonsense", from); err == nil {
		t.Fatalf("expected parse error for invalid expression")
	}
}

func TestTickDispatchesDueScheduleOnce(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-1",
		CronExpr: "* * * * *",
		NextRun:  now,
		Enabled:  true,
		JobType:  "blueprint",
		Payload:  []byte(`{"blueprint":"./bp.yaml"}`),
	}}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if err := eng.TickNow(context.Background()); err != nil {
		t.Fatalf("tick 2 failed: %v", err)
	}

	if got := runner.count(); got != 1 {
		t.Fatalf("expected 1 dispatch, got %d", got)
	}
}

func TestTickSkipsScheduleWithZeroNextRun(t *testing.T) {
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-zero",
		CronExpr: "* * * * *",
		Enabled:  true,
		// NextRun left zero — engine must skip it.
	}}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), time.Now().UTC()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if got := runner.count(); got != 0 {
		t.Fatalf("expected 0 dispatches for zero NextRun, got %d", got)
	}
}

func TestTickPropagatesJobDescriptor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	payload := []byte(`{"workspace":"alpha"}`)
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-job",
		CronExpr: "* * * * *",
		NextRun:  now,
		Enabled:  true,
		JobType:  "ingest",
		Payload:  payload,
	}}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(runner.jobs))
	}
	job := runner.jobs[0]
	if job.ScheduleID != "sch-job" {
		t.Fatalf("expected ScheduleID sch-job, got %q", job.ScheduleID)
	}
	if job.JobType != "ingest" {
		t.Fatalf("expected JobType ingest, got %q", job.JobType)
	}
	if string(job.Payload) != string(payload) {
		t.Fatalf("expected payload propagated, got %q", string(job.Payload))
	}
	if job.RunID == "" {
		t.Fatalf("expected a generated RunID")
	}
	if !job.FiredAt.Equal(now) {
		t.Fatalf("expected FiredAt %s, got %s", now, job.FiredAt)
	}
}

// duplicateRunner fails the first enqueue of a given job type with an error
// wrapping ErrDuplicateJob, then succeeds.
type duplicateRunner struct {
	mu     sync.Mutex
	failed bool
	jobs   []Job
}

func (d *duplicateRunner) Enqueue(_ context.Context, job Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.failed {
		d.failed = true
		return fmt.Errorf("enqueue run: %w", ErrDuplicateJob)
	}
	d.jobs = append(d.jobs, job)
	return nil
}

func TestTickRequeuesAndIgnoresDuplicate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-dup",
		CronExpr: "* * * * *",
		NextRun:  now,
		Enabled:  true,
		JobType:  "blueprint",
	}}}
	runner := &duplicateRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	store.mu.Lock()
	requeued, ok := store.setNext["sch-dup"]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("expected schedule requeued after enqueue error")
	}
	if !requeued.Equal(now) {
		t.Fatalf("expected next_run reset to %s, got %s", now, requeued)
	}
	// A duplicate is benign — it must not be counted as a worker error.
	if st := eng.Status(); st.WorkerErrors != 0 {
		t.Fatalf("expected 0 worker errors for duplicate, got %d", st.WorkerErrors)
	}
}

// boomRunner always fails with a non-duplicate error.
type boomRunner struct{}

func (boomRunner) Enqueue(_ context.Context, _ Job) error {
	return errors.New("backend unavailable")
}

func TestTickCountsWorkerErrorOnRealFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-boom",
		CronExpr: "* * * * *",
		NextRun:  now,
		Enabled:  true,
	}}}
	eng := New(store, boomRunner{})

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	store.mu.Lock()
	_, requeued := store.setNext["sch-boom"]
	store.mu.Unlock()
	if !requeued {
		t.Fatalf("expected schedule requeued after enqueue failure")
	}
	if st := eng.Status(); st.WorkerErrors != 1 {
		t.Fatalf("expected 1 worker error, got %d", st.WorkerErrors)
	}
}

func TestTickDispatchesDistinctSchedules(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []Schedule{
		{ID: "sch-a", CronExpr: "* * * * *", NextRun: now, Enabled: true, JobType: "a"},
		{ID: "sch-b", CronExpr: "* * * * *", NextRun: now, Enabled: true, JobType: "b"},
	}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.jobs) != 2 {
		t.Fatalf("expected 2 dispatches, got %d", len(runner.jobs))
	}
	if runner.jobs[0].ScheduleID == runner.jobs[1].ScheduleID {
		t.Fatalf("expected dispatches for distinct schedules")
	}
}

func TestTickDisablesOneTimeSchedule(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []Schedule{{
		ID:       "sch-once",
		CronExpr: "", // empty expression => one-time schedule
		NextRun:  now,
		Enabled:  true,
		JobType:  "blueprint",
	}}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	if got := runner.count(); got != 1 {
		t.Fatalf("expected 1 dispatch, got %d", got)
	}
	store.mu.Lock()
	disabled := store.disabled["sch-once"]
	store.mu.Unlock()
	if !disabled {
		t.Fatalf("expected one-time schedule to be disabled after firing")
	}
}

func TestStartStop(t *testing.T) {
	eng := New(&fakeStore{}, &fakeRunner{})
	eng.Start()
	if st := eng.Status(); !st.Running {
		t.Fatalf("expected running=true after Start")
	}
	eng.Start() // idempotent
	eng.Stop()
	if st := eng.Status(); st.Running {
		t.Fatalf("expected running=false after Stop")
	}
	eng.Stop() // idempotent
}
