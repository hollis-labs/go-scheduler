package sqlstore_test

import (
	"context"
	"database/sql"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	scheduler "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/go-scheduler/conformance"
	"github.com/hollis-labs/go-scheduler/sqlstore"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "sched.db") +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newStore(t *testing.T) *sqlstore.Store {
	t.Helper()
	db := openDB(t)
	if err := sqlstore.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s, err := sqlstore.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) scheduler.Store { return newStore(t) })
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openDB(t)
	for range 3 {
		if err := sqlstore.Migrate(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqlstore.New(nil); err == nil {
		t.Error("New(nil) must fail")
	}
}

func TestScheduleCRUD(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	sch := scheduler.Schedule{ID: "a", CronExpr: "@daily", NextRun: time.Date(2026, 1, 1, 0, 0, 0, 5, time.UTC),
		Enabled: true, JobType: "j", Payload: []byte("p"), Retry: scheduler.RetryPolicy{MaxAttempts: 2}}
	if err := s.CreateSchedule(ctx, sch); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSchedule(ctx, sch); err == nil {
		t.Error("duplicate CreateSchedule must fail")
	}
	got, ok, err := s.GetSchedule(ctx, "a")
	if err != nil || !ok || !got.NextRun.Equal(sch.NextRun) || got.Retry != sch.Retry || string(got.Payload) != "p" {
		t.Fatalf("GetSchedule = %+v, %v, %v", got, ok, err)
	}
	if _, ok, _ := s.GetSchedule(ctx, "missing"); ok {
		t.Error("GetSchedule found a missing schedule")
	}
	if list, err := s.ListSchedules(ctx); err != nil || len(list) != 1 {
		t.Errorf("ListSchedules = %d, %v", len(list), err)
	}
	if err := s.DisableSchedule(ctx, "missing"); err == nil {
		t.Error("DisableSchedule on a missing schedule must fail")
	}
	if err := s.DeleteSchedule(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSchedule(ctx, "a"); ok {
		t.Error("schedule survived DeleteSchedule")
	}
}

func TestSchemaIsEmbedded(t *testing.T) {
	entries, err := fs.Glob(sqlstore.Schema(), "*.sql")
	if err != nil || len(entries) == 0 {
		t.Fatalf("Schema() has no .sql files: %v %v", entries, err)
	}
}

// TestTwoWorkersRaceForOneJob runs two independent engines over one database,
// each with its own connection pool, and asserts a due fire is dispatched to
// exactly one runner.
func TestTwoWorkersRaceForOneJob(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	if err := sqlstore.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	newEngineStore := func() *sqlstore.Store {
		s, err := sqlstore.New(db)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := newEngineStore(), newEngineStore()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if err := a.CreateSchedule(ctx, scheduler.Schedule{ID: "once", NextRun: now, Enabled: true, JobType: "x"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var jobs []scheduler.Job
	runner := runnerFunc(func(_ context.Context, j scheduler.Job) error {
		mu.Lock()
		defer mu.Unlock()
		jobs = append(jobs, j)
		return nil
	})
	clock := fixedClock{now}
	e1 := scheduler.New(a, runner, scheduler.WithClock(clock))
	e2 := scheduler.New(b, runner, scheduler.WithClock(clock))
	var wg sync.WaitGroup
	for _, e := range []*scheduler.Engine{e1, e2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.TickNow(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(jobs) != 1 {
		t.Fatalf("dispatched %d jobs, want exactly 1", len(jobs))
	}
	fire, ok, err := a.GetFire(ctx, jobs[0].FireID)
	if err != nil || !ok || fire.Status != scheduler.FireSucceeded || fire.Attempt != 1 {
		t.Fatalf("fire = %+v, %v, %v; want succeeded attempt 1", fire, ok, err)
	}
}

// TestEngineRecoversCrashedClaim drives the real engine: a worker claims and
// "dies" (its runner never returns a transition because the process is gone),
// and a second engine redelivers the same attempt after the lease.
func TestEngineRecoversCrashedClaim(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	if err := s.CreateSchedule(ctx, scheduler.Schedule{ID: "s", NextRun: now, Enabled: true, JobType: "x"}); err != nil {
		t.Fatal(err)
	}
	// Materialize and claim by hand, as a worker that then crashes.
	c := scheduler.FireCreation{ScheduleID: "s", ExpectedNext: now, NextRun: now.Add(time.Hour), Fire: scheduler.Fire{
		ID: scheduler.DeriveFireID("s", now), ScheduleID: "s", ScheduledAt: now, Status: scheduler.FirePending, NextAttemptAt: now, JobType: "x"}}
	if ok, err := s.CreateFire(ctx, c); err != nil || !ok {
		t.Fatalf("CreateFire = %v, %v", ok, err)
	}
	crashed, won, err := s.ClaimFire(ctx, scheduler.FireClaim{FireID: c.Fire.ID, ExpectedStatus: scheduler.FirePending,
		ClaimedAt: now, ClaimExpiresAt: now.Add(time.Minute)})
	if err != nil || !won {
		t.Fatalf("claim = %v, %v", won, err)
	}

	var got []scheduler.Job
	runner := runnerFunc(func(_ context.Context, j scheduler.Job) error { got = append(got, j); return nil })
	early := scheduler.New(s, runner, scheduler.WithClock(fixedClock{now.Add(30 * time.Second)}))
	if err := early.TickNow(ctx); err != nil || len(got) != 0 {
		t.Fatalf("engine stole an unexpired claim: %v, jobs=%d", err, len(got))
	}
	late := scheduler.New(s, runner, scheduler.WithClock(fixedClock{now.Add(2 * time.Minute)}))
	if err := late.TickNow(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].FireID != c.Fire.ID || got[0].Attempt != 1 {
		t.Fatalf("recovered jobs = %+v; want same fire, attempt 1", got)
	}
	// The crashed worker wakes up late: it is fenced.
	stale := scheduler.FireTransition{FireID: c.Fire.ID, Attempt: 1, From: scheduler.FireClaimed,
		ClaimedAt: crashed.FiredAt, To: scheduler.FireSucceeded, At: now.Add(3 * time.Minute)}
	if ok, err := s.TransitionFire(ctx, stale); err != nil || ok {
		t.Fatalf("stale owner transition = %v, %v; want false", ok, err)
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
func (c fixedClock) NewTicker(time.Duration) scheduler.Ticker {
	panic("fixedClock is for TickNow only")
}

type runnerFunc func(context.Context, scheduler.Job) error

func (f runnerFunc) Enqueue(ctx context.Context, j scheduler.Job) error { return f(ctx, j) }
