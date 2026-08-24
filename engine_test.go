package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu sync.Mutex

	schedules map[string]Schedule
	fires     map[string]Fire

	createCount     int
	claimCount      int
	transitions     []FireTransition
	disabled        map[string]bool
	scheduleLimit   int
	fireLimit       int
	listErr         error
	listCalled      chan struct{}
	fireListBarrier chan struct{}
	fireListCalls   int
	claimResult     func(Fire) Fire
	transitionErr   error
	transitionLost  bool
	disableErr      error
}

func newFakeStore(schedules ...Schedule) *fakeStore {
	store := &fakeStore{
		schedules: make(map[string]Schedule, len(schedules)),
		fires:     make(map[string]Fire),
		disabled:  make(map[string]bool),
	}
	for _, schedule := range schedules {
		schedule.Payload = append([]byte(nil), schedule.Payload...)
		store.schedules[schedule.ID] = schedule
	}
	return store
}

func (f *fakeStore) ListDueSchedules(_ context.Context, now time.Time, limit int) ([]Schedule, error) {
	f.mu.Lock()
	f.scheduleLimit = limit
	if f.listCalled != nil {
		select {
		case f.listCalled <- struct{}{}:
		default:
		}
	}
	if f.listErr != nil {
		err := f.listErr
		f.mu.Unlock()
		return nil, err
	}
	due := make([]Schedule, 0)
	for _, schedule := range f.schedules {
		if schedule.Enabled && !schedule.NextRun.IsZero() && !schedule.NextRun.After(now) {
			schedule.Payload = append([]byte(nil), schedule.Payload...)
			due = append(due, schedule)
		}
	}
	f.mu.Unlock()

	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })
	if len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (f *fakeStore) CreateFire(_ context.Context, creation FireCreation) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	schedule, ok := f.schedules[creation.ScheduleID]
	if !ok || !schedule.Enabled || !schedule.NextRun.Equal(creation.ExpectedNext) {
		return false, nil
	}
	if _, exists := f.fires[creation.Fire.ID]; exists {
		return false, nil
	}
	fire := creation.Fire
	fire.Payload = append([]byte(nil), creation.Fire.Payload...)
	f.fires[fire.ID] = fire
	schedule.LastRun = creation.Fire.ScheduledAt
	schedule.NextRun = creation.NextRun
	f.schedules[schedule.ID] = schedule
	f.createCount++
	return true, nil
}

func (f *fakeStore) ListDueFires(_ context.Context, now time.Time, limit int) ([]Fire, error) {
	f.mu.Lock()
	f.fireLimit = limit
	due := make([]Fire, 0)
	for _, fire := range f.fires {
		if (fire.Status == FirePending || fire.Status == FireRetrying) && !fire.NextAttemptAt.After(now) {
			fire.Payload = append([]byte(nil), fire.Payload...)
			due = append(due, fire)
		}
	}
	barrier := f.fireListBarrier
	if barrier != nil {
		f.fireListCalls++
		if f.fireListCalls == 2 {
			close(barrier)
		}
	}
	f.mu.Unlock()

	if barrier != nil {
		<-barrier
	}
	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })
	if len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (f *fakeStore) ClaimFire(_ context.Context, claim FireClaim) (Fire, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fire, ok := f.fires[claim.FireID]
	if !ok || fire.Status != claim.ExpectedStatus || fire.Attempt != claim.ExpectedAttempt {
		return Fire{}, false, nil
	}
	fire.Status = FireClaimed
	fire.Attempt++
	fire.FiredAt = claim.ClaimedAt
	fire.NextAttemptAt = time.Time{}
	f.fires[fire.ID] = fire
	f.claimCount++
	if f.claimResult != nil {
		return f.claimResult(fire), true, nil
	}
	return fire, true, nil
}

func (f *fakeStore) TransitionFire(_ context.Context, transition FireTransition) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.transitionErr != nil {
		return false, f.transitionErr
	}
	if f.transitionLost {
		return false, nil
	}

	fire, ok := f.fires[transition.FireID]
	if !ok || fire.Status != transition.From || fire.Attempt != transition.Attempt {
		return false, nil
	}
	fire.Status = transition.To
	fire.NextAttemptAt = transition.NextAttemptAt
	fire.LastError = transition.Error
	f.fires[fire.ID] = fire
	f.transitions = append(f.transitions, transition)
	return true, nil
}

func (f *fakeStore) DisableSchedule(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.disableErr != nil {
		return f.disableErr
	}

	schedule, ok := f.schedules[id]
	if !ok {
		return errors.New("schedule not found")
	}
	schedule.Enabled = false
	f.schedules[id] = schedule
	f.disabled[id] = true
	return nil
}

func (f *fakeStore) fire(id string) Fire {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fires[id]
}

func (f *fakeStore) setScheduleNext(id string, next time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	schedule := f.schedules[id]
	schedule.NextRun = next
	schedule.Enabled = true
	f.schedules[id] = schedule
}

type fakeRunner struct {
	mu        sync.Mutex
	jobs      []Job
	responses []error
}

func (f *fakeRunner) Enqueue(_ context.Context, job Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job.Payload = append([]byte(nil), job.Payload...)
	f.jobs = append(f.jobs, job)
	if len(f.responses) == 0 {
		return nil
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	return response
}

func (f *fakeRunner) snapshot() []Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Job(nil), f.jobs...)
}

type recordingObserver struct {
	mu     sync.Mutex
	events []ObserverEvent
	err    error
	panic  bool
}

func (o *recordingObserver) Observe(_ context.Context, event ObserverEvent) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	if o.panic {
		panic("observer failed")
	}
	return o.err
}

func (o *recordingObserver) kinds() []ObserverEventKind {
	o.mu.Lock()
	defer o.mu.Unlock()
	kinds := make([]ObserverEventKind, len(o.events))
	for index, event := range o.events {
		kinds[index] = event.Kind
	}
	return kinds
}

func (o *recordingObserver) snapshot() []ObserverEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ObserverEvent(nil), o.events...)
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	cadence time.Duration
	ticks   chan time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, ticks: make(chan time.Time, 1)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTicker(cadence time.Duration) Ticker {
	c.mu.Lock()
	c.cadence = cadence
	c.mu.Unlock()
	return fakeTicker{ticks: c.ticks}
}

func (c *fakeClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type fakeTicker struct {
	ticks <-chan time.Time
}

func (t fakeTicker) C() <-chan time.Time { return t.ticks }
func (fakeTicker) Stop()                 {}

func TestValidateCron(t *testing.T) {
	if err := ValidateCron("*/5 * * * *"); err != nil {
		t.Fatalf("expected valid cron: %v", err)
	}
	if err := ValidateCron("bad"); err == nil {
		t.Fatal("expected invalid cron error")
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
		t.Fatal("expected parse error for invalid expression")
	}
}

func TestDeriveFireIDUsesScheduleAndScheduledTime(t *testing.T) {
	scheduled := time.Date(2026, 8, 23, 15, 4, 5, 123, time.UTC)
	sameInstant := scheduled.In(time.FixedZone("offset", -5*60*60))

	first := DeriveFireID("schedule-a", scheduled)
	if second := DeriveFireID("schedule-a", sameInstant); first != second {
		t.Fatalf("same scheduled instant produced different IDs: %q != %q", first, second)
	}
	if otherSchedule := DeriveFireID("schedule-b", scheduled); first == otherSchedule {
		t.Fatal("different schedule identity produced the same fire ID")
	}
	if otherTime := DeriveFireID("schedule-a", scheduled.Add(time.Nanosecond)); first == otherTime {
		t.Fatal("different scheduled time produced the same fire ID")
	}
}

func TestBackoffPolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  BackoffPolicy
		attempt int
		want    time.Duration
	}{
		{name: "none", policy: BackoffPolicy{Strategy: BackoffNone, InitialDelay: time.Second}, attempt: 2, want: 0},
		{name: "constant", policy: BackoffPolicy{Strategy: BackoffConstant, InitialDelay: time.Second}, attempt: 3, want: time.Second},
		{name: "linear", policy: BackoffPolicy{Strategy: BackoffLinear, InitialDelay: time.Second}, attempt: 3, want: 3 * time.Second},
		{name: "exponential", policy: BackoffPolicy{Strategy: BackoffExponential, InitialDelay: time.Second}, attempt: 4, want: 8 * time.Second},
		{name: "capped", policy: BackoffPolicy{Strategy: BackoffExponential, InitialDelay: time.Second, MaxDelay: 3 * time.Second}, attempt: 4, want: 3 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.policy.DelayAfter(test.attempt); got != test.want {
				t.Fatalf("DelayAfter(%d) = %s, want %s", test.attempt, got, test.want)
			}
		})
	}
	if err := (RetryPolicy{MaxAttempts: -1}).Validate(); !errors.Is(err, ErrInvalidRetryPolicy) {
		t.Fatalf("negative maximum validation = %v", err)
	}
	if err := (RetryPolicy{Backoff: BackoffPolicy{Strategy: "unknown"}}).Validate(); !errors.Is(err, ErrInvalidRetryPolicy) {
		t.Fatalf("unknown backoff validation = %v", err)
	}
}

func TestTickMaterializesAndDispatchesStableFire(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	observedAt := scheduledAt.Add(37 * time.Second)
	clock := newFakeClock(observedAt)
	store := newFakeStore(Schedule{
		ID:       "schedule-a",
		CronExpr: "*/5 * * * *",
		NextRun:  scheduledAt,
		Enabled:  true,
		JobType:  "opaque-kind",
		Payload:  []byte(`{"key":"value"}`),
	})
	runner := &fakeRunner{}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(clock), WithDueBatchLimit(7), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("TickNow failed: %v", err)
	}

	jobs := runner.snapshot()
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	job := jobs[0]
	wantID := DeriveFireID("schedule-a", scheduledAt)
	if job.FireID != wantID || job.RunID != wantID {
		t.Fatalf("FireID/RunID = %q/%q, want %q", job.FireID, job.RunID, wantID)
	}
	if !job.ScheduledAt.Equal(scheduledAt) || !job.FiredAt.Equal(observedAt) {
		t.Fatalf("scheduled/fired times = %s/%s, want %s/%s", job.ScheduledAt, job.FiredAt, scheduledAt, observedAt)
	}
	if job.Attempt != 1 || job.JobType != "opaque-kind" || string(job.Payload) != `{"key":"value"}` {
		t.Fatalf("unexpected job contract: %+v", job)
	}
	stored := store.fire(wantID)
	if stored.Status != FireSucceeded || stored.Attempt != 1 || !stored.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("unexpected stored fire: %+v", stored)
	}
	if store.scheduleLimit != 7 || store.fireLimit != 7 {
		t.Fatalf("store limits = %d/%d, want 7/7", store.scheduleLimit, store.fireLimit)
	}
	if got := observer.kinds(); fmt.Sprint(got) != fmt.Sprint([]ObserverEventKind{ObserverClaim, ObserverFire, ObserverSuccess}) {
		t.Fatalf("observer events = %v", got)
	}
}

func TestTickDispatchesDistinctDueSchedulesIndependently(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(
		Schedule{ID: "schedule-a", CronExpr: "* * * * *", NextRun: now, Enabled: true},
		Schedule{ID: "schedule-b", CronExpr: "* * * * *", NextRun: now, Enabled: true},
	)
	runner := &fakeRunner{}
	engine := New(store, runner, WithClock(newFakeClock(now)))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	jobs := runner.snapshot()
	if len(jobs) != 2 {
		t.Fatalf("dispatched %d jobs, want 2", len(jobs))
	}
	if jobs[0].ScheduleID == jobs[1].ScheduleID {
		t.Fatalf("distinct schedules collapsed into one dispatch: %+v", jobs)
	}
	for _, scheduleID := range []string{"schedule-a", "schedule-b"} {
		fire := store.fire(DeriveFireID(scheduleID, now))
		if fire.Status != FireSucceeded || fire.Attempt != 1 {
			t.Fatalf("%s fire = %+v", scheduleID, fire)
		}
	}
}

func TestZeroNextRunRemainsNonDispatchable(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(Schedule{
		ID: "unscheduled", CronExpr: "* * * * *", Enabled: true,
	})
	runner := &fakeRunner{}
	engine := New(store, runner, WithClock(newFakeClock(now)))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if jobs := runner.snapshot(); len(jobs) != 0 {
		t.Fatalf("zero NextRun dispatched jobs: %+v", jobs)
	}
	if store.createCount != 0 || len(store.fires) != 0 {
		t.Fatalf("zero NextRun materialized a fire: creates=%d fires=%d", store.createCount, len(store.fires))
	}
}

func TestMalformedSuccessfulClaimIsRejected(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(Schedule{
		ID: "malformed-claim", CronExpr: "* * * * *", NextRun: now, Enabled: true,
	})
	store.claimResult = func(fire Fire) Fire {
		fire.Attempt++
		return fire
	}
	observer := &recordingObserver{}
	runner := &fakeRunner{}
	engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if jobs := runner.snapshot(); len(jobs) != 0 {
		t.Fatalf("malformed claim reached Runner: %+v", jobs)
	}
	if engine.Status().Dispatches != 0 || engine.Status().WorkerErrors != 1 {
		t.Fatalf("unexpected status after malformed claim: %+v", engine.Status())
	}
	event, ok := findObserverEvent(observer.snapshot(), ObserverEngineError, "validate_claim")
	if !ok || !errors.Is(event.Err, ErrInvalidClaim) {
		t.Fatalf("invalid claim was not surfaced as ErrInvalidClaim: %+v", event)
	}
}

func TestTransitionFailureDoesNotCountDurableSuccess(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	persistErr := errors.New("persistence unavailable")
	tests := []struct {
		name    string
		setHook func(*fakeStore)
		wantErr error
	}{
		{
			name: "lost compare-and-swap",
			setHook: func(store *fakeStore) {
				store.transitionLost = true
			},
			wantErr: ErrTransitionConflict,
		},
		{
			name: "store error",
			setHook: func(store *fakeStore) {
				store.transitionErr = persistErr
			},
			wantErr: persistErr,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newFakeStore(Schedule{
				ID: test.name, CronExpr: "* * * * *", NextRun: now, Enabled: true,
			})
			test.setHook(store)
			observer := &recordingObserver{}
			runner := &fakeRunner{}
			engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

			if err := engine.TickNow(context.Background()); err != nil {
				t.Fatalf("tick failed: %v", err)
			}
			if jobs := runner.snapshot(); len(jobs) != 1 {
				t.Fatalf("Runner calls = %d, want 1", len(jobs))
			}
			fire := store.fire(DeriveFireID(test.name, now))
			if fire.Status != FireClaimed || fire.Attempt != 1 {
				t.Fatalf("failed transition changed durable fire: %+v", fire)
			}
			if engine.Status().Dispatches != 0 || engine.Status().WorkerErrors != 1 {
				t.Fatalf("failed transition counted as success: %+v", engine.Status())
			}
			event, ok := findObserverEvent(observer.snapshot(), ObserverEngineError, "transition_fire")
			if !ok || !errors.Is(event.Err, test.wantErr) {
				t.Fatalf("transition failure was not surfaced: %+v", event)
			}
		})
	}
}

func TestRetryPreservesFireIdentityAndScheduledTime(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	firstObserved := scheduledAt.Add(time.Minute)
	clock := newFakeClock(firstObserved)
	store := newFakeStore(Schedule{
		ID:       "schedule-retry",
		CronExpr: "0 * * * *",
		NextRun:  scheduledAt,
		Enabled:  true,
		Retry: RetryPolicy{
			MaxAttempts: 3,
			Backoff:     BackoffPolicy{Strategy: BackoffConstant, InitialDelay: 5 * time.Minute},
		},
	})
	runner := &fakeRunner{responses: []error{errors.New("queue unavailable"), nil}}
	engine := New(store, runner, WithClock(clock))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("first tick failed: %v", err)
	}
	fireID := DeriveFireID("schedule-retry", scheduledAt)
	first := store.fire(fireID)
	if first.Status != FireRetrying || first.Attempt != 1 {
		t.Fatalf("first attempt state = %+v", first)
	}
	wantRetryAt := firstObserved.Add(5 * time.Minute)
	if !first.NextAttemptAt.Equal(wantRetryAt) {
		t.Fatalf("retry at %s, want %s", first.NextAttemptAt, wantRetryAt)
	}

	clock.set(wantRetryAt.Add(-time.Nanosecond))
	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("early tick failed: %v", err)
	}
	if got := len(runner.snapshot()); got != 1 {
		t.Fatalf("early tick dispatched %d attempts, want 1", got)
	}

	clock.set(wantRetryAt)
	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("retry tick failed: %v", err)
	}
	jobs := runner.snapshot()
	if len(jobs) != 2 {
		t.Fatalf("got %d attempts, want 2", len(jobs))
	}
	if jobs[0].FireID != jobs[1].FireID || jobs[1].FireID != fireID {
		t.Fatalf("retry changed fire identity: %+v", jobs)
	}
	if !jobs[0].ScheduledAt.Equal(jobs[1].ScheduledAt) || !jobs[1].ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("retry changed scheduled time: %+v", jobs)
	}
	if jobs[0].Attempt != 1 || jobs[1].Attempt != 2 {
		t.Fatalf("attempts = %d/%d, want 1/2", jobs[0].Attempt, jobs[1].Attempt)
	}
	if got := store.fire(fireID); got.Status != FireSucceeded || got.Attempt != 2 {
		t.Fatalf("final fire = %+v", got)
	}
}

func TestExhaustedFireCannotResetOnLaterTick(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(scheduledAt)
	store := newFakeStore(Schedule{
		ID:       "schedule-exhaust",
		CronExpr: "0 * * * *",
		NextRun:  scheduledAt,
		Enabled:  true,
		Retry: RetryPolicy{
			MaxAttempts: 2,
			Backoff:     BackoffPolicy{Strategy: BackoffConstant, InitialDelay: time.Minute},
		},
	})
	runner := &fakeRunner{responses: []error{errors.New("first failure"), errors.New("second failure")}}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(clock), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("first tick failed: %v", err)
	}
	clock.set(scheduledAt.Add(time.Minute))
	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("second tick failed: %v", err)
	}

	fireID := DeriveFireID("schedule-exhaust", scheduledAt)
	exhausted := store.fire(fireID)
	if exhausted.Status != FireExhausted || exhausted.Attempt != 2 {
		t.Fatalf("exhausted fire = %+v", exhausted)
	}

	// Even if a host accidentally presents the same scheduled occurrence
	// again, CreateFire's fire-ID uniqueness contract keeps exhaustion durable.
	store.setScheduleNext("schedule-exhaust", scheduledAt)
	clock.set(scheduledAt.Add(2 * time.Minute))
	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("later tick failed: %v", err)
	}
	if got := len(runner.snapshot()); got != 2 {
		t.Fatalf("exhausted fire dispatched %d attempts, want 2", got)
	}
	if got := store.fire(fireID); got.Status != FireExhausted || got.Attempt != 2 {
		t.Fatalf("exhaustion reset on later tick: %+v", got)
	}
	if engine.Status().Exhaustions != 1 {
		t.Fatalf("exhaustions = %d, want 1", engine.Status().Exhaustions)
	}
	if !containsKind(observer.kinds(), ObserverExhaustion) {
		t.Fatal("missing exhaustion observer event")
	}
}

func TestConcurrentEnginesCannotDispatchSameAttemptTwice(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	store := newFakeStore()
	fire := Fire{
		ID:            DeriveFireID("schedule-concurrent", now),
		ScheduleID:    "schedule-concurrent",
		ScheduledAt:   now,
		Status:        FirePending,
		NextAttemptAt: now,
	}
	store.fires[fire.ID] = fire
	store.fireListBarrier = make(chan struct{})
	runner := &fakeRunner{}
	first := New(store, runner, WithClock(clock))
	second := New(store, runner, WithClock(clock))

	var wait sync.WaitGroup
	wait.Add(2)
	errorsCh := make(chan error, 2)
	for _, engine := range []*Engine{first, second} {
		go func(engine *Engine) {
			defer wait.Done()
			errorsCh <- engine.TickNow(context.Background())
		}(engine)
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent tick failed: %v", err)
		}
	}

	if jobs := runner.snapshot(); len(jobs) != 1 {
		t.Fatalf("concurrent engines dispatched %d jobs, want 1", len(jobs))
	}
	if store.claimCount != 1 {
		t.Fatalf("successful claim count = %d, want 1", store.claimCount)
	}
	if got := store.fire(fire.ID); got.Status != FireSucceeded || got.Attempt != 1 {
		t.Fatalf("final fire = %+v", got)
	}
}

func TestOneTimeScheduleEmitsDisableAndStillDispatches(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(Schedule{ID: "once", NextRun: now, Enabled: true})
	runner := &fakeRunner{}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if !store.disabled["once"] {
		t.Fatal("one-time schedule was not disabled")
	}
	if len(runner.snapshot()) != 1 {
		t.Fatal("durable one-time fire was not dispatched")
	}
	kinds := observer.kinds()
	if !containsKind(kinds, ObserverDisable) || !containsKind(kinds, ObserverSuccess) {
		t.Fatalf("observer events = %v", kinds)
	}
}

func TestDisableFailureIsObservableAndMaterializedFireStillDispatches(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	disableErr := errors.New("disable unavailable")
	store := newFakeStore(Schedule{ID: "disable-failure", NextRun: now, Enabled: true})
	store.disableErr = disableErr
	runner := &fakeRunner{}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	jobs := runner.snapshot()
	if len(jobs) != 1 {
		t.Fatalf("materialized fire dispatched %d jobs, want 1", len(jobs))
	}
	fire := store.fire(DeriveFireID("disable-failure", now))
	if fire.Status != FireSucceeded || fire.Attempt != 1 {
		t.Fatalf("materialized fire did not complete after disable failure: %+v", fire)
	}
	if engine.Status().Dispatches != 1 || engine.Status().WorkerErrors != 1 {
		t.Fatalf("unexpected status after disable failure: %+v", engine.Status())
	}
	event, ok := findObserverEvent(observer.snapshot(), ObserverEngineError, "disable_schedule")
	if !ok || !errors.Is(event.Err, disableErr) {
		t.Fatalf("disable failure was not observable: %+v", event)
	}
	if !containsKind(observer.kinds(), ObserverSuccess) {
		t.Fatalf("missing success after disable failure: %v", observer.kinds())
	}
}

func TestDuplicateDispatchIsTerminalSkip(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(Schedule{ID: "duplicate", NextRun: now, Enabled: true})
	runner := &fakeRunner{responses: []error{fmt.Errorf("queue insert: %w", ErrDuplicateJob)}}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	fire := store.fire(DeriveFireID("duplicate", now))
	if fire.Status != FireSkipped || fire.Attempt != 1 {
		t.Fatalf("duplicate fire = %+v", fire)
	}
	if engine.Status().WorkerErrors != 0 {
		t.Fatalf("duplicate counted as worker error: %+v", engine.Status())
	}
	if !containsKind(observer.kinds(), ObserverSkip) {
		t.Fatal("missing skip observer event")
	}
}

func TestObserverFailuresDoNotChangeDispatchOutcome(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		observer *recordingObserver
	}{
		{name: "error", observer: &recordingObserver{err: errors.New("telemetry unavailable")}},
		{name: "panic", observer: &recordingObserver{panic: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newFakeStore(Schedule{ID: test.name, NextRun: now, Enabled: true})
			runner := &fakeRunner{}
			engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(test.observer))

			if err := engine.TickNow(context.Background()); err != nil {
				t.Fatalf("tick failed: %v", err)
			}
			fire := store.fire(DeriveFireID(test.name, now))
			if fire.Status != FireSucceeded || len(runner.snapshot()) != 1 {
				t.Fatalf("observer changed outcome: fire=%+v jobs=%d", fire, len(runner.snapshot()))
			}
			if engine.Status().ObserverErrors != 4 {
				t.Fatalf("observer errors = %d, want 4", engine.Status().ObserverErrors)
			}
		})
	}
}

func TestObserverCannotMutateDispatchPayload(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore(Schedule{
		ID: "observer-payload", NextRun: now, Enabled: true, Payload: []byte("original"),
	})
	observer := ObserverFunc(func(_ context.Context, event ObserverEvent) error {
		if len(event.Fire.Payload) > 0 {
			event.Fire.Payload[0] = 'X'
		}
		return errors.New("observer failed after mutation")
	})
	runner := &fakeRunner{}
	engine := New(store, runner, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	jobs := runner.snapshot()
	if len(jobs) != 1 || string(jobs[0].Payload) != "original" {
		t.Fatalf("observer mutated dispatch payload: %+v", jobs)
	}
}

func TestEngineErrorObserverHook(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	store.listErr = errors.New("database unavailable")
	observer := &recordingObserver{}
	engine := New(store, &fakeRunner{}, WithClock(newFakeClock(now)), WithObserver(observer))

	if err := engine.TickNow(context.Background()); !errors.Is(err, store.listErr) {
		t.Fatalf("TickNow error = %v, want %v", err, store.listErr)
	}
	if got := observer.kinds(); fmt.Sprint(got) != fmt.Sprint([]ObserverEventKind{ObserverEngineError}) {
		t.Fatalf("observer events = %v", got)
	}
	if engine.Status().WorkerErrors != 1 {
		t.Fatalf("worker errors = %d, want 1", engine.Status().WorkerErrors)
	}
}

func TestInjectedClockCadenceAndDefaults(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	store := newFakeStore()
	store.listCalled = make(chan struct{}, 1)
	engine := New(store, &fakeRunner{}, WithClock(clock), WithTickCadence(25*time.Millisecond))

	engine.Start()
	clock.ticks <- now
	select {
	case <-store.listCalled:
	case <-time.After(time.Second):
		engine.Stop()
		t.Fatal("background tick did not use injected ticker")
	}
	engine.Stop()

	clock.mu.Lock()
	cadence := clock.cadence
	clock.mu.Unlock()
	if cadence != 25*time.Millisecond {
		t.Fatalf("cadence = %s, want 25ms", cadence)
	}
	if !engine.Status().LastTickAt.Equal(now) {
		t.Fatalf("last tick = %s, want %s", engine.Status().LastTickAt, now)
	}

	defaultEngine := New(newFakeStore(), &fakeRunner{})
	if defaultEngine.tickCadence != DefaultTickCadence || defaultEngine.dueBatchLimit != DefaultDueBatchLimit {
		t.Fatalf("defaults = %s/%d", defaultEngine.tickCadence, defaultEngine.dueBatchLimit)
	}
}

func TestStartStopIsIdempotent(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	engine := New(newFakeStore(), &fakeRunner{}, WithClock(newFakeClock(now)))

	engine.Start()
	engine.Start()
	if !engine.Status().Running {
		t.Fatal("engine is not running after Start")
	}
	engine.Stop()
	engine.Stop()
	if engine.Status().Running {
		t.Fatal("engine is still running after Stop")
	}
}

func TestRetryAndExhaustionObserverHooks(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	store := newFakeStore(Schedule{
		ID: "hooks", NextRun: now, Enabled: true,
		Retry: RetryPolicy{MaxAttempts: 2, Backoff: BackoffPolicy{Strategy: BackoffConstant, InitialDelay: time.Second}},
	})
	runner := &fakeRunner{responses: []error{errors.New("one"), errors.New("two")}}
	observer := &recordingObserver{}
	engine := New(store, runner, WithClock(clock), WithObserver(observer))

	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("first tick failed: %v", err)
	}
	clock.set(now.Add(time.Second))
	if err := engine.TickNow(context.Background()); err != nil {
		t.Fatalf("second tick failed: %v", err)
	}
	kinds := observer.kinds()
	for _, want := range []ObserverEventKind{
		ObserverClaim, ObserverFire, ObserverRetry, ObserverEngineError, ObserverExhaustion,
	} {
		if !containsKind(kinds, want) {
			t.Fatalf("missing %q in observer events %v", want, kinds)
		}
	}
}

func containsKind(kinds []ObserverEventKind, want ObserverEventKind) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}

func findObserverEvent(events []ObserverEvent, kind ObserverEventKind, operation string) (ObserverEvent, bool) {
	for _, event := range events {
		if event.Kind == kind && event.Operation == operation {
			return event, true
		}
	}
	return ObserverEvent{}, false
}
