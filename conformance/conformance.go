package conformance

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	scheduler "github.com/hollis-labs/go-scheduler"
)

// Factory returns a fresh, empty store for one subtest. It is called once per
// subtest and should register any cleanup with t.Cleanup. The returned store
// must implement Seeder.
type Factory func(t *testing.T) scheduler.Store

// Seeder is the one capability the suite needs beyond scheduler.Store: a way
// to create a schedule, since the engine contract only ever reads them.
type Seeder interface {
	// CreateSchedule stores sch verbatim (ID, NextRun, Enabled, JobType,
	// Payload, Retry). It must fail if the ID already exists.
	CreateSchedule(ctx context.Context, sch scheduler.Schedule) error
}

// base is a fixed instant with sub-microsecond precision so that lossy
// timestamp round-tripping shows up as a failed compare-and-swap.
var base = time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)

const lease = time.Minute

// Run exercises the documented Store contract: CreateFire's schedule-advance
// CAS; ClaimFire's pending/retrying claim and expired-claimed-recovery CAS,
// including that a stale ExpectedFiredAt is rejected at the storage layer;
// TransitionFire's ClaimedAt fencing of a stale owner after recovery; and
// ListDueFires' due/expired-claim selection. Each subtest gets a fresh store
// from newStore.
func Run(t *testing.T, newStore Factory) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, *fixture)
	}{
		{"ListDueSchedules", testListDueSchedules},
		{"CreateFireAdvancesSchedule", testCreateFireAdvances},
		{"CreateFireRejectsStaleExpectedNext", testCreateFireStaleNext},
		{"CreateFireRejectsDuplicateID", testCreateFireDuplicate},
		{"CreateFireNeverRecreatesTerminalFire", testCreateFireTerminal},
		{"CreateFireRejectsDisabledSchedule", testCreateFireDisabled},
		{"DisableScheduleExcludesFromDue", testDisable},
		{"ClaimPendingIncrementsAttempt", testClaimPending},
		{"ClaimPreservesFireSnapshot", testClaimSnapshot},
		{"ClaimRejectsWrongStatusOrAttempt", testClaimWrongStatusAttempt},
		{"ClaimRejectsStaleExpectedFiredAt", testClaimStaleFiredAt},
		{"ClaimRetryingRequiresPriorFiredAt", testClaimRetryingFiredAt},
		{"ClaimRejectsUnexpiredRecovery", testClaimUnexpiredRecovery},
		{"RecoveryKeepsAttemptAndReplacesFiredAt", testRecovery},
		{"RecoveryRejectsStaleExpectedFiredAt", testRecoveryStale},
		{"ClaimUnknownFire", testClaimUnknown},
		{"TransitionFencesStaleOwnerAfterRecovery", testTransitionFencing},
		{"TransitionRejectsMismatch", testTransitionMismatch},
		{"TransitionRetryAndTerminal", testTransitionRetryTerminal},
		{"ListDueFiresSelection", testListDueFires},
		{"ListDueFiresLimit", testListDueFiresLimit},
		{"CrashBeforeCompleteIsRecoverable", testCrashBeforeComplete},
		{"ConcurrentClaimSingleWinner", testConcurrentClaim},
		{"ConcurrentRecoverySingleWinner", testConcurrentRecovery},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			seeder, ok := store.(Seeder)
			if !ok {
				t.Fatalf("conformance: %T must implement conformance.Seeder", store)
			}
			tc.fn(t, &fixture{store: store, seed: seeder})
		})
	}
}

type fixture struct {
	store scheduler.Store
	seed  Seeder
	n     int
}

func ctx() context.Context { return context.Background() }

// schedule seeds an enabled schedule due at base.
func (f *fixture) schedule(t *testing.T, mutate ...func(*scheduler.Schedule)) scheduler.Schedule {
	t.Helper()
	f.n++
	sch := scheduler.Schedule{
		ID:       fmt.Sprintf("sched-%d", f.n),
		CronExpr: "* * * * *",
		NextRun:  base,
		Enabled:  true,
		JobType:  "conformance.job",
		Payload:  []byte(`{"k":"v"}`),
		Retry: scheduler.RetryPolicy{MaxAttempts: 3, Backoff: scheduler.BackoffPolicy{
			Strategy: scheduler.BackoffExponential, InitialDelay: 2 * time.Second, MaxDelay: time.Minute,
		}},
	}
	for _, m := range mutate {
		m(&sch)
	}
	if err := f.seed.CreateSchedule(ctx(), sch); err != nil {
		t.Fatalf("seed schedule: %v", err)
	}
	return sch
}

func (f *fixture) creation(sch scheduler.Schedule) scheduler.FireCreation {
	scheduledAt := sch.NextRun
	return scheduler.FireCreation{
		ScheduleID:   sch.ID,
		ExpectedNext: scheduledAt,
		NextRun:      scheduledAt.Add(time.Hour),
		Fire: scheduler.Fire{
			ID:            scheduler.DeriveFireID(sch.ID, scheduledAt),
			ScheduleID:    sch.ID,
			ScheduledAt:   scheduledAt,
			Status:        scheduler.FirePending,
			NextAttemptAt: scheduledAt,
			Retry:         sch.Retry,
			JobType:       sch.JobType,
			Payload:       append([]byte(nil), sch.Payload...),
		},
	}
}

// fire seeds a schedule and materializes its pending fire.
func (f *fixture) fire(t *testing.T) scheduler.Fire {
	t.Helper()
	sch := f.schedule(t)
	c := f.creation(sch)
	created, err := f.store.CreateFire(ctx(), c)
	if err != nil || !created {
		t.Fatalf("CreateFire = %v, %v; want true, nil", created, err)
	}
	return c.Fire
}

// claim claims a pending fire at base+at for the standard lease.
func (f *fixture) claim(t *testing.T, fire scheduler.Fire, at time.Duration) scheduler.Fire {
	t.Helper()
	claimed, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
		FireID: fire.ID, ExpectedStatus: fire.Status, ExpectedAttempt: fire.Attempt,
		ExpectedFiredAt: fire.FiredAt, ClaimedAt: base.Add(at), ClaimExpiresAt: base.Add(at + lease),
	})
	if err != nil || !won {
		t.Fatalf("ClaimFire = _, %v, %v; want true, nil", won, err)
	}
	return claimed
}

func (f *fixture) due(t *testing.T, at time.Duration, limit int) []scheduler.Fire {
	t.Helper()
	fires, err := f.store.ListDueFires(ctx(), base.Add(at), limit)
	if err != nil {
		t.Fatalf("ListDueFires: %v", err)
	}
	return fires
}

func (f *fixture) isDue(t *testing.T, id string, at time.Duration) bool {
	t.Helper()
	for _, fire := range f.due(t, at, 1000) {
		if fire.ID == id {
			return true
		}
	}
	return false
}

func (f *fixture) dueSchedules(t *testing.T, at time.Duration) []scheduler.Schedule {
	t.Helper()
	list, err := f.store.ListDueSchedules(ctx(), base.Add(at), 1000)
	if err != nil {
		t.Fatalf("ListDueSchedules: %v", err)
	}
	return list
}

func hasSchedule(list []scheduler.Schedule, id string) bool {
	for _, s := range list {
		if s.ID == id {
			return true
		}
	}
	return false
}

func mustClaimFail(t *testing.T, f *fixture, claim scheduler.FireClaim, why string) {
	t.Helper()
	_, won, err := f.store.ClaimFire(ctx(), claim)
	if err != nil {
		t.Fatalf("%s: ClaimFire error %v; a lost CAS must be (false, nil)", why, err)
	}
	if won {
		t.Fatalf("%s: ClaimFire won; want rejected", why)
	}
}

func transition(fire scheduler.Fire, to scheduler.FireStatus, at time.Duration) scheduler.FireTransition {
	return scheduler.FireTransition{
		FireID: fire.ID, Attempt: fire.Attempt, From: scheduler.FireClaimed,
		ClaimedAt: fire.FiredAt, To: to, At: base.Add(at),
	}
}

// --- schedules ---------------------------------------------------------

func testListDueSchedules(t *testing.T, f *fixture) {
	due := f.schedule(t)
	future := f.schedule(t, func(s *scheduler.Schedule) { s.NextRun = base.Add(time.Hour) })
	disabled := f.schedule(t, func(s *scheduler.Schedule) { s.Enabled = false })
	unscheduled := f.schedule(t, func(s *scheduler.Schedule) { s.NextRun = time.Time{} })

	got := f.dueSchedules(t, 0)
	if !hasSchedule(got, due.ID) {
		t.Errorf("due schedule %s missing", due.ID)
	}
	for _, s := range []scheduler.Schedule{future, disabled, unscheduled} {
		if hasSchedule(got, s.ID) {
			t.Errorf("schedule %s must not be due", s.ID)
		}
	}
	if !hasSchedule(f.dueSchedules(t, time.Hour), future.ID) {
		t.Error("future schedule not due once time reaches NextRun (boundary is inclusive)")
	}
	for _, s := range got {
		if s.ID == due.ID {
			if !s.NextRun.Equal(due.NextRun) || s.JobType != due.JobType || string(s.Payload) != string(due.Payload) || s.Retry != due.Retry {
				t.Errorf("schedule round trip = %+v, want %+v", s, due)
			}
		}
	}
	limited, err := f.store.ListDueSchedules(ctx(), base.Add(2*time.Hour), 1)
	if err != nil || len(limited) != 1 {
		t.Errorf("limit 1 returned %d schedules, err %v", len(limited), err)
	}
}

func testDisable(t *testing.T, f *fixture) {
	sch := f.schedule(t)
	if err := f.store.DisableSchedule(ctx(), sch.ID); err != nil {
		t.Fatal(err)
	}
	if hasSchedule(f.dueSchedules(t, 0), sch.ID) {
		t.Error("disabled schedule still due")
	}
}

// --- CreateFire --------------------------------------------------------

func testCreateFireAdvances(t *testing.T, f *fixture) {
	sch := f.schedule(t)
	c := f.creation(sch)
	created, err := f.store.CreateFire(ctx(), c)
	if err != nil || !created {
		t.Fatalf("CreateFire = %v, %v", created, err)
	}
	if hasSchedule(f.dueSchedules(t, 0), sch.ID) {
		t.Error("schedule still due at the old NextRun after CreateFire advanced it")
	}
	if !hasSchedule(f.dueSchedules(t, time.Hour), sch.ID) {
		t.Error("schedule not due at the advanced NextRun")
	}
	got := f.due(t, 0, 10)
	if len(got) != 1 || got[0].ID != c.Fire.ID {
		t.Fatalf("due fires = %+v, want exactly %s", got, c.Fire.ID)
	}
	fire := got[0]
	if fire.Status != scheduler.FirePending || fire.Attempt != 0 || !fire.ScheduledAt.Equal(c.Fire.ScheduledAt) ||
		!fire.NextAttemptAt.Equal(c.Fire.NextAttemptAt) || !fire.FiredAt.IsZero() || !fire.ClaimExpiresAt.IsZero() {
		t.Errorf("materialized fire = %+v", fire)
	}
	if fire.ScheduleID != sch.ID || fire.JobType != sch.JobType || string(fire.Payload) != string(sch.Payload) || fire.Retry != sch.Retry {
		t.Errorf("fire snapshot = %+v, want schedule's job/payload/retry", fire)
	}
}

func testCreateFireStaleNext(t *testing.T, f *fixture) {
	sch := f.schedule(t)
	c := f.creation(sch)
	c.ExpectedNext = base.Add(-time.Second)
	created, err := f.store.CreateFire(ctx(), c)
	if err != nil || created {
		t.Fatalf("CreateFire with stale ExpectedNext = %v, %v; want false, nil", created, err)
	}
	if !hasSchedule(f.dueSchedules(t, 0), sch.ID) {
		t.Error("rejected CreateFire changed the schedule")
	}
	if len(f.due(t, time.Hour, 10)) != 0 {
		t.Error("rejected CreateFire created a fire")
	}
}

func testCreateFireDuplicate(t *testing.T, f *fixture) {
	sch := f.schedule(t)
	c := f.creation(sch)
	if created, err := f.store.CreateFire(ctx(), c); err != nil || !created {
		t.Fatalf("first CreateFire = %v, %v", created, err)
	}
	// Same ExpectedNext is now stale, so also try the fire ID collision
	// with a matching ExpectedNext by using the advanced value.
	c2 := c
	c2.ExpectedNext = c.NextRun
	c2.NextRun = c.NextRun.Add(time.Hour)
	created, err := f.store.CreateFire(ctx(), c2)
	if err != nil || created {
		t.Fatalf("CreateFire with existing Fire.ID = %v, %v; want false, nil", created, err)
	}
	// Neither record may change: the schedule must still be at c.NextRun.
	if !hasSchedule(f.dueSchedules(t, time.Hour), sch.ID) || hasSchedule(f.dueSchedules(t, time.Hour-time.Nanosecond), sch.ID) {
		t.Error("rejected duplicate CreateFire advanced the schedule")
	}
	if got := f.due(t, time.Hour, 10); len(got) != 1 {
		t.Errorf("due fires = %d, want 1", len(got))
	}
}

func testCreateFireTerminal(t *testing.T, f *fixture) {
	sch := f.schedule(t)
	c := f.creation(sch)
	if created, err := f.store.CreateFire(ctx(), c); err != nil || !created {
		t.Fatalf("CreateFire = %v, %v", created, err)
	}
	claimed := f.claim(t, c.Fire, 0)
	ok, err := f.store.TransitionFire(ctx(), transition(claimed, scheduler.FireSucceeded, time.Second))
	if err != nil || !ok {
		t.Fatalf("terminal transition = %v, %v", ok, err)
	}
	again := c
	again.ExpectedNext = c.NextRun
	again.NextRun = c.NextRun.Add(time.Hour)
	created, err := f.store.CreateFire(ctx(), again)
	if err != nil || created {
		t.Fatalf("recreating a terminal fire = %v, %v; want false, nil", created, err)
	}
	if f.isDue(t, c.Fire.ID, 24*time.Hour) {
		t.Error("terminal fire became due again")
	}
}

func testCreateFireDisabled(t *testing.T, f *fixture) {
	sch := f.schedule(t, func(s *scheduler.Schedule) { s.Enabled = false })
	created, err := f.store.CreateFire(ctx(), f.creation(sch))
	if err != nil || created {
		t.Fatalf("CreateFire on disabled schedule = %v, %v; want false, nil", created, err)
	}
}

// --- ClaimFire ---------------------------------------------------------

func testClaimPending(t *testing.T, f *fixture) {
	fire := f.fire(t)
	claimed := f.claim(t, fire, time.Second)
	if claimed.Status != scheduler.FireClaimed || claimed.Attempt != 1 {
		t.Errorf("claimed = status %s attempt %d, want claimed/1", claimed.Status, claimed.Attempt)
	}
	if !claimed.FiredAt.Equal(base.Add(time.Second)) || !claimed.ClaimExpiresAt.Equal(base.Add(time.Second+lease)) {
		t.Errorf("claim times = %v / %v (nanosecond precision must round-trip)", claimed.FiredAt, claimed.ClaimExpiresAt)
	}
	if claimed.ID != fire.ID || !claimed.ScheduledAt.Equal(fire.ScheduledAt) {
		t.Errorf("claim changed identity: %+v", claimed)
	}
	// The same claim repeated must lose: status/attempt/fired_at all moved.
	mustClaimFail(t, f, scheduler.FireClaim{
		FireID: fire.ID, ExpectedStatus: scheduler.FirePending, ExpectedAttempt: 0,
		ClaimedAt: base.Add(2 * time.Second), ClaimExpiresAt: base.Add(2*time.Second + lease),
	}, "repeating a won claim")
	if f.isDue(t, fire.ID, 10*time.Second) {
		t.Error("unexpired claimed fire listed as due")
	}
}

func testClaimSnapshot(t *testing.T, f *fixture) {
	fire := f.fire(t)
	claimed := f.claim(t, fire, 0)
	if claimed.JobType != fire.JobType || string(claimed.Payload) != string(fire.Payload) || claimed.Retry != fire.Retry {
		t.Errorf("claimed fire lost its snapshot: %+v vs %+v", claimed, fire)
	}
	if !claimed.NextAttemptAt.IsZero() {
		t.Errorf("claimed NextAttemptAt = %v, want zero", claimed.NextAttemptAt)
	}
}

func testClaimWrongStatusAttempt(t *testing.T, f *fixture) {
	fire := f.fire(t)
	at := base.Add(time.Second)
	mustClaimFail(t, f, scheduler.FireClaim{FireID: fire.ID, ExpectedStatus: scheduler.FireRetrying, ExpectedAttempt: 0,
		ClaimedAt: at, ClaimExpiresAt: at.Add(lease)}, "wrong ExpectedStatus")
	mustClaimFail(t, f, scheduler.FireClaim{FireID: fire.ID, ExpectedStatus: scheduler.FirePending, ExpectedAttempt: 5,
		ClaimedAt: at, ClaimExpiresAt: at.Add(lease)}, "wrong ExpectedAttempt")
	mustClaimFail(t, f, scheduler.FireClaim{FireID: fire.ID, ExpectedStatus: scheduler.FireSucceeded, ExpectedAttempt: 0,
		ClaimedAt: at, ClaimExpiresAt: at.Add(lease)}, "non-claimable ExpectedStatus")
	if !f.isDue(t, fire.ID, time.Second) {
		t.Error("failed claims must leave the fire pending and due")
	}
}

// testClaimStaleFiredAt is the key contract test: every other precondition
// matches, only ExpectedFiredAt is stale, and the claim must still lose.
func testClaimStaleFiredAt(t *testing.T, f *fixture) {
	fire := f.fire(t)
	// Pending fire has FiredAt zero; a non-zero expectation is stale.
	mustClaimFail(t, f, scheduler.FireClaim{
		FireID: fire.ID, ExpectedStatus: scheduler.FirePending, ExpectedAttempt: 0,
		ExpectedFiredAt: base.Add(-time.Hour),
		ClaimedAt:       base.Add(time.Second), ClaimExpiresAt: base.Add(time.Second + lease),
	}, "pending claim with stale ExpectedFiredAt")
	if !f.isDue(t, fire.ID, time.Second) {
		t.Error("fire left pending/due after rejected stale claim")
	}
	f.claim(t, fire, time.Second) // correct expectation still wins
}

func testClaimRetryingFiredAt(t *testing.T, f *fixture) {
	fire := f.fire(t)
	first := f.claim(t, fire, 0)
	retry := transition(first, scheduler.FireRetrying, time.Second)
	retry.NextAttemptAt = base.Add(10 * time.Second)
	retry.Error = "boom"
	if ok, err := f.store.TransitionFire(ctx(), retry); err != nil || !ok {
		t.Fatalf("retry transition = %v, %v", ok, err)
	}
	if f.isDue(t, fire.ID, 9*time.Second) {
		t.Error("retrying fire due before NextAttemptAt")
	}
	dueRetry := f.due(t, 10*time.Second, 10)
	if len(dueRetry) != 1 || dueRetry[0].Status != scheduler.FireRetrying || dueRetry[0].Attempt != 1 ||
		dueRetry[0].LastError != "boom" || !dueRetry[0].ClaimExpiresAt.IsZero() {
		t.Fatalf("due retrying fire = %+v", dueRetry)
	}
	stale := scheduler.FireClaim{
		FireID: fire.ID, ExpectedStatus: scheduler.FireRetrying, ExpectedAttempt: 1,
		ExpectedFiredAt: time.Time{}, // the prior FiredAt was first.FiredAt
		ClaimedAt:       base.Add(11 * time.Second), ClaimExpiresAt: base.Add(11*time.Second + lease),
	}
	mustClaimFail(t, f, stale, "retrying claim with stale ExpectedFiredAt")
	stale.ExpectedFiredAt = first.FiredAt
	got, won, err := f.store.ClaimFire(ctx(), stale)
	if err != nil || !won || got.Attempt != 2 || got.Status != scheduler.FireClaimed {
		t.Fatalf("retrying claim = %+v, %v, %v; want attempt 2 claimed", got, won, err)
	}
}

func testClaimUnexpiredRecovery(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	mustClaimFail(t, f, scheduler.FireClaim{
		FireID: claimed.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: claimed.Attempt,
		ExpectedFiredAt: claimed.FiredAt,
		ClaimedAt:       claimed.ClaimExpiresAt.Add(-time.Nanosecond), ClaimExpiresAt: claimed.ClaimExpiresAt.Add(lease),
	}, "recovering a claim whose lease has not elapsed")
}

func testRecovery(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	at := claimed.ClaimExpiresAt // boundary: expiry == ClaimedAt is expired
	recovered, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
		FireID: claimed.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: claimed.Attempt,
		ExpectedFiredAt: claimed.FiredAt, ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
	})
	if err != nil || !won {
		t.Fatalf("recovery = %v, %v; want won", won, err)
	}
	if recovered.Attempt != claimed.Attempt || recovered.Status != scheduler.FireClaimed {
		t.Errorf("recovery attempt/status = %d/%s, want %d/claimed", recovered.Attempt, recovered.Status, claimed.Attempt)
	}
	if !recovered.FiredAt.Equal(at) || recovered.FiredAt.Equal(claimed.FiredAt) || !recovered.ClaimExpiresAt.Equal(at.Add(lease)) {
		t.Errorf("recovery must replace FiredAt and lease: %+v", recovered)
	}
}

func testRecoveryStale(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	at := claimed.ClaimExpiresAt
	first, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
		FireID: claimed.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: claimed.Attempt,
		ExpectedFiredAt: claimed.FiredAt, ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
	})
	if err != nil || !won {
		t.Fatalf("first recovery = %v, %v", won, err)
	}
	// A second recoverer that read the fire before the first recovery still
	// holds the old FiredAt. Status and attempt match and time has passed, so
	// only ExpectedFiredAt can reject it.
	later := first.ClaimExpiresAt.Add(time.Second)
	mustClaimFail(t, f, scheduler.FireClaim{
		FireID: claimed.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: claimed.Attempt,
		ExpectedFiredAt: claimed.FiredAt, ClaimedAt: later, ClaimExpiresAt: later.Add(lease),
	}, "recovery with stale ExpectedFiredAt")
}

func testClaimUnknown(t *testing.T, f *fixture) {
	mustClaimFail(t, f, scheduler.FireClaim{
		FireID: "fire-does-not-exist", ExpectedStatus: scheduler.FirePending,
		ClaimedAt: base, ClaimExpiresAt: base.Add(lease),
	}, "unknown fire")
}

// --- TransitionFire ----------------------------------------------------

func testTransitionFencing(t *testing.T, f *fixture) {
	oldOwner := f.claim(t, f.fire(t), 0)
	at := oldOwner.ClaimExpiresAt
	newOwner, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
		FireID: oldOwner.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: oldOwner.Attempt,
		ExpectedFiredAt: oldOwner.FiredAt, ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
	})
	if err != nil || !won {
		t.Fatalf("recovery = %v, %v", won, err)
	}
	if ok, err := f.store.TransitionFire(ctx(), transition(oldOwner, scheduler.FireSucceeded, 0)); err != nil || ok {
		t.Fatalf("stale owner transition = %v, %v; want false, nil", ok, err)
	}
	if ok, err := f.store.TransitionFire(ctx(), transition(newOwner, scheduler.FireSucceeded, 0)); err != nil || !ok {
		t.Fatalf("current owner transition = %v, %v; want true, nil", ok, err)
	}
	if f.isDue(t, newOwner.ID, 24*time.Hour) {
		t.Error("succeeded fire is due")
	}
}

func testTransitionMismatch(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	badAttempt := transition(claimed, scheduler.FireSucceeded, time.Second)
	badAttempt.Attempt++
	badFrom := transition(claimed, scheduler.FireSucceeded, time.Second)
	badFrom.From = scheduler.FireRetrying
	badAt := transition(claimed, scheduler.FireSucceeded, time.Second)
	badAt.ClaimedAt = claimed.FiredAt.Add(time.Nanosecond)
	unknown := transition(claimed, scheduler.FireSucceeded, time.Second)
	unknown.FireID = "fire-does-not-exist"
	for name, tr := range map[string]scheduler.FireTransition{
		"attempt": badAttempt, "from": badFrom, "claimed_at": badAt, "unknown": unknown,
	} {
		if ok, err := f.store.TransitionFire(ctx(), tr); err != nil || ok {
			t.Errorf("mismatched %s transition = %v, %v; want false, nil", name, ok, err)
		}
	}
	// The failed attempts left the claim intact: a matching one still applies.
	if ok, err := f.store.TransitionFire(ctx(), transition(claimed, scheduler.FireSucceeded, time.Second)); err != nil || !ok {
		t.Errorf("matching transition after rejects = %v, %v", ok, err)
	}
	// And it cannot be applied twice.
	if ok, err := f.store.TransitionFire(ctx(), transition(claimed, scheduler.FireSucceeded, time.Second)); err != nil || ok {
		t.Errorf("replayed transition = %v, %v; want false, nil", ok, err)
	}
}

func testTransitionRetryTerminal(t *testing.T, f *fixture) {
	for _, to := range []scheduler.FireStatus{scheduler.FireSucceeded, scheduler.FireSkipped, scheduler.FireExhausted} {
		claimed := f.claim(t, f.fire(t), 0)
		if ok, err := f.store.TransitionFire(ctx(), transition(claimed, to, time.Second)); err != nil || !ok {
			t.Fatalf("%s transition = %v, %v", to, ok, err)
		}
		if f.isDue(t, claimed.ID, 24*time.Hour) {
			t.Errorf("%s fire is still due", to)
		}
	}
	// A retry transition clears the lease, so the fire is due by
	// NextAttemptAt only, never through the expired-claim path.
	claimed := f.claim(t, f.fire(t), 0)
	tr := transition(claimed, scheduler.FireRetrying, time.Second)
	tr.NextAttemptAt = base.Add(time.Hour)
	if ok, err := f.store.TransitionFire(ctx(), tr); err != nil || !ok {
		t.Fatalf("retry transition = %v, %v", ok, err)
	}
	if f.isDue(t, claimed.ID, 30*time.Minute) {
		t.Error("retrying fire due before NextAttemptAt (lease not cleared?)")
	}
	if !f.isDue(t, claimed.ID, time.Hour) {
		t.Error("retrying fire not due at NextAttemptAt")
	}
}

// --- ListDueFires ------------------------------------------------------

func testListDueFires(t *testing.T, f *fixture) {
	pending := f.fire(t)
	claimedLive := f.claim(t, f.fire(t), 0)
	claimedExpired := f.claim(t, f.fire(t), 0)
	done := f.claim(t, f.fire(t), 0)
	if ok, err := f.store.TransitionFire(ctx(), transition(done, scheduler.FireSucceeded, time.Second)); err != nil || !ok {
		t.Fatalf("transition = %v, %v", ok, err)
	}
	future := f.schedule(t, func(s *scheduler.Schedule) { s.NextRun = base.Add(24 * time.Hour) })
	fc := f.creation(future)
	if created, err := f.store.CreateFire(ctx(), fc); err != nil || !created {
		t.Fatalf("CreateFire = %v, %v", created, err)
	}

	// Just before the leases elapse: pending only.
	before := lease - time.Nanosecond
	for _, id := range []string{claimedLive.ID, claimedExpired.ID, done.ID, fc.Fire.ID} {
		if f.isDue(t, id, before) {
			t.Errorf("fire %s must not be due at +lease-1ns", id)
		}
	}
	if !f.isDue(t, pending.ID, before) {
		t.Error("pending fire missing")
	}
	// At lease expiry both claimed fires become due, terminal stays out.
	for _, id := range []string{claimedLive.ID, claimedExpired.ID, pending.ID} {
		if !f.isDue(t, id, lease) {
			t.Errorf("fire %s should be due at lease expiry", id)
		}
	}
	if f.isDue(t, done.ID, lease) || f.isDue(t, fc.Fire.ID, lease) {
		t.Error("terminal or future fire listed as due")
	}
	if !f.isDue(t, fc.Fire.ID, 24*time.Hour) {
		t.Error("future fire not due at its NextAttemptAt")
	}
	// The expired claim keeps the claimed status: recovery preserves Attempt
	// and must not be relabelled as a retry by the store.
	for _, fire := range f.due(t, lease, 100) {
		if fire.ID == claimedExpired.ID && (fire.Status != scheduler.FireClaimed || fire.Attempt != 1 || !fire.FiredAt.Equal(claimedExpired.FiredAt)) {
			t.Errorf("expired claim listed as %+v; want status claimed attempt 1 with its FiredAt", fire)
		}
	}
}

func testListDueFiresLimit(t *testing.T, f *fixture) {
	for range 5 {
		f.fire(t)
	}
	if got := f.due(t, 0, 3); len(got) != 3 {
		t.Errorf("limit 3 returned %d fires", len(got))
	}
	if got := f.due(t, 0, 100); len(got) != 5 {
		t.Errorf("limit 100 returned %d fires, want 5", len(got))
	}
}

// --- crash and concurrency ---------------------------------------------

// testCrashBeforeComplete models a worker that claims and then dies: no
// transition is ever written. After the lease the fire must be listed with its
// claimed state, recoverable without consuming an attempt, and completable by
// the new owner only.
func testCrashBeforeComplete(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	if f.isDue(t, claimed.ID, lease/2) {
		t.Fatal("crashed worker's claim redelivered before its lease elapsed")
	}
	var listed *scheduler.Fire
	for _, fire := range f.due(t, lease+time.Second, 10) {
		if fire.ID == claimed.ID {
			listed = &fire
		}
	}
	if listed == nil {
		t.Fatal("expired claim of a crashed worker is not listed as due")
	}
	at := base.Add(lease + time.Second)
	rec, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
		FireID: listed.ID, ExpectedStatus: listed.Status, ExpectedAttempt: listed.Attempt,
		ExpectedFiredAt: listed.FiredAt, ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
	})
	if err != nil || !won || rec.Attempt != 1 {
		t.Fatalf("recovery after crash = %+v, %v, %v; want attempt 1", rec, won, err)
	}
	if ok, err := f.store.TransitionFire(ctx(), transition(rec, scheduler.FireSucceeded, lease+2*time.Second)); err != nil || !ok {
		t.Fatalf("completion by recovering owner = %v, %v", ok, err)
	}
}

const racers = 16

// race releases n goroutines at once and returns how many reported success.
func race(n int, fn func(i int) bool) int {
	var wins atomic.Int32
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	done.Add(n)
	for i := range n {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			if fn(i) {
				wins.Add(1)
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return int(wins.Load())
}

func testConcurrentClaim(t *testing.T, f *fixture) {
	fire := f.fire(t)
	var errs sync.Map
	wins := race(racers, func(i int) bool {
		at := base.Add(time.Duration(i+1) * time.Millisecond)
		_, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
			FireID: fire.ID, ExpectedStatus: scheduler.FirePending, ExpectedAttempt: 0,
			ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
		})
		if err != nil {
			errs.Store(i, err)
		}
		return won
	})
	errs.Range(func(k, v any) bool { t.Errorf("racer %v: %v", k, v); return true })
	if wins != 1 {
		t.Fatalf("%d of %d concurrent claims of the same pending fire won; want exactly 1", wins, racers)
	}
}

// testConcurrentRecovery races recoverers that all hold the same stale
// ExpectedFiredAt against an expired claim. Exactly one may win.
func testConcurrentRecovery(t *testing.T, f *fixture) {
	claimed := f.claim(t, f.fire(t), 0)
	var errs sync.Map
	wins := race(racers, func(i int) bool {
		at := claimed.ClaimExpiresAt.Add(time.Duration(i) * time.Millisecond)
		_, won, err := f.store.ClaimFire(ctx(), scheduler.FireClaim{
			FireID: claimed.ID, ExpectedStatus: scheduler.FireClaimed, ExpectedAttempt: claimed.Attempt,
			ExpectedFiredAt: claimed.FiredAt, ClaimedAt: at, ClaimExpiresAt: at.Add(lease),
		})
		if err != nil {
			errs.Store(i, err)
		}
		return won
	})
	errs.Range(func(k, v any) bool { t.Errorf("racer %v: %v", k, v); return true })
	if wins != 1 {
		t.Fatalf("%d of %d concurrent recoveries won; want exactly 1", wins, racers)
	}
}
