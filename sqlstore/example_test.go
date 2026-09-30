package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	scheduler "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/go-scheduler/sqlstore"
)

func exampleStore() (*sqlstore.Store, func()) {
	dir, err := os.MkdirTemp("", "sqlstore-example")
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "s.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		panic(err)
	}
	if merr := sqlstore.Migrate(context.Background(), db); merr != nil {
		panic(merr)
	}
	store, err := sqlstore.New(db)
	if err != nil {
		panic(err)
	}
	return store, func() { _ = db.Close(); _ = os.RemoveAll(dir) }
}

func ExampleMigrate() {
	db, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1) // an in-memory database is per connection
	fmt.Println(sqlstore.Migrate(context.Background(), db))
	fmt.Println(sqlstore.Migrate(context.Background(), db)) // idempotent
	// Output:
	// <nil>
	// <nil>
}

func ExampleSchema() {
	names, _ := fs.Glob(sqlstore.Schema(), "*.sql")
	fmt.Println(names)
	// Output: [001_init.sql]
}

func ExampleNew() {
	store, cleanup := exampleStore()
	defer cleanup()
	_, err := sqlstore.New(store.DB())
	fmt.Println(err)
	// Output: <nil>
}

func ExampleStore_DB() {
	store, cleanup := exampleStore()
	defer cleanup()
	fmt.Println(store.DB().PingContext(context.Background()))
	// Output: <nil>
}

func ExampleStore_CreateSchedule() {
	store, cleanup := exampleStore()
	defer cleanup()
	ctx := context.Background()
	err := store.CreateSchedule(ctx, scheduler.Schedule{
		ID: "nightly", CronExpr: "0 3 * * *", NextRun: time.Now().UTC(), Enabled: true, JobType: "backup",
	})
	sch, ok, _ := store.GetSchedule(ctx, "nightly")
	fmt.Println(err, ok, sch.JobType)
	// Output: <nil> true backup
}

func ExampleStore_GetSchedule() {
	store, cleanup := exampleStore()
	defer cleanup()
	_, ok, err := store.GetSchedule(context.Background(), "missing")
	fmt.Println(ok, err)
	// Output: false <nil>
}

func ExampleStore_ListSchedules() {
	store, cleanup := exampleStore()
	defer cleanup()
	list, err := store.ListSchedules(context.Background())
	fmt.Println(len(list), err)
	// Output: 0 <nil>
}

func ExampleStore_DeleteSchedule() {
	store, cleanup := exampleStore()
	defer cleanup()
	fmt.Println(store.DeleteSchedule(context.Background(), "missing")) // no-op
	// Output: <nil>
}

func ExampleStore_ListDueSchedules() {
	store, cleanup := exampleStore()
	defer cleanup()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.CreateSchedule(ctx, scheduler.Schedule{ID: "due", NextRun: now, Enabled: true})
	_ = store.CreateSchedule(ctx, scheduler.Schedule{ID: "later", NextRun: now.Add(time.Hour), Enabled: true})
	due, _ := store.ListDueSchedules(ctx, now, 10)
	fmt.Println(len(due), due[0].ID)
	// Output: 1 due
}

// The full claim lifecycle against a real database.
func ExampleStore_ClaimFire() {
	store, cleanup := exampleStore()
	defer cleanup()
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.CreateSchedule(ctx, scheduler.Schedule{ID: "s", NextRun: at, Enabled: true})
	fire := scheduler.Fire{ID: scheduler.DeriveFireID("s", at), ScheduleID: "s", ScheduledAt: at,
		Status: scheduler.FirePending, NextAttemptAt: at}
	created, _ := store.CreateFire(ctx, scheduler.FireCreation{ScheduleID: "s", ExpectedNext: at, NextRun: at.Add(time.Hour), Fire: fire})

	claim := scheduler.FireClaim{FireID: fire.ID, ExpectedStatus: scheduler.FirePending,
		ClaimedAt: at, ClaimExpiresAt: at.Add(time.Minute)}
	claimed, won, _ := store.ClaimFire(ctx, claim)
	_, wonAgain, _ := store.ClaimFire(ctx, claim) // a second worker loses the CAS
	fmt.Println(created, won, claimed.Attempt, wonAgain)
	// Output: true true 1 false
}

func ExampleStore_CreateFire() {
	store, cleanup := exampleStore()
	defer cleanup()
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.CreateSchedule(ctx, scheduler.Schedule{ID: "s", NextRun: at, Enabled: true})
	c := scheduler.FireCreation{ScheduleID: "s", ExpectedNext: at, NextRun: at.Add(time.Hour),
		Fire: scheduler.Fire{ID: scheduler.DeriveFireID("s", at), ScheduledAt: at, Status: scheduler.FirePending, NextAttemptAt: at}}
	first, _ := store.CreateFire(ctx, c)
	second, _ := store.CreateFire(ctx, c) // ExpectedNext is now stale
	fmt.Println(first, second)
	// Output: true false
}

func ExampleStore_GetFire() {
	store, cleanup := exampleStore()
	defer cleanup()
	_, ok, err := store.GetFire(context.Background(), "missing")
	fmt.Println(ok, err)
	// Output: false <nil>
}

func ExampleStore_ListDueFires() {
	store, cleanup := exampleStore()
	defer cleanup()
	fires, err := store.ListDueFires(context.Background(), time.Now(), 10)
	fmt.Println(len(fires), err)
	// Output: 0 <nil>
}

func ExampleStore_TransitionFire() {
	store, cleanup := exampleStore()
	defer cleanup()
	ok, err := store.TransitionFire(context.Background(), scheduler.FireTransition{FireID: "missing", From: scheduler.FireClaimed, To: scheduler.FireSucceeded})
	fmt.Println(ok, err)
	// Output: false <nil>
}

func ExampleStore_DisableSchedule() {
	store, cleanup := exampleStore()
	defer cleanup()
	ctx := context.Background()
	_ = store.CreateSchedule(ctx, scheduler.Schedule{ID: "s", Enabled: true})
	fmt.Println(store.DisableSchedule(ctx, "s"))
	// Output: <nil>
}
