package conformance_test

import (
	"testing"

	scheduler "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/go-scheduler/conformance"
)

// Run is called from an ordinary test. The factory must return a fresh store
// that also implements conformance.Seeder; here the in-memory test store does.
func ExampleRun() {
	_ = func(t *testing.T) {
		conformance.Run(t, func(t *testing.T) scheduler.Store {
			return newMemStore() // or: your adapter over a temp database
		})
	}
}

// Factory returns a new store for each subtest.
func ExampleFactory() {
	var f conformance.Factory = func(t *testing.T) scheduler.Store { return newMemStore() }
	_ = f
}

// Seeder is how the suite creates the schedules the engine contract only reads.
func ExampleSeeder() {
	var s conformance.Seeder = newMemStore()
	_ = s
}
