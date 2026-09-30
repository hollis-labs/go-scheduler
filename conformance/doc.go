// Package conformance is a portable test suite for any scheduler.Store
// implementation: the caller's own schema and adapter, not only
// sqlstore.Store.
//
// Run drives the store exclusively through the scheduler.Store interface, so
// it assumes no schema. Because the interface has no way to create a schedule,
// the store returned by the Factory must also implement Seeder. Adapters over
// an application's own tables typically wrap their store in a small type that
// inserts a schedule row.
//
// The suite checks the documented contract, including the two properties that
// are easiest to get subtly wrong: that ExpectedFiredAt is a storage-level
// precondition of ClaimFire (a stale owner must lose even when the status and
// attempt still match), and that TransitionFire fences a stale owner by
// ClaimedAt after an expired claim has been recovered.
//
// Concurrency subtests are meaningful under `go test -race` and are worth
// repeating with -count.
package conformance
