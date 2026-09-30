// Package sqlstore is a reference SQLite implementation of go-scheduler's
// Store over a minimal schema dedicated to this contract only. It exists for
// new adopters; existing applications keep their own schemas and can verify
// them with the sibling package conformance instead of migrating.
//
// Every compare-and-swap is a single SQL statement (or a transaction whose
// first statement is the write), so correctness rests on the database's
// statement atomicity rather than on an application-level lock. ExpectedFiredAt
// is part of the ClaimFire WHERE clause, which is what fences a stale owner
// at the storage layer.
//
// The package does not import a SQL driver. The caller opens the *sql.DB and
// should enable a busy timeout (and WAL) so concurrent writers wait instead of
// failing. ClaimFire uses UPDATE ... RETURNING, so SQLite 3.35 or newer is
// required. Timestamps are stored as fixed-width UTC text with nanosecond
// precision; the zero time round-trips as the zero time.
package sqlstore
