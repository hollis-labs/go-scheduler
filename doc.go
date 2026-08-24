// Package scheduler provides an application-neutral timed activation engine
// that materializes durable scheduled fires and dispatches claimed attempts to
// a Runner.
//
// The engine is decoupled from application persistence, policy, and job types
// through neutral Schedule, Fire, Job, Store, Runner, and Observer contracts.
// Store implementations provide atomic fire materialization, attempt claims,
// and result transitions over their own schemas. The engine itself depends
// only on this package and github.com/robfig/cron/v3.
package scheduler
