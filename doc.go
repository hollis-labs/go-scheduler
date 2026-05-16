// Package scheduler provides a cron-driven schedule engine that dispatches
// due jobs to a Runner.
//
// The engine is decoupled from any application's persistence layer and job
// types through two neutral descriptors — Schedule and Job — and two
// interfaces — Store and Runner. An application implements Store over its own
// schedule records and Runner over its own queue or executor, converting to
// and from the neutral types at the seam. The engine itself depends only on
// this package and github.com/robfig/cron/v3.
package scheduler
