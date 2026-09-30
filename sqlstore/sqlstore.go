package sqlstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	scheduler "github.com/hollis-labs/go-scheduler"
)

//go:embed schema/*.sql
var schemaFS embed.FS

// timeLayout is fixed-width so text comparison orders like time comparison.
const timeLayout = "2006-01-02T15:04:05.000000000Z"

var zeroTime = formatTime(time.Time{})

// Schema returns the embedded DDL, one idempotent .sql file per revision, at
// the root of the returned file system. Applications that manage migrations
// themselves can read it instead of calling Migrate.
func Schema() fs.FS {
	sub, err := fs.Sub(schemaFS, "schema")
	if err != nil {
		panic(err) // the embedded directory is fixed at build time
	}
	return sub
}

// Migrate applies the reference schema to db. It is idempotent.
func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("sqlstore: nil database")
	}
	fsys := Schema()
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		for _, stmt := range splitStatements(string(body)) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqlstore: migrate %s: %w", name, err)
			}
		}
	}
	return nil
}

// splitStatements splits on semicolons and drops comment-only fragments. The
// embedded DDL contains no semicolons inside literals.
func splitStatements(script string) []string {
	var out []string
	for _, part := range strings.Split(script, ";") {
		var kept []string
		for _, line := range strings.Split(part, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			kept = append(kept, line)
		}
		if stmt := strings.TrimSpace(strings.Join(kept, "\n")); stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

// Store implements scheduler.Store over a *sql.DB.
type Store struct {
	db *sql.DB
}

var _ scheduler.Store = (*Store)(nil)

// New returns a Store over db. It does not apply the schema; call Migrate
// first or manage the DDL from Schema yourself.
func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("sqlstore: nil database")
	}
	return &Store{db: db}, nil
}

// DB returns the underlying database handle.
func (s *Store) DB() *sql.DB { return s.db }

func formatTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlstore: parse time %q: %w", s, err)
	}
	return t, nil
}

// CreateSchedule inserts a schedule. The engine never creates schedules; this
// is the application-facing seed operation. It fails if the ID already exists.
func (s *Store) CreateSchedule(ctx context.Context, sch scheduler.Schedule) error {
	retry, err := json.Marshal(sch.Retry)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO gosched_schedules
(id, cron_expr, last_run, next_run, enabled, job_type, payload, retry_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		sch.ID, sch.CronExpr, formatTime(sch.LastRun), formatTime(sch.NextRun),
		boolInt(sch.Enabled), sch.JobType, sch.Payload, string(retry))
	if err != nil {
		return fmt.Errorf("sqlstore: create schedule %q: %w", sch.ID, err)
	}
	return nil
}

// GetSchedule returns the schedule with the given ID, and false if absent.
func (s *Store) GetSchedule(ctx context.Context, id string) (scheduler.Schedule, bool, error) {
	rows, err := s.db.QueryContext(ctx, scheduleSelect+` WHERE id = ?`, id)
	if err != nil {
		return scheduler.Schedule{}, false, err
	}
	list, err := scanSchedules(rows)
	if err != nil || len(list) == 0 {
		return scheduler.Schedule{}, false, err
	}
	return list[0], true, nil
}

// ListSchedules returns every schedule ordered by ID.
func (s *Store) ListSchedules(ctx context.Context) ([]scheduler.Schedule, error) {
	rows, err := s.db.QueryContext(ctx, scheduleSelect+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

// DeleteSchedule removes a schedule. Fires already materialized are kept so
// their stable IDs continue to deduplicate. Deleting a missing ID is a no-op.
func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM gosched_schedules WHERE id = ?`, id)
	return err
}

const scheduleSelect = `SELECT id, cron_expr, last_run, next_run, enabled, job_type, payload, retry_json FROM gosched_schedules`

// ListDueSchedules returns up to limit enabled schedules whose NextRun is set
// and at or before now, ordered by NextRun then ID.
func (s *Store) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]scheduler.Schedule, error) {
	rows, err := s.db.QueryContext(ctx, scheduleSelect+`
WHERE enabled = 1 AND next_run <> ? AND next_run <= ? ORDER BY next_run, id LIMIT ?`,
		zeroTime, formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

func scanSchedules(rows *sql.Rows) ([]scheduler.Schedule, error) {
	defer func() { _ = rows.Close() }()
	var out []scheduler.Schedule
	for rows.Next() {
		var sch scheduler.Schedule
		var last, next, retry string
		var enabled int
		if err := rows.Scan(&sch.ID, &sch.CronExpr, &last, &next, &enabled, &sch.JobType, &sch.Payload, &retry); err != nil {
			return nil, err
		}
		var err error
		if sch.LastRun, err = parseTime(last); err != nil {
			return nil, err
		}
		if sch.NextRun, err = parseTime(next); err != nil {
			return nil, err
		}
		sch.Enabled = enabled != 0
		if err := json.Unmarshal([]byte(retry), &sch.Retry); err != nil {
			return nil, err
		}
		out = append(out, sch)
	}
	return out, rows.Err()
}

// CreateFire advances the schedule and inserts the fire in one transaction.
// The schedule UPDATE is the first statement, so it is the compare-and-swap on
// ExpectedNext and takes the write lock before anything is read. If the fire
// ID already exists (including terminal fires) the transaction rolls back and
// neither record changes.
func (s *Store) CreateFire(ctx context.Context, creation scheduler.FireCreation) (bool, error) {
	retry, err := json.Marshal(creation.Fire.Retry)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `UPDATE gosched_schedules SET last_run = ?, next_run = ?
WHERE id = ? AND enabled = 1 AND next_run = ?`,
		formatTime(creation.Fire.ScheduledAt), formatTime(creation.NextRun),
		creation.ScheduleID, formatTime(creation.ExpectedNext))
	if err != nil {
		return false, err
	}
	if n, rerr := res.RowsAffected(); rerr != nil || n != 1 {
		return false, rerr
	}
	res, err = tx.ExecContext(ctx, `INSERT INTO gosched_fires
(id, schedule_id, scheduled_at, fired_at, claim_expires_at, attempt, status, next_attempt_at, last_error, retry_json, job_type, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		creation.Fire.ID, creation.ScheduleID, formatTime(creation.Fire.ScheduledAt),
		formatTime(creation.Fire.FiredAt), formatTime(creation.Fire.ClaimExpiresAt),
		creation.Fire.Attempt, string(creation.Fire.Status), formatTime(creation.Fire.NextAttemptAt),
		creation.Fire.LastError, string(retry), creation.Fire.JobType, creation.Fire.Payload)
	if err != nil {
		return false, err
	}
	if n, rerr := res.RowsAffected(); rerr != nil || n != 1 {
		return false, rerr // duplicate ID: rollback restores the schedule
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

const fireColumns = `id, schedule_id, scheduled_at, fired_at, claim_expires_at, attempt, status, next_attempt_at, last_error, retry_json, job_type, payload`

// GetFire returns one fire by ID, and false if absent. It is not part of
// scheduler.Store; it exists for inspection and tests.
func (s *Store) GetFire(ctx context.Context, id string) (scheduler.Fire, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+fireColumns+` FROM gosched_fires WHERE id = ?`, id)
	fire, err := scanFire(row)
	if errors.Is(err, sql.ErrNoRows) {
		return scheduler.Fire{}, false, nil
	}
	if err != nil {
		return scheduler.Fire{}, false, err
	}
	return fire, true, nil
}

// ListDueFires returns up to limit pending or retrying fires whose
// NextAttemptAt is at or before now, plus claimed fires whose lease has
// elapsed (a claimed fire with a zero lease counts as elapsed).
func (s *Store) ListDueFires(ctx context.Context, now time.Time, limit int) ([]scheduler.Fire, error) {
	ts := formatTime(now)
	rows, err := s.db.QueryContext(ctx, `SELECT `+fireColumns+` FROM gosched_fires
WHERE (status IN (?, ?) AND next_attempt_at <= ?) OR (status = ? AND claim_expires_at <= ?)
ORDER BY CASE status WHEN ? THEN claim_expires_at ELSE next_attempt_at END, id LIMIT ?`,
		string(scheduler.FirePending), string(scheduler.FireRetrying), ts,
		string(scheduler.FireClaimed), ts, string(scheduler.FireClaimed), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []scheduler.Fire
	for rows.Next() {
		fire, err := scanFire(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, fire)
	}
	return out, rows.Err()
}

type rowScanner interface{ Scan(dest ...any) error }

func scanFire(r rowScanner) (scheduler.Fire, error) {
	var f scheduler.Fire
	var scheduled, fired, expires, next, retry, status string
	if err := r.Scan(&f.ID, &f.ScheduleID, &scheduled, &fired, &expires, &f.Attempt, &status, &next, &f.LastError, &retry, &f.JobType, &f.Payload); err != nil {
		return scheduler.Fire{}, err
	}
	f.Status = scheduler.FireStatus(status)
	var err error
	if f.ScheduledAt, err = parseTime(scheduled); err != nil {
		return scheduler.Fire{}, err
	}
	if f.FiredAt, err = parseTime(fired); err != nil {
		return scheduler.Fire{}, err
	}
	if f.ClaimExpiresAt, err = parseTime(expires); err != nil {
		return scheduler.Fire{}, err
	}
	if f.NextAttemptAt, err = parseTime(next); err != nil {
		return scheduler.Fire{}, err
	}
	if err := json.Unmarshal([]byte(retry), &f.Retry); err != nil {
		return scheduler.Fire{}, err
	}
	return f, nil
}

// ClaimFire is one UPDATE ... RETURNING whose WHERE clause carries every
// precondition: status, attempt and ExpectedFiredAt, plus, when recovering a
// claimed fire, that the stored lease has elapsed. Because the checks and the
// write are one statement, two racing claimants cannot both succeed, and a
// stale ExpectedFiredAt is rejected by the database itself.
func (s *Store) ClaimFire(ctx context.Context, claim scheduler.FireClaim) (scheduler.Fire, bool, error) {
	var query string
	args := []any{
		string(scheduler.FireClaimed), formatTime(claim.ClaimedAt), formatTime(claim.ClaimExpiresAt),
		zeroTime, claim.FireID, string(claim.ExpectedStatus), claim.ExpectedAttempt, formatTime(claim.ExpectedFiredAt),
	}
	switch claim.ExpectedStatus {
	case scheduler.FirePending, scheduler.FireRetrying:
		query = `UPDATE gosched_fires SET status = ?, attempt = attempt + 1, fired_at = ?, claim_expires_at = ?, next_attempt_at = ?
WHERE id = ? AND status = ? AND attempt = ? AND fired_at = ? RETURNING ` + fireColumns
	case scheduler.FireClaimed:
		// Recovery keeps the attempt and requires the old lease to be elapsed.
		query = `UPDATE gosched_fires SET status = ?, fired_at = ?, claim_expires_at = ?, next_attempt_at = ?
WHERE id = ? AND status = ? AND attempt = ? AND fired_at = ? AND claim_expires_at <= ? RETURNING ` + fireColumns
		args = append(args, formatTime(claim.ClaimedAt))
	default:
		return scheduler.Fire{}, false, nil
	}
	fire, err := scanFire(s.db.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return scheduler.Fire{}, false, nil
	}
	if err != nil {
		return scheduler.Fire{}, false, err
	}
	return fire, true, nil
}

// TransitionFire applies an attempt result if Attempt, From and ClaimedAt
// (matched against the stored FiredAt) still hold. Every success clears the
// stored lease. The Reason field has no column: Fire carries no reason.
func (s *Store) TransitionFire(ctx context.Context, tr scheduler.FireTransition) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE gosched_fires
SET status = ?, next_attempt_at = ?, last_error = ?, claim_expires_at = ?
WHERE id = ? AND status = ? AND attempt = ? AND fired_at = ?`,
		string(tr.To), formatTime(tr.NextAttemptAt), tr.Error, zeroTime,
		tr.FireID, string(tr.From), tr.Attempt, formatTime(tr.ClaimedAt))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// DisableSchedule marks a schedule disabled. It returns an error if the
// schedule does not exist. Disabling an already-disabled schedule succeeds.
func (s *Store) DisableSchedule(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE gosched_schedules SET enabled = 0 WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("sqlstore: schedule %q not found", id)
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
