-- Reference schema for go-scheduler's sqlstore. Every statement is idempotent
-- so Migrate can be run on each start. Timestamps are UTC, fixed-width text
-- (2006-01-02T15:04:05.000000000Z) so lexicographic order equals time order
-- and nanosecond precision round-trips exactly for compare-and-swap.

CREATE TABLE IF NOT EXISTS gosched_schedules (
    id         TEXT PRIMARY KEY,
    cron_expr  TEXT    NOT NULL DEFAULT '',
    last_run   TEXT    NOT NULL,
    next_run   TEXT    NOT NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    job_type   TEXT    NOT NULL DEFAULT '',
    payload    BLOB,
    retry_json TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS gosched_schedules_due
    ON gosched_schedules (enabled, next_run);

CREATE TABLE IF NOT EXISTS gosched_fires (
    id               TEXT PRIMARY KEY,
    schedule_id      TEXT    NOT NULL,
    scheduled_at     TEXT    NOT NULL,
    fired_at         TEXT    NOT NULL,
    claim_expires_at TEXT    NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    status           TEXT    NOT NULL,
    next_attempt_at  TEXT    NOT NULL,
    last_error       TEXT    NOT NULL DEFAULT '',
    retry_json       TEXT    NOT NULL DEFAULT '{}',
    job_type         TEXT    NOT NULL DEFAULT '',
    payload          BLOB
);

CREATE INDEX IF NOT EXISTS gosched_fires_due
    ON gosched_fires (status, next_attempt_at);
