-- Additive, permanent room-level notification history. No retention job.
CREATE TABLE IF NOT EXISTS iaq_notification_runs (
    period_kind TEXT NOT NULL CHECK (period_kind IN ('hourly', 'daily')),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL CHECK (period_end > period_start),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    room_count INTEGER NOT NULL,
    observed_metric_count INTEGER NOT NULL,
    notification_count INTEGER NOT NULL,
    PRIMARY KEY (period_kind, period_start)
);

CREATE TABLE IF NOT EXISTS iaq_notifications (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    school_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    room_label TEXT NOT NULL,
    metric TEXT NOT NULL CHECK (metric IN ('co2', 'pm25')),
    period_kind TEXT NOT NULL CHECK (period_kind IN ('hourly', 'daily')),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL CHECK (period_end > period_start),
    average DOUBLE PRECISION NOT NULL CHECK (average > '-Infinity'::float8 AND average < 'Infinity'::float8),
    sample_count BIGINT NOT NULL CHECK (sample_count > 0),
    threshold DOUBLE PRECISION NOT NULL,
    policy_version TEXT NOT NULL,
    unit TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (school_id, room_id, metric, period_kind, period_start)
);
CREATE INDEX IF NOT EXISTS idx_iaq_notifications_school_history
    ON iaq_notifications (school_id, id DESC);

CREATE TABLE IF NOT EXISTS iaq_notification_reads (
    notification_id BIGINT NOT NULL REFERENCES iaq_notifications(id),
    username TEXT NOT NULL REFERENCES app_users(username) ON UPDATE CASCADE ON DELETE CASCADE,
    read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, notification_id)
);
