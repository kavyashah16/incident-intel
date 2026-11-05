-- 001_init: tenancy, partitioned events, issues, 5-minute issue stats.
-- Hand-written SQL on purpose: Drizzle cannot express partitioned tables, so everything that
-- touches `events` lives here. Small relational tables could move to Drizzle later.

-- ---------------------------------------------------------------- tenancy

CREATE TABLE tenants (
  id         uuid PRIMARY KEY,
  name       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projects (
  id                   uuid PRIMARY KEY,
  tenant_id            uuid          NOT NULL REFERENCES tenants (id),
  name                 text          NOT NULL,
  rate_limit_eps       int           NOT NULL DEFAULT 500,
  monthly_quota        bigint        NOT NULL DEFAULT 1000000,
  llm_daily_budget_usd numeric(8, 2) NOT NULL DEFAULT 1.00,
  created_at           timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id         uuid PRIMARY KEY,
  project_id uuid        NOT NULL REFERENCES projects (id),
  prefix     text        NOT NULL UNIQUE,   -- lookup handle, safe to display
  key_hash   bytea       NOT NULL,          -- sha256 of the secret part
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz
);

-- ---------------------------------------------------------------- hot path

-- Partitioned by day on occurred_at (the client event time, clamped at ingest). The key must
-- be deterministic across client retries so the primary key can dedupe them, and Postgres
-- requires the partition key inside every unique constraint.
CREATE TABLE events (
  project_id  uuid        NOT NULL,
  occurred_at timestamptz NOT NULL,
  event_id    uuid        NOT NULL,
  received_at timestamptz NOT NULL,                  -- ingest acceptance time (freshness metric)
  inserted_at timestamptz NOT NULL DEFAULT now(),    -- DB write time (freshness metric)
  issue_id    bigint      NOT NULL,
  level       smallint    NOT NULL,
  environment text,
  release     text,
  title       text        NOT NULL,
  payload     jsonb       NOT NULL,
  PRIMARY KEY (project_id, event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- The only secondary index at first; Week 2 measures what each extra index costs.
CREATE INDEX events_issue_idx ON events (project_id, issue_id, occurred_at DESC);

-- Create a UTC-day partition if it does not exist. Built as a standalone table and then
-- ATTACHed: ATTACH takes a lock that does not block concurrent inserts, whereas
-- CREATE TABLE ... PARTITION OF locks the parent exclusively. Safe to call concurrently.
CREATE OR REPLACE FUNCTION ensure_events_partition(p_day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  part_name text := 'events_' || to_char(p_day, 'YYYYMMDD');
BEGIN
  IF to_regclass(part_name) IS NOT NULL THEN
    RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtext('ensure_events_partition'));
  IF to_regclass(part_name) IS NULL THEN
    EXECUTE format('CREATE TABLE %I (LIKE events INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)', part_name);
    EXECUTE format(
      'ALTER TABLE events ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
      part_name,
      p_day::text || ' 00:00:00+00',
      (p_day + 1)::text || ' 00:00:00+00'
    );
  END IF;
END;
$$;

-- ---------------------------------------------------------------- grouping

CREATE TABLE issues (
  id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id          uuid        NOT NULL REFERENCES projects (id),
  fingerprint         text        NOT NULL,
  fingerprint_version smallint    NOT NULL,
  title               text        NOT NULL,
  level               smallint    NOT NULL,
  status              text        NOT NULL DEFAULT 'unresolved',
  first_seen          timestamptz NOT NULL,
  last_seen           timestamptz NOT NULL,
  event_count         bigint      NOT NULL DEFAULT 0,
  cluster_id          bigint,
  embedded_at         timestamptz,
  UNIQUE (project_id, fingerprint)
);
CREATE INDEX issues_project_last_seen_idx ON issues (project_id, last_seen DESC);

-- Counts per issue per 5 minutes: sparklines and spike detection read this, never `events`.
CREATE TABLE issue_stats_5m (
  project_id uuid        NOT NULL,
  issue_id   bigint      NOT NULL,
  bucket     timestamptz NOT NULL,
  count      int         NOT NULL,
  PRIMARY KEY (issue_id, bucket)
);
CREATE INDEX issue_stats_5m_project_bucket_idx ON issue_stats_5m (project_id, bucket);