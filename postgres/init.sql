-- =============================================================================
-- Oil & Gas use case - Postgres schema
-- Auto-loaded by the postgres container on first boot.
-- Tables per brief Section 3 + an audit table for audit_log_writer.
-- =============================================================================

-- Normalised sensor events (clean readings landing from the ingestion pipeline)
CREATE TABLE IF NOT EXISTS sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    well_id         VARCHAR(20)   NOT NULL,
    field_name      VARCHAR(40),
    event_ts        TIMESTAMPTZ   NOT NULL,
    pressure_bar    NUMERIC(8,2),
    temp_celsius    NUMERIC(6,2),
    flow_rate_m3h   NUMERIC(8,2),
    vibration_g     NUMERIC(5,2),
    valve_status    VARCHAR(10),
    quality_score   NUMERIC(4,2),       -- set by data_quality_tagger
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_readings_well_ts ON sensor_readings (well_id, event_ts);

-- Anomaly events (flagged readings with severity). Written BEFORE the API call.
CREATE TABLE IF NOT EXISTS anomaly_events (
    id              BIGSERIAL PRIMARY KEY,
    well_id         VARCHAR(20)   NOT NULL,
    field_name      VARCHAR(40),
    event_ts        TIMESTAMPTZ   NOT NULL,
    metric          VARCHAR(20)   NOT NULL,
    value           NUMERIC(10,2),
    severity        VARCHAR(10)   NOT NULL,   -- WARN | CRITICAL
    threshold_type  VARCHAR(20),              -- which band was breached
    detected_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_anomaly_well_ts ON anomaly_events (well_id, detected_at);
CREATE INDEX IF NOT EXISTS idx_anomaly_severity ON anomaly_events (severity);

-- Maintenance cases (returned from the FastAPI maintenance API)
CREATE TABLE IF NOT EXISTS maintenance_cases (
    id              BIGSERIAL PRIMARY KEY,
    case_id         VARCHAR(40)   UNIQUE NOT NULL,
    well_id         VARCHAR(20)   NOT NULL,
    anomaly_type    VARCHAR(40),
    severity        VARCHAR(10),
    assigned_team   VARCHAR(40),
    created_ts      TIMESTAMPTZ   NOT NULL,
    recorded_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cases_well ON maintenance_cases (well_id);

-- Shift report metadata (the SDK generator records each report it produces)
CREATE TABLE IF NOT EXISTS shift_reports (
    id              BIGSERIAL PRIMARY KEY,
    shift           VARCHAR(10)   NOT NULL,   -- morning | afternoon | night
    shift_start     TIMESTAMPTZ   NOT NULL,
    shift_end       TIMESTAMPTZ   NOT NULL,
    anomaly_count   INTEGER,
    cases_opened    INTEGER,
    worst_well      VARCHAR(20),
    report_object   VARCHAR(200),             -- MinIO object path
    generated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Audit log (written by audit_log_writer fragment for every pipeline run)
CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(80)   NOT NULL,
    record_count    INTEGER,
    error_count     INTEGER,
    operator        VARCHAR(40),
    environment     VARCHAR(20),
    run_ts          TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_pipeline ON audit_log (pipeline_name, run_ts);
