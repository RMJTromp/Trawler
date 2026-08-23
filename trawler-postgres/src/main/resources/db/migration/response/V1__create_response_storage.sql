CREATE SEQUENCE IF NOT EXISTS response_sequence;

CREATE TABLE IF NOT EXISTS response_records (
    response_id VARCHAR(255) PRIMARY KEY,
    sequence BIGINT NOT NULL UNIQUE,
    pipeline_id VARCHAR(255),
    request_id VARCHAR(255),
    target VARCHAR(16384) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL,
    codec_id VARCHAR(255) NOT NULL,
    codec_version INTEGER NOT NULL,
    body_size INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    CONSTRAINT response_records_codec_version_check CHECK (codec_version >= 0),
    CONSTRAINT response_records_body_size_check CHECK (body_size >= 0)
);

CREATE INDEX IF NOT EXISTS response_records_request_idx
    ON response_records (request_id, sequence);

CREATE INDEX IF NOT EXISTS response_records_pipeline_idx
    ON response_records (pipeline_id, sequence);

CREATE INDEX IF NOT EXISTS response_records_target_idx
    ON response_records (target, sequence);

CREATE TABLE IF NOT EXISTS response_lineage (
    response_reference VARCHAR(255) NOT NULL,
    owner_kind VARCHAR(32) NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (response_reference, owner_kind, owner_id),
    CONSTRAINT response_lineage_response_fk
        FOREIGN KEY (response_reference) REFERENCES response_records (response_id) ON DELETE RESTRICT,
    CONSTRAINT response_lineage_owner_kind_check CHECK (owner_kind IN ('OUTPUT', 'OUTBOX'))
);

CREATE INDEX IF NOT EXISTS response_lineage_reference_idx
    ON response_lineage (response_reference);
