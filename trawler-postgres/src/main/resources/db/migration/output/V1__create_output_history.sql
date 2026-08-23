CREATE SEQUENCE IF NOT EXISTS output_sequence;

CREATE TABLE IF NOT EXISTS output_entries (
    output_entry_id VARCHAR(255) PRIMARY KEY,
    sequence BIGINT NOT NULL UNIQUE,
    output_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL,
    codec_id VARCHAR(255) NOT NULL,
    codec_version INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    response_reference VARCHAR(255),
    CONSTRAINT output_entries_codec_version_check CHECK (codec_version >= 0)
);

CREATE INDEX IF NOT EXISTS output_entries_retention_idx
    ON output_entries (output_id, stored_at, sequence);
