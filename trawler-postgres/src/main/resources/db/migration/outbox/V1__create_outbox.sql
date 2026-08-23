CREATE SEQUENCE IF NOT EXISTS outbox_sequence;
CREATE SEQUENCE IF NOT EXISTS outbox_fence_sequence;

CREATE TABLE IF NOT EXISTS outbox_entries (
    outbox_id VARCHAR(255) PRIMARY KEY,
    sequence BIGINT NOT NULL UNIQUE,
    output_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    status VARCHAR(16) NOT NULL,
    codec_id VARCHAR(255) NOT NULL,
    codec_version INTEGER NOT NULL,
    payload BYTEA NOT NULL,
    response_reference VARCHAR(255),
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(4096),
    lease_token BIGINT,
    lease_owner VARCHAR(255),
    lease_expires_at TIMESTAMPTZ,
    CONSTRAINT outbox_entries_status_check CHECK (status IN ('PENDING', 'LEASED', 'DELIVERED', 'FAILED')),
    CONSTRAINT outbox_entries_codec_version_check CHECK (codec_version >= 0),
    CONSTRAINT outbox_entries_attempts_check CHECK (delivery_attempts >= 0)
);

CREATE INDEX IF NOT EXISTS outbox_entries_claim_idx
    ON outbox_entries (output_id, status, lease_expires_at, sequence);

CREATE INDEX IF NOT EXISTS outbox_entries_retention_idx
    ON outbox_entries (output_id, created_at, sequence);
