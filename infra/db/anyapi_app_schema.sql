-- AnyApi app schema (PostgreSQL)
-- Stores mappings, jobs, and idempotency keys for ingestion.

CREATE SCHEMA IF NOT EXISTS anyapi_app;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS anyapi_app.mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_internal_id BIGINT NOT NULL REFERENCES anyapi_auth.partners(internal_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    schema_definition JSONB NOT NULL,
    default_mapping JSONB,
    metadata JSONB,
    api_key TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mappings_partner_idx
    ON anyapi_app.mappings (partner_internal_id);

CREATE INDEX IF NOT EXISTS mappings_updated_idx
    ON anyapi_app.mappings (partner_internal_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS anyapi_app.jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_internal_id BIGINT NOT NULL REFERENCES anyapi_auth.partners(internal_id) ON DELETE CASCADE,
    mapping_id UUID NOT NULL REFERENCES anyapi_app.mappings(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input_s3_key TEXT,
    input_checksum TEXT,
    result_s3_key TEXT,
    result_checksum TEXT,
    issues JSONB,
    metrics JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jobs_partner_idx
    ON anyapi_app.jobs (partner_internal_id);

CREATE INDEX IF NOT EXISTS jobs_mapping_idx
    ON anyapi_app.jobs (mapping_id, created_at DESC);

CREATE TABLE IF NOT EXISTS anyapi_app.idempotency_keys (
    id BIGSERIAL PRIMARY KEY,
    partner_internal_id BIGINT NOT NULL REFERENCES anyapi_auth.partners(internal_id) ON DELETE CASCADE,
    mapping_id UUID NOT NULL REFERENCES anyapi_app.mappings(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    job_id UUID NOT NULL REFERENCES anyapi_app.jobs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (partner_internal_id, mapping_id, idempotency_key)
);
