-- AnyApi auth schema (PostgreSQL)
-- Creates schema + tables for partners, partner_users, auth_sessions.

CREATE SCHEMA IF NOT EXISTS anyapi_auth;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS anyapi_auth.partners (
    internal_id BIGSERIAL PRIMARY KEY,
    partner_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anyapi_auth.partner_users (
    user_id BIGSERIAL PRIMARY KEY,
    partner_internal_id BIGINT NOT NULL REFERENCES anyapi_auth.partners(internal_id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'member',
    status TEXT NOT NULL DEFAULT 'active',
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS partner_users_email_unique
    ON anyapi_auth.partner_users (lower(email));

CREATE TABLE IF NOT EXISTS anyapi_auth.auth_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL REFERENCES anyapi_auth.partner_users(user_id) ON DELETE CASCADE,
    partner_internal_id BIGINT NOT NULL REFERENCES anyapi_auth.partners(internal_id) ON DELETE CASCADE,
    refresh_token_hash TEXT NOT NULL,
    user_agent TEXT,
    ip_address TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS auth_sessions_refresh_hash_idx
    ON anyapi_auth.auth_sessions (refresh_token_hash);

CREATE INDEX IF NOT EXISTS auth_sessions_user_id_idx
    ON anyapi_auth.auth_sessions (user_id);
