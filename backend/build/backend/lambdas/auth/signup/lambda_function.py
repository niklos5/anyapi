import logging
import re
import time
from typing import Any, Dict
from uuid import uuid4

from ..common import (  # noqa: E402
    bad_request,
    build_refresh_cookie,
    client_ip,
    cookie_config,
    create_access_token,
    execute_statement,
    fetch_one,
    get_origin,
    hash_password,
    hash_refresh_token,
    ok,
    parse_body_json,
    server_error,
    token_config,
    user_agent,
    generate_refresh_token,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

EMAIL_EXISTS = """
SELECT user_id
FROM partner_users
WHERE lower(email) = :email
LIMIT 1;
"""

PARTNER_EXISTS = """
SELECT internal_id
FROM partners
WHERE partner_id = :partner_id
LIMIT 1;
"""

CREATE_PARTNER = """
INSERT INTO partners (partner_id, name, created_at, updated_at)
VALUES (:partner_id, :name, NOW(), NOW())
RETURNING internal_id, partner_id;
"""

CREATE_USER = """
INSERT INTO partner_users (
    partner_internal_id,
    email,
    role,
    status,
    password_hash,
    created_at,
    updated_at
) VALUES (
    :partner_internal_id,
    :email,
    :role,
    :status,
    :password_hash,
    NOW(),
    NOW()
)
RETURNING user_id;
"""

CREATE_SESSION = """
INSERT INTO auth_sessions (
    user_id,
    partner_internal_id,
    refresh_token_hash,
    user_agent,
    ip_address,
    expires_at,
    last_used_at
) VALUES (
    :user_id,
    :partner_internal_id,
    :refresh_token_hash,
    :user_agent,
    :ip_address,
    to_timestamp(:expires_at_epoch),
    NOW()
);
"""


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    cleaned = cleaned.strip("-")
    return cleaned or f"partner-{uuid4().hex[:6]}"


def _unique_partner_id(base: str) -> str:
    candidate = base
    for _ in range(3):
        existing = fetch_one(PARTNER_EXISTS, {"partner_id": candidate})
        if not existing:
            return candidate
        candidate = f"{base}-{uuid4().hex[:4]}"
    return f"{base}-{uuid4().hex[:6]}"


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    origin = get_origin(event)
    body = parse_body_json(event)
    if body is None:
        return bad_request("Invalid JSON body", origin=origin)

    name = (body.get("company") or body.get("companyName") or body.get("name") or "").strip()
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    if isinstance(password, dict):
        password = ""
    password = str(password).strip()

    if not name or not email or not password:
        return bad_request("company, email, and password are required", origin=origin)
    if not EMAIL_REGEX.match(email):
        return bad_request("Invalid email address", origin=origin)
    if len(password) < 8:
        return bad_request("Password must be at least 8 characters", origin=origin)

    try:
        existing = fetch_one(EMAIL_EXISTS, {"email": email})
        if existing:
            return bad_request("Email already registered", origin=origin)

        partner_id = _unique_partner_id(_slugify(name))
        partner_row = fetch_one(CREATE_PARTNER, {"partner_id": partner_id, "name": name})
        if not partner_row:
            return server_error("Failed to create partner", origin=origin)

        password_hash = hash_password(password)
        user_row = fetch_one(
            CREATE_USER,
            {
                "partner_internal_id": int(partner_row["internal_id"]),
                "email": email,
                "role": "owner",
                "status": "active",
                "password_hash": password_hash,
            },
        )
        if not user_row:
            return server_error("Failed to create user", origin=origin)

        tokens_cfg = token_config()
        cookie_cfg = cookie_config()

        refresh_token = generate_refresh_token()
        refresh_hash = hash_refresh_token(refresh_token, tokens_cfg["refresh_pepper"])
        refresh_expires = int(time.time()) + tokens_cfg["refresh_ttl"]

        execute_statement(
            CREATE_SESSION,
            {
                "user_id": int(user_row["user_id"]),
                "partner_internal_id": int(partner_row["internal_id"]),
                "refresh_token_hash": refresh_hash,
                "user_agent": user_agent(event),
                "ip_address": client_ip(event),
                "expires_at_epoch": refresh_expires,
            },
        )

        access = create_access_token(
            {
                "sub": str(user_row["user_id"]),
                "partner_internal_id": int(partner_row["internal_id"]),
                "partner_id": partner_row.get("partner_id"),
                "email": email,
                "role": "owner",
            },
            tokens_cfg["jwt_secret"],
            tokens_cfg["access_ttl"],
        )

        cookie = build_refresh_cookie(
            refresh_token,
            tokens_cfg["refresh_ttl"],
            secure=cookie_cfg["secure"],
            same_site=cookie_cfg["same_site"],
            path=cookie_cfg["path"],
            domain=cookie_cfg["domain"],
        )

        return ok(
            {
                "access_token": access["token"],
                "expires_in": tokens_cfg["access_ttl"],
                "partner_id": partner_row.get("partner_id"),
                "email": email,
            },
            origin=origin,
            cookies=[cookie],
        )
    except Exception:
        logger.exception("Signup failed")
        return server_error("Internal server error", origin=origin)
