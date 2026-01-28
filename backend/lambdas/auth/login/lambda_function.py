import logging
import time
from typing import Any, Dict

from ..common import (  # noqa: E402
    bad_request,
    build_refresh_cookie,
    client_ip,
    cookie_config,
    create_access_token,
    execute_statement,
    forbidden,
    get_origin,
    hash_refresh_token,
    ok,
    parse_body_json,
    server_error,
    token_config,
    unauthorized,
    user_agent,
    verify_password,
    generate_refresh_token,
    fetch_one,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


USER_QUERY = """
SELECT
    u.user_id,
    u.partner_internal_id,
    p.partner_id,
    u.email,
    u.role,
    u.status,
    u.password_hash
FROM partner_users u
JOIN partners p ON p.internal_id = u.partner_internal_id
WHERE lower(u.email) = :email
LIMIT 1;
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


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    origin = get_origin(event)
    body = parse_body_json(event)
    if body is None:
        return bad_request("Invalid JSON body", origin=origin)

    email = (body.get("email") or "").strip().lower()
    password = body.get("password")
    if not email or not password:
        return bad_request("email and password are required", origin=origin)

    try:
        user = fetch_one(USER_QUERY, {"email": email})
    except Exception:
        logger.exception("Failed to fetch user")
        return server_error("Internal server error", origin=origin)

    if not user or not user.get("password_hash"):
        return unauthorized("Invalid credentials", origin=origin)

    status = str(user.get("status") or "").lower()
    if status and status not in ("active", "enabled"):
        return forbidden("User is disabled", origin=origin)

    try:
        if not verify_password(password, user["password_hash"]):
            return unauthorized("Invalid credentials", origin=origin)
    except Exception:
        logger.exception("Password verification failed")
        return server_error("Internal server error", origin=origin)

    try:
        tokens_cfg = token_config()
        cookie_cfg = cookie_config()
    except Exception:
        logger.exception("Token/cookie configuration failed")
        return server_error("Internal server error", origin=origin)
    refresh_token = generate_refresh_token()
    refresh_hash = hash_refresh_token(refresh_token, tokens_cfg["refresh_pepper"])
    refresh_expires = int(time.time()) + tokens_cfg["refresh_ttl"]

    try:
        execute_statement(
            CREATE_SESSION,
            {
                "user_id": int(user["user_id"]),
                "partner_internal_id": int(user["partner_internal_id"]),
                "refresh_token_hash": refresh_hash,
                "user_agent": user_agent(event),
                "ip_address": client_ip(event),
                "expires_at_epoch": refresh_expires,
            },
        )
    except Exception:
        logger.exception("Failed to create auth session")
        return server_error("Internal server error", origin=origin)

    access = create_access_token(
        {
            "sub": str(user["user_id"]),
            "partner_internal_id": int(user["partner_internal_id"]),
            "partner_id": user.get("partner_id"),
            "email": user["email"],
            "role": user.get("role"),
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
        {"access_token": access["token"], "expires_in": tokens_cfg["access_ttl"]},
        origin=origin,
        cookies=[cookie],
    )
