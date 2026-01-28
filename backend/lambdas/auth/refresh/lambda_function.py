import logging
import time
from typing import Any, Dict

from ..common import (  # noqa: E402
    build_refresh_cookie,
    client_ip,
    cookie_config,
    create_access_token,
    execute_statement,
    fetch_one,
    get_origin,
    hash_refresh_token,
    ok,
    parse_cookies,
    server_error,
    token_config,
    unauthorized,
    user_agent,
    generate_refresh_token,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


SESSION_LOOKUP = """
SELECT
    s.session_id,
    s.user_id,
    s.partner_internal_id,
    p.partner_id,
    extract(epoch from s.expires_at) AS expires_at_epoch,
    s.revoked_at,
    u.email,
    u.role,
    u.status
FROM auth_sessions s
JOIN partner_users u ON u.user_id = s.user_id
JOIN partners p ON p.internal_id = s.partner_internal_id
WHERE s.refresh_token_hash = :refresh_hash
  AND s.revoked_at IS NULL
  AND s.expires_at > NOW()
LIMIT 1;
"""

ROTATE_SESSION = """
UPDATE auth_sessions
SET
    refresh_token_hash = :new_hash,
    expires_at = to_timestamp(:expires_at_epoch),
    last_used_at = NOW(),
    user_agent = :user_agent,
    ip_address = :ip_address
WHERE session_id = :session_id::uuid;
"""


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    origin = get_origin(event)
    cookies = parse_cookies(event)
    refresh_token = cookies.get("refresh_token")
    if not refresh_token:
        return unauthorized("Missing refresh token", origin=origin)

    try:
        tokens_cfg = token_config()
        cookie_cfg = cookie_config()
    except Exception:
        logger.exception("Token/cookie configuration failed")
        return server_error("Internal server error", origin=origin)
    refresh_hash = hash_refresh_token(refresh_token, tokens_cfg["refresh_pepper"])

    try:
        session = fetch_one(SESSION_LOOKUP, {"refresh_hash": refresh_hash})
    except Exception:
        logger.exception("Failed to fetch session for refresh")
        return server_error("Internal server error", origin=origin)

    if not session:
        return unauthorized("Invalid or expired refresh token", origin=origin)

    status = str(session.get("status") or "").lower()
    if status and status not in ("active", "enabled"):
        return unauthorized("Invalid or expired refresh token", origin=origin)

    new_refresh_token = generate_refresh_token()
    new_refresh_hash = hash_refresh_token(new_refresh_token, tokens_cfg["refresh_pepper"])
    new_refresh_expires = int(time.time()) + tokens_cfg["refresh_ttl"]

    try:
        rotate_resp = execute_statement(
            ROTATE_SESSION,
            {
                "new_hash": new_refresh_hash,
                "expires_at_epoch": new_refresh_expires,
                "user_agent": user_agent(event),
                "ip_address": client_ip(event),
                "session_id": str(session["session_id"]),
            },
        )
        if rotate_resp.get("numberOfRecordsUpdated", 0) < 1:
            logger.error("Refresh rotation did not update any records")
            return server_error("Internal server error", origin=origin)
    except Exception:
        logger.exception("Failed to rotate refresh token")
        return server_error("Internal server error", origin=origin)

    access = create_access_token(
        {
            "sub": str(session["user_id"]),
            "partner_internal_id": int(session["partner_internal_id"]),
            "partner_id": session.get("partner_id"),
            "email": session.get("email"),
            "role": session.get("role"),
        },
        tokens_cfg["jwt_secret"],
        tokens_cfg["access_ttl"],
    )

    cookie = build_refresh_cookie(
        new_refresh_token,
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
