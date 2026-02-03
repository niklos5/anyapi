import logging
from typing import Any, Dict

from ..common import (  # noqa: E402
    clear_refresh_cookie,
    cookie_config,
    execute_statement,
    fetch_one,
    get_origin,
    hash_refresh_token,
    ok,
    parse_cookies,
    server_error,
    token_config,
    unauthorized,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


SESSION_LOOKUP = """
SELECT session_id
FROM auth_sessions
WHERE refresh_token_hash = :refresh_hash
  AND revoked_at IS NULL
  AND expires_at > NOW()
LIMIT 1;
"""

REVOKE_SESSION = """
UPDATE auth_sessions
SET revoked_at = NOW(), last_used_at = NOW()
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
        logger.exception("Failed to fetch session for logout")
        return server_error("Internal server error", origin=origin)

    if not session:
        return unauthorized("Invalid or expired refresh token", origin=origin)

    try:
        revoke_resp = execute_statement(REVOKE_SESSION, {"session_id": str(session["session_id"])})
        if revoke_resp.get("numberOfRecordsUpdated", 0) < 1:
            logger.error("Logout did not revoke any session rows")
            return server_error("Internal server error", origin=origin)
    except Exception:
        logger.exception("Failed to revoke session")
        return server_error("Internal server error", origin=origin)

    clear_cookie = clear_refresh_cookie(
        secure=cookie_cfg["secure"],
        same_site=cookie_cfg["same_site"],
        path=cookie_cfg["path"],
        domain=cookie_cfg["domain"],
    )

    return ok({"success": True}, origin=origin, cookies=[clear_cookie])
