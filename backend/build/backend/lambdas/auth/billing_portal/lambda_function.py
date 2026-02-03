import logging
import os
from typing import Any, Dict, Optional

import stripe

from ..common import (  # noqa: E402
    bad_request,
    fetch_one,
    get_bearer_token,
    get_origin,
    ok,
    server_error,
    token_config,
    unauthorized,
    verify_access_token,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PARTNER_QUERY = """
SELECT internal_id, stripe_customer_id
FROM partners
WHERE internal_id = :partner_internal_id
LIMIT 1;
"""


def _stripe_config() -> Dict[str, Any]:
    return {
        "secret_key": os.environ.get("STRIPE_SECRET_KEY"),
        "return_url": os.environ.get("STRIPE_BILLING_PORTAL_RETURN_URL"),
    }


def _require_claims(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    token = get_bearer_token(event)
    if not token:
        return None
    tokens_cfg = token_config()
    return verify_access_token(token, tokens_cfg["jwt_secret"])


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    origin = get_origin(event)
    claims = _require_claims(event)
    if not claims:
        return unauthorized("Missing or invalid token", origin=origin)

    partner_internal_id = claims.get("partner_internal_id")
    if not partner_internal_id:
        return unauthorized("Missing tenant scope", origin=origin)

    cfg = _stripe_config()
    if not cfg["secret_key"] or not cfg["return_url"]:
        return server_error("Stripe is not configured", origin=origin)

    try:
        partner = fetch_one(PARTNER_QUERY, {"partner_internal_id": int(partner_internal_id)})
    except Exception:
        logger.exception("Failed to load partner")
        return server_error("Internal server error", origin=origin)

    if not partner:
        return unauthorized("Invalid tenant", origin=origin)

    customer_id = partner.get("stripe_customer_id")
    if not customer_id:
        return bad_request("No billing profile yet", origin=origin)

    stripe.api_key = cfg["secret_key"]
    try:
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=cfg["return_url"],
        )
    except Exception:
        logger.exception("Stripe portal creation failed")
        return server_error("Unable to open billing portal", origin=origin)

    return ok({"url": session.url}, origin=origin)
