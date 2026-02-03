import logging
from typing import Any, Dict, Optional

from ..common import (  # noqa: E402
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
SELECT
    stripe_customer_id,
    stripe_subscription_id,
    stripe_subscription_status,
    stripe_trial_ends_at,
    stripe_current_period_end,
    stripe_price_id
FROM partners
WHERE internal_id = :partner_internal_id
LIMIT 1;
"""


def _require_claims(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    token = get_bearer_token(event)
    if not token:
        return None
    tokens_cfg = token_config()
    return verify_access_token(token, tokens_cfg["jwt_secret"])


def _iso_format(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    origin = get_origin(event)
    claims = _require_claims(event)
    if not claims:
        return unauthorized("Missing or invalid token", origin=origin)

    partner_internal_id = claims.get("partner_internal_id")
    if not partner_internal_id:
        return unauthorized("Missing tenant scope", origin=origin)

    try:
        partner = fetch_one(PARTNER_QUERY, {"partner_internal_id": int(partner_internal_id)})
    except Exception:
        logger.exception("Failed to load billing status")
        return server_error("Internal server error", origin=origin)

    if not partner:
        return unauthorized("Invalid tenant", origin=origin)

    return ok(
        {
            "stripeCustomerId": partner.get("stripe_customer_id"),
            "stripeSubscriptionId": partner.get("stripe_subscription_id"),
            "stripeSubscriptionStatus": partner.get("stripe_subscription_status"),
            "stripeTrialEndsAt": _iso_format(partner.get("stripe_trial_ends_at")),
            "stripeCurrentPeriodEnd": _iso_format(partner.get("stripe_current_period_end")),
            "stripePriceId": partner.get("stripe_price_id"),
        },
        origin=origin,
    )
