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
    execute_statement,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PARTNER_QUERY = """
SELECT internal_id, partner_id, name, stripe_customer_id, stripe_subscription_status
FROM partners
WHERE internal_id = :partner_internal_id
LIMIT 1;
"""

UPDATE_STRIPE_CUSTOMER = """
UPDATE partners
SET stripe_customer_id = :stripe_customer_id,
    updated_at = NOW()
WHERE internal_id = :partner_internal_id;
"""


def _stripe_config() -> Dict[str, Any]:
    return {
        "secret_key": os.environ.get("STRIPE_SECRET_KEY"),
        "price_id": os.environ.get("STRIPE_PRICE_ID"),
        "trial_days": int(os.environ.get("STRIPE_TRIAL_DAYS", "14")),
        "success_url": os.environ.get("STRIPE_CHECKOUT_SUCCESS_URL"),
        "cancel_url": os.environ.get("STRIPE_CHECKOUT_CANCEL_URL"),
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
    if not cfg["secret_key"] or not cfg["price_id"]:
        return server_error("Stripe is not configured", origin=origin)
    if not cfg["success_url"] or not cfg["cancel_url"]:
        return server_error("Stripe URLs are not configured", origin=origin)

    try:
        partner = fetch_one(PARTNER_QUERY, {"partner_internal_id": int(partner_internal_id)})
    except Exception:
        logger.exception("Failed to load partner")
        return server_error("Internal server error", origin=origin)

    if not partner:
        return unauthorized("Invalid tenant", origin=origin)

    status = str(partner.get("stripe_subscription_status") or "").lower()
    if status in {"active", "trialing", "past_due", "unpaid"}:
        return bad_request("Subscription already active", origin=origin)

    stripe.api_key = cfg["secret_key"]

    try:
        customer_id = partner.get("stripe_customer_id")
        if not customer_id:
            customer = stripe.Customer.create(
                name=partner.get("name"),
                email=claims.get("email"),
                metadata={
                    "partner_internal_id": str(partner_internal_id),
                    "partner_id": str(partner.get("partner_id")),
                },
            )
            customer_id = customer.id
            execute_statement(
                UPDATE_STRIPE_CUSTOMER,
                {"stripe_customer_id": customer_id, "partner_internal_id": int(partner_internal_id)},
            )

        session = stripe.checkout.Session.create(
            mode="subscription",
            customer=customer_id,
            line_items=[{"price": cfg["price_id"], "quantity": 1}],
            success_url=cfg["success_url"],
            cancel_url=cfg["cancel_url"],
            subscription_data={
                "trial_period_days": cfg["trial_days"],
                "metadata": {
                    "partner_internal_id": str(partner_internal_id),
                    "partner_id": str(partner.get("partner_id")),
                },
            },
            metadata={
                "partner_internal_id": str(partner_internal_id),
                "partner_id": str(partner.get("partner_id")),
            },
        )
    except Exception:
        logger.exception("Stripe checkout creation failed")
        return server_error("Unable to start checkout", origin=origin)

    return ok({"url": session.url}, origin=origin)
