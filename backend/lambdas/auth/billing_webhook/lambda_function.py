import json
import logging
import os
from typing import Any, Dict, Optional

import stripe

from ..common import (  # noqa: E402
    build_response,
    execute_statement,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


UPDATE_SUBSCRIPTION_BY_CUSTOMER = """
UPDATE partners
SET stripe_subscription_id = :stripe_subscription_id,
    stripe_subscription_status = :stripe_subscription_status,
    stripe_trial_ends_at = CASE
        WHEN :stripe_trial_ends_at IS NULL THEN NULL
        ELSE to_timestamp(:stripe_trial_ends_at)
    END,
    stripe_current_period_end = CASE
        WHEN :stripe_current_period_end IS NULL THEN NULL
        ELSE to_timestamp(:stripe_current_period_end)
    END,
    stripe_price_id = :stripe_price_id,
    updated_at = NOW()
WHERE stripe_customer_id = :stripe_customer_id;
"""


def _stripe_config() -> Dict[str, Any]:
    return {
        "secret_key": os.environ.get("STRIPE_SECRET_KEY"),
        "webhook_secret": os.environ.get("STRIPE_WEBHOOK_SECRET"),
    }


def _decode_body(event: Dict[str, Any]) -> Optional[str]:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            import base64

            body = base64.b64decode(body).decode("utf-8")
        except Exception:
            return None
    return body if isinstance(body, str) else None


def _extract_subscription_fields(subscription: Dict[str, Any]) -> Dict[str, Any]:
    items = subscription.get("items", {}).get("data", [])
    price_id = None
    if items and isinstance(items, list):
        price = items[0].get("price") if isinstance(items[0], dict) else None
        if isinstance(price, dict):
            price_id = price.get("id")
    return {
        "stripe_subscription_id": subscription.get("id"),
        "stripe_subscription_status": subscription.get("status"),
        "stripe_trial_ends_at": subscription.get("trial_end"),
        "stripe_current_period_end": subscription.get("current_period_end"),
        "stripe_price_id": price_id,
    }


def _update_subscription(customer_id: str, subscription: Dict[str, Any]) -> None:
    payload = _extract_subscription_fields(subscription)
    payload["stripe_customer_id"] = customer_id
    execute_statement(UPDATE_SUBSCRIPTION_BY_CUSTOMER, payload)


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    cfg = _stripe_config()
    if not cfg["secret_key"] or not cfg["webhook_secret"]:
        return build_response(500, {"error": "Stripe not configured"})

    stripe.api_key = cfg["secret_key"]
    payload = _decode_body(event)
    signature = (event.get("headers") or {}).get("Stripe-Signature") or (
        event.get("headers") or {}
    ).get("stripe-signature")

    if not payload or not signature:
        return build_response(400, {"error": "Missing payload or signature"})

    try:
        evt = stripe.Webhook.construct_event(payload, signature, cfg["webhook_secret"])
    except Exception:
        logger.exception("Stripe webhook signature failed")
        return build_response(400, {"error": "Invalid signature"})

    event_type = evt.get("type")
    data = evt.get("data", {}).get("object") or {}

    try:
        if event_type in {"customer.subscription.created", "customer.subscription.updated"}:
            customer_id = data.get("customer")
            if customer_id and isinstance(data, dict):
                _update_subscription(customer_id, data)
        elif event_type == "customer.subscription.deleted":
            customer_id = data.get("customer")
            if customer_id and isinstance(data, dict):
                _update_subscription(
                    customer_id,
                    {
                        "id": data.get("id"),
                        "status": "canceled",
                        "trial_end": None,
                        "current_period_end": data.get("current_period_end"),
                        "items": data.get("items"),
                    },
                )
        elif event_type == "checkout.session.completed":
            customer_id = data.get("customer")
            subscription_id = data.get("subscription")
            if customer_id and subscription_id:
                subscription = stripe.Subscription.retrieve(subscription_id)
                if isinstance(subscription, dict):
                    _update_subscription(customer_id, subscription)
        elif event_type == "invoice.payment_failed":
            customer_id = data.get("customer")
            subscription_id = data.get("subscription")
            if customer_id and subscription_id:
                subscription = stripe.Subscription.retrieve(subscription_id)
                if isinstance(subscription, dict):
                    _update_subscription(customer_id, subscription)
    except Exception:
        logger.exception("Stripe webhook processing failed")
        return build_response(500, {"error": "Webhook processing failed"})

    return build_response(200, {"received": True})
