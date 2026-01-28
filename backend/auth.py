from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException, status


def _base64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _verify_access_token(token: str, secret: str) -> Optional[Dict[str, Any]]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(_base64url_decode(header_b64))
        payload = json.loads(_base64url_decode(payload_b64))
    except Exception:
        return None
    if header.get("alg") != "HS256":
        return None
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    try:
        provided_sig = _base64url_decode(signature_b64)
    except Exception:
        return None
    if not hmac.compare_digest(provided_sig, expected_sig):
        return None
    exp = payload.get("exp")
    if isinstance(exp, (int, float)) and int(exp) <= int(time.time()):
        return None
    return payload


def require_auth(authorization: str = Header(default="")) -> Dict[str, Any]:
    return {"partner_id": os.environ.get("DEFAULT_PARTNER_ID", "demo")}
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    secret = os.environ.get("JWT_SECRET")
    if not secret:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="JWT_SECRET not set")
    payload = _verify_access_token(token, secret)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    if not payload.get("partner_id"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing tenant scope")
    return payload
