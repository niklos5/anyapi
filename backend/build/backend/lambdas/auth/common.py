import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import re
from urllib.parse import urlparse

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None

try:
    import pg8000  # type: ignore
except ImportError:  # pragma: no cover
    pg8000 = None

try:
    import bcrypt  # type: ignore
except ImportError:  # pragma: no cover - deployment packaging needs bcrypt wheel
    bcrypt = None


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_db_conn = None


# ---------- Environment helpers ----------

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# ---------- HTTP helpers ----------

def get_origin(event: Dict[str, Any]) -> Optional[str]:
    headers = (event.get("headers") or {})
    origin = headers.get("origin") or headers.get("Origin")
    if not origin:
        return None
    allowed = os.environ.get("ALLOWED_ORIGINS")
    if not allowed:
        # If unset, allow any origin presented. Caller should set ALLOWED_ORIGINS in prod.
        return origin
    allowed_list = [o.strip() for o in allowed.split(",") if o.strip()]
    return origin if origin in allowed_list else None


def build_response(
    status_code: int,
    body: Dict[str, Any],
    *,
    origin: Optional[str] = None,
    cookies: Optional[List[str]] = None,
) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(body),
    }
    if origin:
        resp["headers"]["Access-Control-Allow-Origin"] = origin
        resp["headers"]["Access-Control-Allow-Credentials"] = "true"
        resp["headers"]["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
        resp["headers"]["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        resp["headers"]["Vary"] = "Origin"
    if cookies:
        # HTTP API supports the "cookies" field. Also set Set-Cookie for compatibility.
        resp["cookies"] = cookies
        resp["headers"]["Set-Cookie"] = cookies[0]
    return resp


def parse_body_json(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode()
        except Exception:
            return None
    if not body:
        return {}
    try:
        return json.loads(body)
    except Exception:
        return None


def parse_cookies(event: Dict[str, Any]) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    cookie_headers: List[str] = []
    if event.get("cookies"):
        cookie_headers.extend(event["cookies"])
    headers = event.get("headers") or {}
    header_cookie = headers.get("cookie") or headers.get("Cookie")
    if header_cookie:
        cookie_headers.append(header_cookie)
    for header in cookie_headers:
        parts = header.split(";")
        for part in parts:
            if "=" in part:
                name, value = part.strip().split("=", 1)
                cookies[name] = value
    return cookies


def client_ip(event: Dict[str, Any]) -> Optional[str]:
    headers = (event.get("headers") or {})
    forwarded_for = headers.get("X-Forwarded-For") or headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    request_ctx = event.get("requestContext") or {}
    identity = request_ctx.get("identity") or {}
    return identity.get("sourceIp")


def user_agent(event: Dict[str, Any]) -> Optional[str]:
    headers = (event.get("headers") or {})
    return headers.get("User-Agent") or headers.get("user-agent")


# ---------- Crypto helpers ----------

def base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("utf-8")


def base64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def generate_refresh_token(bytes_len: int = 32) -> str:
    return base64url_encode(os.urandom(bytes_len))


def hash_refresh_token(token: str, pepper: str) -> str:
    material = (token + pepper).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def hash_password(password: str, rounds: Optional[int] = None) -> str:
    if not bcrypt:
        logger.error("bcrypt module is not available; deploy lambda with bcrypt dependency.")
        raise RuntimeError("bcrypt dependency missing")
    salt_rounds = rounds if rounds is not None else _env_int("BCRYPT_ROUNDS", 12)
    salt = bcrypt.gensalt(rounds=salt_rounds)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    if not bcrypt:
        logger.error("bcrypt module is not available; deploy lambda with bcrypt dependency.")
        raise RuntimeError("bcrypt dependency missing")
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def get_bearer_token(event: Dict[str, Any]) -> Optional[str]:
    headers = event.get("headers") or {}
    auth = headers.get("Authorization") or headers.get("authorization")
    if not auth:
        return None
    parts = auth.strip().split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def verify_access_token(token: str, secret: str) -> Optional[Dict[str, Any]]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(base64url_decode(header_b64))
        payload = json.loads(base64url_decode(payload_b64))
    except Exception:
        return None
    if header.get("alg") != "HS256":
        return None
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    try:
        provided_sig = base64url_decode(signature_b64)
    except Exception:
        return None
    if not hmac.compare_digest(provided_sig, expected_sig):
        return None
    exp = payload.get("exp")
    if isinstance(exp, (int, float)) and int(exp) <= int(time.time()):
        return None
    return payload


def create_access_token(
    payload: Dict[str, Any],
    secret: str,
    ttl_seconds: int,
) -> Dict[str, Any]:
    issued_at = int(time.time())
    exp = issued_at + ttl_seconds
    claims = dict(payload)
    claims["iat"] = issued_at
    claims["exp"] = exp
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = (
        f"{base64url_encode(json.dumps(header, separators=(',', ':')).encode())}."
        f"{base64url_encode(json.dumps(claims, separators=(',', ':')).encode())}"
    )
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    token = f"{signing_input}.{base64url_encode(signature)}"
    return {"token": token, "exp": exp}


# ---------- Cookie helpers ----------

def build_refresh_cookie(
    token: str,
    max_age: int,
    *,
    secure: bool,
    same_site: str,
    path: str = "/auth",
    domain: Optional[str] = None,
) -> str:
    parts = [f"refresh_token={token}", f"Max-Age={max_age}", f"Path={path}", "HttpOnly"]
    if domain:
        parts.append(f"Domain={domain}")
    if secure:
        parts.append("Secure")
    if same_site:
        parts.append(f"SameSite={same_site}")
    return "; ".join(parts)


def clear_refresh_cookie(*, secure: bool, same_site: str, path: str = "/auth", domain: Optional[str] = None) -> str:
    return build_refresh_cookie(
        token="",
        max_age=0,
        secure=secure,
        same_site=same_site,
        path=path,
        domain=domain,
    )


# ---------- Database helpers (psycopg2/pg8000) ----------

_NAMED_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


def _normalize_sql(sql: str) -> str:
    return _NAMED_PARAM_RE.sub(r"%(\1)s", sql)


def _get_db_connection():
    global _db_conn
    if _db_conn is not None:
        try:
            if psycopg2 is not None:
                if _db_conn.closed == 0:
                    return _db_conn
            else:
                if _db_conn is not None:
                    return _db_conn
        except Exception:
            _db_conn = None

    database_url = _env("DATABASE_URL")
    if database_url:
        if psycopg2 is not None:
            _db_conn = psycopg2.connect(database_url)
        elif pg8000 is not None:
            parsed = urlparse(database_url)
            if not parsed.hostname or not parsed.path:
                raise RuntimeError("Invalid DATABASE_URL for pg8000.")
            _db_conn = pg8000.connect(
                host=parsed.hostname,
                user=parsed.username or "",
                password=parsed.password or "",
                port=parsed.port or 5432,
                database=parsed.path.lstrip("/"),
            )
        else:
            raise RuntimeError("Missing database client (psycopg2 or pg8000).")
    else:
        host = _required_env("DB_HOST")
        user = _required_env("DB_USER")
        password = _required_env("DB_PASSWORD")
        port = int(_env("DB_PORT", "5432") or 5432)
        db_name = _required_env("DB_NAME")
        if psycopg2 is not None:
            _db_conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                port=port,
                dbname=db_name,
            )
        elif pg8000 is not None:
            _db_conn = pg8000.connect(
                host=host,
                user=user,
                password=password,
                port=port,
                database=db_name,
            )
        else:
            raise RuntimeError("Missing database client (psycopg2 or pg8000).")

    try:
        _db_conn.autocommit = True
    except Exception:
        pass
    return _db_conn


def _value_to_field(value: Any) -> Dict[str, Any]:
    if value is None:
        return {"isNull": True}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"longValue": value}
    if isinstance(value, float):
        return {"doubleValue": value}
    return {"stringValue": str(value)}


def _decode_field(field: Dict[str, Any]) -> Any:
    if "isNull" in field and field["isNull"]:
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    # Fallback for more exotic types
    if "blobValue" in field:
        return field["blobValue"]
    return None


def execute_statement(sql: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    conn = _get_db_connection()
    normalized = _normalize_sql(sql)
    params = parameters or {}
    with conn.cursor() as cursor:
        cursor.execute(normalized, params)
        if cursor.description:
            rows = cursor.fetchall()
            column_metadata = [{"name": col[0]} for col in cursor.description]
            records = [[_value_to_field(value) for value in row] for row in rows]
        else:
            column_metadata = []
            records = []
        rowcount = cursor.rowcount if cursor.rowcount is not None else 0
    return {
        "columnMetadata": column_metadata,
        "records": records,
        "numberOfRecordsUpdated": rowcount,
    }


def fetch_one(sql: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    resp = execute_statement(sql, parameters)
    records = resp.get("records") or []
    if not records:
        return None
    metadata = resp.get("columnMetadata") or []
    columns = [col["name"] for col in metadata]
    row = records[0]
    return {col: _decode_field(val) for col, val in zip(columns, row)}


# ---------- High-level config ----------

def cookie_config() -> Dict[str, Any]:
    return {
        "secure": _env_bool("COOKIE_SECURE", default=True),
        "same_site": _env("COOKIE_SAMESITE", "None"),
        "path": _env("COOKIE_PATH", "/auth"),
        "domain": _env("COOKIE_DOMAIN"),
    }


def token_config() -> Dict[str, Any]:
    return {
        "access_ttl": _env_int("ACCESS_TOKEN_TTL_SECONDS", 900),
        "refresh_ttl": _env_int("REFRESH_TOKEN_TTL_SECONDS", 60 * 60 * 24 * 30),
        "jwt_secret": _required_env("JWT_SECRET"),
        "refresh_pepper": _required_env("REFRESH_TOKEN_PEPPER"),
    }


def ok(body: Dict[str, Any], *, origin: Optional[str], cookies: Optional[List[str]] = None) -> Dict[str, Any]:
    return build_response(200, body, origin=origin, cookies=cookies)


def bad_request(message: str, *, origin: Optional[str]) -> Dict[str, Any]:
    return build_response(400, {"error": message}, origin=origin)


def unauthorized(message: str, *, origin: Optional[str]) -> Dict[str, Any]:
    return build_response(401, {"error": message}, origin=origin)


def forbidden(message: str, *, origin: Optional[str]) -> Dict[str, Any]:
    return build_response(403, {"error": message}, origin=origin)


def server_error(message: str, *, origin: Optional[str]) -> Dict[str, Any]:
    return build_response(500, {"error": message}, origin=origin)
