# AnyApi MVP

React + Next.js MVP for the AnyApi ingestion flow, backed by a lightweight
Python service adapted from the roaster mapping engine.

## Running locally

### Backend (FastAPI)

```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --reload --port 8080
```

### Frontend (Next.js)

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

### Environment

The frontend expects the backend on `http://localhost:8080` by default. If
needed, set:

```
NEXT_PUBLIC_BACKEND_URL=http://localhost:8080
```

## Auth (AWS Lambda)

Auth is implemented as AWS Lambda handlers using the `login/logout/refresh`
templates. The handlers live under `backend/lambdas/auth/` and are intended to
be deployed behind API Gateway.

Required env vars (auth lambdas):

- `JWT_SECRET`
- `REFRESH_TOKEN_PEPPER`
- `ACCESS_TOKEN_TTL_SECONDS` (default 900)
- `REFRESH_TOKEN_TTL_SECONDS` (default 30 days)
- `DATABASE_URL` or `DB_HOST`/`DB_USER`/`DB_PASSWORD`/`DB_NAME`/`DB_PORT`
- `ALLOWED_ORIGINS` (comma-separated)
- `COOKIE_SECURE`, `COOKIE_SAMESITE`, `COOKIE_DOMAIN`, `COOKIE_PATH`

Frontend auth configuration:

```
NEXT_PUBLIC_AUTH_URL=https://your-api-gateway-domain/auth
```

## Backend auth

The FastAPI service validates the bearer token using `JWT_SECRET` and scopes all
job data by `partner_id` from the token.

## Mapping config

Mapping specs are user-defined JSON with `mappings` entries that map source
paths to target fields. See `backend/README.md` for the full shape and example.
