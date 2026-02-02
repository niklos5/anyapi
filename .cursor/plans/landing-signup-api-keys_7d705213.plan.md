---
name: landing-signup-api-keys
overview: Add a public landing page, a real signup flow backed by the existing auth DB, and per-schema API keys so users can call the ingest endpoint directly with their key.
todos: []
isProject: false
---

# Landing + Signup + API Key Flow

## What changes

- Make `/` a public marketing/landing page based on your research positioning.
- Move the app shell/dashboard to an authenticated route group (e.g. `/app`).
- Add a **signup** lambda + UI that creates `partners` + `partner_users` in the existing auth DB schema.
- Generate and store a **per‑schema API key** (plain text in schema record per your choice), and allow API calls to `/schemas/{id}/ingest` using that key.

## Backend changes

- **Auth: signup lambda**
  - Add `backend/lambdas/auth/signup/lambda_function.py` and wire it like login/refresh/logout.
  - Insert into `partners` and `partner_users` (same DB as login lambda) and return access token + refresh cookie.
  - Reuse helpers from `backend/lambdas/auth/common.py` (password hashing, cookie config, etc.).
- **API key storage per schema**
  - Extend schema record in `[backend/storage.py](C:\Users\Nikita\OneDrive - UW\Desktop\anyapi\backend\storage.py)` to include `api_key`.
  - When `POST /schemas`, generate an API key and save it with the schema record.
- **API key auth on ingest**
  - Update `/schemas/{schema_id}/ingest` in `[backend/app.py](C:\Users\Nikita\OneDrive - UW\Desktop\anyapi\backend\app.py)` to accept either:
    - JWT (existing) **or**
    - `x-api-key` header matching the schema’s stored key.
  - Add a lightweight guard: if JWT is missing and api key is invalid → 401.

## Frontend changes

- **Landing page**
  - Replace the current dashboard content in `[app/page.tsx](C:\Users\Nikita\OneDrive - UW\Desktop\anyapi\app\page.tsx)` with a public landing page (problem, use cases, value, CTA).
  - CTA buttons: “Sign up” and “Log in”.
- **Move app to `/app**`
  - Create a route group for authenticated app UI (e.g. `app/(app)/...`) and move dashboard + schemas + jobs there.
  - Ensure `AuthGate` wraps only app routes, not landing.
- **Signup UI**
  - Add `[app/signup/page.tsx](C:\Users\Nikita\OneDrive - UW\Desktop\anyapi\app\signup\page.tsx)` and call new signup lambda.
  - After signup, redirect into `/app` and show API key on schema creation.
- **Show API keys**
  - On schema detail/list pages, show the generated key once with “copy” button (no rotation in MVP).

## API key flow diagram

```mermaid
flowchart TD
  User --> Landing
  Landing --> Signup
  Signup --> AuthLambda
  User --> AppUI
  AppUI --> ApiLambda
  User -->|
```



