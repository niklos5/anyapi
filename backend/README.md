# AnyApi Roaster Service

Lightweight HTTP wrapper around the roaster mapping engine, adapted for
any-schema to any-schema transformations.

## Endpoints

- `POST /analyze` – Analyze incoming data and return schema + preview.
- `POST /jobs` – Create an ingestion job and run mapping immediately.
- `GET /jobs` – List jobs.
- `GET /jobs/{job_id}` – Get job status.
- `GET /jobs/{job_id}/results` – Fetch mapped results.

## Mapping spec (JSON)

```json
{
  "targetSchema": {
    "external_id": "string",
    "customer_name": "string",
    "order_total": "number"
  },
  "mappings": [
    { "source": "order_id", "target": "external_id", "transform": "string" },
    { "source": "customer.name", "target": "customer_name" },
    { "source": "total", "target": "order_total", "transform": "number" }
  ],
  "defaults": {
    "currency": "USD"
  }
}
```

## Local dev

```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --reload --port 8080
```

## Auth

All endpoints require `Authorization: Bearer <token>` and validate using
`JWT_SECRET`. Tokens are issued by the AWS Lambda auth handlers under
`backend/lambdas/auth/`.

## Lambda (zip deploy)

Use Python 3.12 runtime and set the handler to `lambda_handler.handler`.

### Build zip (PowerShell)

```powershell
cd backend
Remove-Item -Recurse -Force build 2>$null
New-Item -ItemType Directory -Force build | Out-Null
python -m pip install -r requirements.txt -t build
Copy-Item *.py -Destination build
Copy-Item -Recurse lambdas -Destination build\lambdas
Compress-Archive -Path build\* -DestinationPath lambda.zip -Force
```

Upload `backend/lambda.zip` to Lambda and configure an HTTP API Gateway
integration with Lambda proxy enabled.
