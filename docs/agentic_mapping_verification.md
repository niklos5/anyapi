# Agentic Mapping Verification Notes

## Automated Tests
- Location: `backend/tests/test_mapping_agent.py`
- Run (from repo root):
  - `python -m unittest backend.tests.test_mapping_agent`

## Staging Validation Steps
- Load sample payloads and schemas from `backend/validation_samples/`.
- For each case, run ingestion with `mappingAgent` enabled via:
  - `POST /schemas/{schema_id}/ingest` or `POST /jobs`
  - Include `mappingAgent: { "enabled": true, "maxIterations": 3 }`
- Verify:
  - Iteration stops when issues clear or `maxIterations` reached.
  - Mapping respects schema fields and does not introduce unknown targets.
  - Results include non-null values for fields when sources exist.
  - Bedrock-unavailable scenarios fall back without failing the request.

## Observability Checklist
- Confirm logs include:
  - Mapping agent start + max iteration count
  - Per-iteration issue summary
  - Stop reason (converged, missing model, empty response, no improvement, max iterations)
- For Lambda path, confirm mapping agent logs appear in CloudWatch.

## Acceptance Summary (Current Status)
- Iteration stop conditions: implemented, not executed in staging.
- Schema constraint enforcement: guarded via validation + repair, not executed in staging.
- Bedrock-unavailable fallback: implemented, covered by unit test.
- Determinism for identical inputs: expected, not benchmarked.

## Staging Run Notes (Fill In)
- Environment:
- Date/time:
- Payload cases executed:
- Iteration counts observed:
- Issues resolved:
- Output validation summary:
- Follow-up actions:
