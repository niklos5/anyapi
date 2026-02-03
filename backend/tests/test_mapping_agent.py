import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import backend.app as app_module
from backend import mapping_service


def _build_roaster_mapping(source_id, source_name):
    return {
        "version": "1.0",
        "defaults": {},
        "broadcast": {},
        "mappings": {
            "items": {
                "path": "$.items[]",
                "map": {
                    "items.id": {"source": source_id},
                    "items.name": {"source": source_name},
                },
            }
        },
    }


class MappingAgentUnitTests(unittest.TestCase):
    def setUp(self):
        self.payload = {"items": [{"id": "123", "name": "Widget"}]}
        self.target_schema = {"items": [{"id": "string", "name": "string"}]}

    def test_agent_refines_mapping_until_issues_resolved(self):
        base_mapping = _build_roaster_mapping(None, None)
        improved_mapping = _build_roaster_mapping("$.id", "$.name")

        with patch.object(mapping_service, "_bedrock_model_id", return_value="model"), patch.object(
            mapping_service, "_invoke_bedrock", return_value=json.dumps(improved_mapping)
        ) as invoke_mock:
            result = mapping_service._prepare_roaster_mapping(
                base_mapping,
                self.payload,
                self.target_schema,
                mapping_agent={"enabled": True, "maxIterations": 2},
            )

        self.assertTrue(invoke_mock.called)
        result_map = result["mappings"]["items"]["map"]
        self.assertEqual(result_map["items.id"]["source"], "$.id")
        self.assertEqual(result_map["items.name"]["source"], "$.name")

    def test_agent_stops_when_bedrock_unavailable(self):
        base_mapping = _build_roaster_mapping(None, None)

        with patch.object(mapping_service, "_bedrock_model_id", return_value=None), patch.object(
            mapping_service, "_invoke_bedrock"
        ) as invoke_mock:
            result = mapping_service._prepare_roaster_mapping(
                base_mapping,
                self.payload,
                self.target_schema,
                mapping_agent={"enabled": True, "maxIterations": 2},
            )

        self.assertFalse(invoke_mock.called)
        result_map = result["mappings"]["items"]["map"]
        self.assertIsNone(result_map["items.id"]["source"])

    def test_agent_respects_max_iterations(self):
        base_mapping = _build_roaster_mapping(None, None)

        with patch.object(mapping_service, "_bedrock_model_id", return_value="model"), patch.object(
            mapping_service, "_invoke_bedrock", return_value=json.dumps(base_mapping)
        ) as invoke_mock:
            result = mapping_service._prepare_roaster_mapping(
                base_mapping,
                self.payload,
                self.target_schema,
                mapping_agent={"enabled": True, "maxIterations": 1},
            )

        self.assertEqual(invoke_mock.call_count, 1)
        result_map = result["mappings"]["items"]["map"]
        self.assertIsNone(result_map["items.id"]["source"])


class MappingAgentEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app_module.app)
        self.payload = {"items": [{"id": "123", "name": "Widget"}]}
        self.target_schema = {"items": [{"id": "string", "name": "string"}]}

    def test_ingest_endpoint_passes_mapping_agent(self):
        captured = {}
        schema = SimpleNamespace(
            id="schema_1",
            partner_id="partner_1",
            schema_definition=self.target_schema,
            default_mapping=None,
        )

        def fake_prepare(mapping_spec, payload, target_schema, *, mapping_agent=None):
            captured["mapping_agent"] = mapping_agent
            return _build_roaster_mapping("$.id", "$.name")

        with patch.object(app_module, "_prepare_roaster_mapping", side_effect=fake_prepare), patch.object(
            app_module, "get_schema_by_api_key", return_value=schema
        ), patch.object(app_module, "create_job", return_value=SimpleNamespace(
            id="job_1", name="job", source_type="api", created_at="now"
        )), patch.object(app_module, "update_job", return_value=None):
            response = self.client.post(
                "/schemas/schema_1/ingest",
                headers={"x-api-key": "key"},
                json={
                    "data": self.payload,
                    "mappingAgent": {"enabled": True, "maxIterations": 2},
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["mapping_agent"]["enabled"], True)

    def test_jobs_endpoint_passes_mapping_agent(self):
        captured = {}
        schema = SimpleNamespace(
            id="schema_1",
            partner_id="partner_1",
            schema_definition=self.target_schema,
        )

        def fake_prepare(mapping_spec, payload, target_schema, *, mapping_agent=None):
            captured["mapping_agent"] = mapping_agent
            return _build_roaster_mapping("$.id", "$.name")

        with patch.object(app_module, "_prepare_roaster_mapping", side_effect=fake_prepare), patch.object(
            app_module, "get_schema_by_api_key", return_value=schema
        ), patch.object(app_module, "create_job", return_value=SimpleNamespace(
            id="job_1", name="job", source_type="api", created_at="now"
        )), patch.object(app_module, "update_job", return_value=None):
            response = self.client.post(
                "/jobs",
                headers={"x-api-key": "key"},
                json={
                    "name": "job",
                    "sourceType": "api",
                    "data": self.payload,
                    "mapping": {"targetSchema": self.target_schema, "mappings": []},
                    "mappingAgent": {"enabled": True, "maxIterations": 2},
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["mapping_agent"]["enabled"], True)
