from __future__ import annotations

from typing import Any, Dict, Optional


def _describe_primitive(value: Any) -> str:
    """Return a stable description for primitive JSON values."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    return "unknown"


class SchemaStructureExtractor:
    """Extracts schema fingerprints for arbitrary JSON payloads."""

    def __init__(self, max_items_per_array: Optional[int] = None):
        if max_items_per_array is not None and max_items_per_array < 1:
            raise ValueError("max_items_per_array must be None or a positive integer")
        self.max_items_per_array = max_items_per_array

    def extract(self, payload: Any) -> Dict[str, str]:
        schema = self._extract_schema(payload, "$")
        return {path: schema[path] for path in sorted(schema)}

    def _extract_schema(self, obj: Any, prefix: str) -> Dict[str, str]:
        if isinstance(obj, dict):
            if not obj:
                return {prefix: "object (empty)"}
            schema: Dict[str, str] = {}
            for key, value in obj.items():
                str_key = key if isinstance(key, str) else str(key)
                new_prefix = f"{prefix}.{str_key}" if prefix else str_key
                schema.update(self._extract_schema(value, new_prefix))
            return schema

        if isinstance(obj, list):
            array_prefix = f"{prefix}[]"
            if not obj:
                return {array_prefix: "array (empty)"}

            schema: Dict[str, str] = {}
            items = obj if self.max_items_per_array is None else obj[: self.max_items_per_array]
            non_null_seen = False
            primitive_type: Optional[str] = None
            container_type: Optional[str] = None

            for value in items:
                if value is None:
                    continue
                non_null_seen = True
                if isinstance(value, dict):
                    container_type = container_type or "object"
                    schema.update(self._extract_schema(value, array_prefix))
                elif isinstance(value, list):
                    container_type = container_type or "array"
                    schema.update(self._extract_schema(value, array_prefix))
                else:
                    primitive_type = primitive_type or _describe_primitive(value)
                    schema[array_prefix] = f"array<{primitive_type}>"

            if not non_null_seen:
                schema[array_prefix] = "array<null>"
            else:
                if primitive_type is None:
                    inferred = container_type or "unknown"
                    schema[array_prefix] = f"array<{inferred}>"
            return schema

        return {prefix: _describe_primitive(obj)}
