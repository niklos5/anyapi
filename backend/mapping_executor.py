from __future__ import annotations

import copy
from typing import Any, Dict, Iterable, List, Optional, Sequence


class MappingExecutor:
    """Executes mapping specifications against incoming payloads."""

    def __init__(self, mapping_spec: Dict[str, Any], target_paths: Optional[Sequence[str]] = None):
        if not isinstance(mapping_spec, dict):
            raise TypeError("mapping_spec must be a dict")
        self.mapping_spec = mapping_spec
        self._target_paths = [p for p in (target_paths or []) if isinstance(p, str)]

    def execute(self, payload: Any) -> Dict[str, Any]:
        mappings = self.mapping_spec.get("mappings")
        if not isinstance(mappings, list):
            raise ValueError("mapping_spec.mappings must be a list")

        output: Dict[str, Any] = {}
        defaults = self.mapping_spec.get("defaults") or {}

        for entry in mappings:
            if not isinstance(entry, dict):
                continue
            target = entry.get("target")
            if not isinstance(target, str) or not target.strip():
                continue
            value = self._evaluate_field(payload, entry)
            if value is None and "default" in entry:
                value = entry.get("default")
            if value is None and entry.get("required"):
                value = None
            if value is None:
                continue
            self._assign_nested(output, target, value)

        for key, value in defaults.items():
            if self._get_nested(output, key) is None:
                self._assign_nested(output, key, _clone_value(value))

        self._ensure_target_fields(output)
        return output

    def _evaluate_field(self, source: Any, spec: Dict[str, Any]) -> Any:
        sources = spec.get("source")
        transform = spec.get("transform")
        match_map = spec.get("match")
        if sources is None:
            return None
        if isinstance(sources, str):
            sources = [sources]
        if not isinstance(sources, list):
            raise TypeError("source must be a string or list of strings")

        value = None
        for path in sources:
            values = self._evaluate_path(source, path)
            non_null = [v for v in values if v is not None]
            if not non_null:
                continue
            value = non_null if len(non_null) > 1 else non_null[0]
            break

        if value is None:
            return None

        if transform:
            value = self._apply_transform(value, transform)

        if match_map:
            value = self._apply_match(value, match_map)

        return value

    def _apply_transform(self, value: Any, transform: str) -> Any:
        if transform == "ensure_array":
            if isinstance(value, list):
                return value
            return [] if value is None else [value]

        def _convert(val: Any) -> Any:
            if val is None:
                return None
            try:
                if transform in {"number", "to_float"}:
                    return float(val)
                if transform in {"integer", "to_int"}:
                    return int(val)
                if transform in {"string", "to_string"}:
                    return str(val)
                if transform in {"boolean", "to_boolean"}:
                    if isinstance(val, bool):
                        return val
                    if isinstance(val, str):
                        lowered = val.strip().lower()
                        if lowered in {"true", "1", "yes", "y"}:
                            return True
                        if lowered in {"false", "0", "no", "n"}:
                            return False
                        return None
                    return bool(val)
                if transform == "date":
                    return str(val)
            except Exception:
                return None
            return val

        if isinstance(value, list):
            return [_convert(v) for v in value]
        return _convert(value)

    def _apply_match(self, value: Any, match_map: Dict[str, Any]) -> Any:
        default = match_map.get("default")

        def _map(val: Any) -> Any:
            if val is None:
                return default
            key = str(val)
            return match_map.get(key, default)

        if isinstance(value, list):
            return [_map(v) for v in value]
        return _map(value)

    def _evaluate_path(self, root: Any, path: Optional[str]) -> List[Any]:
        if path is None:
            return []
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:]
        if not path:
            return [root]

        tokens = path.split(".")
        current: List[Any] = [root]
        for token in tokens:
            next_values: List[Any] = []
            is_array = token.endswith("[]") or token.endswith("[*]")
            key = token
            if token.endswith("[]"):
                key = token[:-2]
            elif token.endswith("[*]"):
                key = token[:-3]

            for value in current:
                if isinstance(value, dict):
                    candidate = value.get(key)
                elif isinstance(value, list):
                    candidate = value
                else:
                    candidate = None

                if candidate is None:
                    continue

                if is_array:
                    if isinstance(candidate, list):
                        next_values.extend(candidate)
                else:
                    next_values.append(candidate)

            current = next_values

        return current

    def _assign_nested(self, target: Dict[str, Any], dotted_path: str, value: Any) -> None:
        parts = dotted_path.split(".")
        cursor = target
        for part in parts[:-1]:
            if part not in cursor or not isinstance(cursor[part], dict):
                cursor[part] = {}
            cursor = cursor[part]
        cursor[parts[-1]] = value

    def _get_nested(self, data: Dict[str, Any], dotted_path: str) -> Any:
        parts = dotted_path.split(".")
        cursor: Any = data
        for part in parts:
            if not isinstance(cursor, dict) or part not in cursor:
                return None
            cursor = cursor[part]
        return cursor

    def _ensure_target_fields(self, target: Dict[str, Any]) -> None:
        for path in self._target_paths:
            if not path:
                continue
            if self._get_nested(target, path) is None:
                self._assign_nested(target, path, None)


def _clone_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    return value


def flatten_target_schema(schema: Any, prefix: str = "$") -> Dict[str, Any]:
    if isinstance(schema, dict):
        if not schema:
            return {prefix: "object"}
        flattened: Dict[str, Any] = {}
        for key, value in schema.items():
            str_key = key if isinstance(key, str) else str(key)
            new_prefix = f"{prefix}.{str_key}" if prefix else str_key
            flattened.update(flatten_target_schema(value, new_prefix))
        return flattened

    if isinstance(schema, list):
        array_prefix = f"{prefix}[]"
        if not schema:
            return {array_prefix: "array"}
        return flatten_target_schema(schema[0], array_prefix)

    return {prefix: schema}
