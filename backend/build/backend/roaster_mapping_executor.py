from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Sequence


class MappingExecutor:
    """Executes mapping specifications against partner payloads."""

    def __init__(self, mapping_spec: Dict[str, Any], canonical_schema_paths: Optional[Sequence[str]] = None):
        if not isinstance(mapping_spec, dict):
            raise TypeError("mapping_spec must be a dict")
        self.mapping_spec = mapping_spec
        self._canonical_paths = [
            _normalize_canonical_path(p) for p in (canonical_schema_paths or []) if p
        ]

    def execute(self, payload: Any) -> Dict[str, Any]:
        """Execute the mapping spec and return canonical items."""
        mappings = self.mapping_spec.get("mappings")
        if not isinstance(mappings, dict):
            raise ValueError("mapping_spec.mappings must be a dict")

        broadcast_values = self._compute_broadcast(payload)
        defaults = self.mapping_spec.get("defaults") or {}

        items_spec = mappings.get("items")
        if not isinstance(items_spec, dict):
            raise ValueError("mapping_spec.mappings.items must be a dict")

        root_items = self._evaluate_path(payload, items_spec.get("path"))
        mapped_items = []
        for item in root_items:
            mapped_item: Dict[str, Any] = {}
            self._apply_broadcast(mapped_item, broadcast_values)
            self._apply_map_block(item, items_spec.get("map", {}), mapped_item)
            self._apply_defaults(mapped_item, defaults)
            self._ensure_canonical_fields(mapped_item)
            mapped_items.append(mapped_item)

        result = {"items": mapped_items}
        partner_id = self.mapping_spec.get("partner_id")
        if partner_id:
            result["partner_id"] = partner_id
        return result

    def _compute_broadcast(self, payload: Any) -> Dict[str, Any]:
        broadcast_spec = self.mapping_spec.get("broadcast") or {}
        results: Dict[str, Any] = {}
        for target_field, spec in broadcast_spec.items():
            if not isinstance(spec, dict):
                continue
            value = self._evaluate_field(payload, spec)
            if value is not None:
                self._assign_nested(results, target_field, value)
        return results

    def _apply_broadcast(self, target: Dict[str, Any], broadcast_values: Dict[str, Any]) -> None:
        for key, value in broadcast_values.items():
            self._assign_nested(target, key, _clone_value(value))

    def _apply_defaults(self, target: Dict[str, Any], defaults: Dict[str, Any]) -> None:
        for key, value in defaults.items():
            current = self._get_nested(target, key)
            if current is None:
                self._assign_nested(target, key, _clone_value(value))

    def _apply_map_block(self, source: Any, map_block: Dict[str, Any], target: Dict[str, Any]) -> None:
        for target_field, spec in map_block.items():
            if not isinstance(spec, dict):
                continue
            if "path" in spec and "map" in spec:
                elements = self._evaluate_path(source, spec["path"])
                nested_results = []
                for element in elements:
                    nested_item: Dict[str, Any] = {}
                    self._apply_map_block(element, spec.get("map", {}), nested_item)
                    nested_results.append(nested_item)
                self._assign_nested(target, target_field, nested_results)
                continue

            value = self._evaluate_field(source, spec)
            required = spec.get("required", False)
            if value is None and not required:
                continue
            self._assign_nested(target, target_field, value)

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
                if transform == "to_float":
                    return float(val)
                if transform == "to_int":
                    return int(val)
                if transform == "to_string":
                    return str(val)
                if transform == "to_boolean":
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

    def _ensure_canonical_fields(self, target: Dict[str, Any]) -> None:
        for path in self._canonical_paths:
            if not path:
                continue
            parts = path.split(".")
            if _path_conflicts_with_list(target, parts):
                continue
            if self._get_nested(target, path) is None:
                self._assign_nested(target, path, None)


def _normalize_canonical_path(path: str) -> str:
    if not isinstance(path, str):
        return ""
    normalized = path
    if normalized.startswith("$."):
        normalized = normalized[2:]
    elif normalized.startswith("$"):
        normalized = normalized[1:]
    if normalized.startswith("items[]."):
        normalized = normalized[len("items[].") :]
    elif normalized.startswith("items."):
        normalized = normalized[len("items.") :]
    normalized = normalized.replace("[*]", "")
    normalized = normalized.replace("[]", "")
    return normalized


def _path_conflicts_with_list(target: Dict[str, Any], parts: List[str]) -> bool:
    cursor: Any = target
    for part in parts:
        if isinstance(cursor, list):
            return True
        if not isinstance(cursor, dict):
            return False
        cursor = cursor.get(part)
    return isinstance(cursor, list)


def _clone_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    return value
