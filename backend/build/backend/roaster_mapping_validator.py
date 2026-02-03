from __future__ import annotations

from typing import Any, Dict, List


FEED_LEVEL_PREFIXES = (
    "$.feed_metadata",
    "$.meta",
    "$.source",
    "$.partner",
    "$.schema_version",
    "$.default_operation_type",
)


def validate_mapping_spec(mapping_spec: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(mapping_spec, dict):
        return ["mapping_spec must be a JSON object"]

    mappings = mapping_spec.get("mappings")
    if not isinstance(mappings, dict):
        errors.append("mapping_spec.mappings must be an object")
        return errors

    items = mappings.get("items")
    if not isinstance(items, dict):
        errors.append("mapping_spec.mappings.items must be an object")
        return errors

    items_path = items.get("path")
    if not _is_array_path(items_path):
        errors.append("mappings.items.path must be a JSONPath array (e.g., $.items[])")

    items_map = items.get("map")
    if not isinstance(items_map, dict):
        errors.append("mappings.items.map must be an object")
        return errors

    errors.extend(_validate_map_block(items_map, "mappings.items.map", in_item_context=True))

    for section in ("broadcast", "defaults"):
        section_obj = mapping_spec.get(section)
        if section_obj is None:
            continue
        if not isinstance(section_obj, dict):
            errors.append(f"mapping_spec.{section} must be an object")
            continue
        for target_field in section_obj.keys():
            if _target_has_illegal_tokens(target_field):
                errors.append(f"{section} target '{target_field}' must not contain '$' or '[]'")

    return errors


def _validate_map_block(map_block: Dict[str, Any], context: str, in_item_context: bool) -> List[str]:
    errors: List[str] = []
    for target_field, spec in map_block.items():
        if _target_has_illegal_tokens(target_field):
            errors.append(f"{context} target '{target_field}' must not contain '$' or '[]'")

        if not isinstance(spec, dict):
            errors.append(f"{context}.{target_field} must be an object")
            continue

        if "path" in spec and "map" in spec:
            if not _is_array_path(spec.get("path")):
                errors.append(f"{context}.{target_field}.path must be a JSONPath array")
            nested_map = spec.get("map")
            if not isinstance(nested_map, dict):
                errors.append(f"{context}.{target_field}.map must be an object")
            else:
                errors.extend(_validate_map_block(nested_map, f"{context}.{target_field}.map", True))
            continue

        sources = spec.get("source")
        if sources is None:
            continue
        if isinstance(sources, str):
            sources = [sources]
        if not isinstance(sources, list):
            errors.append(f"{context}.{target_field}.source must be a string or list")
            continue

        if in_item_context:
            for source in sources:
                if isinstance(source, str) and source.startswith(FEED_LEVEL_PREFIXES):
                    errors.append(
                        f"{context}.{target_field}.source references feed metadata; use broadcast/defaults"
                    )

    return errors


def _is_array_path(path: Any) -> bool:
    if not isinstance(path, str):
        return False
    return path.endswith("[]") or path.endswith("[*]")


def _target_has_illegal_tokens(target_field: Any) -> bool:
    if not isinstance(target_field, str):
        return True
    return "[]" in target_field or "$" in target_field
