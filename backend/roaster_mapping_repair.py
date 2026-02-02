from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set, Tuple


FEED_LEVEL_PREFIXES = (
    "$.feed_metadata",
    "$.meta",
    "$.source",
    "$.partner",
    "$.schema_version",
    "$.default_operation_type",
)


def repair_mapping_spec(
    mapping_spec_or_text: Any,
    *,
    allowed_targets: Optional[Set[str]] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    repairs: List[str] = []
    mapping_spec = _coerce_mapping_spec(mapping_spec_or_text, repairs)
    if mapping_spec is None:
        return None, repairs

    if "defaults" not in mapping_spec or not isinstance(mapping_spec.get("defaults"), dict):
        mapping_spec["defaults"] = {}
        repairs.append("Initialized missing defaults to {}")
    if "broadcast" not in mapping_spec or not isinstance(mapping_spec.get("broadcast"), dict):
        mapping_spec["broadcast"] = {}
        repairs.append("Initialized missing broadcast to {}")

    mapping_spec["defaults"], changed = _normalize_target_key_dict(mapping_spec["defaults"])
    repairs.extend(changed)
    mapping_spec["broadcast"], changed = _normalize_target_key_dict(mapping_spec["broadcast"])
    repairs.extend(changed)

    mappings = mapping_spec.get("mappings")
    if not isinstance(mappings, dict):
        return mapping_spec, repairs

    items = mappings.get("items")
    if not isinstance(items, dict):
        return mapping_spec, repairs

    items_map = items.get("map")
    if not isinstance(items_map, dict):
        return mapping_spec, repairs

    repaired_map, changed = _repair_map_block(
        items_map,
        broadcast=mapping_spec["broadcast"],
        defaults=mapping_spec["defaults"],
        allowed_targets=allowed_targets,
    )
    items["map"] = repaired_map
    repairs.extend(changed)

    if allowed_targets:
        for target in sorted(allowed_targets):
            if target not in repaired_map:
                repaired_map[target] = {"source": None}
                repairs.append(f"Added missing target '{target}' with null source")

    return mapping_spec, repairs


def _coerce_mapping_spec(mapping_spec_or_text: Any, repairs: List[str]) -> Optional[Dict[str, Any]]:
    if isinstance(mapping_spec_or_text, dict):
        return mapping_spec_or_text
    if isinstance(mapping_spec_or_text, str):
        extracted = extract_first_json_object(mapping_spec_or_text)
        if extracted is None:
            repairs.append("Failed to extract JSON object from mapping text")
            return None
        repairs.append("Extracted JSON object from mapping text wrapper")
        return extracted
    return None


def extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass

    start = stripped.find("{")
    if start == -1:
        return None

    in_string = False
    escape = False
    depth = 0
    for i in range(start, len(stripped)):
        ch = stripped[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        else:
            if ch == '"':
                in_string = True
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = stripped[start : i + 1]
                    try:
                        parsed = json.loads(candidate)
                        return parsed if isinstance(parsed, dict) else None
                    except json.JSONDecodeError:
                        return None
    return None


def _normalize_target_key(key: str) -> str:
    return key.replace("[]", "")


def _normalize_target_key_dict(d: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    repairs: List[str] = []
    out: Dict[str, Any] = {}
    for k, v in d.items():
        nk = _normalize_target_key(k) if isinstance(k, str) else k
        if nk != k:
            repairs.append(f"Normalized target key '{k}' -> '{nk}'")
        out[nk] = v
    return out, repairs


def _looks_like_jsonpath(value: str) -> bool:
    return isinstance(value, str) and value.startswith("$")


def _looks_like_expression(value: str) -> bool:
    if not isinstance(value, str):
        return False
    if not value.startswith("$"):
        return False
    return any(token in value for token in [" + ", " - ", " * ", " / ", "'", '"', "(", ")"])


def _repair_leaf_mapping(
    target_field: str,
    spec: Dict[str, Any],
    *,
    broadcast: Dict[str, Any],
    defaults: Dict[str, Any],
    in_item_context: bool,
    repairs: List[str],
) -> Optional[Dict[str, Any]]:
    normalized_target = _normalize_target_key(target_field)
    if normalized_target != target_field:
        repairs.append(f"Normalized target key '{target_field}' -> '{normalized_target}'")
        target_field = normalized_target

    sources = spec.get("source")
    if sources is None:
        return spec

    source_list: List[Any]
    if isinstance(sources, str) or sources is None:
        source_list = [sources]
    elif isinstance(sources, list):
        source_list = sources
    else:
        return spec

    cleaned_sources: List[Any] = []
    for s in source_list:
        if isinstance(s, str) and _looks_like_expression(s):
            repairs.append(f"Removed expression source for '{target_field}' (set to null)")
            continue
        cleaned_sources.append(s)

    if in_item_context:
        feed_sources = [s for s in cleaned_sources if isinstance(s, str) and s.startswith(FEED_LEVEL_PREFIXES)]
        non_feed_sources = [s for s in cleaned_sources if s not in feed_sources]
        if feed_sources:
            if target_field not in broadcast:
                broadcast[target_field] = {"source": feed_sources[0]}
                repairs.append(f"Moved feed-level source to broadcast for '{target_field}'")
            cleaned_sources = non_feed_sources

    if cleaned_sources:
        first = cleaned_sources[0]
        if isinstance(first, str) and not _looks_like_jsonpath(first):
            defaults[target_field] = first
            repairs.append(f"Moved constant source into defaults for '{target_field}'")
            cleaned_sources = []

    if not cleaned_sources:
        spec = dict(spec)
        spec["source"] = None
        return spec

    spec = dict(spec)
    spec["source"] = cleaned_sources[0] if len(cleaned_sources) == 1 else cleaned_sources
    return spec


def _repair_map_block(
    map_block: Dict[str, Any],
    *,
    broadcast: Dict[str, Any],
    defaults: Dict[str, Any],
    allowed_targets: Optional[Set[str]],
    in_item_context: bool = True,
) -> Tuple[Dict[str, Any], List[str]]:
    repairs: List[str] = []
    out: Dict[str, Any] = {}

    for target_field, spec in map_block.items():
        if not isinstance(target_field, str) or not isinstance(spec, dict):
            continue

        normalized_target = _normalize_target_key(target_field)
        if allowed_targets is not None and normalized_target not in allowed_targets:
            repairs.append(f"Dropped unknown target field '{target_field}'")
            continue

        if "path" in spec and "map" in spec and isinstance(spec.get("map"), dict):
            nested_map, nested_repairs = _repair_map_block(
                spec["map"],
                broadcast=broadcast,
                defaults=defaults,
                allowed_targets=None,
                in_item_context=True,
            )
            new_spec = dict(spec)
            new_spec["map"] = nested_map
            out[normalized_target] = new_spec
            repairs.extend(nested_repairs)
            continue

        repaired_leaf = _repair_leaf_mapping(
            normalized_target,
            spec,
            broadcast=broadcast,
            defaults=defaults,
            in_item_context=in_item_context,
            repairs=repairs,
        )
        if repaired_leaf is None:
            continue
        out[normalized_target] = repaired_leaf

    return out, repairs
