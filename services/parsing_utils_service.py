import json
import re
from datetime import datetime, timezone


def extract_ttp_ids_core(
    text: str,
    *,
    mitre_valid_technique_ids,
) -> list[str]:
    matches = re.findall(r'\bT\d{4}(?:\.\d{3})?\b', text, flags=re.IGNORECASE)
    valid_ids = mitre_valid_technique_ids()
    deduped: list[str] = []
    for value in matches:
        norm = value.upper()
        if valid_ids and norm not in valid_ids:
            continue
        if norm not in deduped:
            deduped.append(norm)
    return deduped


def safe_json_string_list_core(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            return []
        result: list[str] = []
        for item in parsed:
            if isinstance(item, str):
                result.append(item)
        return result
    except Exception:
        return []


def parse_iso_for_sort_core(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)
