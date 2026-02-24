import json


PROMPT_SCHEMA_VERSION = '2026-02-24'


def parse_ollama_json_object(raw_response: object) -> dict[str, object] | None:
    if not isinstance(raw_response, dict):
        return None
    content = raw_response.get('response')
    if not isinstance(content, str):
        return None
    try:
        parsed = json.loads(content)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def normalize_string_list(value: object, *, max_items: int, max_len: int) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = ' '.join(item.split()).strip()
        if not normalized:
            continue
        output.append(normalized[:max_len])
        if len(output) >= max_items:
            break
    return output
