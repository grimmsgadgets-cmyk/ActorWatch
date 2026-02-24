import json

from services.llm_schema_service import parse_ollama_json_object
from services.prompt_templates import with_template_header


def generate_ioc_hunt_queries_core(
    actor_name: str,
    cards: list[dict[str, object]],
    environment_profile: dict[str, object] | None = None,
    *,
    deps: dict[str, object],
) -> dict[str, object]:
    _ollama_available = deps['ollama_available']
    _get_env = deps['get_env']
    _http_post = deps['http_post']
    _personalize_query = deps.get('personalize_query', lambda query, **_kwargs: query)

    prepared_cards: list[dict[str, object]] = []
    card_iocs: dict[str, set[str]] = {}
    card_evidence_ids: dict[str, set[str]] = {}

    for raw_card in cards:
        card_id = str(raw_card.get('id') or '').strip()
        if not card_id:
            continue
        iocs_raw = raw_card.get('related_iocs')
        evidence_raw = raw_card.get('evidence')
        iocs = iocs_raw if isinstance(iocs_raw, list) else []
        evidence = evidence_raw if isinstance(evidence_raw, list) else []
        clean_iocs: list[dict[str, str]] = []
        clean_evidence: list[dict[str, str]] = []
        for ioc in iocs:
            if not isinstance(ioc, dict):
                continue
            ioc_type = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type or not ioc_value:
                continue
            clean_iocs.append({'ioc_type': ioc_type, 'ioc_value': ioc_value})
        for evidence_item in evidence:
            if not isinstance(evidence_item, dict):
                continue
            evidence_id = str(evidence_item.get('id') or '').strip()
            source_url = str(evidence_item.get('source_url') or '').strip()
            if not evidence_id or not source_url:
                continue
            clean_evidence.append(
                {
                    'id': evidence_id,
                    'source_url': source_url,
                    'source_title': str(evidence_item.get('source_title') or '').strip(),
                    'source_date': str(evidence_item.get('source_date') or '').strip(),
                    'excerpt': str(evidence_item.get('excerpt') or '').strip()[:320],
                }
            )

        if not clean_iocs or not clean_evidence:
            continue

        card_iocs[card_id] = {item['ioc_value'].lower() for item in clean_iocs}
        card_evidence_ids[card_id] = {item['id'] for item in clean_evidence}
        prepared_cards.append(
            {
                'id': card_id,
                'title': str(raw_card.get('quick_check_title') or raw_card.get('question_text') or '').strip()[:220],
                'iocs': clean_iocs[:8],
                'evidence': clean_evidence[:10],
            }
        )

    if not prepared_cards:
        return {
            'available': False,
            'reason': 'No cards with both IOC context and evidence were available.',
            'items_by_card': {},
        }
    if not _ollama_available():
        return {
            'available': False,
            'reason': 'Ollama is unavailable, so evidence-backed hunt queries could not be generated.',
            'items_by_card': {},
        }

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    timeout_seconds = max(4.0, float(_get_env('IOC_HUNT_OLLAMA_TIMEOUT_SECONDS', '12')))
    prompt = with_template_header(
        'You are generating SOC hunt queries from concrete evidence only. '
        'Return ONLY JSON with schema '
        '{"items":[{"card_id":"...","platform":"...","ioc_value":"...","query":"...",'
        '"why_this_query":"...","evidence_source_ids":["..."]}]}. '
        'Rules: '
        '1) Use ONLY IOC values provided for each card. '
        '2) Use ONLY evidence_source_ids provided for that card. '
        '3) Query MUST include the IOC value verbatim. '
        '4) If evidence is not enough for a useful query, omit that item. '
        '5) Never invent telemetry fields not implied by evidence text. '
        f'Actor: {actor_name}. '
        f'Cards: {json.dumps(prepared_cards)}.'
    )

    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
    }
    parsed: dict[str, object] | None = None
    try:
        response = _http_post(f'{base_url}/api/generate', json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        loaded = parse_ollama_json_object(response.json())
        if isinstance(loaded, dict):
            parsed = loaded
    except Exception:
        parsed = None

    if parsed is None:
        # Retry with a reduced payload in case context size caused model failure.
        retry_payload = {
            'model': model,
            'prompt': (
                'Generate IOC hunt queries from validated evidence only. Return JSON with "items". '
                f'Actor: {actor_name}. Cards: {json.dumps(prepared_cards[:2])}.'
            ),
            'stream': False,
            'format': 'json',
        }
        try:
            response = _http_post(f'{base_url}/api/generate', json=retry_payload, timeout=max(timeout_seconds, 20.0))
            response.raise_for_status()
            loaded = parse_ollama_json_object(response.json())
            if isinstance(loaded, dict):
                parsed = loaded
        except Exception:
            return {
                'available': False,
                'reason': 'Model call failed while generating IOC hunt queries.',
                'items_by_card': {},
            }

    raw_items = parsed.get('items') if isinstance(parsed, dict) else None
    if not isinstance(raw_items, list):
        return {
            'available': True,
            'reason': 'Model returned no valid hunt-query payload.',
            'items_by_card': {},
        }

    items_by_card: dict[str, list[dict[str, object]]] = {}
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        card_id = str(item.get('card_id') or '').strip()
        if not card_id or card_id not in card_iocs:
            continue
        ioc_value = str(item.get('ioc_value') or '').strip()
        query = ' '.join(str(item.get('query') or '').split()).strip()[:500]
        platform = ' '.join(str(item.get('platform') or '').split()).strip()[:80]
        why_this_query = ' '.join(str(item.get('why_this_query') or '').split()).strip()[:220]
        evidence_ids_raw = item.get('evidence_source_ids')
        evidence_ids = []
        if isinstance(evidence_ids_raw, list):
            evidence_ids = [str(value).strip() for value in evidence_ids_raw if str(value).strip()]

        if not ioc_value or not query or ioc_value.lower() not in card_iocs[card_id]:
            continue
        if ioc_value.lower() not in query.lower():
            continue
        if not evidence_ids:
            continue
        if any(evidence_id not in card_evidence_ids[card_id] for evidence_id in evidence_ids):
            continue

        personalized_query = _personalize_query(
            query,
            ioc_value=ioc_value,
            profile=environment_profile or {},
        )

        items_by_card.setdefault(card_id, []).append(
            {
                'platform': platform or 'SIEM',
                'ioc_value': ioc_value,
                'query': personalized_query,
                'why_this_query': why_this_query,
                'evidence_source_ids': evidence_ids,
            }
        )

    for card_id in list(items_by_card.keys()):
        items_by_card[card_id] = items_by_card[card_id][:5]

    return {
        'available': True,
        'reason': '',
        'items_by_card': items_by_card,
    }
