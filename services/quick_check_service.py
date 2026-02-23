import json


def generate_quick_check_overrides_core(
    actor_name: str,
    cards: list[dict[str, object]],
    *,
    deps: dict[str, object],
) -> dict[str, dict[str, str]]:
    _ollama_available = deps['ollama_available']
    _get_env = deps['get_env']
    _http_post = deps['http_post']

    if not cards or not _ollama_available():
        return {}

    prepared_cards: list[dict[str, str]] = []
    for card in cards[:5]:
        card_id = str(card.get('id') or '').strip()
        if not card_id:
            continue
        prepared_cards.append(
            {
                'id': card_id,
                'question_text': ' '.join(str(card.get('question_text') or '').split()).strip()[:240],
                'where_to_check': ' '.join(str(card.get('where_to_check') or '').split()).strip()[:120],
                'what_to_look_for': ' '.join(str(card.get('what_to_look_for') or '').split()).strip()[:220],
                'expected_output': ' '.join(str(card.get('expected_output') or '').split()).strip()[:220],
            }
        )
    if not prepared_cards:
        return {}

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    timeout_seconds = max(2.0, float(_get_env('QUICK_CHECK_OLLAMA_TIMEOUT_SECONDS', '6')))
    prompt = (
        'You are writing SOC quick-check cards for junior analysts. '
        'Return ONLY valid JSON with schema: '
        '{"items":[{"id":"...","first_step":"...","what_to_look_for":"...","expected_output":"..."}]}. '
        'Rules: '
        '1) Keep each field short and operational (<= 180 chars). '
        '2) first_step must be concrete and tool-specific (what to open, time window, and filter focus). '
        '3) what_to_look_for must list the exact signal pattern. '
        '4) expected_output must specify what to record and include confidence shift. '
        '5) Use only ids provided in input. '
        f'Actor: {actor_name}. '
        f'Cards: {json.dumps(prepared_cards)}.'
    )

    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
    }
    try:
        response = _http_post(f'{base_url}/api/generate', json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        content = response.json().get('response', '{}')
        parsed = json.loads(content)
    except Exception:
        return {}

    if not isinstance(parsed, dict):
        return {}
    raw_items = parsed.get('items')
    if not isinstance(raw_items, list):
        return {}

    allowed_ids = {item['id'] for item in prepared_cards}
    results: dict[str, dict[str, str]] = {}
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        card_id = str(raw.get('id') or '').strip()
        if not card_id or card_id not in allowed_ids or card_id in results:
            continue
        first_step = ' '.join(str(raw.get('first_step') or '').split()).strip()[:220]
        what_to_look_for = ' '.join(str(raw.get('what_to_look_for') or '').split()).strip()[:220]
        expected_output = ' '.join(str(raw.get('expected_output') or '').split()).strip()[:220]
        if not first_step:
            continue
        results[card_id] = {
            'first_step': first_step,
            'what_to_look_for': what_to_look_for,
            'expected_output': expected_output,
        }
    return results


def replace_quick_check_overrides_core(
    connection,
    *,
    actor_id: str,
    overrides: dict[str, dict[str, str]],
    generated_at: str,
) -> None:
    connection.execute(
        'DELETE FROM quick_check_overrides WHERE actor_id = ?',
        (actor_id,),
    )
    for thread_id, item in overrides.items():
        connection.execute(
            '''
            INSERT INTO quick_check_overrides (
                actor_id, thread_id, first_step, what_to_look_for, expected_output, generated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                actor_id,
                thread_id,
                str(item.get('first_step') or ''),
                str(item.get('what_to_look_for') or ''),
                str(item.get('expected_output') or ''),
                generated_at,
            ),
        )


def load_quick_check_overrides_core(connection, *, actor_id: str) -> dict[str, dict[str, str]]:
    rows = connection.execute(
        '''
        SELECT thread_id, first_step, what_to_look_for, expected_output
        FROM quick_check_overrides
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchall()
    results: dict[str, dict[str, str]] = {}
    for row in rows:
        thread_id = str(row[0] or '').strip()
        if not thread_id:
            continue
        results[thread_id] = {
            'first_step': str(row[1] or '').strip(),
            'what_to_look_for': str(row[2] or '').strip(),
            'expected_output': str(row[3] or '').strip(),
        }
    return results
