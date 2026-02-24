import json
from datetime import datetime, timezone

from services.llm_schema_service import normalize_string_list, parse_ollama_json_object
from services.prompt_templates import with_template_header


def sentence_mentions_actor_core(sentence: str, actor_name: str, *, deps: dict[str, object]) -> bool:
    _re_findall = deps['re_findall']

    lowered = sentence.lower()
    actor_tokens = [token for token in _re_findall(r'[a-z0-9]+', actor_name.lower()) if len(token) > 2]
    return bool(actor_tokens and any(token in lowered for token in actor_tokens))


def looks_like_navigation_noise_core(sentence: str) -> bool:
    lowered = sentence.lower()
    noise_markers = (
        'contact sales',
        'get started for free',
        'solutions & technology',
        'inside google cloud',
        'developers & practitioners',
        'training & certifications',
        'ecosystem it leaders',
    )
    if any(marker in lowered for marker in noise_markers):
        return True
    if lowered.count('&') >= 4:
        return True
    if len(sentence.split()) > 70:
        return True
    return False


def build_actor_profile_summary_core(
    actor_name: str,
    source_texts: list[str],
    *,
    deps: dict[str, object],
) -> str:
    _split_sentences = deps['split_sentences']
    _looks_like_navigation_noise = deps['looks_like_navigation_noise']
    _sentence_mentions_actor = deps['sentence_mentions_actor']
    _normalize_text = deps['normalize_text']
    _token_overlap = deps['token_overlap']

    candidate_sentences: list[str] = []
    for text in source_texts:
        for sentence in _split_sentences(text):
            if _looks_like_navigation_noise(sentence):
                continue
            if _sentence_mentions_actor(sentence, actor_name):
                candidate_sentences.append(' '.join(sentence.split()))
            if len(candidate_sentences) >= 24:
                break
        if len(candidate_sentences) >= 24:
            break

    selected: list[str] = []
    for sentence in candidate_sentences:
        normalized = _normalize_text(sentence)
        if any(_token_overlap(normalized, _normalize_text(existing)) >= 0.7 for existing in selected):
            continue
        selected.append(sentence)
        if len(selected) >= 3:
            break

    if selected:
        return ' '.join(selected)
    return (
        f'No actor-specific executive summary is available for {actor_name} yet. '
        'Current sources do not provide clear, attributable details about this actor. '
        'Add a source that explicitly profiles this actor and refresh the notebook.'
    )


def ollama_generate_questions_core(
    actor_name: str,
    scope_statement: str | None,
    excerpts: list[str],
    *,
    deps: dict[str, object],
) -> list[str]:
    _ollama_available = deps['ollama_available']
    _get_env = deps['get_env']
    _http_post = deps['http_post']
    _sanitize_question_text = deps['sanitize_question_text']

    def _fallback_questions() -> list[str]:
        fallback: list[str] = []
        for excerpt in excerpts[:6]:
            cleaned = ' '.join(str(excerpt).split()).strip()
            if not cleaned:
                continue
            candidate = _sanitize_question_text(f'What should we verify next based on: {cleaned[:120]}?')
            if candidate and candidate not in fallback:
                fallback.append(candidate)
            if len(fallback) >= 4:
                break
        return fallback

    if not excerpts or not _ollama_available():
        return _fallback_questions()

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    prompt = with_template_header(
        'You are helping a cybersecurity analyst write practical intelligence questions. '
        'Return ONLY valid JSON with key "questions" as an array of short plain-language strings. '
        'Avoid military and intelligence-jargon phrasing. '
        'Use plain English a junior analyst can follow. '
        'Focus on what to verify next for defensive operations. '
        f'Actor: {actor_name}. Scope: {scope_statement or "n/a"}. '
        f'Evidence excerpts: {json.dumps(excerpts[:8])}'
    )

    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
    }
    try:
        response = _http_post(f'{base_url}/api/generate', json=payload, timeout=20.0)
        response.raise_for_status()
        parsed = parse_ollama_json_object(response.json())
        questions = parsed.get('questions', []) if isinstance(parsed, dict) else []
        strict_questions = normalize_string_list(questions, max_items=6, max_len=220)
        clean = [
            _sanitize_question_text(item)
            for item in strict_questions
        ]
        clean = [item for item in clean if item]
        return clean[:6] if clean else _fallback_questions()
    except Exception:
        return _fallback_questions()


def ollama_review_change_signals_core(
    actor_name: str,
    source_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    *,
    deps: dict[str, object],
) -> list[dict[str, object]]:
    _ollama_available = deps['ollama_available']
    _get_env = deps['get_env']
    _http_post = deps['http_post']
    _parse_published_datetime = deps['parse_published_datetime']

    if not _ollama_available():
        return []

    now = datetime.now(timezone.utc)

    def _source_dt(item: dict[str, object]):
        raw = str(item.get('published_at') or item.get('retrieved_at') or '').strip()
        dt = _parse_published_datetime(raw)
        return dt, raw

    recent_30: list[dict[str, object]] = []
    recent_60: list[dict[str, object]] = []
    recent_90: list[dict[str, object]] = []
    baseline_older: list[dict[str, object]] = []
    rolling_baseline_31_90: list[dict[str, object]] = []

    sorted_sources = sorted(
        source_items,
        key=lambda item: str(item.get('published_at') or item.get('retrieved_at') or ''),
        reverse=True,
    )

    for source in sorted_sources:
        dt, raw_date = _source_dt(source)
        if dt is None:
            continue
        age_days = max(0, (now - dt).days)
        normalized = {
            'date': raw_date or dt.date().isoformat(),
            'title': str(
                source.get('title')
                or source.get('headline')
                or source.get('og_title')
                or source.get('html_title')
                or source.get('source_name')
                or 'Untitled'
            ).strip(),
            'source': str(source.get('site_name') or source.get('publisher') or source.get('source_name') or '').strip(),
            'url': str(source.get('url') or '').strip(),
            'summary': str(source.get('pasted_text') or '').strip()[:420],
        }
        if age_days <= 30 and len(recent_30) < 10:
            recent_30.append(normalized)
        if age_days <= 60 and len(recent_60) < 12:
            recent_60.append(normalized)
        if age_days <= 90 and len(recent_90) < 14:
            recent_90.append(normalized)
        if 30 < age_days <= 90 and len(rolling_baseline_31_90) < 14:
            rolling_baseline_31_90.append(normalized)
        if 90 < age_days <= 180 and len(baseline_older) < 14:
            baseline_older.append(normalized)

    candidate_signals: list[dict[str, object]] = []
    for item in recent_activity_highlights:
        date_raw = str(item.get('source_published_at') or item.get('date') or '').strip()
        dt = _parse_published_datetime(date_raw)
        if dt is None or (now - dt).days > 90:
            continue
        candidate_signals.append(
            {
                'date': date_raw or dt.date().isoformat(),
                'title': str(item.get('evidence_title') or '').strip(),
                'source': str(item.get('evidence_source_label') or item.get('source_name') or '').strip(),
                'url': str(item.get('source_url') or '').strip(),
                'category': str(item.get('category') or '').strip(),
                'ttp_ids': str(item.get('ttp_ids') or '').strip(),
                'target': str(item.get('target_text') or '').strip(),
                'text': str(item.get('text') or '').strip(),
            }
        )
        if len(candidate_signals) >= 12:
            break

    if not recent_90:
        return []
    if not baseline_older:
        baseline_older = list(rolling_baseline_31_90)

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')

    prompt = with_template_header(
        'You are a CTI analyst assistant. Identify ONLY genuinely new changes for this actor. '
        'A "change" means new technique/tactic, target shift, tooling/infrastructure shift, '
        'or material operational shift vs older baseline. '
        'Do not return generic article headlines, generic victim-count summaries, or duplicate statements. '
        'If evidence is weak, return fewer items. '
        'Return ONLY JSON with schema: '
        '{"changes":[{"summary":"...","why_new":"...","window_days":30|60|90,'
        '"category":"...","ttp_ids":["..."],"target":"...","confidence":"high|medium|low",'
        '"evidence":[{"source_url":"...","source_label":"...","source_date":"...","proof":"..."}]}]}. '
        'Each change must include at least one evidence item with a source_url. '
        f'Actor: {actor_name}. '
        f'Recent_30: {json.dumps(recent_30)}. '
        f'Recent_60: {json.dumps(recent_60)}. '
        f'Recent_90: {json.dumps(recent_90)}. '
        f'Older_baseline_91_180_days_or_rolling_31_90: {json.dumps(baseline_older)}. '
        f'Candidate_signals: {json.dumps(candidate_signals)}.'
    )

    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
    }

    try:
        response = _http_post(f'{base_url}/api/generate', json=payload, timeout=25.0)
        response.raise_for_status()
        parsed = parse_ollama_json_object(response.json())
    except Exception:
        return []

    if not isinstance(parsed, dict):
        return []
    changes_raw = parsed.get('changes')
    if not isinstance(changes_raw, list):
        return []

    results: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in changes_raw:
        if not isinstance(item, dict):
            continue
        summary = ' '.join(str(item.get('summary') or '').split()).strip()
        why_new = ' '.join(str(item.get('why_new') or '').split()).strip()
        if not summary:
            continue
        key = summary.lower()
        if key in seen:
            continue
        seen.add(key)
        window_value = str(item.get('window_days') or '90').strip()
        if window_value not in {'30', '60', '90'}:
            window_value = '90'
        confidence = str(item.get('confidence') or 'medium').strip().lower()
        if confidence not in {'high', 'medium', 'low'}:
            confidence = 'medium'
        if confidence == 'low':
            continue
        ttp_items = item.get('ttp_ids', [])
        ttp_values: list[str] = []
        if isinstance(ttp_items, list):
            for ttp in ttp_items:
                token = str(ttp or '').strip().upper()
                if token and token not in ttp_values:
                    ttp_values.append(token)

        evidence_items_raw = item.get('evidence', [])
        evidence_items: list[dict[str, str]] = []
        if isinstance(evidence_items_raw, list):
            for ev in evidence_items_raw:
                if not isinstance(ev, dict):
                    continue
                ev_url = str(ev.get('source_url') or '').strip()
                if not ev_url:
                    continue
                evidence_items.append(
                    {
                        'source_url': ev_url,
                        'source_label': str(ev.get('source_label') or '').strip(),
                        'source_date': str(ev.get('source_date') or '').strip(),
                        'proof': ' '.join(str(ev.get('proof') or '').split()).strip()[:220],
                    }
                )
                if len(evidence_items) >= 3:
                    break
        if not evidence_items:
            # Backward compatibility if model returns older single-source schema.
            ev_url = str(item.get('source_url') or '').strip()
            if not ev_url:
                continue
            evidence_items = [
                {
                    'source_url': ev_url,
                    'source_label': str(item.get('source_label') or '').strip(),
                    'source_date': str(item.get('source_date') or '').strip(),
                    'proof': why_new[:220],
                }
            ]

        results.append(
            {
                'change_summary': summary[:180],
                'change_why_new': why_new[:300],
                'category': str(item.get('category') or ''),
                'ttp_ids': ', '.join(ttp_values[:4]),
                'target_text': str(item.get('target') or ''),
                'change_window_days': window_value,
                'change_confidence': confidence,
                'validated_sources': evidence_items,
                'validated_source_count': str(len(evidence_items)),
            }
        )
        if len(results) >= 3:
            break
    return results
