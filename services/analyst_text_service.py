import json
import logging
from datetime import datetime, timezone

from services.llm_schema_service import normalize_string_list, parse_ollama_json_object
from services.prompt_templates import with_template_header

LOGGER = logging.getLogger(__name__)


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
    timeout_seconds = max(8.0, float(_get_env('REVIEW_CHANGE_OLLAMA_TIMEOUT_SECONDS', '25')))
    retry_timeout_seconds = max(
        timeout_seconds,
        float(_get_env('REVIEW_CHANGE_OLLAMA_RETRY_TIMEOUT_SECONDS', '35')),
    )
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
    max_recent_30 = max(4, min(10, int(_get_env('REVIEW_CHANGE_RECENT_30_MAX', '6'))))
    max_recent_60 = max(6, min(12, int(_get_env('REVIEW_CHANGE_RECENT_60_MAX', '8'))))
    max_recent_90 = max(8, min(14, int(_get_env('REVIEW_CHANGE_RECENT_90_MAX', '10'))))
    max_baseline = max(6, min(14, int(_get_env('REVIEW_CHANGE_BASELINE_MAX', '10'))))
    max_candidate = max(6, min(12, int(_get_env('REVIEW_CHANGE_CANDIDATE_MAX', '8'))))
    summary_max_len = max(160, min(420, int(_get_env('REVIEW_CHANGE_SUMMARY_MAX_CHARS', '220'))))

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
            'summary': str(source.get('pasted_text') or '').strip()[:summary_max_len],
        }
        if age_days <= 30 and len(recent_30) < max_recent_30:
            recent_30.append(normalized)
        if age_days <= 60 and len(recent_60) < max_recent_60:
            recent_60.append(normalized)
        if age_days <= 90 and len(recent_90) < max_recent_90:
            recent_90.append(normalized)
        if 30 < age_days <= 90 and len(rolling_baseline_31_90) < max_baseline:
            rolling_baseline_31_90.append(normalized)
        if 90 < age_days <= 180 and len(baseline_older) < max_baseline:
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
        if len(candidate_signals) >= max_candidate:
            break

    if not recent_90:
        return []
    if not baseline_older:
        baseline_older = list(rolling_baseline_31_90)

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    timeout_seconds = max(12.0, float(_get_env('REVIEW_CHANGE_OLLAMA_TIMEOUT_SECONDS', '40')))
    retry_timeout_seconds = max(
        timeout_seconds,
        float(_get_env('REVIEW_CHANGE_OLLAMA_RETRY_TIMEOUT_SECONDS', '60')),
    )

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
        'options': {
            'temperature': 0.1,
            'num_predict': 600,
        },
    }

    parsed = None
    try:
        response = _http_post(f'{base_url}/api/generate', json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        parsed = parse_ollama_json_object(response.json())
    except Exception as exc:
        LOGGER.warning(
            'ollama_change_signal_review_failed attempt=1 timeout=%.1fs actor=%s error=%s',
            timeout_seconds,
            actor_name,
            exc,
        )
    if not isinstance(parsed, dict):
        retry_prompt = with_template_header(
            'You are a CTI analyst assistant. Identify ONLY genuinely new changes for this actor. '
            'Return ONLY JSON with schema: '
            '{"changes":[{"summary":"...","why_new":"...","window_days":30|60|90,'
            '"category":"...","ttp_ids":["..."],"target":"...","confidence":"high|medium|low",'
            '"evidence":[{"source_url":"...","source_label":"...","source_date":"...","proof":"..."}]}]}. '
            f'Actor: {actor_name}. '
            f'Recent_30: {json.dumps(recent_30[:6])}. '
            f'Recent_60: {json.dumps(recent_60[:8])}. '
            f'Recent_90: {json.dumps(recent_90[:10])}. '
            f'Older_baseline_91_180_days_or_rolling_31_90: {json.dumps(baseline_older[:8])}. '
            f'Candidate_signals: {json.dumps(candidate_signals[:8])}.'
        )
        retry_payload = {
            'model': model,
            'prompt': retry_prompt,
            'stream': False,
            'format': 'json',
            'options': {
                'temperature': 0.1,
                'num_predict': 500,
            },
        }
        try:
            response = _http_post(f'{base_url}/api/generate', json=retry_payload, timeout=retry_timeout_seconds)
            response.raise_for_status()
            parsed = parse_ollama_json_object(response.json())
        except Exception as exc:
            LOGGER.warning(
                'ollama_change_signal_review_failed attempt=2 timeout=%.1fs actor=%s error=%s',
                retry_timeout_seconds,
                actor_name,
                exc,
            )
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


def ollama_synthesize_recent_activity_core(
    actor_name: str,
    highlights: list[dict[str, object]],
    *,
    deps: dict[str, object],
) -> list[dict[str, str]]:
    _ollama_available = deps['ollama_available']
    _get_env = deps['get_env']
    _http_post = deps['http_post']

    if not highlights or not _ollama_available():
        return []

    prepared: list[dict[str, str]] = []
    for item in highlights[:14]:
        if not isinstance(item, dict):
            continue
        prepared.append(
            {
                'date': str(item.get('date') or ''),
                'category': str(item.get('category') or ''),
                'target': str(item.get('target_text') or ''),
                'ttp_ids': str(item.get('ttp_ids') or ''),
                'source': str(item.get('evidence_source_label') or item.get('source_name') or ''),
                'url': str(item.get('source_url') or ''),
                'text': ' '.join(str(item.get('text') or '').split())[:320],
            }
        )
    if not prepared:
        return []

    lineage_count = len(
        {
            str(item.get('url') or '').strip()
            for item in prepared
            if str(item.get('url') or '').strip()
        }
    )
    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    timeout_seconds = max(8.0, float(_get_env('RECENT_ACTIVITY_OLLAMA_TIMEOUT_SECONDS', '15')))
    retry_timeout_seconds = max(
        timeout_seconds,
        float(_get_env('RECENT_ACTIVITY_OLLAMA_RETRY_TIMEOUT_SECONDS', '30')),
    )
    retry_highlight_count = max(3, min(8, int(_get_env('RECENT_ACTIVITY_OLLAMA_RETRY_HIGHLIGHTS', '6'))))
    prompt = with_template_header(
        'You are a CTI analyst writing concise dashboard synthesis for a defender. '
        'Return ONLY JSON with schema: '
        '{"items":[{"label":"What changed|Who is affected|What to do next","text":"...","confidence":"High|Medium|Low"}]}. '
        'Rules: use only provided highlights, be concrete, avoid source titles as the main sentence, '
        'avoid generic filler, and keep each text <= 220 characters. '
        f'Actor: {actor_name}. Highlights: {json.dumps(prepared)}.'
    )
    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
        'options': {
            'temperature': 0.1,
            'num_predict': 220,
        },
    }

    def _parse_response_payload(raw_payload: object) -> list[dict[str, str]]:
        parsed = parse_ollama_json_object(raw_payload)
        raw_items = parsed.get('items') if isinstance(parsed, dict) else None
        if not isinstance(raw_items, list):
            return []
        allowed_labels = {'What changed', 'Who is affected', 'What to do next'}
        by_label: dict[str, dict[str, str]] = {}
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            label = ' '.join(str(raw.get('label') or '').split()).strip()
            text = ' '.join(str(raw.get('text') or '').split()).strip()[:260]
            confidence = ' '.join(str(raw.get('confidence') or 'Medium').split()).strip().title()
            if label not in allowed_labels or not text:
                continue
            if confidence not in {'High', 'Medium', 'Low'}:
                confidence = 'Medium'
            if label in by_label:
                continue
            by_label[label] = {
                'label': label,
                'text': text,
                'confidence': confidence,
                'lineage': f'{lineage_count} sources',
            }
        ordered = [by_label.get(name) for name in ('What changed', 'Who is affected', 'What to do next')]
        return [item for item in ordered if isinstance(item, dict)]

    def _try_generate(payload_to_send: dict[str, object], *, timeout_value: float, attempt: int) -> list[dict[str, str]]:
        try:
            response = _http_post(f'{base_url}/api/generate', json=payload_to_send, timeout=timeout_value)
            response.raise_for_status()
            return _parse_response_payload(response.json())
        except Exception as exc:
            LOGGER.warning(
                'ollama_recent_activity_synthesis_failed attempt=%s timeout=%.1fs actor=%s error=%s',
                attempt,
                timeout_value,
                actor_name,
                exc,
            )
            return []

    rows = _try_generate(payload, timeout_value=timeout_seconds, attempt=1)
    if rows:
        return rows

    compact_highlights: list[dict[str, str]] = []
    for item in prepared[:retry_highlight_count]:
        compact_highlights.append(
            {
                'date': str(item.get('date') or ''),
                'category': str(item.get('category') or ''),
                'target': str(item.get('target') or ''),
                'ttp_ids': str(item.get('ttp_ids') or ''),
                'source': str(item.get('source') or ''),
                'url': str(item.get('url') or ''),
                'text': ' '.join(str(item.get('text') or '').split())[:180],
            }
        )
    compact_prompt = with_template_header(
        'You are a CTI analyst writing concise dashboard synthesis for a defender. '
        'Return ONLY JSON with schema: '
        '{"items":[{"label":"What changed|Who is affected|What to do next","text":"...","confidence":"High|Medium|Low"}]}. '
        'Rules: use only provided highlights, be concrete, avoid source titles as the main sentence, '
        'avoid generic filler, and keep each text <= 220 characters. '
        f'Actor: {actor_name}. Highlights: {json.dumps(compact_highlights)}.'
    )
    retry_payload = {
        'model': model,
        'prompt': compact_prompt,
        'stream': False,
        'format': 'json',
        'options': {
            'temperature': 0.1,
            'num_predict': 180,
        },
    }
    rows = _try_generate(retry_payload, timeout_value=retry_timeout_seconds, attempt=2)
    if rows:
        return rows

    LOGGER.warning(
        'ollama_recent_activity_synthesis_exhausted actor=%s timeout_primary=%.1fs timeout_retry=%.1fs',
        actor_name,
        timeout_seconds,
        retry_timeout_seconds,
    )
    return []
