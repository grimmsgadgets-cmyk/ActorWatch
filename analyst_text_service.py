import json


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

    if not excerpts or not _ollama_available():
        return []

    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    prompt = (
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
        content = response.json().get('response', '{}')
        parsed = json.loads(content)
        questions = parsed.get('questions', []) if isinstance(parsed, dict) else []
        clean = [
            _sanitize_question_text(str(item))
            for item in questions
            if isinstance(item, str) and str(item).strip()
        ]
        clean = [item for item in clean if item]
        return clean[:6]
    except Exception:
        return []
