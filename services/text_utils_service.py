import re
import string


def normalize_text_core(value: str) -> str:
    lowered = value.lower()
    translator = str.maketrans('', '', string.punctuation)
    return lowered.translate(translator)


def token_set_core(value: str, *, normalize_text) -> set[str]:
    return {token for token in normalize_text(value).split() if len(token) > 2}


def token_overlap_core(a: str, b: str, *, token_set) -> float:
    a_tokens = token_set(a)
    b_tokens = token_set(b)
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens.intersection(b_tokens)) / len(a_tokens.union(b_tokens))


def split_sentences_core(text: str) -> list[str]:
    sentences = [segment.strip() for segment in re.split(r'(?<=[.!?])\s+', text) if segment.strip()]
    return [sentence for sentence in sentences if len(sentence) >= 25]


def extract_question_sentences_core(
    text: str,
    *,
    split_sentences,
    question_seed_keywords: set[str],
) -> list[str]:
    matches: list[str] = []
    for sentence in split_sentences(text):
        lowered = sentence.lower()
        if any(keyword in lowered for keyword in question_seed_keywords):
            matches.append(sentence)
    return matches


def question_from_sentence_core(sentence: str) -> str:
    lowered = sentence.lower()
    if any(token in lowered for token in ('phish', 'email')):
        return 'What evidence shows this actor is using email or phishing delivery right now?'
    if any(token in lowered for token in ('cve', 'vpn', 'edge', 'exploit')):
        return 'Which exposed systems are most at risk from this actor’s current exploit activity?'
    if any(token in lowered for token in ('powershell', 'wmi', 'scheduled task')):
        return 'Which endpoint execution patterns should we validate for this actor immediately?'
    if any(token in lowered for token in ('dns', 'domain', 'c2', 'beacon')):
        return 'What network indicators suggest active command-and-control behavior by this actor?'
    if any(token in lowered for token in ('hash', 'file', 'process', 'command line')):
        return 'Which endpoint artifacts best confirm this actor’s latest operational behavior?'
    compact = ' '.join(sentence.split())
    if len(compact) > 170:
        compact = compact[:170].rsplit(' ', 1)[0] + '...'
    return f'What should analysts verify next based on this report: {compact}'


def sanitize_question_text_core(question: str) -> str:
    cleaned = ' '.join(question.strip().split())
    if not cleaned:
        return ''
    for pattern in (
        r'\bPIRs?\b',
        r'\bpriority intelligence requirements?\b',
        r'\bintelligence requirements?\b',
        r'\bcollection requirements?\b',
        r'\bkill chain\b',
    ):
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    if not cleaned:
        return ''
    if not cleaned.endswith('?'):
        cleaned = cleaned.rstrip('.!') + '?'
    if len(cleaned) > 220:
        cleaned = cleaned[:220].rsplit(' ', 1)[0] + '?'
    if not cleaned.lower().startswith(('what ', 'how ', 'which ', 'where ', 'when ', 'who ')):
        cleaned = f'What should we ask next: {cleaned}'
    return cleaned


def first_sentences_core(text: str, *, split_sentences, count: int = 2) -> str:
    sentences = split_sentences(text)
    if not sentences:
        compact = ' '.join(text.split())
        return compact[:240]
    return ' '.join(sentences[:count])


def normalize_actor_key_core(value: str, *, re_findall) -> str:
    return ' '.join(re_findall(r'[a-z0-9]+', value.lower()))


def dedupe_actor_terms_core(values: list[str], *, normalize_actor_key) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        key = normalize_actor_key(text)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def mitre_alias_values_core(obj: dict[str, object], *, dedupe_actor_terms) -> list[str]:
    alias_candidates: list[str] = []
    for field in ('aliases', 'x_mitre_aliases'):
        raw = obj.get(field)
        if isinstance(raw, list):
            alias_candidates.extend(str(item).strip() for item in raw if str(item).strip())
    return dedupe_actor_terms(alias_candidates)


def candidate_overlap_score_core(actor_tokens: set[str], search_keys: set[str]) -> float:
    best_score = 0.0
    for search_key in search_keys:
        key_tokens = set(search_key.split())
        if not key_tokens:
            continue
        overlap = len(actor_tokens.intersection(key_tokens)) / len(actor_tokens.union(key_tokens))
        if overlap > best_score:
            best_score = overlap
    return best_score
