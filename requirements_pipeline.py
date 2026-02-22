import json
import os
import re
import sqlite3
from typing import Callable

import httpx
from fastapi import HTTPException


def ollama_generate_requirements(
    actor_name: str,
    priority_mode: str,
    org_context: str,
    evidence_rows: list[dict[str, str | None]],
    *,
    ollama_available: Callable[[], bool],
) -> list[dict[str, str]]:
    if not evidence_rows or not ollama_available():
        return []

    model = os.environ.get('OLLAMA_MODEL', 'llama3.1:8b')
    base_url = os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    evidence_payload = [
        {
            'source_name': row.get('source_name') or '',
            'source_url': row.get('source_url') or '',
            'source_published_at': row.get('source_published_at') or '',
            'excerpt': row.get('excerpt') or '',
        }
        for row in evidence_rows[:10]
    ]
    prompt = (
        'You generate cybersecurity intelligence requirements for analysts. '
        'Return ONLY strict JSON: {"requirements":[{'
        '"req_type":"PIR|GIR|IR","requirement_text":"...","rationale":"...",'
        '"source_url":"...","source_name":"...","source_published_at":"..."}]}. '
        'Use plain English. Keep each requirement <= 22 words. '
        f'Actor: {actor_name}. Priority mode: {priority_mode}. '
        f'Org context: {org_context or "none"}. '
        f'Evidence: {json.dumps(evidence_payload)}'
    )
    payload = {
        'model': model,
        'prompt': prompt,
        'stream': False,
        'format': 'json',
    }
    try:
        response = httpx.post(f'{base_url}/api/generate', json=payload, timeout=30.0)
        response.raise_for_status()
        content = response.json().get('response', '{}')
        parsed = json.loads(content)
        reqs = parsed.get('requirements', []) if isinstance(parsed, dict) else []
        cleaned: list[dict[str, str]] = []
        for item in reqs:
            if not isinstance(item, dict):
                continue
            req_type = str(item.get('req_type') or 'IR').upper()
            if req_type not in {'PIR', 'GIR', 'IR'}:
                req_type = 'IR'
            requirement_text = ' '.join(str(item.get('requirement_text') or '').split()).strip()
            rationale = ' '.join(str(item.get('rationale') or '').split()).strip()
            source_url = str(item.get('source_url') or '').strip()
            source_name = str(item.get('source_name') or '').strip()
            source_published_at = str(item.get('source_published_at') or '').strip()
            if not requirement_text:
                continue
            cleaned.append(
                {
                    'req_type': req_type,
                    'requirement_text': requirement_text[:220],
                    'rationale': rationale[:320],
                    'source_url': source_url,
                    'source_name': source_name,
                    'source_published_at': source_published_at,
                }
            )
            if len(cleaned) >= 8:
                break
        return cleaned
    except Exception:
        return []


def generate_requirements_fallback(
    actor_name: str,
    priority_mode: str,
    evidence_rows: list[dict[str, str | None]],
    *,
    sanitize_question_text: Callable[[str], str],
    question_from_sentence: Callable[[str], str],
) -> list[dict[str, str]]:
    type_hint = 'PIR' if priority_mode == 'Strategic' else ('GIR' if priority_mode == 'Operational' else 'IR')
    output: list[dict[str, str]] = []
    for row in evidence_rows[:6]:
        excerpt = str(row.get('excerpt') or '').strip()
        if not excerpt:
            continue
        question = sanitize_question_text(question_from_sentence(excerpt))
        if not question:
            continue
        output.append(
            {
                'req_type': type_hint,
                'requirement_text': question,
                'rationale': f'Based on recent {actor_name} reporting and observed activity.',
                'source_url': str(row.get('source_url') or ''),
                'source_name': str(row.get('source_name') or ''),
                'source_published_at': str(row.get('source_published_at') or ''),
            }
        )
    return output


def _expected_req_type(priority_mode: str) -> str:
    if priority_mode == 'Strategic':
        return 'PIR'
    if priority_mode == 'Operational':
        return 'GIR'
    return 'IR'


def _clean_requirement_text(value: str) -> str:
    text = ' '.join(value.split()).strip()
    if not text:
        return ''
    if not text.endswith('?'):
        text = text.rstrip('.!') + '?'
    return text[:220]


def _best_evidence_for_requirement(
    requirement_text: str,
    evidence_rows: list[dict[str, str | None]],
    *,
    token_overlap: Callable[[str, str], float],
) -> dict[str, str | None] | None:
    best_row: dict[str, str | None] | None = None
    best_score = 0.0
    for row in evidence_rows:
        excerpt = str(row.get('excerpt') or '')
        score = token_overlap(requirement_text, excerpt)
        if score > best_score:
            best_score = score
            best_row = row
    if best_row is None and evidence_rows:
        return evidence_rows[0]
    return best_row


def _kraven_style_requirement_check(
    item: dict[str, str],
    actor_name: str,
    expected_type: str,
    org_context: str,
) -> tuple[bool, int, list[str]]:
    issues: list[str] = []
    score = 0
    req_text = str(item.get('requirement_text') or '')
    rationale = str(item.get('rationale') or '')
    source_url = str(item.get('source_url') or '')
    source_name = str(item.get('source_name') or '')

    if source_url and source_name:
        score += 2
    else:
        issues.append('missing source lineage')

    words = req_text.rstrip('?').split()
    if 7 <= len(words) <= 30:
        score += 1
    else:
        issues.append('question length out of range')

    if req_text.endswith('?'):
        score += 1
    else:
        issues.append('not phrased as a question')

    if req_text.lower().startswith(('what ', 'which ', 'how ', 'where ', 'when ', 'who ')):
        score += 1
    else:
        issues.append('question not interrogative')

    actor_tokens = [tok for tok in re.findall(r'[a-z0-9]+', actor_name.lower()) if len(tok) > 2]
    if actor_tokens and any(token in req_text.lower() for token in actor_tokens):
        score += 1
    else:
        issues.append('actor reference missing')

    if rationale and any(token in rationale.lower() for token in ('decision', 'priority', 'risk', 'action', 'impact')):
        score += 1
    else:
        issues.append('weak decision linkage in rationale')

    if org_context.strip():
        ctx_tokens = [tok for tok in re.findall(r'[a-z0-9]+', org_context.lower()) if len(tok) > 3][:8]
        if ctx_tokens and any(tok in (req_text + ' ' + rationale).lower() for tok in ctx_tokens):
            score += 1

    text_lower = (req_text + ' ' + rationale).lower()
    if expected_type == 'PIR':
        if any(tok in text_lower for tok in ('intent', 'objective', 'risk', 'impact', 'campaign', 'targeting')):
            score += 2
        else:
            issues.append('PIR missing strategic framing')
    elif expected_type == 'GIR':
        if any(tok in text_lower for tok in ('trend', 'change', 'pattern', 'activity', 'capability', 'infrastructure')):
            score += 2
        else:
            issues.append('GIR missing operational framing')
    else:
        if any(tok in text_lower for tok in ('ioc', 'domain', 'ip', 'hash', 'process', 'command', 'technique', 't1')):
            score += 2
        else:
            issues.append('IR missing observable indicators')

    is_valid = score >= 7 and 'missing source lineage' not in issues
    return is_valid, score, issues


def normalize_and_validate_requirements(
    generated: list[dict[str, str]],
    actor_name: str,
    priority_mode: str,
    org_context: str,
    evidence_rows: list[dict[str, str | None]],
    *,
    token_overlap: Callable[[str, str], float],
    normalize_text: Callable[[str], str],
) -> list[dict[str, str]]:
    expected_type = _expected_req_type(priority_mode)
    validated: list[dict[str, str]] = []
    seen_questions: set[str] = set()

    for raw in generated:
        requirement_text = _clean_requirement_text(str(raw.get('requirement_text') or ''))
        if not requirement_text:
            continue
        evidence = _best_evidence_for_requirement(
            requirement_text,
            evidence_rows,
            token_overlap=token_overlap,
        ) or {}
        normalized = {
            'req_type': expected_type,
            'requirement_text': requirement_text,
            'rationale': str(raw.get('rationale') or '').strip() or 'Supports analyst decision-making for this actor.',
            'source_url': str(raw.get('source_url') or evidence.get('source_url') or '').strip(),
            'source_name': str(raw.get('source_name') or evidence.get('source_name') or '').strip(),
            'source_published_at': str(raw.get('source_published_at') or evidence.get('source_published_at') or '').strip(),
        }
        key = normalize_text(normalized['requirement_text'])
        if key in seen_questions:
            continue

        ok, score, issues = _kraven_style_requirement_check(normalized, actor_name, expected_type, org_context)
        if not ok:
            continue
        normalized['validation_score'] = str(score)
        normalized['validation_notes'] = 'passed' if not issues else '; '.join(issues)
        seen_questions.add(key)
        validated.append(normalized)
        if len(validated) >= 8:
            break

    return validated


def generate_actor_requirements_core(
    actor_id: str,
    org_context: str,
    priority_mode: str,
    *,
    db_path: str,
    deps: dict[str, object],
) -> int:
    _now_iso = deps['now_iso']
    _actor_exists = deps['actor_exists']
    _build_actor_profile_from_mitre = deps['build_actor_profile_from_mitre']
    _actor_terms = deps['actor_terms']
    _split_sentences = deps['split_sentences']
    _sentence_mentions_actor_terms = deps['sentence_mentions_actor_terms']
    _looks_like_activity_sentence = deps['looks_like_activity_sentence']
    _ollama_available = deps['ollama_available']
    _sanitize_question_text = deps['sanitize_question_text']
    _question_from_sentence = deps['question_from_sentence']
    _token_overlap = deps['token_overlap']
    _normalize_text = deps['normalize_text']
    _new_id = deps['new_id']

    now = _now_iso()
    with sqlite3.connect(db_path) as connection:
        if not _actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')

        actor_row = connection.execute(
            'SELECT display_name FROM actor_profiles WHERE id = ?',
            (actor_id,),
        ).fetchone()
        actor_name = str(actor_row[0] if actor_row else 'actor')

        evidence_rows_raw = connection.execute(
            '''
            SELECT s.source_name, s.url, s.published_at, qu.trigger_excerpt
            FROM question_updates qu
            JOIN question_threads qt ON qt.id = qu.thread_id
            JOIN sources s ON s.id = qu.source_id
            WHERE qt.actor_id = ?
            ORDER BY qu.created_at DESC
            LIMIT 16
            ''',
            (actor_id,),
        ).fetchall()
        evidence_rows: list[dict[str, str | None]] = [
            {
                'source_name': row[0],
                'source_url': row[1],
                'source_published_at': row[2],
                'excerpt': row[3],
            }
            for row in evidence_rows_raw
        ]

        if not evidence_rows:
            source_rows = connection.execute(
                '''
                SELECT source_name, url, published_at, pasted_text
                FROM sources
                WHERE actor_id = ?
                ORDER BY retrieved_at DESC
                LIMIT 12
                ''',
                (actor_id,),
            ).fetchall()
            mitre_profile = _build_actor_profile_from_mitre(actor_name)
            actor_terms = _actor_terms(
                actor_name,
                str(mitre_profile.get('group_name') or ''),
                str(mitre_profile.get('aliases_csv') or ''),
            )
            for row in source_rows:
                text = str(row[3] or '')
                for sentence in _split_sentences(text):
                    if actor_terms and not _sentence_mentions_actor_terms(sentence, actor_terms):
                        continue
                    if not _looks_like_activity_sentence(sentence):
                        continue
                    evidence_rows.append(
                        {
                            'source_name': row[0],
                            'source_url': row[1],
                            'source_published_at': row[2],
                            'excerpt': sentence,
                        }
                    )
                    if len(evidence_rows) >= 16:
                        break
                if len(evidence_rows) >= 16:
                    break

        generated = ollama_generate_requirements(
            actor_name,
            priority_mode,
            org_context,
            evidence_rows,
            ollama_available=_ollama_available,
        )
        validated = normalize_and_validate_requirements(
            generated,
            actor_name,
            priority_mode,
            org_context,
            evidence_rows,
            token_overlap=_token_overlap,
            normalize_text=_normalize_text,
        )
        if len(validated) < 3:
            fallback = generate_requirements_fallback(
                actor_name,
                priority_mode,
                evidence_rows,
                sanitize_question_text=_sanitize_question_text,
                question_from_sentence=_question_from_sentence,
            )
            fallback_validated = normalize_and_validate_requirements(
                fallback,
                actor_name,
                priority_mode,
                org_context,
                evidence_rows,
                token_overlap=_token_overlap,
                normalize_text=_normalize_text,
            )
            for item in fallback_validated:
                key = _normalize_text(str(item.get('requirement_text') or ''))
                if any(_normalize_text(str(existing.get('requirement_text') or '')) == key for existing in validated):
                    continue
                validated.append(item)
                if len(validated) >= 8:
                    break

        connection.execute(
            '''
            INSERT INTO requirement_context (actor_id, org_context, priority_mode, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(actor_id) DO UPDATE SET
                org_context = excluded.org_context,
                priority_mode = excluded.priority_mode,
                updated_at = excluded.updated_at
            ''',
            (actor_id, org_context, priority_mode, now),
        )

        connection.execute('DELETE FROM requirement_items WHERE actor_id = ?', (actor_id,))
        inserted = 0
        for item in validated:
            connection.execute(
                '''
                INSERT INTO requirement_items (
                    id, actor_id, req_type, requirement_text, rationale_text,
                    source_name, source_url, source_published_at,
                    validation_score, validation_notes,
                    status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    _new_id(),
                    actor_id,
                    str(item.get('req_type') or 'IR'),
                    str(item.get('requirement_text') or ''),
                    str(item.get('rationale') or ''),
                    str(item.get('source_name') or ''),
                    str(item.get('source_url') or ''),
                    str(item.get('source_published_at') or ''),
                    int(str(item.get('validation_score') or '0')),
                    str(item.get('validation_notes') or ''),
                    'open',
                    now,
                ),
            )
            inserted += 1
        connection.commit()
    return inserted
