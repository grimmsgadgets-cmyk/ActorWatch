import json
import re
import sqlite3
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def _parse_iso(value: str | None) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _recency_score(published_at: str | None) -> float:
    published = _parse_iso(published_at)
    if published is None:
        return 0.4
    age_days = max(0.0, (datetime.now(timezone.utc) - published).total_seconds() / 86400.0)
    return round(max(0.0, 1.0 - min(age_days / 180.0, 1.0)), 3)


def _extract_entities(source_text: str) -> list[tuple[str, str, str]]:
    text = str(source_text or '')
    found: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    patterns: list[tuple[str, re.Pattern[str], Callable[[str], str]]] = [
        ('ip', re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'), lambda value: value.strip()),
        ('hash', re.compile(r'\b[a-fA-F0-9]{32}\b|\b[a-fA-F0-9]{40}\b|\b[a-fA-F0-9]{64}\b'), lambda value: value.lower()),
        ('domain', re.compile(r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b'), lambda value: value.strip('.').lower()),
        ('url', re.compile(r'\bhttps?://[^\s<>"\']+\b', flags=re.IGNORECASE), lambda value: value.strip()),
        ('email', re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'), lambda value: value.lower()),
        ('ttp', re.compile(r'\bT[0-9]{4}(?:\.[0-9]{3})?\b', flags=re.IGNORECASE), lambda value: value.upper()),
    ]

    for entity_type, pattern, normalize in patterns:
        for match in pattern.findall(text):
            value = str(match or '').strip()
            if not value:
                continue
            normalized = str(normalize(value))
            key = (entity_type, normalized)
            if key in seen:
                continue
            seen.add(key)
            found.append((entity_type, value, normalized))
    return found


def _normalize_entity_value(entity_type: str, value: str) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    kind = str(entity_type or '').strip().lower()
    if kind == 'url':
        try:
            parsed = urlparse(text)
        except Exception:
            return text
        if parsed.scheme.lower() not in {'http', 'https'}:
            return text
        query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
        filtered = [(k, v) for (k, v) in query_pairs if not str(k).lower().startswith('utm_')]
        normalized_query = urlencode(sorted(filtered), doseq=True)
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            query=normalized_query,
            fragment='',
        )
        return urlunparse(normalized).rstrip('/')
    if kind == 'domain':
        candidate = text.strip('.').lower()
        if candidate.startswith('www.') and candidate.count('.') >= 2:
            candidate = candidate[4:]
        return candidate
    if kind == 'email':
        return text.lower()
    if kind == 'hash':
        return text.lower()
    if kind == 'ttp':
        return text.upper()
    return text


def _is_benign_entity(entity_type: str, normalized_value: str) -> bool:
    kind = str(entity_type or '').strip().lower()
    value = str(normalized_value or '').strip().lower()
    if not value:
        return True
    if kind in {'domain', 'url'}:
        if any(marker in value for marker in ('example.com', 'example.org', 'example.net', 'localhost')):
            return True
    if kind == 'domain' and (value.endswith('.local') or value.endswith('.lan')):
        return True
    return False


def _corroboration_source_count(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    source_id: str,
    entities: list[tuple[str, str, str]],
) -> int:
    corroborating_sources: set[str] = set()
    candidate_entities = [(entity_type, normalized) for entity_type, _raw, normalized in entities if normalized][:40]
    for entity_type, normalized in candidate_entities:
        try:
            rows = connection.execute(
                '''
                SELECT DISTINCT se.source_id
                FROM source_entities se
                JOIN sources s ON s.id = se.source_id
                WHERE s.actor_id = ?
                  AND se.entity_type = ?
                  AND se.normalized_value = ?
                  AND se.source_id <> ?
                LIMIT 50
                ''',
                (actor_id, entity_type, normalized, source_id),
            ).fetchall()
        except sqlite3.OperationalError:
            return 0
        for row in rows:
            value = str(row[0] or '').strip()
            if value:
                corroborating_sources.add(value)
    return len(corroborating_sources)


def _best_matched_actor_term(*, actor_terms: list[str], source_text: str) -> str:
    text = str(source_text or '').lower()
    for term in actor_terms:
        candidate = str(term or '').strip()
        if candidate and candidate.lower() in text:
            return candidate[:120]
    return str(actor_terms[0] if actor_terms else '').strip()[:120]


def persist_source_evidence_core(
    connection: sqlite3.Connection,
    *,
    source_id: str,
    actor_id: str,
    source_url: str,
    source_text: str,
    raw_html: str | None,
    fetched_at: str,
    published_at: str | None,
    http_status: int | None,
    content_type: str | None,
    parse_status: str,
    parse_error: str | None,
    actor_terms: list[str],
    relevance_score: float,
    match_type: str,
    match_reasons: list[str],
    matched_terms: list[str],
    source_trust_score: int,
    novelty_score: float = 0.5,
    extractor: str = 'ingest-v1',
) -> None:
    source_id_value = str(source_id or '').strip()
    if not source_id_value:
        return
    safe_source_url = str(source_url or '').strip()
    safe_text = str(source_text or '')
    safe_html = str(raw_html or '')
    safe_parse_status = str(parse_status or 'parsed').strip() or 'parsed'
    safe_parse_error = str(parse_error or '').strip()
    safe_match_type = str(match_type or 'actor_term').strip() or 'actor_term'
    safe_extractor = str(extractor or 'ingest-v1').strip() or 'ingest-v1'
    safe_fetched_at = str(fetched_at or '').strip() or datetime.now(timezone.utc).isoformat()
    rel = max(0.0, min(1.0, float(relevance_score or 0.0)))
    trust = max(0.0, min(1.0, float(source_trust_score or 0) / 4.0))
    recency = _recency_score(published_at)
    novelty = max(0.0, min(1.0, float(novelty_score or 0.0)))
    entity_rows = _extract_entities(safe_text)
    normalized_entities: list[tuple[str, str, str]] = []
    seen_entity_keys: set[tuple[str, str]] = set()
    for entity_type, entity_value, _normalized in entity_rows:
        canonical = _normalize_entity_value(entity_type, entity_value)
        if not canonical:
            continue
        if _is_benign_entity(entity_type, canonical):
            continue
        key = (str(entity_type).lower(), canonical)
        if key in seen_entity_keys:
            continue
        seen_entity_keys.add(key)
        normalized_entities.append((str(entity_type).lower(), entity_value, canonical))
    corroboration_sources = _corroboration_source_count(
        connection,
        actor_id=actor_id,
        source_id=source_id_value,
        entities=normalized_entities,
    )
    corroboration = max(0.0, min(1.0, float(corroboration_sources) / 3.0))
    final_score = round((rel * 0.4) + (trust * 0.18) + (recency * 0.22) + (novelty * 0.1) + (corroboration * 0.1), 3)

    try:
        connection.execute(
            '''
            INSERT INTO source_documents (
                id, source_id, raw_text, html_text, fetched_at, http_status, content_type, parse_status, parse_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(uuid.uuid4()),
                source_id_value,
                safe_text[:20000],
                safe_html[:120000] if safe_html else '',
                safe_fetched_at,
                int(http_status) if http_status is not None else None,
                str(content_type or '')[:120],
                safe_parse_status,
                safe_parse_error[:500],
            ),
        )
    except sqlite3.OperationalError:
        return

    try:
        connection.execute(
            'DELETE FROM actor_resolution WHERE actor_id = ? AND source_id = ?',
            (actor_id, source_id_value),
        )
        connection.execute(
            '''
            INSERT INTO actor_resolution (
                id, source_id, actor_id, match_type, matched_term, confidence, explanation_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(uuid.uuid4()),
                source_id_value,
                actor_id,
                safe_match_type[:80],
                _best_matched_actor_term(actor_terms=actor_terms, source_text=safe_text),
                rel,
                json.dumps(
                    {
                        'match_reasons': [str(item) for item in match_reasons],
                        'matched_terms': [str(item) for item in matched_terms],
                        'source_host': (urlparse(safe_source_url).hostname or '').lower() if safe_source_url else '',
                    }
                ),
                safe_fetched_at,
            ),
        )
    except sqlite3.OperationalError:
        pass

    try:
        connection.execute('DELETE FROM source_entities WHERE source_id = ?', (source_id_value,))
        for entity_type, entity_value, normalized in normalized_entities[:200]:
            connection.execute(
                '''
                INSERT INTO source_entities (
                    id, source_id, entity_type, entity_value, normalized_value, confidence, extractor, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    str(uuid.uuid4()),
                    source_id_value,
                    entity_type,
                    entity_value[:500],
                    normalized[:500],
                    rel,
                    safe_extractor[:120],
                    safe_fetched_at,
                ),
            )
    except sqlite3.OperationalError:
        pass

    try:
        connection.execute(
            '''
            INSERT INTO source_scoring (
                source_id, relevance_score, trust_score, recency_score, novelty_score, final_score, scored_at, features_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                relevance_score = excluded.relevance_score,
                trust_score = excluded.trust_score,
                recency_score = excluded.recency_score,
                novelty_score = excluded.novelty_score,
                final_score = excluded.final_score,
                scored_at = excluded.scored_at,
                features_json = excluded.features_json
            ''',
            (
                source_id_value,
                rel,
                trust,
                recency,
                novelty,
                final_score,
                safe_fetched_at,
                json.dumps(
                    {
                        'match_type': safe_match_type,
                        'match_reasons': [str(item) for item in match_reasons],
                        'matched_terms': [str(item) for item in matched_terms],
                        'source_url': safe_source_url,
                        'corroboration_sources': corroboration_sources,
                        'entity_count': len(normalized_entities),
                    }
                ),
            ),
        )
    except sqlite3.OperationalError:
        pass
