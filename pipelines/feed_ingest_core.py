import sqlite3
import time
import json
import uuid
import re
from datetime import datetime, timedelta, timezone
from typing import Callable
from urllib.parse import urlparse

from fastapi import HTTPException
import services.source_evidence_service as source_evidence_service


def _is_google_news_wrapper_url(value: str | None) -> bool:
    if not value:
        return False
    try:
        parsed = urlparse(str(value).strip())
    except Exception:
        return False
    host = (parsed.hostname or '').strip('.').lower()
    path = (parsed.path or '').strip()
    return host.endswith('news.google.com') and path.startswith('/rss/articles/')


def _ensure_actor_feed_state_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_feed_state (
            actor_id TEXT NOT NULL,
            feed_name TEXT NOT NULL,
            feed_url TEXT NOT NULL,
            last_checked_at TEXT,
            last_success_at TEXT,
            last_success_published_at TEXT,
            last_imported_count INTEGER NOT NULL DEFAULT 0,
            total_imported INTEGER NOT NULL DEFAULT 0,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            total_failures INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            PRIMARY KEY (actor_id, feed_name, feed_url)
        )
        '''
    )


def _safe_parse_iso(value: str | None) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _load_actor_feed_state(
    connection: sqlite3.Connection,
    actor_id: str,
) -> dict[tuple[str, str], dict[str, object]]:
    rows = connection.execute(
        '''
        SELECT
            feed_name,
            feed_url,
            last_checked_at,
            last_success_at,
            last_success_published_at,
            last_imported_count,
            total_imported,
            consecutive_failures,
            total_failures,
            last_error
        FROM actor_feed_state
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchall()
    state_map: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (str(row[0]), str(row[1]))
        state_map[key] = {
            'last_checked_at': str(row[2] or '').strip() or None,
            'last_success_at': str(row[3] or '').strip() or None,
            'last_success_published_at': str(row[4] or '').strip() or None,
            'last_imported_count': int(row[5] or 0),
            'total_imported': int(row[6] or 0),
            'consecutive_failures': int(row[7] or 0),
            'total_failures': int(row[8] or 0),
            'last_error': str(row[9] or '').strip() or None,
        }
    return state_map


def _upsert_actor_feed_state(
    connection: sqlite3.Connection,
    actor_id: str,
    feed_name: str,
    feed_url: str,
    values: dict[str, object],
) -> None:
    connection.execute(
        '''
        INSERT INTO actor_feed_state (
            actor_id,
            feed_name,
            feed_url,
            last_checked_at,
            last_success_at,
            last_success_published_at,
            last_imported_count,
            total_imported,
            consecutive_failures,
            total_failures,
            last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_id, feed_name, feed_url) DO UPDATE SET
            last_checked_at = excluded.last_checked_at,
            last_success_at = excluded.last_success_at,
            last_success_published_at = excluded.last_success_published_at,
            last_imported_count = excluded.last_imported_count,
            total_imported = excluded.total_imported,
            consecutive_failures = excluded.consecutive_failures,
            total_failures = excluded.total_failures,
            last_error = excluded.last_error
        ''',
        (
            actor_id,
            feed_name,
            feed_url,
            str(values.get('last_checked_at') or '').strip() or None,
            str(values.get('last_success_at') or '').strip() or None,
            str(values.get('last_success_published_at') or '').strip() or None,
            int(values.get('last_imported_count') or 0),
            int(values.get('total_imported') or 0),
            int(values.get('consecutive_failures') or 0),
            int(values.get('total_failures') or 0),
            str(values.get('last_error') or '').strip() or None,
        ),
    )


def _feed_backoff_active(state: dict[str, object], now_utc: datetime) -> bool:
    failures = int(state.get('consecutive_failures') or 0)
    if failures < 3:
        return False
    last_checked = _safe_parse_iso(str(state.get('last_checked_at') or ''))
    if last_checked is None:
        return False
    cooldown_minutes = min(360, failures * 30)
    return (now_utc - last_checked) < timedelta(minutes=cooldown_minutes)


def _feed_priority_key(
    feed: tuple[str, str],
    state_map: dict[tuple[str, str], dict[str, object]],
) -> tuple[int, float]:
    state = state_map.get(feed, {})
    failures = int(state.get('consecutive_failures') or 0)
    last_success = _safe_parse_iso(str(state.get('last_success_at') or ''))
    recency_score = -last_success.timestamp() if last_success is not None else 0.0
    return failures, recency_score


def _record_ingest_decision(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    stage: str,
    decision: str,
    reason_code: str,
    now_iso: str,
    source_id: str | None = None,
    details: dict[str, object] | None = None,
) -> None:
    connection.execute(
        '''
        INSERT INTO ingest_decisions (
            id, source_id, actor_id, stage, decision, reason_code, details_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            str(uuid.uuid4()),
            str(source_id or '').strip() or None,
            actor_id,
            stage,
            decision,
            reason_code,
            json.dumps(details or {}),
            now_iso,
        ),
    )


def _acquire_feed_entries(
    *,
    safe_http_get: Callable[..., object],
    parse_feed_entries: Callable[[str], list[dict[str, str | None]]],
    feed_url: str,
    timeout_seconds: float,
) -> list[dict[str, str | None]]:
    response = safe_http_get(feed_url, timeout=timeout_seconds)
    response.raise_for_status()
    return parse_feed_entries(response.text)


def _resolve_source_from_link(
    *,
    derive_source_from_url: Callable[..., dict[str, str | None]],
    link: str,
    fallback_source_name: str,
    published_hint: str | None = None,
    timeout_seconds: float = 20.0,
) -> dict[str, str | None]:
    return derive_source_from_url(
        link,
        fallback_source_name=fallback_source_name,
        published_hint=published_hint,
        fetch_timeout_seconds=timeout_seconds,
    )


def _candidate_passes_score(
    *,
    actor_terms: list[str],
    text_contains_actor_term: Callable[[str, list[str]], bool],
    combined_text: str,
) -> bool:
    if not actor_terms:
        return True
    return bool(text_contains_actor_term(combined_text, actor_terms))


def _actor_term_token_set(value: str) -> set[str]:
    return {token for token in re.findall(r'[a-z0-9]+', str(value or '').lower()) if len(token) > 2}


def _actor_relevance_features(
    *,
    combined_text: str,
    actor_terms: list[str],
    text_contains_actor_term: Callable[[str, list[str]], bool],
) -> dict[str, object]:
    exact_match = _candidate_passes_score(
        actor_terms=actor_terms,
        text_contains_actor_term=text_contains_actor_term,
        combined_text=combined_text,
    )
    text_tokens = _actor_term_token_set(combined_text)
    strongest_overlap = 0.0
    matching_terms = 0
    for term in actor_terms:
        term_tokens = _actor_term_token_set(term)
        if not term_tokens:
            continue
        overlap = len(text_tokens.intersection(term_tokens)) / len(term_tokens)
        if overlap > 0:
            matching_terms += 1
        if overlap > strongest_overlap:
            strongest_overlap = overlap

    if exact_match:
        score = 1.0
    else:
        bonus = 0.15 if matching_terms >= 1 else 0.0
        score = min(0.85, max(0.0, strongest_overlap * 0.8 + bonus))

    if score >= 0.8:
        label = 'high'
    elif score >= 0.55:
        label = 'medium'
    elif score >= 0.3:
        label = 'low'
    else:
        label = 'none'
    return {
        'score': score,
        'label': label,
        'exact_match': bool(exact_match),
        'matching_terms': int(matching_terms),
        'strongest_overlap': float(round(strongest_overlap, 3)),
    }


def _quality_overrides_for_candidate(
    *,
    relevance_features: dict[str, object],
    evidence_pipeline_v2: bool,
) -> dict[str, object]:
    if not evidence_pipeline_v2:
        return {}
    if bool(relevance_features.get('exact_match')):
        return {}
    score = float(relevance_features.get('score') or 0.0)
    if score >= 0.55:
        return {
            'source_tier': 'trusted',
            'confidence_weight': 2,
            'source_type': 'feed_partial_match',
        }
    # Keep weakly-matched sources for traceability, but mark them low-confidence
    # so default notebook filters prioritize higher-attribution evidence.
    return {
        'source_tier': 'context',
        'confidence_weight': 1,
        'source_type': 'feed_soft_match',
    }


def _soft_match_reason_code(relevance_features: dict[str, object]) -> str:
    if float(relevance_features.get('score') or 0.0) >= 0.55:
        return 'actor_term_partial_match_soft'
    return 'actor_term_miss_soft'


def _is_high_signal_relevance(relevance_features: dict[str, object]) -> bool:
    return bool(relevance_features.get('exact_match')) or float(relevance_features.get('score') or 0.0) >= 0.55


def _entry_context_actor_overlap(*, entry_context: str, actor_terms: list[str]) -> float:
    context_tokens = _actor_term_token_set(entry_context)
    if not context_tokens:
        return 0.0
    strongest_overlap = 0.0
    for term in actor_terms:
        term_tokens = _actor_term_token_set(term)
        if not term_tokens:
            continue
        overlap = len(context_tokens.intersection(term_tokens)) / len(term_tokens)
        if overlap > strongest_overlap:
            strongest_overlap = overlap
    return float(strongest_overlap)


def _promote_relevance_from_entry_context(
    *,
    relevance_features: dict[str, object],
    entry_context_overlap: float,
) -> dict[str, object]:
    if float(entry_context_overlap) < 0.6:
        return relevance_features
    if _is_high_signal_relevance(relevance_features):
        return relevance_features
    promoted = dict(relevance_features)
    promoted['score'] = max(0.56, float(promoted.get('score') or 0.0))
    promoted['label'] = 'medium'
    promoted['promoted_from_entry_context'] = True
    return promoted


def _linkage_signal_score(combined_text: str) -> dict[str, object]:
    text = str(combined_text or '')
    lowered = text.lower()
    score = 0.0
    reasons: list[str] = []
    if re.search(r'\bT[0-9]{4}(?:\.[0-9]{3})?\b', text, flags=re.IGNORECASE):
        score += 0.25
        reasons.append('ttp_id')
    if re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', text):
        score += 0.2
        reasons.append('ip')
    if re.search(r'\b[a-fA-F0-9]{32}\b|\b[a-fA-F0-9]{40}\b|\b[a-fA-F0-9]{64}\b', text):
        score += 0.2
        reasons.append('hash')
    if re.search(r'\b(?:https?://[^\s]+|(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,})\b', text):
        score += 0.15
        reasons.append('domain_or_url')
    if re.search(r'\b(?:UNC[0-9]{3,5}|DEV-[0-9]{3,5}|TA[0-9]{4}|APT[0-9]{1,2}|FIN[0-9]{1,2})\b', text, flags=re.IGNORECASE):
        score += 0.2
        reasons.append('cluster_label')
    if re.search(r'\b(c2|command and control|beacon|loader|dropper|ransomware|intrusion|campaign)\b', lowered):
        score += 0.1
        reasons.append('attack_language')
    return {
        'score': min(1.0, round(score, 3)),
        'reasons': reasons,
    }


def _promote_relevance_from_linkage(
    *,
    relevance_features: dict[str, object],
    linkage_features: dict[str, object],
) -> dict[str, object]:
    linkage_score = float(linkage_features.get('score') or 0.0)
    if linkage_score < 0.5:
        return relevance_features
    if _is_high_signal_relevance(relevance_features):
        return relevance_features
    promoted = dict(relevance_features)
    promoted['score'] = max(0.56, float(promoted.get('score') or 0.0))
    promoted['label'] = 'medium'
    promoted['promoted_by_linkage'] = True
    promoted['linkage_reasons'] = [str(item) for item in linkage_features.get('reasons', [])][:6]
    return promoted


def _apply_source_trust_boost(
    *,
    relevance_features: dict[str, object],
    source_url: str,
    source_trust_score: Callable[[str], int] | None,
) -> dict[str, object]:
    if not callable(source_trust_score):
        return relevance_features
    trust_score = int(source_trust_score(str(source_url or '')) or 0)
    if trust_score <= 0:
        return relevance_features
    if _is_high_signal_relevance(relevance_features):
        return relevance_features
    current_score = float(relevance_features.get('score') or 0.0)
    promoted = dict(relevance_features)
    if trust_score >= 4 and current_score >= 0.2:
        promoted['score'] = max(current_score, 0.56)
        promoted['label'] = 'medium'
        promoted['promoted_by_trust'] = True
    elif trust_score >= 3 and current_score >= 0.25:
        promoted['score'] = max(current_score, 0.45)
        promoted['label'] = 'low'
        promoted['promoted_by_trust'] = True
    return promoted


def _corroboration_keys_from_text(*, source_url: str, text: str) -> set[str]:
    keys: set[str] = set()
    raw = str(text or '')
    if not raw:
        return keys
    for value in re.findall(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', raw):
        keys.add(f'ip:{value}')
    for value in re.findall(r'\b[a-fA-F0-9]{32}\b|\b[a-fA-F0-9]{40}\b|\b[a-fA-F0-9]{64}\b', raw):
        keys.add(f'hash:{value.lower()}')
    for value in re.findall(r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b', raw):
        normalized = str(value or '').strip('.').lower()
        if normalized and len(normalized) <= 253 and not normalized.startswith('www.'):
            keys.add(f'domain:{normalized}')
    try:
        host = (urlparse(str(source_url or '')).hostname or '').strip('.').lower()
    except Exception:
        host = ''
    if host:
        keys.add(f'host:{host}')
    return keys


def _promote_soft_sources_from_corroboration(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    now_iso: str,
    record_decision: Callable[..., None],
    parse_published_datetime: Callable[[str | None], datetime | None],
    lookback_days: int,
) -> int:
    try:
        rows = connection.execute(
            '''
            SELECT id, url, title, headline, og_title, html_title, pasted_text, published_at, retrieved_at
            FROM sources
            WHERE actor_id = ?
              AND COALESCE(source_type, '') = 'feed_soft_match'
            ''',
            (actor_id,),
        ).fetchall()
    except Exception:
        return 0
    if not rows:
        return 0
    now_utc = datetime.now(timezone.utc)
    key_to_sources: dict[str, set[str]] = {}
    for row in rows:
        source_id = str(row[0] or '').strip()
        if not source_id:
            continue
        published_value = str(row[7] or row[8] or '').strip()
        if published_value:
            published_dt = parse_published_datetime(published_value)
            if published_dt is not None and (now_utc - published_dt) > timedelta(days=max(1, int(lookback_days))):
                continue
        combined_text = ' '.join(
            str(item or '')
            for item in (row[2], row[3], row[4], row[5], row[6])
        )
        keys = _corroboration_keys_from_text(source_url=str(row[1] or ''), text=combined_text)
        for key in keys:
            key_to_sources.setdefault(key, set()).add(source_id)
    promotable_ids: set[str] = set()
    supporting_keys: list[str] = []
    for key, ids in key_to_sources.items():
        if len(ids) >= 2:
            promotable_ids.update(ids)
            supporting_keys.append(key)
    if not promotable_ids:
        return 0
    placeholders = ','.join('?' for _ in promotable_ids)
    params = ['feed_partial_match', 'trusted', 2, actor_id, *sorted(promotable_ids)]
    cursor = connection.execute(
        f'''
        UPDATE sources
        SET source_type = ?,
            source_tier = ?,
            confidence_weight = ?
        WHERE actor_id = ?
          AND id IN ({placeholders})
          AND COALESCE(source_type, '') = 'feed_soft_match'
        ''',
        tuple(params),
    )
    promoted_count = max(0, int(cursor.rowcount or 0))
    if promoted_count > 0:
        sample_keys = supporting_keys[:5]
        for source_id in sorted(promotable_ids):
            record_decision(
                connection,
                actor_id=actor_id,
                source_id=source_id,
                stage='score',
                decision='accepted',
                reason_code='soft_candidate_promoted_corroborated',
                details={
                    'supporting_key_count': len(supporting_keys),
                    'supporting_keys_sample': sample_keys,
                },
                now_iso=now_iso,
            )
    return promoted_count


def _decision_details_with_relevance(
    *,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> dict[str, object]:
    return {
        'feed_name': feed_name,
        'url': url,
        'relevance_score': float(relevance_features.get('score') or 0.0),
        'relevance_label': str(relevance_features.get('label') or ''),
        'exact_match': bool(relevance_features.get('exact_match')),
        'matching_terms': int(relevance_features.get('matching_terms') or 0),
        'strongest_overlap': float(relevance_features.get('strongest_overlap') or 0.0),
    }


def _should_reject_candidate(*, relevance_features: dict[str, object], evidence_pipeline_v2: bool) -> bool:
    return (not bool(relevance_features.get('exact_match'))) and (not evidence_pipeline_v2)


def _record_soft_match_acceptance(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    if bool(relevance_features.get('exact_match')):
        return
    record_decision(
        connection,
        actor_id=actor_id,
        stage='score',
        decision='accepted',
        reason_code=_soft_match_reason_code(relevance_features),
        details=_decision_details_with_relevance(
            feed_name=feed_name,
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_hard_rejection(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='score',
        decision='rejected',
        reason_code='actor_term_miss',
        details=_decision_details_with_relevance(
            feed_name=feed_name,
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_source_upserted(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='acquire_feed',
        decision='accepted',
        reason_code='source_upserted',
        details=_decision_details_with_relevance(
            feed_name=feed_name,
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_search_source_upserted(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='acquire_search',
        decision='accepted',
        reason_code='source_upserted',
        details=_decision_details_with_relevance(
            feed_name='Actor Search',
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_missing_published_rejection(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='resolve',
        decision='rejected',
        reason_code='missing_published_at',
        details={'feed_name': feed_name, 'url': url},
        now_iso=now_iso,
    )


def _record_feed_fetch_failure(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    feed_url: str,
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='acquire_feed',
        decision='rejected',
        reason_code='feed_fetch_failed',
        details={'feed_name': feed_name, 'feed_url': feed_url},
        now_iso=now_iso,
    )


def _quality_from_relevance(
    *,
    relevance_features: dict[str, object],
    evidence_pipeline_v2: bool,
) -> dict[str, object]:
    return _quality_overrides_for_candidate(
        relevance_features=relevance_features,
        evidence_pipeline_v2=evidence_pipeline_v2,
    )


def _should_record_soft_match(*, relevance_features: dict[str, object], evidence_pipeline_v2: bool) -> bool:
    return evidence_pipeline_v2 and not bool(relevance_features.get('exact_match'))


def _should_reject_on_missing_published(*, feed_require_published_at: bool, resolved_published: str) -> bool:
    return bool(feed_require_published_at and not resolved_published)


def _should_skip_google_wrapper(*, link: str, resolved_source_url: str) -> bool:
    return _is_google_news_wrapper_url(link) and _is_google_news_wrapper_url(resolved_source_url)


def _search_feed_name() -> str:
    return 'Actor Search'


def _record_search_soft_acceptance(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    if bool(relevance_features.get('exact_match')):
        return
    record_decision(
        connection,
        actor_id=actor_id,
        stage='score',
        decision='accepted',
        reason_code=_soft_match_reason_code(relevance_features),
        details=_decision_details_with_relevance(
            feed_name=_search_feed_name(),
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_search_hard_rejection(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    record_decision(
        connection,
        actor_id=actor_id,
        stage='score',
        decision='rejected',
        reason_code='actor_term_miss',
        details=_decision_details_with_relevance(
            feed_name=_search_feed_name(),
            url=url,
            relevance_features=relevance_features,
        ),
        now_iso=now_iso,
    )


def _record_search_missing_published_rejection(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    url: str,
) -> None:
    _record_missing_published_rejection(
        connection=connection,
        record_decision=record_decision,
        actor_id=actor_id,
        now_iso=now_iso,
        feed_name=_search_feed_name(),
        url=url,
    )


def _resolve_candidate_relevance(
    *,
    combined_text: str,
    actor_terms: list[str],
    text_contains_actor_term: Callable[[str, list[str]], bool],
) -> dict[str, object]:
    return _actor_relevance_features(
        combined_text=combined_text,
        actor_terms=actor_terms,
        text_contains_actor_term=text_contains_actor_term,
    )


def _record_feed_soft_acceptance(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    _record_soft_match_acceptance(
        connection=connection,
        record_decision=record_decision,
        actor_id=actor_id,
        now_iso=now_iso,
        feed_name=feed_name,
        url=url,
        relevance_features=relevance_features,
    )


def _record_feed_hard_rejection(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    _record_hard_rejection(
        connection=connection,
        record_decision=record_decision,
        actor_id=actor_id,
        now_iso=now_iso,
        feed_name=feed_name,
        url=url,
        relevance_features=relevance_features,
    )


def _record_feed_acceptance(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    feed_name: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    _record_source_upserted(
        connection=connection,
        record_decision=record_decision,
        actor_id=actor_id,
        now_iso=now_iso,
        feed_name=feed_name,
        url=url,
        relevance_features=relevance_features,
    )


def _record_search_acceptance(
    *,
    connection: sqlite3.Connection,
    record_decision: Callable[..., None],
    actor_id: str,
    now_iso: str,
    url: str,
    relevance_features: dict[str, object],
) -> None:
    _record_search_source_upserted(
        connection=connection,
        record_decision=record_decision,
        actor_id=actor_id,
        now_iso=now_iso,
        url=url,
        relevance_features=relevance_features,
    )
    return {}


def import_default_feeds_for_actor_core(
    actor_id: str,
    *,
    db_path: str,
    default_cti_feeds: list[tuple[str, str]],
    primary_cti_feeds: list[tuple[str, str]] | None = None,
    secondary_context_feeds: list[tuple[str, str]] | None = None,
    actor_feed_lookback_days: int,
    feed_import_max_seconds: int = 90,
    feed_fetch_timeout_seconds: float = 10.0,
    feed_entry_scan_limit: int = 12,
    feed_imported_limit: int = 30,
    feed_soft_match_limit: int = 0,
    import_mode: str = 'background',
    high_signal_target: int = 2,
    retain_soft_candidates: bool = False,
    actor_search_link_limit: int = 6,
    feed_require_published_at: bool = True,
    evidence_pipeline_v2: bool = False,
    deps: dict[str, object],
) -> int:
    _actor_exists = deps['actor_exists']
    _build_actor_profile_from_mitre = deps['build_actor_profile_from_mitre']
    _actor_terms = deps['actor_terms']
    _actor_query_feeds = deps['actor_query_feeds']
    _import_ransomware_live_actor_activity = deps['import_ransomware_live_actor_activity']
    _safe_http_get = deps['safe_http_get']
    _parse_feed_entries = deps['parse_feed_entries']
    _text_contains_actor_term = deps['text_contains_actor_term']
    _within_lookback = deps['within_lookback']
    _parse_published_datetime = deps.get('parse_published_datetime', lambda _value: None)
    _derive_source_from_url = deps['derive_source_from_url']
    _upsert_source_for_actor = deps['upsert_source_for_actor']
    _duckduckgo_actor_search_urls = deps['duckduckgo_actor_search_urls']
    _utc_now_iso = deps.get('utc_now_iso', lambda: datetime.now(timezone.utc).isoformat())
    _record_decision = deps.get('record_ingest_decision', _record_ingest_decision)
    _source_trust_score = deps.get('source_trust_score')

    imported = 0
    high_signal_imported = 0
    soft_match_imported = 0
    interactive_mode = str(import_mode or 'background').strip().lower() in {'interactive', 'fast', 'manual'}
    high_signal_goal = max(1, int(high_signal_target))
    soft_match_cap = max(0, int(feed_soft_match_limit))
    effective_entry_scan_limit = max(5, int(feed_entry_scan_limit))
    effective_fetch_timeout_seconds = max(2.0, float(feed_fetch_timeout_seconds))
    effective_derive_timeout_seconds = 20.0
    started_at = time.perf_counter()
    deadline = started_at + float(max(20, int(feed_import_max_seconds)))
    search_stage_reserve_seconds = min(25.0, max(8.0, float(max(1, int(actor_search_link_limit)))))
    if interactive_mode:
        search_stage_reserve_seconds = max(35.0, search_stage_reserve_seconds)
        effective_entry_scan_limit = max(5, min(effective_entry_scan_limit, 6))
        effective_fetch_timeout_seconds = min(effective_fetch_timeout_seconds, 5.0)
        effective_derive_timeout_seconds = 8.0
    elif evidence_pipeline_v2:
        search_stage_reserve_seconds = max(30.0, search_stage_reserve_seconds)
        if soft_match_cap <= 0:
            effective_entry_scan_limit = max(5, min(effective_entry_scan_limit, 8))
    effective_soft_match_cap = soft_match_cap
    if evidence_pipeline_v2 and retain_soft_candidates and (not interactive_mode) and effective_soft_match_cap <= 0:
        effective_soft_match_cap = max(12, int(max(10, int(feed_imported_limit)) * 2))
    with sqlite3.connect(db_path) as connection:
        _ensure_actor_feed_state_schema(connection)
        if not _actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')

        actor_row = connection.execute(
            'SELECT display_name FROM actor_profiles WHERE id = ?',
            (actor_id,),
        ).fetchone()
        actor_name = str(actor_row[0] if actor_row else '')
        mitre_profile = _build_actor_profile_from_mitre(actor_name)
        actor_terms = _actor_terms(
            actor_name,
            str(mitre_profile.get('group_name') or ''),
            str(mitre_profile.get('aliases_csv') or ''),
        )

        primary_feeds = list(primary_cti_feeds) if primary_cti_feeds is not None else list(default_cti_feeds)
        secondary_feeds = list(secondary_context_feeds) if secondary_context_feeds is not None else []
        actor_query_feeds = _actor_query_feeds(actor_terms)
        actor_query_feed_keys = {(name, url) for name, url in actor_query_feeds}
        if interactive_mode:
            primary_feeds = primary_feeds[: max(4, min(8, len(primary_feeds)))]
            secondary_feeds = []
        secondary_feed_keys = {(name, url) for name, url in secondary_feeds}
        secondary_import_cap = max(3, int(max(10, int(feed_imported_limit)) * 0.35))
        secondary_imported = 0
        feed_state = _load_actor_feed_state(connection, actor_id)
        primary_and_query = actor_query_feeds + primary_feeds
        prioritized_primary_and_query = sorted(
            primary_and_query,
            key=lambda feed: _feed_priority_key(feed, feed_state),
        )
        prioritized_secondary = sorted(
            secondary_feeds,
            key=lambda feed: _feed_priority_key(feed, feed_state),
        )
        feed_list = prioritized_primary_and_query + prioritized_secondary
        seen_links: set[str] = set()

        ransomware_imported = int(_import_ransomware_live_actor_activity(connection, actor_id, actor_terms) or 0)
        imported += ransomware_imported
        high_signal_imported += ransomware_imported

        for feed_name, feed_url in feed_list:
            remaining_time = deadline - time.perf_counter()
            if remaining_time <= max(1.0, search_stage_reserve_seconds):
                break
            is_secondary_feed = (feed_name, feed_url) in secondary_feed_keys
            is_actor_query_feed = (feed_name, feed_url) in actor_query_feed_keys
            if is_secondary_feed and secondary_imported >= secondary_import_cap:
                continue
            state_key = (feed_name, feed_url)
            state = dict(feed_state.get(state_key, {}))
            now_utc = datetime.now(timezone.utc)
            now_iso = _utc_now_iso()
            if _feed_backoff_active(state, now_utc):
                continue
            checkpoint_dt = _parse_published_datetime(str(state.get('last_success_published_at') or '').strip())
            try:
                remaining_seconds = max(1.0, deadline - time.perf_counter())
                entries = _acquire_feed_entries(
                    safe_http_get=_safe_http_get,
                    parse_feed_entries=_parse_feed_entries,
                    feed_url=feed_url,
                    timeout_seconds=min(float(effective_fetch_timeout_seconds), float(remaining_seconds)),
                )
            except Exception:
                failure_state = {
                    'last_checked_at': now_iso,
                    'last_success_at': state.get('last_success_at'),
                    'last_success_published_at': state.get('last_success_published_at'),
                    'last_imported_count': 0,
                    'total_imported': int(state.get('total_imported') or 0),
                    'consecutive_failures': int(state.get('consecutive_failures') or 0) + 1,
                    'total_failures': int(state.get('total_failures') or 0) + 1,
                    'last_error': 'feed fetch failed',
                }
                _upsert_actor_feed_state(connection, actor_id, feed_name, feed_url, failure_state)
                feed_state[state_key] = failure_state
                if evidence_pipeline_v2:
                    _record_decision(
                        connection,
                        actor_id=actor_id,
                        stage='acquire_feed',
                        decision='rejected',
                        reason_code='feed_fetch_failed',
                        details={'feed_name': feed_name, 'feed_url': feed_url},
                        now_iso=now_iso,
                    )
                continue

            prioritized = sorted(
                entries,
                key=lambda entry: 0 if _text_contains_actor_term(
                    f'{entry.get("title") or ""} {entry.get("link") or ""}',
                    actor_terms,
                ) else 1,
            )
            imported_from_feed = 0
            latest_imported_published_dt: datetime | None = None

            for entry in prioritized[:effective_entry_scan_limit]:
                if time.perf_counter() >= deadline:
                    break
                link = entry.get('link')
                if not link:
                    continue
                if link in seen_links:
                    continue
                if not _within_lookback(entry.get('published_at'), actor_feed_lookback_days):
                    continue
                entry_published_dt = _parse_published_datetime(entry.get('published_at'))
                if checkpoint_dt is not None and entry_published_dt is not None and entry_published_dt <= checkpoint_dt:
                    continue
                title_text = str(entry.get('title') or '')
                entry_context = f'{title_text} {link}'
                seen_links.add(link)
                if (
                    evidence_pipeline_v2
                    and soft_match_cap <= 0
                    and is_secondary_feed
                    and actor_terms
                    and (not _text_contains_actor_term(entry_context, actor_terms))
                ):
                    _record_decision(
                        connection,
                        actor_id=actor_id,
                        stage='score',
                        decision='rejected',
                        reason_code='entry_context_actor_miss_secondary',
                        details={'feed_name': feed_name, 'url': link},
                        now_iso=now_iso,
                    )
                    continue
                # Actor-query feeds (targeted Google News RSS searches) return
                # wrapper links that can't be resolved via HTTP. Instead of
                # discarding these entries, store them using the RSS title and
                # publisher info extracted from the <source url="..."> attribute.
                # The title alone is sufficient for LLM notebook context.
                if is_actor_query_feed and _is_google_news_wrapper_url(link):
                    source_domain_hint = str(entry.get('source_domain') or '').strip()
                    source_name_hint = str(entry.get('source_name') or feed_name).strip()
                    fallback_published = str(entry.get('published_at') or '').strip()
                    if not fallback_published and feed_require_published_at:
                        pass  # skip if published_at is required but missing
                    elif title_text:
                        try:
                            _upsert_source_for_actor(
                                connection,
                                actor_id,
                                source_name_hint,
                                link,  # wrapper URL is unique per article
                                fallback_published or None,
                                title_text,
                                title_text,
                                title_text,
                                title_text,
                                title_text,
                                title_text,
                                source_name_hint,
                                source_name_hint,
                            )
                            imported += 1
                            imported_from_feed += 1
                            high_signal_imported += 1
                            fallback_dt = _parse_published_datetime(fallback_published or None)
                            if fallback_dt is not None and (
                                latest_imported_published_dt is None
                                or fallback_dt > latest_imported_published_dt
                            ):
                                latest_imported_published_dt = fallback_dt
                        except Exception:
                            pass
                    continue
                try:
                    remaining_seconds = max(1.0, deadline - time.perf_counter())
                    derived = _resolve_source_from_link(
                        derive_source_from_url=_derive_source_from_url,
                        link=link,
                        fallback_source_name=feed_name,
                        published_hint=entry.get('published_at'),
                        timeout_seconds=min(float(effective_derive_timeout_seconds), float(remaining_seconds)),
                    )
                    combined_text = (
                        f'{entry.get("title") or ""} '
                        f'{derived.get("source_name") or ""} '
                        f'{derived.get("source_url") or ""} '
                        f'{derived.get("pasted_text") or ""}'
                    )
                    relevance_features = _resolve_candidate_relevance(
                        combined_text=combined_text,
                        actor_terms=actor_terms,
                        text_contains_actor_term=_text_contains_actor_term,
                    )
                    entry_context_overlap = _entry_context_actor_overlap(
                        entry_context=entry_context,
                        actor_terms=actor_terms,
                    )
                    relevance_features = _promote_relevance_from_entry_context(
                        relevance_features=relevance_features,
                        entry_context_overlap=entry_context_overlap,
                    )
                    linkage_features = _linkage_signal_score(combined_text)
                    relevance_features = _promote_relevance_from_linkage(
                        relevance_features=relevance_features,
                        linkage_features=linkage_features,
                    )
                    relevance_features = _apply_source_trust_boost(
                        relevance_features=relevance_features,
                        source_url=str(derived.get('source_url') or link),
                        source_trust_score=_source_trust_score if callable(_source_trust_score) else None,
                    )
                    is_high_signal = _is_high_signal_relevance(relevance_features)
                    if _should_reject_candidate(
                        relevance_features=relevance_features,
                        evidence_pipeline_v2=evidence_pipeline_v2,
                    ):
                        _record_feed_hard_rejection(
                            connection=connection,
                            record_decision=_record_decision,
                            actor_id=actor_id,
                            now_iso=now_iso,
                            feed_name=feed_name,
                            url=str(derived.get('source_url') or link),
                            relevance_features=relevance_features,
                        )
                        continue
                    if evidence_pipeline_v2 and (not is_high_signal):
                        if effective_soft_match_cap <= 0 or soft_match_imported >= effective_soft_match_cap:
                            _record_decision(
                                connection,
                                actor_id=actor_id,
                                stage='score',
                                decision='rejected',
                                reason_code='soft_match_cap_exceeded',
                                details=_decision_details_with_relevance(
                                    feed_name=feed_name,
                                    url=str(derived.get('source_url') or link),
                                    relevance_features=relevance_features,
                                ),
                                now_iso=now_iso,
                            )
                            continue
                    if _should_record_soft_match(
                        relevance_features=relevance_features,
                        evidence_pipeline_v2=evidence_pipeline_v2,
                    ):
                        _record_feed_soft_acceptance(
                            connection=connection,
                            record_decision=_record_decision,
                            actor_id=actor_id,
                            now_iso=now_iso,
                            feed_name=feed_name,
                            url=str(derived.get('source_url') or link),
                            relevance_features=relevance_features,
                        )
                    resolved_source_url = str(derived.get('source_url') or '').strip()
                    if _should_skip_google_wrapper(link=link, resolved_source_url=resolved_source_url):
                        continue
                    resolved_published = str(derived.get('published_at') or '').strip() or (
                        str(entry.get('published_at') or '').strip()
                    )
                    if _should_reject_on_missing_published(
                        feed_require_published_at=feed_require_published_at,
                        resolved_published=resolved_published,
                    ):
                        if evidence_pipeline_v2:
                            _record_missing_published_rejection(
                                connection=connection,
                                record_decision=_record_decision,
                                actor_id=actor_id,
                                now_iso=now_iso,
                                feed_name=feed_name,
                                url=resolved_source_url or link,
                            )
                        continue
                    if not _within_lookback(resolved_published or None, actor_feed_lookback_days):
                        continue
                    resolved_title = str(derived.get('title') or title_text or '').strip() or None
                    resolved_headline = str(derived.get('headline') or title_text or '').strip() or None
                    resolved_og_title = str(derived.get('og_title') or title_text or '').strip() or None
                    resolved_html_title = str(derived.get('html_title') or title_text or '').strip() or None
                    quality_overrides = _quality_from_relevance(
                        relevance_features=relevance_features,
                        evidence_pipeline_v2=evidence_pipeline_v2,
                    )
                    source_id = _upsert_source_for_actor(
                        connection,
                        actor_id,
                        str(derived['source_name']),
                        str(derived['source_url']),
                        resolved_published or None,
                        str(derived['pasted_text']),
                        str(derived['trigger_excerpt']) if derived['trigger_excerpt'] else None,
                        resolved_title,
                        resolved_headline,
                        resolved_og_title,
                        resolved_html_title,
                        str(derived.get('publisher') or '') or None,
                        str(derived.get('site_name') or '') or None,
                        quality_overrides.get('source_tier'),
                        quality_overrides.get('confidence_weight'),
                        quality_overrides.get('source_type'),
                    )
                    if evidence_pipeline_v2:
                        match_reasons: list[str] = []
                        if bool(relevance_features.get('exact_match')):
                            match_reasons.append('actor_term_exact')
                        if bool(relevance_features.get('promoted_from_entry_context')):
                            match_reasons.append('entry_context_overlap')
                        if bool(relevance_features.get('promoted_by_trust')):
                            match_reasons.append('trusted_domain_boost')
                        if bool(relevance_features.get('promoted_by_linkage')):
                            match_reasons.append('technical_linkage')
                        if not match_reasons:
                            match_reasons.append('actor_term_partial')
                        matched_terms = [
                            term for term in actor_terms if term and term.lower() in combined_text.lower()
                        ][:8]
                        trust_score_value = (
                            int(_source_trust_score(str(derived.get('source_url') or link)) or 0)
                            if callable(_source_trust_score)
                            else 0
                        )
                        source_evidence_service.persist_source_evidence_core(
                            connection,
                            source_id=str(source_id),
                            actor_id=actor_id,
                            source_url=str(derived.get('source_url') or link),
                            source_text=str(derived.get('pasted_text') or ''),
                            raw_html=str(derived.get('raw_html') or ''),
                            fetched_at=now_iso,
                            published_at=resolved_published or None,
                            http_status=(
                                int(derived.get('http_status'))
                                if str(derived.get('http_status') or '').strip().isdigit()
                                else None
                            ),
                            content_type=str(derived.get('content_type') or ''),
                            parse_status=str(derived.get('parse_status') or 'parsed'),
                            parse_error=str(derived.get('parse_error') or ''),
                            actor_terms=actor_terms,
                            relevance_score=float(relevance_features.get('score') or 0.0),
                            match_type=(
                                'exact_actor_term'
                                if bool(relevance_features.get('exact_match'))
                                else 'soft_actor_match'
                            ),
                            match_reasons=match_reasons,
                            matched_terms=matched_terms,
                            source_trust_score=trust_score_value,
                            novelty_score=0.5,
                            extractor='feed_ingest_v2',
                        )
                    imported += 1
                    if evidence_pipeline_v2:
                        _record_feed_acceptance(
                            connection=connection,
                            record_decision=_record_decision,
                            actor_id=actor_id,
                            now_iso=now_iso,
                            feed_name=feed_name,
                            url=str(derived.get('source_url') or link),
                            relevance_features=relevance_features,
                        )
                    imported_from_feed += 1
                    if is_high_signal:
                        high_signal_imported += 1
                    else:
                        soft_match_imported += 1
                    if is_secondary_feed:
                        secondary_imported += 1
                    resolved_dt = _parse_published_datetime(resolved_published or None)
                    if resolved_dt is not None and (
                        latest_imported_published_dt is None or resolved_dt > latest_imported_published_dt
                    ):
                        latest_imported_published_dt = resolved_dt
                    if high_signal_imported >= max(10, int(feed_imported_limit)) or (
                        interactive_mode and high_signal_imported >= high_signal_goal
                    ):
                        updated_state = {
                            'last_checked_at': now_iso,
                            'last_success_at': now_iso,
                            'last_success_published_at': (
                                latest_imported_published_dt.isoformat()
                                if latest_imported_published_dt is not None
                                else state.get('last_success_published_at')
                            ),
                            'last_imported_count': imported_from_feed,
                            'total_imported': int(state.get('total_imported') or 0) + imported_from_feed,
                            'consecutive_failures': 0,
                            'total_failures': int(state.get('total_failures') or 0),
                            'last_error': None,
                        }
                        _upsert_actor_feed_state(connection, actor_id, feed_name, feed_url, updated_state)
                        feed_state[state_key] = updated_state
                        connection.commit()
                        return imported
                except Exception:
                    if actor_terms and _text_contains_actor_term(entry_context, actor_terms):
                        if _is_google_news_wrapper_url(link):
                            continue
                        fallback_published = str(entry.get('published_at') or '').strip()
                        if feed_require_published_at and not fallback_published:
                            continue
                        try:
                            _upsert_source_for_actor(
                                connection,
                                actor_id,
                                feed_name,
                                link,
                                fallback_published or None,
                                title_text or f'Actor-matched feed item from {feed_name}.',
                                title_text or None,
                                title_text or None,
                                title_text or None,
                                title_text or None,
                                title_text or None,
                                None,
                                feed_name,
                            )
                            imported += 1
                            imported_from_feed += 1
                            high_signal_imported += 1
                            if is_secondary_feed:
                                secondary_imported += 1
                            fallback_dt = _parse_published_datetime(fallback_published or None)
                            if fallback_dt is not None and (
                                latest_imported_published_dt is None or fallback_dt > latest_imported_published_dt
                            ):
                                latest_imported_published_dt = fallback_dt
                            if high_signal_imported >= max(10, int(feed_imported_limit)) or (
                                interactive_mode and high_signal_imported >= high_signal_goal
                            ):
                                updated_state = {
                                    'last_checked_at': now_iso,
                                    'last_success_at': now_iso,
                                    'last_success_published_at': (
                                        latest_imported_published_dt.isoformat()
                                        if latest_imported_published_dt is not None
                                        else state.get('last_success_published_at')
                                    ),
                                    'last_imported_count': imported_from_feed,
                                    'total_imported': int(state.get('total_imported') or 0) + imported_from_feed,
                                    'consecutive_failures': 0,
                                    'total_failures': int(state.get('total_failures') or 0),
                                    'last_error': None,
                                }
                                _upsert_actor_feed_state(connection, actor_id, feed_name, feed_url, updated_state)
                                feed_state[state_key] = updated_state
                                connection.commit()
                                return imported
                        except Exception:
                            pass
                    continue
            updated_state = {
                'last_checked_at': now_iso,
                'last_success_at': now_iso,
                'last_success_published_at': (
                    latest_imported_published_dt.isoformat()
                    if latest_imported_published_dt is not None
                    else state.get('last_success_published_at')
                ),
                'last_imported_count': imported_from_feed,
                'total_imported': int(state.get('total_imported') or 0) + imported_from_feed,
                'consecutive_failures': 0,
                'total_failures': int(state.get('total_failures') or 0),
                'last_error': None,
            }
            _upsert_actor_feed_state(connection, actor_id, feed_name, feed_url, updated_state)
            feed_state[state_key] = updated_state

        if time.perf_counter() < deadline:
            actor_search_urls = _duckduckgo_actor_search_urls(
                actor_terms,
                limit=max(1, int(actor_search_link_limit)),
            )
        else:
            actor_search_urls = []
        for link in actor_search_urls:
            if time.perf_counter() >= deadline:
                break
            if link in seen_links:
                continue
            seen_links.add(link)
            try:
                remaining_seconds = max(1.0, deadline - time.perf_counter())
                derived = _resolve_source_from_link(
                    derive_source_from_url=_derive_source_from_url,
                    link=link,
                    fallback_source_name='Actor Search',
                    timeout_seconds=min(float(effective_derive_timeout_seconds), float(remaining_seconds)),
                )
                combined_text = (
                    f'{derived.get("source_name") or ""} '
                    f'{derived.get("source_url") or ""} '
                    f'{derived.get("pasted_text") or ""}'
                )
                relevance_features = _resolve_candidate_relevance(
                    combined_text=combined_text,
                    actor_terms=actor_terms,
                    text_contains_actor_term=_text_contains_actor_term,
                )
                linkage_features = _linkage_signal_score(combined_text)
                relevance_features = _promote_relevance_from_linkage(
                    relevance_features=relevance_features,
                    linkage_features=linkage_features,
                )
                relevance_features = _apply_source_trust_boost(
                    relevance_features=relevance_features,
                    source_url=str(derived.get('source_url') or link),
                    source_trust_score=_source_trust_score if callable(_source_trust_score) else None,
                )
                is_high_signal = _is_high_signal_relevance(relevance_features)
                now_iso = _utc_now_iso()
                if _should_reject_candidate(
                    relevance_features=relevance_features,
                    evidence_pipeline_v2=evidence_pipeline_v2,
                ):
                    _record_search_hard_rejection(
                        connection=connection,
                        record_decision=_record_decision,
                        actor_id=actor_id,
                        now_iso=now_iso,
                        url=str(derived.get('source_url') or link),
                        relevance_features=relevance_features,
                    )
                    continue
                if evidence_pipeline_v2 and (not is_high_signal):
                    if effective_soft_match_cap <= 0 or soft_match_imported >= effective_soft_match_cap:
                        _record_decision(
                            connection,
                            actor_id=actor_id,
                            stage='score',
                            decision='rejected',
                            reason_code='soft_match_cap_exceeded',
                            details=_decision_details_with_relevance(
                                feed_name=_search_feed_name(),
                                url=str(derived.get('source_url') or link),
                                relevance_features=relevance_features,
                            ),
                            now_iso=now_iso,
                        )
                        continue
                if _should_record_soft_match(
                    relevance_features=relevance_features,
                    evidence_pipeline_v2=evidence_pipeline_v2,
                ):
                    _record_search_soft_acceptance(
                        connection=connection,
                        record_decision=_record_decision,
                        actor_id=actor_id,
                        now_iso=now_iso,
                        url=str(derived.get('source_url') or link),
                        relevance_features=relevance_features,
                    )
                quality_overrides = _quality_from_relevance(
                    relevance_features=relevance_features,
                    evidence_pipeline_v2=evidence_pipeline_v2,
                )
                resolved_search_published = str(derived.get('published_at') or '').strip()
                if _should_reject_on_missing_published(
                    feed_require_published_at=feed_require_published_at,
                    resolved_published=resolved_search_published,
                ):
                    if evidence_pipeline_v2:
                        _record_search_missing_published_rejection(
                            connection=connection,
                            record_decision=_record_decision,
                            actor_id=actor_id,
                            now_iso=now_iso,
                            url=str(derived.get('source_url') or link),
                        )
                    continue
                if not _within_lookback(resolved_search_published or None, actor_feed_lookback_days):
                    continue
                source_id = _upsert_source_for_actor(
                    connection,
                    actor_id,
                    str(derived['source_name']),
                    str(derived['source_url']),
                    resolved_search_published or None,
                    str(derived['pasted_text']),
                    str(derived['trigger_excerpt']) if derived['trigger_excerpt'] else None,
                    str(derived.get('title') or '') or None,
                    str(derived.get('headline') or '') or None,
                    str(derived.get('og_title') or '') or None,
                    str(derived.get('html_title') or '') or None,
                    str(derived.get('publisher') or '') or None,
                    str(derived.get('site_name') or '') or None,
                    quality_overrides.get('source_tier'),
                    quality_overrides.get('confidence_weight'),
                    quality_overrides.get('source_type'),
                )
                if evidence_pipeline_v2:
                    match_reasons: list[str] = []
                    if bool(relevance_features.get('exact_match')):
                        match_reasons.append('actor_term_exact')
                    if bool(relevance_features.get('promoted_by_trust')):
                        match_reasons.append('trusted_domain_boost')
                    if bool(relevance_features.get('promoted_by_linkage')):
                        match_reasons.append('technical_linkage')
                    if not match_reasons:
                        match_reasons.append('actor_term_partial')
                    matched_terms = [
                        term for term in actor_terms if term and term.lower() in combined_text.lower()
                    ][:8]
                    trust_score_value = (
                        int(_source_trust_score(str(derived.get('source_url') or link)) or 0)
                        if callable(_source_trust_score)
                        else 0
                    )
                    source_evidence_service.persist_source_evidence_core(
                        connection,
                        source_id=str(source_id),
                        actor_id=actor_id,
                        source_url=str(derived.get('source_url') or link),
                        source_text=str(derived.get('pasted_text') or ''),
                        raw_html=str(derived.get('raw_html') or ''),
                        fetched_at=now_iso,
                        published_at=resolved_search_published or None,
                        http_status=(
                            int(derived.get('http_status'))
                            if str(derived.get('http_status') or '').strip().isdigit()
                            else None
                        ),
                        content_type=str(derived.get('content_type') or ''),
                        parse_status=str(derived.get('parse_status') or 'parsed'),
                        parse_error=str(derived.get('parse_error') or ''),
                        actor_terms=actor_terms,
                        relevance_score=float(relevance_features.get('score') or 0.0),
                        match_type=(
                            'exact_actor_term'
                            if bool(relevance_features.get('exact_match'))
                            else 'soft_actor_match'
                        ),
                        match_reasons=match_reasons,
                        matched_terms=matched_terms,
                        source_trust_score=trust_score_value,
                        novelty_score=0.5,
                        extractor='feed_search_v2',
                    )
                imported += 1
                if evidence_pipeline_v2:
                    _record_search_acceptance(
                        connection=connection,
                        record_decision=_record_decision,
                        actor_id=actor_id,
                        now_iso=now_iso,
                        url=str(derived.get('source_url') or link),
                        relevance_features=relevance_features,
                    )
                if is_high_signal:
                    high_signal_imported += 1
                else:
                    soft_match_imported += 1
                if high_signal_imported >= max(10, int(feed_imported_limit)) or (
                    interactive_mode and high_signal_imported >= high_signal_goal
                ):
                    connection.commit()
                    return imported
            except Exception:
                continue

        if evidence_pipeline_v2 and retain_soft_candidates and (not interactive_mode):
            _promote_soft_sources_from_corroboration(
                connection,
                actor_id=actor_id,
                now_iso=_utc_now_iso(),
                record_decision=_record_decision,
                parse_published_datetime=_parse_published_datetime,
                lookback_days=int(actor_feed_lookback_days),
            )
        connection.commit()
    return imported
