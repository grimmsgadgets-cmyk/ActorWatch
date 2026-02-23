import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Callable
from urllib.parse import urlparse

from fastapi import HTTPException


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


def import_default_feeds_for_actor_core(
    actor_id: str,
    *,
    db_path: str,
    default_cti_feeds: list[tuple[str, str]],
    actor_feed_lookback_days: int,
    feed_import_max_seconds: int = 90,
    feed_fetch_timeout_seconds: float = 10.0,
    feed_entry_scan_limit: int = 12,
    feed_imported_limit: int = 30,
    actor_search_link_limit: int = 6,
    feed_require_published_at: bool = True,
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

    imported = 0
    started_at = time.perf_counter()
    deadline = started_at + float(max(20, int(feed_import_max_seconds)))
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

        feed_list: list[tuple[str, str]] = list(default_cti_feeds)
        feed_list.extend(_actor_query_feeds(actor_terms))
        feed_state = _load_actor_feed_state(connection, actor_id)
        feed_list = sorted(feed_list, key=lambda feed: _feed_priority_key(feed, feed_state))
        seen_links: set[str] = set()

        imported += _import_ransomware_live_actor_activity(connection, actor_id, actor_terms)

        for feed_name, feed_url in feed_list:
            if time.perf_counter() >= deadline:
                break
            state_key = (feed_name, feed_url)
            state = dict(feed_state.get(state_key, {}))
            now_utc = datetime.now(timezone.utc)
            now_iso = _utc_now_iso()
            if _feed_backoff_active(state, now_utc):
                continue
            checkpoint_dt = _parse_published_datetime(str(state.get('last_success_published_at') or '').strip())
            try:
                remaining_seconds = max(1.0, deadline - time.perf_counter())
                feed_resp = _safe_http_get(
                    feed_url,
                    timeout=min(float(feed_fetch_timeout_seconds), float(remaining_seconds)),
                )
                feed_resp.raise_for_status()
                entries = _parse_feed_entries(feed_resp.text)
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

            for entry in prioritized[: max(5, int(feed_entry_scan_limit))]:
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
                try:
                    remaining_seconds = max(1.0, deadline - time.perf_counter())
                    derived = _derive_source_from_url(
                        link,
                        fallback_source_name=feed_name,
                        published_hint=entry.get('published_at'),
                        fetch_timeout_seconds=min(20.0, float(remaining_seconds)),
                    )
                    combined_text = (
                        f'{entry.get("title") or ""} '
                        f'{derived.get("source_name") or ""} '
                        f'{derived.get("source_url") or ""} '
                        f'{derived.get("pasted_text") or ""}'
                    )
                    if actor_terms and not _text_contains_actor_term(combined_text, actor_terms):
                        continue
                    resolved_source_url = str(derived.get('source_url') or '').strip()
                    if _is_google_news_wrapper_url(link) and _is_google_news_wrapper_url(resolved_source_url):
                        continue
                    resolved_published = str(derived.get('published_at') or '').strip() or (
                        str(entry.get('published_at') or '').strip()
                    )
                    if feed_require_published_at and not resolved_published:
                        continue
                    if not _within_lookback(resolved_published or None, actor_feed_lookback_days):
                        continue
                    resolved_title = str(derived.get('title') or title_text or '').strip() or None
                    resolved_headline = str(derived.get('headline') or title_text or '').strip() or None
                    resolved_og_title = str(derived.get('og_title') or title_text or '').strip() or None
                    resolved_html_title = str(derived.get('html_title') or title_text or '').strip() or None
                    _upsert_source_for_actor(
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
                    )
                    imported += 1
                    imported_from_feed += 1
                    resolved_dt = _parse_published_datetime(resolved_published or None)
                    if resolved_dt is not None and (
                        latest_imported_published_dt is None or resolved_dt > latest_imported_published_dt
                    ):
                        latest_imported_published_dt = resolved_dt
                    if imported >= max(10, int(feed_imported_limit)):
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
                            fallback_dt = _parse_published_datetime(fallback_published or None)
                            if fallback_dt is not None and (
                                latest_imported_published_dt is None or fallback_dt > latest_imported_published_dt
                            ):
                                latest_imported_published_dt = fallback_dt
                            if imported >= max(10, int(feed_imported_limit)):
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
                derived = _derive_source_from_url(
                    link,
                    fallback_source_name='Actor Search',
                    fetch_timeout_seconds=min(20.0, float(remaining_seconds)),
                )
                combined_text = (
                    f'{derived.get("source_name") or ""} '
                    f'{derived.get("source_url") or ""} '
                    f'{derived.get("pasted_text") or ""}'
                )
                if actor_terms and not _text_contains_actor_term(combined_text, actor_terms):
                    continue
                resolved_search_published = str(derived.get('published_at') or '').strip()
                if feed_require_published_at and not resolved_search_published:
                    continue
                if not _within_lookback(resolved_search_published or None, actor_feed_lookback_days):
                    continue
                _upsert_source_for_actor(
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
                )
                imported += 1
                if imported >= max(10, int(feed_imported_limit)):
                    connection.commit()
                    return imported
            except Exception:
                continue

        connection.commit()
    return imported
