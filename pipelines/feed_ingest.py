import sqlite3
import time
from typing import Callable

from fastapi import HTTPException


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
    _derive_source_from_url = deps['derive_source_from_url']
    _upsert_source_for_actor = deps['upsert_source_for_actor']
    _duckduckgo_actor_search_urls = deps['duckduckgo_actor_search_urls']

    imported = 0
    started_at = time.perf_counter()
    deadline = started_at + float(max(20, int(feed_import_max_seconds)))
    with sqlite3.connect(db_path) as connection:
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
        seen_links: set[str] = set()

        imported += _import_ransomware_live_actor_activity(connection, actor_id, actor_terms)

        for feed_name, feed_url in feed_list:
            if time.perf_counter() >= deadline:
                break
            try:
                feed_resp = _safe_http_get(feed_url, timeout=feed_fetch_timeout_seconds)
                feed_resp.raise_for_status()
                entries = _parse_feed_entries(feed_resp.text)
            except Exception:
                continue

            prioritized = sorted(
                entries,
                key=lambda entry: 0 if _text_contains_actor_term(
                    f'{entry.get("title") or ""} {entry.get("link") or ""}',
                    actor_terms,
                ) else 1,
            )

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
                title_text = str(entry.get('title') or '')
                entry_context = f'{title_text} {link}'
                if actor_terms and not _text_contains_actor_term(entry_context, actor_terms):
                    continue
                seen_links.add(link)
                try:
                    derived = _derive_source_from_url(
                        link,
                        fallback_source_name=feed_name,
                        published_hint=entry.get('published_at'),
                    )
                    combined_text = (
                        f'{entry.get("title") or ""} '
                        f'{derived.get("source_name") or ""} '
                        f'{derived.get("source_url") or ""} '
                        f'{derived.get("pasted_text") or ""}'
                    )
                    if actor_terms and not _text_contains_actor_term(combined_text, actor_terms):
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
                    if imported >= max(10, int(feed_imported_limit)):
                        connection.commit()
                        return imported
                except Exception:
                    if actor_terms and _text_contains_actor_term(entry_context, actor_terms):
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
                            if imported >= max(10, int(feed_imported_limit)):
                                connection.commit()
                                return imported
                        except Exception:
                            pass
                    continue

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
                derived = _derive_source_from_url(link, fallback_source_name='Actor Search')
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
