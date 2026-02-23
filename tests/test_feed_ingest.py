import sqlite3
from pathlib import Path

from pipelines.feed_ingest import _is_google_news_wrapper_url
from pipelines.feed_ingest import import_default_feeds_for_actor_core


class _OkResponse:
    def __init__(self, text: str = '<rss/>'):
        self.text = text

    def raise_for_status(self) -> None:
        return None


def _seed_actor_db(db_path: Path, actor_id: str, actor_name: str) -> None:
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE actor_profiles (
                id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL
            )
            '''
        )
        connection.execute(
            'INSERT INTO actor_profiles (id, display_name) VALUES (?, ?)',
            (actor_id, actor_name),
        )
        connection.commit()


def test_is_google_news_wrapper_url():
    assert _is_google_news_wrapper_url('https://news.google.com/rss/articles/ABC123?oc=5')
    assert not _is_google_news_wrapper_url('https://news.google.com/search?q=akira')
    assert not _is_google_news_wrapper_url('https://www.cisa.gov/news')


def test_feed_ingest_skips_google_news_wrapper_fallback(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-1'
    _seed_actor_db(db_path, actor_id, 'Akira')

    saved: list[dict[str, str | None]] = []
    google_wrapper = 'https://news.google.com/rss/articles/ABC123?oc=5'

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Google News Actor Query', 'https://news.google.com/rss/search?q=akira')],
        actor_feed_lookback_days=180,
        feed_import_max_seconds=90,
        feed_fetch_timeout_seconds=10.0,
        feed_entry_scan_limit=12,
        feed_imported_limit=30,
        actor_search_link_limit=1,
        feed_require_published_at=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {
                    'title': 'Akira Ransomware Group Update',
                    'link': google_wrapper,
                    'published_at': '2026-02-20T00:00:00Z',
                }
            ],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('fetch failed')),
            'upsert_source_for_actor': lambda *_args: saved.append({'url': google_wrapper}),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
        },
    )

    assert imported == 0
    assert saved == []


def test_feed_ingest_uses_derived_actor_match_when_feed_title_does_not_match(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-2'
    _seed_actor_db(db_path, actor_id, 'Scattered Spider')

    saved: list[dict[str, str | None]] = []

    def fake_text_contains_actor_term(text, _terms):
        lowered = str(text or '').lower()
        return 'scattered spider' in lowered or 'derived-hit' in lowered

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Generic Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_import_max_seconds=90,
        feed_fetch_timeout_seconds=10.0,
        feed_entry_scan_limit=12,
        feed_imported_limit=30,
        actor_search_link_limit=1,
        feed_require_published_at=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Scattered Spider', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['scattered spider'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {
                    'title': 'Weekly cyber roundup',
                    'link': 'https://example.com/post-1',
                    'published_at': '2026-02-20T00:00:00Z',
                }
            ],
            'text_contains_actor_term': fake_text_contains_actor_term,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example Security',
                'source_url': 'https://example.com/post-1',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'derived-hit observed in recent actor operations',
                'trigger_excerpt': 'derived-hit observed',
                'title': 'Actor activity update',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example Security',
            },
            'upsert_source_for_actor': lambda *_args: saved.append({'url': 'https://example.com/post-1'}),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
        },
    )

    assert imported == 1
    assert saved == [{'url': 'https://example.com/post-1'}]
