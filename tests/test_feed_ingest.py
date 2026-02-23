import sqlite3
from datetime import datetime, timezone
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


def test_feed_ingest_incremental_checkpoint_skips_older_entries(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-3'
    _seed_actor_db(db_path, actor_id, 'Akira')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE actor_feed_state (
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
        connection.execute(
            '''
            INSERT INTO actor_feed_state (
                actor_id, feed_name, feed_url, last_success_published_at, last_success_at
            ) VALUES (?, ?, ?, ?, ?)
            ''',
            (
                actor_id,
                'Primary Feed',
                'https://example.com/feed.xml',
                '2026-02-20T00:00:00Z',
                '2026-02-20T01:00:00Z',
            ),
        )
        connection.commit()

    saved: list[dict[str, str]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Akira old', 'link': 'https://example.com/old', 'published_at': '2026-02-19T00:00:00Z'},
                {'title': 'Akira new', 'link': 'https://example.com/new', 'published_at': '2026-02-22T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'parse_published_datetime': (
                lambda value: datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                if str(value or '').strip()
                else None
            ),
            'derive_source_from_url': lambda link, **_kwargs: {
                'source_name': 'Example',
                'source_url': link,
                'published_at': '2026-02-22T00:00:00Z' if link.endswith('/new') else '2026-02-19T00:00:00Z',
                'pasted_text': 'Akira activity details',
                'trigger_excerpt': 'Akira activity details',
                'title': 'Akira source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, *_args: saved.append({'url': _args[2]}),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved == [{'url': 'https://example.com/new'}]


def test_feed_ingest_backs_off_after_repeated_failures(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-4'
    _seed_actor_db(db_path, actor_id, 'Akira')
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE actor_feed_state (
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
        connection.execute(
            '''
            INSERT INTO actor_feed_state (
                actor_id, feed_name, feed_url, last_checked_at, consecutive_failures, total_failures
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                actor_id,
                'Primary Feed',
                'https://example.com/feed.xml',
                now_iso,
                4,
                4,
            ),
        )
        connection.commit()

    called = {'fetch': 0}

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: called.__setitem__('fetch', called['fetch'] + 1),
            'parse_feed_entries': lambda _xml: [],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'parse_published_datetime': lambda _value: datetime.now(timezone.utc),
            'derive_source_from_url': lambda *_args, **_kwargs: {},
            'upsert_source_for_actor': lambda *_args: None,
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:05:00+00:00',
        },
    )

    assert imported == 0
    assert called['fetch'] == 0


def test_feed_ingest_caps_secondary_context_volume(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-5'
    _seed_actor_db(db_path, actor_id, 'Akira')

    secondary_feeds = [
        ('Secondary A', 'https://sec.example/a.xml'),
        ('Secondary B', 'https://sec.example/b.xml'),
        ('Secondary C', 'https://sec.example/c.xml'),
        ('Secondary D', 'https://sec.example/d.xml'),
        ('Secondary E', 'https://sec.example/e.xml'),
    ]
    saved: list[str] = []

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[],
        primary_cti_feeds=[],
        secondary_context_feeds=secondary_feeds,
        actor_feed_lookback_days=180,
        feed_imported_limit=10,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda url, timeout=10.0: _OkResponse(text=url),
            'parse_feed_entries': lambda xml: [
                {'title': 'Akira update', 'link': f'{xml}/post', 'published_at': '2026-02-23T00:00:00Z'}
            ],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'parse_published_datetime': (
                lambda value: datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                if str(value or '').strip()
                else None
            ),
            'derive_source_from_url': lambda link, **_kwargs: {
                'source_name': 'Example',
                'source_url': link,
                'published_at': '2026-02-23T00:00:00Z',
                'pasted_text': 'Akira details',
                'trigger_excerpt': 'Akira details',
                'title': 'Akira details',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, _actor_id, _name, source_url, *_args: saved.append(source_url),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 3
    assert len(saved) == 3
