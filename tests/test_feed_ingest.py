import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from pipelines.feed_ingest import _is_google_news_wrapper_url
from pipelines.feed_ingest import _apply_source_trust_boost
from pipelines.feed_ingest import _promote_soft_sources_from_corroboration
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


def test_feed_ingest_actor_query_feed_stores_google_news_wrapper_entries(tmp_path):
    """Google News wrapper links from actor_query_feeds should be stored using the
    RSS title and publisher name — no URL resolution attempted."""
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-gn'
    _seed_actor_db(db_path, actor_id, 'Akira')

    saved: list[dict] = []
    google_news_search_url = 'https://news.google.com/rss/search?q=akira+ransomware'
    google_wrapper_link = 'https://news.google.com/rss/articles/CBMi_UNIQUEKEY?oc=5'

    derive_called = []

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[],
        actor_feed_lookback_days=180,
        feed_import_max_seconds=90,
        feed_fetch_timeout_seconds=10.0,
        feed_entry_scan_limit=12,
        feed_imported_limit=30,
        actor_search_link_limit=1,
        feed_require_published_at=False,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [('Google News Actor Query', google_news_search_url)],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {
                    'title': 'Akira Ransomware Targets Healthcare Sector - SecurityWeek',
                    'link': google_wrapper_link,
                    'published_at': '2026-02-20T00:00:00Z',
                    'source_domain': 'https://www.securityweek.com',
                    'source_name': 'SecurityWeek',
                }
            ],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: derive_called.append(True) or {},
            'upsert_source_for_actor': lambda _conn, *_args: saved.append({'url': _args[2]}),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
        },
    )

    # Entry should be stored using the Google News wrapper URL as the source URL
    assert imported == 1
    assert len(saved) == 1
    assert saved[0]['url'] == google_wrapper_link
    # URL resolution should NOT have been attempted for actor_query_feed wrapper entries
    assert derive_called == []


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


def test_feed_ingest_v2_logs_decision_for_missing_published_date(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-5'
    _seed_actor_db(db_path, actor_id, 'Akira')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_require_published_at=True,
        evidence_pipeline_v2=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Akira', 'aliases_csv': 'Akira'},
            'actor_terms': lambda *_args: ['akira'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Akira update', 'link': 'https://example.com/new', 'published_at': ''},
            ],
            'text_contains_actor_term': lambda _text, _terms: True,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '',
                'pasted_text': 'Akira activity details',
                'trigger_excerpt': 'Akira activity details',
                'title': 'Akira source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda *_args: None,
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 0
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            '''
            SELECT stage, decision, reason_code
            FROM ingest_decisions
            WHERE actor_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            ''',
            (actor_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == 'resolve'
    assert row[1] == 'rejected'
    assert row[2] == 'missing_published_at'


def test_feed_ingest_v2_soft_match_is_capped_by_default(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-6'
    _seed_actor_db(db_path, actor_id, 'APT29')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved: list[str] = []
    saved_meta: list[tuple[object, object, object]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_require_published_at=True,
        evidence_pipeline_v2=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'APT29', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['apt29'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Weekly cyber roundup', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'No direct actor mention in this sample text.',
                'trigger_excerpt': 'Sample trigger',
                'title': 'Sample source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': (
                lambda _connection, *_args: (
                    saved.append(str(_args[2])),
                    saved_meta.append((_args[12], _args[13], _args[14])),
                )
            ),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 0
    assert saved == []
    assert saved_meta == []
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            '''
            SELECT decision, reason_code
            FROM ingest_decisions
            WHERE actor_id = ? AND stage = 'score'
            ORDER BY created_at DESC
            LIMIT 1
            ''',
            (actor_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == 'rejected'
    assert row[1] == 'soft_match_cap_exceeded'


def test_feed_ingest_v2_background_retains_soft_candidates_when_enabled(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-6c'
    _seed_actor_db(db_path, actor_id, 'APT29')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved: list[str] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        evidence_pipeline_v2=True,
        retain_soft_candidates=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'APT29', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['apt29'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Weekly roundup', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'No direct actor mention in this sample text.',
                'trigger_excerpt': 'Sample trigger',
                'title': 'Sample source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, *_args: saved.append(str(_args[2])),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved == ['https://example.com/new']


def test_feed_ingest_promotes_when_entry_context_matches_actor_terms(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-6d'
    _seed_actor_db(db_path, actor_id, 'Qilin')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved_meta: list[tuple[object, object, object]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        evidence_pipeline_v2=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Qilin', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['qilin'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Qilin campaign update', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'This report discusses campaign operations without direct naming.',
                'trigger_excerpt': 'Campaign operations updated',
                'title': 'Sample source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, *_args: saved_meta.append((_args[12], _args[13], _args[14])),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved_meta == [('trusted', 2, 'feed_partial_match')]


def test_feed_ingest_v2_soft_match_can_be_ingested_when_cap_configured(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-6b'
    _seed_actor_db(db_path, actor_id, 'APT29')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved: list[str] = []
    saved_meta: list[tuple[object, object, object]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_require_published_at=True,
        evidence_pipeline_v2=True,
        feed_soft_match_limit=1,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'APT29', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['apt29'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Weekly cyber roundup', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'No direct actor mention in this sample text.',
                'trigger_excerpt': 'Sample trigger',
                'title': 'Sample source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': (
                lambda _connection, *_args: (
                    saved.append(str(_args[2])),
                    saved_meta.append((_args[12], _args[13], _args[14])),
                )
            ),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved == ['https://example.com/new']
    assert saved_meta == [('context', 1, 'feed_soft_match')]


def test_feed_ingest_v2_partial_match_gets_trusted_quality(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-7'
    _seed_actor_db(db_path, actor_id, 'Cozy Bear')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved_meta: list[tuple[object, object, object]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_require_published_at=True,
        evidence_pipeline_v2=True,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Cozy Bear', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['cozy bear'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Weekly cyber roundup', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': 'Analysts observed cozy operators using bear-themed lure infrastructure.',
                'trigger_excerpt': 'Analysts observed cozy operators',
                'title': 'Sample source',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, *_args: saved_meta.append((_args[12], _args[13], _args[14])),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved_meta == [('trusted', 2, 'feed_partial_match')]
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            '''
            SELECT decision, reason_code
            FROM ingest_decisions
            WHERE actor_id = ? AND stage = 'score'
            ORDER BY created_at DESC
            LIMIT 1
            ''',
            (actor_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == 'accepted'
    assert row[1] == 'actor_term_partial_match_soft'


def test_feed_ingest_v2_linkage_signals_can_promote_actor_term_miss(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-7b'
    _seed_actor_db(db_path, actor_id, 'Ghostwriter')
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.commit()

    saved_meta: list[tuple[object, object, object]] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[('Primary Feed', 'https://example.com/feed.xml')],
        actor_feed_lookback_days=180,
        feed_require_published_at=True,
        evidence_pipeline_v2=True,
        feed_soft_match_limit=1,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Ghostwriter', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['ghostwriter'],
            'actor_query_feeds': lambda _terms: [],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda _url, timeout=10.0: _OkResponse(),
            'parse_feed_entries': lambda _xml: [
                {'title': 'Intrusion tradecraft update', 'link': 'https://example.com/new', 'published_at': '2026-02-20T00:00:00Z'},
            ],
            'text_contains_actor_term': lambda _text, _terms: False,
            'within_lookback': lambda _published_at, _days: True,
            'derive_source_from_url': lambda *_args, **_kwargs: {
                'source_name': 'Example',
                'source_url': 'https://example.com/new',
                'published_at': '2026-02-20T00:00:00Z',
                'pasted_text': (
                    'Investigation mapped T1059 and T1566 activity with beacon domain bad.example '
                    'and hash 44d88612fea8a8f36de82e1278abb02f plus C2 callbacks.'
                ),
                'trigger_excerpt': 'Investigation mapped T1059 activity',
                'title': 'Tradecraft with indicators',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda _connection, *_args: saved_meta.append((_args[12], _args[13], _args[14])),
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert saved_meta == [('trusted', 2, 'feed_partial_match')]


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


def test_feed_ingest_interactive_mode_prioritizes_actor_query_and_skips_secondary(tmp_path):
    db_path = tmp_path / 'feed_ingest.db'
    actor_id = 'actor-8'
    _seed_actor_db(db_path, actor_id, 'Qilin')

    fetched_urls: list[str] = []
    imported = import_default_feeds_for_actor_core(
        actor_id,
        db_path=str(db_path),
        default_cti_feeds=[],
        primary_cti_feeds=[('Primary Feed', 'https://primary.example/feed.xml')],
        secondary_context_feeds=[('Secondary Feed', 'https://secondary.example/feed.xml')],
        actor_feed_lookback_days=180,
        import_mode='interactive',
        high_signal_target=1,
        deps={
            'actor_exists': lambda connection, _actor_id: True,
            'build_actor_profile_from_mitre': lambda _name: {'group_name': 'Qilin', 'aliases_csv': ''},
            'actor_terms': lambda *_args: ['qilin'],
            'actor_query_feeds': lambda _terms: [('Actor Query', 'https://query.example/feed.xml')],
            'import_ransomware_live_actor_activity': lambda *_args: 0,
            'safe_http_get': lambda url, timeout=10.0: (fetched_urls.append(url), _OkResponse(text=url))[1],
            'parse_feed_entries': lambda xml: [
                {'title': 'Qilin campaign update', 'link': f'{xml}/post', 'published_at': '2026-02-23T00:00:00Z'}
            ],
            'text_contains_actor_term': lambda text, _terms: 'qilin' in str(text or '').lower(),
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
                'pasted_text': 'Qilin operators updated infrastructure.',
                'trigger_excerpt': 'Qilin operators updated infrastructure.',
                'title': 'Qilin update',
                'headline': None,
                'og_title': None,
                'html_title': None,
                'publisher': 'Example',
                'site_name': 'Example',
            },
            'upsert_source_for_actor': lambda *_args: None,
            'duckduckgo_actor_search_urls': lambda _terms, limit=1: [],
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        },
    )

    assert imported == 1
    assert fetched_urls
    assert fetched_urls[0] == 'https://query.example/feed.xml'
    assert 'https://secondary.example/feed.xml' not in fetched_urls


def test_trust_boost_promotes_medium_confidence_when_domain_is_high_confidence():
    boosted = _apply_source_trust_boost(
        relevance_features={'score': 0.22, 'label': 'low', 'exact_match': False},
        source_url='https://cisa.gov/news-update',
        source_trust_score=lambda _url: 4,
    )

    assert float(boosted.get('score') or 0.0) >= 0.56
    assert str(boosted.get('label') or '') == 'medium'


def test_corroboration_promotes_soft_sources(tmp_path):
    db_path = tmp_path / 'promote.db'
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            CREATE TABLE sources (
                id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                headline TEXT,
                og_title TEXT,
                html_title TEXT,
                pasted_text TEXT NOT NULL,
                published_at TEXT,
                retrieved_at TEXT NOT NULL,
                source_type TEXT,
                source_tier TEXT,
                confidence_weight INTEGER
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE ingest_decisions (
                id TEXT PRIMARY KEY,
                source_id TEXT,
                actor_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_code TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        actor_id = 'actor-promote'
        rows = [
            (
                's1', actor_id, 'https://intel.example/a', 'Report A', None, None, None,
                'Beacon observed on sharednode.badinfra.example and 10.2.3.4',
                '2026-02-20T00:00:00+00:00', '2026-02-20T00:00:00+00:00', 'feed_soft_match', 'context', 1,
            ),
            (
                's2', actor_id, 'https://intel2.example/b', 'Report B', None, None, None,
                'Related activity tied to sharednode.badinfra.example in victim telemetry',
                '2026-02-21T00:00:00+00:00', '2026-02-21T00:00:00+00:00', 'feed_soft_match', 'context', 1,
            ),
        ]
        connection.executemany(
            '''
            INSERT INTO sources (
                id, actor_id, url, title, headline, og_title, html_title,
                pasted_text, published_at, retrieved_at, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            rows,
        )
        promoted = _promote_soft_sources_from_corroboration(
            connection,
            actor_id=actor_id,
            now_iso='2026-02-22T00:00:00+00:00',
            record_decision=lambda conn, **kwargs: conn.execute(
                '''
                INSERT INTO ingest_decisions (
                    id, source_id, actor_id, stage, decision, reason_code, details_json, created_at
                ) VALUES ('x'||hex(randomblob(8)), ?, ?, ?, ?, ?, '{}', ?)
                ''',
                (
                    str(kwargs.get('source_id') or ''),
                    str(kwargs.get('actor_id') or ''),
                    str(kwargs.get('stage') or ''),
                    str(kwargs.get('decision') or ''),
                    str(kwargs.get('reason_code') or ''),
                    str(kwargs.get('now_iso') or ''),
                ),
            ),
            parse_published_datetime=lambda value: datetime.fromisoformat(str(value).replace('Z', '+00:00')) if value else None,
            lookback_days=180,
        )
        updated = connection.execute(
            '''
            SELECT COUNT(*)
            FROM sources
            WHERE actor_id = ?
              AND source_type = 'feed_partial_match'
              AND source_tier = 'trusted'
              AND confidence_weight = 2
            ''',
            (actor_id,),
        ).fetchone()[0]

    assert promoted >= 2
    assert updated == 2
