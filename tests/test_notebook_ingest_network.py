import sqlite3
import time
import asyncio
from datetime import datetime, timezone
from fastapi import BackgroundTasks

import pytest

import app as app_module
import route_paths
from tests.notebook_test_helpers import JsonRequest as _JsonRequest
from tests.notebook_test_helpers import app_endpoint as _app_endpoint
from tests.notebook_test_helpers import http_request as _http_request
from tests.notebook_test_helpers import setup_db as _setup_db


def test_validate_outbound_url_blocks_localhost():
    with pytest.raises(app_module.HTTPException):
        app_module._validate_outbound_url('http://localhost/internal')  # noqa: SLF001


def test_validate_outbound_url_honors_allowlist(monkeypatch):
    monkeypatch.setattr(
        app_module.socket,
        'getaddrinfo',
        lambda *_args, **_kwargs: [(None, None, None, None, ('93.184.216.34', 0))],
    )
    with pytest.raises(app_module.HTTPException):
        app_module._validate_outbound_url(  # noqa: SLF001
            'https://example.org/report',
            allowed_domains={'example.com'},
        )


def test_validate_outbound_url_blocks_private_ip(monkeypatch):
    monkeypatch.setattr(
        app_module.socket,
        'getaddrinfo',
        lambda *_args, **_kwargs: [(None, None, None, None, ('127.0.0.1', 0))],
    )
    with pytest.raises(app_module.HTTPException):
        app_module._validate_outbound_url('https://example.com')  # noqa: SLF001


def test_safe_http_get_revalidates_redirect_target(monkeypatch):
    class _Response:
        def __init__(self, url: str, status_code: int, location: str | None = None):
            self.url = url
            self.status_code = status_code
            self.headers = {'location': location} if location else {}

        @property
        def is_redirect(self) -> bool:
            return self.status_code in {301, 302, 303, 307, 308}

    def _validate(url: str, allowed_domains=None):
        if 'localhost' in url:
            raise app_module.HTTPException(status_code=400, detail='blocked')
        return url

    monkeypatch.setattr(app_module, '_validate_outbound_url', _validate)
    monkeypatch.setattr(
        app_module.httpx,
        'get',
        lambda *args, **kwargs: _Response('https://safe.example/path', 302, 'http://localhost/admin'),
    )
    with pytest.raises(app_module.HTTPException):
        app_module._safe_http_get('https://safe.example/path', timeout=5.0)  # noqa: SLF001


def test_domain_allowed_for_actor_search_blocks_spoofed_hosts():
    assert app_module._domain_allowed_for_actor_search('https://www.mandiant.com/blog/post')  # noqa: SLF001
    assert app_module._domain_allowed_for_actor_search('https://sub.mandiant.com/report')  # noqa: SLF001
    assert not app_module._domain_allowed_for_actor_search('https://evilmandiant.com/report')  # noqa: SLF001
    assert not app_module._domain_allowed_for_actor_search('https://mandiant.com.evil.org/report')  # noqa: SLF001


def test_add_source_uses_manual_text_without_remote_fetch(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Manual', 'Manual source scope')
    from routes.routes_actor_ops import create_actor_ops_router

    def _should_not_fetch(*_args, **_kwargs):
        raise AssertionError('derive_source_from_url should not be called for manual text imports')

    router = create_actor_ops_router(
        deps={
            'enforce_request_size': lambda _request, _limit: asyncio.sleep(0),
            'source_upload_body_limit_bytes': 1024 * 1024,
            'default_body_limit_bytes': 1024 * 1024,
            'db_path': lambda: app_module.DB_PATH,
            'actor_exists': app_module.actor_exists,
            'derive_source_from_url': _should_not_fetch,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
            'import_default_feeds_for_actor': app_module.import_default_feeds_for_actor,
            'parse_ioc_values': app_module._parse_ioc_values,  # noqa: SLF001
            'upsert_ioc_item': app_module._upsert_ioc_item,  # noqa: SLF001
            'export_actor_stix_bundle': app_module._export_actor_stix_bundle,  # noqa: SLF001
            'import_actor_stix_bundle': app_module._import_actor_stix_bundle,  # noqa: SLF001
            'utc_now_iso': app_module.utc_now_iso,
            'set_actor_notebook_status': app_module.set_actor_notebook_status,
            'get_actor_refresh_stats': app_module.get_actor_refresh_stats,
            'get_actor_refresh_timeline': app_module.get_actor_refresh_timeline,
            'submit_actor_refresh_job': app_module.submit_actor_refresh_job,
            'get_actor_refresh_job': app_module.get_actor_refresh_job,
            'enqueue_actor_generation': app_module.enqueue_actor_generation,
            'run_actor_generation': app_module.run_actor_generation,
        }
    )

    add_source_endpoint = next(
        route.endpoint
        for route in router.routes
        if getattr(route, 'path', '') == '/actors/{actor_id}/sources'
    )

    class _FakeRequest:
        async def form(self):
            return {
                'source_url': 'https://example.com/report',
                'pasted_text': 'Manual analyst text about APT-Manual operations and observed tactics.',
                'published_at': '2026-02-10',
            }

    response = asyncio.run(add_source_endpoint(actor['id'], _FakeRequest()))
    assert response.status_code == 303
    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT source_name, url, pasted_text
            FROM sources
            WHERE actor_id = ?
            ''',
            (actor['id'],),
        ).fetchone()
    assert row is not None
    assert row[0] == 'example.com'
    assert row[1] == 'https://example.com/report'
    assert 'Manual analyst text' in row[2]


def test_upsert_source_skips_near_duplicate_content_by_fingerprint(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Dedupe', 'Dedupe source scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        first_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=str(actor['id']),
            source_name='CISA',
            source_url='https://example.com/advisory-a',
            published_at='2026-02-20',
            pasted_text='APT-Dedupe targeted edge devices and used phishing for access.',
            trigger_excerpt='APT-Dedupe targeted edge devices.',
            title='APT-Dedupe campaign update',
        )
        second_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=str(actor['id']),
            source_name='Mandiant',
            source_url='https://another.example.org/report-b',
            published_at='2026-02-21',
            pasted_text='APT-Dedupe targeted edge devices and used phishing for access.',
            trigger_excerpt='APT-Dedupe targeted edge devices.',
            title='APT-Dedupe campaign update',
        )
        connection.commit()
        count = connection.execute(
            'SELECT COUNT(*) FROM sources WHERE actor_id = ?',
            (actor['id'],),
        ).fetchone()[0]

    assert first_id == second_id
    assert count == 1


def test_upsert_source_same_url_merges_duplicates_and_keeps_latest_id(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-URL-Dedupe', 'URL dedupe scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text, title
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-old',
                actor['id'],
                'Ransomware.live',
                'https://api.ransomware.live/v2/groupvictims/example',
                '2026-02-20T00:00:00+00:00',
                '2026-02-20T00:00:00+00:00',
                'Old content',
                'Old title',
            ),
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text, title
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-new',
                actor['id'],
                'Ransomware.live',
                'https://api.ransomware.live/v2/groupvictims/example',
                '2026-02-21T00:00:00+00:00',
                '2026-02-21T00:00:00+00:00',
                'New content',
                'New title',
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-old',
                actor['id'],
                '2026-02-20T00:00:00+00:00',
                'impact',
                'Old event',
                'Old summary',
                'src-old',
                '',
                '[]',
            ),
        )

        kept_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=str(actor['id']),
            source_name='Ransomware.live',
            source_url='https://api.ransomware.live/v2/groupvictims/example',
            published_at='2026-02-22T00:00:00+00:00',
            pasted_text='Refreshed content',
            title='Refreshed title',
            refresh_existing_content=True,
        )
        connection.commit()

        source_ids = [row[0] for row in connection.execute(
            'SELECT id FROM sources WHERE actor_id = ? AND url = ?',
            (actor['id'], 'https://api.ransomware.live/v2/groupvictims/example'),
        ).fetchall()]
        timeline_source_id = connection.execute(
            'SELECT source_id FROM timeline_events WHERE id = ?',
            ('evt-old',),
        ).fetchone()[0]

    assert kept_id == 'src-new'
    assert source_ids == ['src-new']
    assert timeline_source_id == 'src-new'


def test_upsert_source_refresh_does_not_downgrade_rich_text_with_short_payload(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Refresh-Guard', 'Refresh downgrade guard scope')

    rich_text = (
        'APT-Refresh-Guard operators have multiple reported incidents across sectors with geographic distribution '
        'details and recent examples. ' * 3
    )
    with sqlite3.connect(app_module.DB_PATH) as connection:
        source_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=str(actor['id']),
            source_name='Ransomware.live',
            source_url='https://api.ransomware.live/v2/groupvictims/example-guard',
            published_at='2026-02-20T00:00:00+00:00',
            pasted_text=rich_text,
            title='Initial rich source',
            refresh_existing_content=True,
        )
        app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=str(actor['id']),
            source_name='Ransomware.live',
            source_url='https://api.ransomware.live/v2/groupvictims/example-guard',
            published_at='2026-02-21T00:00:00+00:00',
            pasted_text='Short text',
            title='Short refresh',
            refresh_existing_content=True,
        )
        connection.commit()
        row = connection.execute(
            'SELECT id, pasted_text FROM sources WHERE actor_id = ? AND url = ?',
            (actor['id'], 'https://api.ransomware.live/v2/groupvictims/example-guard'),
        ).fetchone()

    assert row is not None
    assert row[0] == source_id
    assert len(str(row[1] or '')) >= len(rich_text) - 5


def test_timeline_dedupe_prefers_latest_duplicate_event(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Timeline-Newest', 'Timeline newest dedupe scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text, title
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-older',
                actor['id'],
                'Example',
                'https://example.com/older',
                '2026-02-20T00:00:00+00:00',
                '2026-02-20T00:00:00+00:00',
                'APT-Timeline-Newest targeted Acme Hospital and used PowerShell execution for access.',
                'Older report',
            ),
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text, title
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-newer',
                actor['id'],
                'Example',
                'https://example.com/newer',
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T00:00:00+00:00',
                'APT-Timeline-Newest targeted Acme Hospital and used PowerShell execution for access.',
                'Newer report',
            ),
        )
        connection.commit()

    app_module.build_notebook(actor['id'], generate_questions=False, rebuild_timeline=True)

    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT occurred_at, source_id
            FROM timeline_events
            WHERE actor_id = ?
            ORDER BY occurred_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()

    assert row is not None
    assert row[0] == '2026-02-22T00:00:00+00:00'
    assert row[1] == 'src-newer'

