import sqlite3
from pathlib import Path

from fastapi import HTTPException

import app as app_module
from routes.routes_dashboard import render_dashboard_root
from services import db_schema_service
from services import stix_service


def _init_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path))
    db_schema_service.ensure_schema(connection)
    return connection


def test_ioc_unvalidated_status_is_preserved(tmp_path: Path) -> None:
    db_path = tmp_path / 'app.db'
    with _init_db(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                validation_reason, confidence_score, extraction_method, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('ioc-1', 'actor-1', 'domain', 'evil.example', 'evil.example', 'unvalidated', '', 0, 'manual', '2026-01-01T00:00:00Z'),
        )
        connection.commit()
        db_schema_service.ensure_schema(connection)
        row = connection.execute(
            'SELECT validation_status FROM ioc_items WHERE id = ?',
            ('ioc-1',),
        ).fetchone()
    assert row is not None
    assert str(row[0]) == 'unvalidated'


def test_outbound_url_requires_https_by_default() -> None:
    try:
        app_module._validate_outbound_url('http://example.com/path')  # noqa: SLF001
    except HTTPException as exc:
        assert exc.status_code == 400
        assert 'https' in str(exc.detail).lower()
    else:
        raise AssertionError('expected HTTPException for non-HTTPS outbound URL')


def test_stix_export_import_round_trip(tmp_path: Path) -> None:
    db_path = tmp_path / 'app.db'
    with _init_db(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            ('actor-1', 'APT Demo', 'apt demo', None, '2026-01-01T00:00:00Z', 1),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                validation_reason, confidence_score, extraction_method, created_at, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-1',
                'actor-1',
                'domain',
                'evil.example',
                'evil.example',
                'valid',
                '',
                4,
                'manual',
                '2026-01-01T00:00:00Z',
                '2026-01-01T00:00:00Z',
                '2026-01-02T00:00:00Z',
            ),
        )
        bundle = stix_service.export_actor_bundle_core(connection, actor_id='actor-1', actor_name='APT Demo')
        assert str(bundle.get('type')) == 'bundle'
        stats = stix_service.import_actor_bundle_core(
            connection,
            actor_id='actor-1',
            bundle=bundle,
            now_iso='2026-01-03T00:00:00Z',
            upsert_ioc_item=app_module._upsert_ioc_item,  # noqa: SLF001
        )
        connection.commit()
    assert stats['imported_iocs'] >= 1


def test_dashboard_get_has_no_write_side_effects() -> None:
    tracker = {'set_called': 0, 'enqueue_called': 0}

    def _set_status(*_args, **_kwargs):
        tracker['set_called'] += 1

    def _enqueue(*_args, **_kwargs):
        tracker['enqueue_called'] += 1

    html_response = render_dashboard_root(
        request=object(),
        background_tasks=object(),
        actor_id='actor-1',
        notice=None,
        source_tier=None,
        min_confidence_weight=None,
        source_days=None,
        deps={
            'list_actor_profiles': lambda: [{'id': 'actor-1', 'display_name': 'APT Demo', 'is_tracked': 1, 'created_at': '2026-01-01T00:00:00Z'}],
            'fetch_actor_notebook': lambda *_args, **_kwargs: {
                'actor': {'is_tracked': 1, 'notebook_status': 'idle', 'notebook_updated_at': '2026-01-01T00:00:00Z'},
                'counts': {'sources': 0},
            },
            'set_actor_notebook_status': _set_status,
            'run_actor_generation': _enqueue,
            'enqueue_actor_generation': _enqueue,
            'get_ollama_status': lambda: {'available': False, 'base_url': '', 'model': ''},
            'get_actor_refresh_stats': lambda _actor_id: {},
            'page_refresh_auto_trigger_minutes': 0,
            'running_stale_recovery_minutes': 10,
            'recover_stale_running_states': lambda: 0,
            'format_duration_ms': lambda _v: 'n/a',
            'templates': type(
                'Templates',
                (),
                {
                    'TemplateResponse': staticmethod(
                        lambda _request, _name, context: context
                    )
                },
            )(),
        },
    )
    assert isinstance(html_response, dict)
    assert tracker['set_called'] == 0
    assert tracker['enqueue_called'] == 0


def test_dashboard_running_state_skips_heavy_fetch() -> None:
    tracker = {'fetch_called': 0}

    def _fetch_notebook(*_args, **_kwargs):
        tracker['fetch_called'] += 1
        raise AssertionError('fetch should not be called while running')

    html_response = render_dashboard_root(
        request=object(),
        background_tasks=object(),
        actor_id='actor-1',
        notice=None,
        source_tier=None,
        min_confidence_weight=None,
        source_days=None,
        deps={
            'list_actor_profiles': lambda: [
                {
                    'id': 'actor-1',
                    'display_name': 'APT Demo',
                    'is_tracked': 1,
                    'created_at': '2026-01-01T00:00:00Z',
                    'notebook_status': 'running',
                    'notebook_message': 'Refresh in progress',
                    'notebook_updated_at': '2026-01-02T00:00:00Z',
                    'last_refresh_duration_ms': None,
                    'last_refresh_sources_processed': None,
                }
            ],
            'fetch_actor_notebook': _fetch_notebook,
            'set_actor_notebook_status': lambda *_args, **_kwargs: None,
            'run_actor_generation': lambda *_args, **_kwargs: None,
            'enqueue_actor_generation': lambda *_args, **_kwargs: None,
            'get_ollama_status': lambda: {'available': False, 'base_url': '', 'model': ''},
            'get_actor_refresh_stats': lambda _actor_id: {},
            'page_refresh_auto_trigger_minutes': 0,
            'running_stale_recovery_minutes': 10,
            'recover_stale_running_states': lambda: 0,
            'format_duration_ms': lambda _v: 'n/a',
            'templates': type(
                'Templates',
                (),
                {
                    'TemplateResponse': staticmethod(
                        lambda _request, _name, context: context
                    )
                },
            )(),
        },
    )
    assert isinstance(html_response, dict)
    notebook = html_response.get('notebook')
    assert isinstance(notebook, dict)
    actor = notebook.get('actor')
    assert isinstance(actor, dict)
    assert actor.get('notebook_status') == 'running'
    assert tracker['fetch_called'] == 0
