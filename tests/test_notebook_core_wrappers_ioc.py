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


def test_build_notebook_creates_thread_and_update_with_excerpt(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Test', 'Test scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
                (
                    'src-1',
                    actor['id'],
                    'CISA',
                    'https://example.com/report',
                    '2026-02-15',
                    '2026-02-15T00:00:00+00:00',
                    'APT-Test operators should review suspicious PowerShell activity and hunt for indicators.',
                ),
            )
        connection.commit()

    app_module.build_notebook(actor['id'])

    with sqlite3.connect(app_module.DB_PATH) as connection:
        thread = connection.execute(
            'SELECT id, question_text FROM question_threads WHERE actor_id = ?',
            (actor['id'],),
        ).fetchone()
        assert thread is not None

        update = connection.execute(
            '''
            SELECT qu.trigger_excerpt, s.source_name, s.url, s.published_at
            FROM question_updates qu
            JOIN sources s ON s.id = qu.source_id
            WHERE qu.thread_id = ?
            ''',
            (thread[0],),
        ).fetchone()
        assert update is not None
        assert update[0]
        assert update[1] == 'CISA'
        assert update[2] == 'https://example.com/report'
        assert update[3] == '2026-02-15'


def test_build_notebook_wrapper_delegates_to_builder_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_build_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)

    monkeypatch.setattr(app_module, 'build_notebook_core', _fake_build_core)

    app_module.build_notebook('actor-wrapper-test', generate_questions=False, rebuild_timeline=False)

    assert captured['actor_id'] == 'actor-wrapper-test'
    assert captured['db_path'] == app_module.DB_PATH
    assert captured['generate_questions'] is False
    assert captured['rebuild_timeline'] is False


def test_fetch_actor_notebook_wrapper_delegates_to_pipeline_core(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    captured: dict[str, object] = {}

    def _fake_fetch_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)
        return {'ok': True}

    monkeypatch.setattr(app_module, 'pipeline_fetch_actor_notebook_core', _fake_fetch_core)

    result = app_module._fetch_actor_notebook('actor-wrapper-test')  # noqa: SLF001

    assert isinstance(result, dict)
    assert result.get('ok') is True
    assert captured['actor_id'] == 'actor-wrapper-test'
    assert captured['db_path'] == app_module.DB_PATH
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert 'build_recent_activity_highlights' in deps
    assert 'build_notebook_kpis' in deps
    assert 'format_date_or_unknown' in deps
    assert 'load_quick_check_overrides' in deps


def test_quick_check_overrides_are_applied_to_priority_cards(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Actionable', 'Actionability scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-qc-1',
                actor['id'],
                'Unit42',
                'https://example.com/apt-actionable',
                '2026-02-20',
                '2026-02-20T00:00:00+00:00',
                'APT-Actionable operators should review suspicious PowerShell activity and DNS beaconing in finance systems.',
            ),
        )
        connection.commit()

    def _fake_generate_quick_checks(actor_name, cards, *, deps):
        _ = actor_name
        _ = deps
        if not cards:
            return {}
        first_id = str(cards[0].get('id') or '')
        if not first_id:
            return {}
        return {
            first_id: {
                'first_step': 'Open Defender Advanced Hunting and run EmailEvents for the last 24h.',
                'what_to_look_for': 'Repeated sender domains targeting finance users.',
                'expected_output': 'Record sender pattern delta and confidence shift with source links.',
            }
        }

    monkeypatch.setattr(
        app_module.quick_check_service,
        'generate_quick_check_overrides_core',
        _fake_generate_quick_checks,
    )

    app_module.build_notebook(actor['id'])
    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001

    cards = notebook.get('priority_questions', [])
    assert cards
    assert any(
        'defender advanced hunting' in str(card.get('first_step') or '').lower()
        for card in cards
    )


def test_priority_questions_autopopulate_relevant_iocs(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-IOC', 'IOC relevance scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-ioc-1',
                actor['id'],
                'CISA',
                'https://example.com/apt-ioc',
                '2026-02-22',
                '2026-02-22T00:00:00+00:00',
                'APT-IOC operators were linked to DNS beaconing infrastructure.',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_threads (
                id, actor_id, question_text, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'thread-ioc-1',
                actor['id'],
                'Is APT-IOC using DNS beaconing to known malicious domains?',
                'open',
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T01:00:00+00:00',
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
                'evt-ioc-1',
                actor['id'],
                '2026-02-22T00:30:00+00:00',
                'command_and_control',
                'Beaconing activity',
                'Observed DNS beaconing behavior tied to actor infrastructure.',
                'src-ioc-1',
                'Enterprise network',
                '[]',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_updates (
                id, thread_id, source_id, trigger_excerpt, update_note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
                (
                    'update-ioc-1',
                    'thread-ioc-1',
                    'src-ioc-1',
                    'Recent APT-IOC DNS beaconing was observed against managed networks.',
                    '',
                    '2026-02-22T01:00:00+00:00',
                ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-domain-1',
                actor['id'],
                'domain',
                'malicious-example.net',
                'CISA alert',
                'valid',
                'active',
                0,
                1,
                '2026-02-22T01:05:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-hash-1',
                actor['id'],
                'hash',
                '9f86d081884c7d659a2feaa0c55ad015',
                'Sample feed',
                'valid',
                'active',
                0,
                1,
                '2026-02-22T01:06:00+00:00',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    cards = notebook.get('priority_questions', [])
    assert cards

    card = next((item for item in cards if str(item.get('id') or '') == 'thread-ioc-1'), None)
    assert card is not None
    related_iocs = card.get('related_iocs', [])
    assert any(
        str(ioc.get('ioc_type') or '').lower() == 'domain'
        and str(ioc.get('ioc_value') or '') == 'malicious-example.net'
        for ioc in related_iocs
    )
    assert not any(str(ioc.get('ioc_type') or '').lower() == 'hash' for ioc in related_iocs)


def test_priority_questions_use_source_derived_iocs_when_manual_iocs_missing(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Qilin', 'Derived IOC scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-derived-1',
                actor['id'],
                'IntelBlog',
                'https://intel.example/qilin-update',
                '2026-02-22',
                '2026-02-22T00:00:00+00:00',
                'Qilin infrastructure observed contacting beacon.qilin-test.net and 185.88.1.45 over HTTPS.',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_threads (
                id, actor_id, question_text, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'thread-derived-1',
                actor['id'],
                'Is Qilin beaconing via suspicious DNS domains?',
                'open',
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T01:00:00+00:00',
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
                'evt-derived-1',
                actor['id'],
                '2026-02-22T00:30:00+00:00',
                'command_and_control',
                'Beaconing update',
                'Beaconing and suspicious domain lookups observed for Qilin.',
                'src-derived-1',
                'Enterprise',
                '[]',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_updates (
                id, thread_id, source_id, trigger_excerpt, update_note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'update-derived-1',
                'thread-derived-1',
                'src-derived-1',
                'Suspicious beacon domain and IP linked to Qilin activity.',
                '',
                '2026-02-22T01:00:00+00:00',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    cards = notebook.get('priority_questions', [])
    assert cards
    card = next((item for item in cards if str(item.get('id') or '') == 'thread-derived-1'), None)
    assert card is not None
    related_iocs = card.get('related_iocs', [])
    assert any(str(ioc.get('ioc_type') or '').lower() == 'domain' for ioc in related_iocs)
    assert any('qilin-test.net' in str(ioc.get('ioc_value') or '').lower() for ioc in related_iocs)


