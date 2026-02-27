import sqlite3
import time
from datetime import datetime, timezone
from starlette.requests import Request
from fastapi import BackgroundTasks
from fastapi.testclient import TestClient

import pytest

import app as app_module


def _setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()


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


def test_fetch_actor_notebook_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_fetch_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)
        return {'ok': True}

    monkeypatch.setattr(app_module, 'pipeline_fetch_actor_notebook_core', _fake_fetch_core)

    result = app_module._fetch_actor_notebook('actor-wrapper-test')  # noqa: SLF001

    assert result == {'ok': True}
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
        str(card.get('first_step') or '') == 'Open Defender Advanced Hunting and run EmailEvents for the last 24h.'
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


def test_fetch_actor_notebook_payload_shape_regression(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Payload', 'Payload shape scope')

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001

    required_keys = {
        'actor',
        'recent_activity_highlights',
        'priority_questions',
        'kpis',
    }
    assert required_keys.issubset(notebook.keys())
    assert isinstance(notebook['actor'], dict)
    assert isinstance(notebook['recent_activity_highlights'], list)
    assert isinstance(notebook['priority_questions'], list)
    assert isinstance(notebook['kpis'], dict)


def test_ioc_hunts_route_enforces_actor_scoping_end_to_end(tmp_path):
    _setup_db(tmp_path)
    actor_a = app_module.create_actor_profile('APT-A', 'Scope A')
    actor_b = app_module.create_actor_profile('APT-B', 'Scope B')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-a-1',
                actor_a['id'],
                'domain',
                'actor-a-only.example',
                'feed-a',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                2,
                4,
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-b-1',
                actor_b['id'],
                'domain',
                'actor-b-only.example',
                'feed-b',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                2,
                4,
            ),
        )
        connection.commit()

    client = TestClient(app_module.app)
    response = client.get(f'/actors/{actor_a["id"]}/hunts/iocs?window_days=30')
    assert response.status_code == 200
    html = response.text
    assert 'actor-a-only.example' in html
    assert 'actor-b-only.example' not in html


def test_actor_live_route_enforces_actor_scoping(tmp_path):
    _setup_db(tmp_path)
    actor_a = app_module.create_actor_profile('APT-Live-A', 'Live scope A')
    actor_b = app_module.create_actor_profile('APT-Live-B', 'Live scope B')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-live-a',
                actor_a['id'],
                'domain',
                'actor-a-live.example',
                'feed-a',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                1,
                4,
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-live-b',
                actor_b['id'],
                'domain',
                'actor-b-live.example',
                'feed-b',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                1,
                4,
            ),
        )
        connection.commit()

    with TestClient(app_module.app) as client:
        response = client.get(f'/actors/{actor_a["id"]}/ui/live')
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor_a['id']
    assert 'actor-b-live.example' not in str(body)


def test_actor_live_route_short_circuits_while_running(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Live-Running', 'Live running scope')
    app_module.set_actor_notebook_status(actor['id'], 'running', 'Refreshing sources now...')

    def _should_not_run(*_args, **_kwargs):
        raise AssertionError('pipeline fetch should be skipped while notebook is running')

    monkeypatch.setattr(app_module, 'pipeline_fetch_actor_notebook_core', _should_not_run)

    with TestClient(app_module.app) as client:
        response = client.get(f'/actors/{actor["id"]}/ui/live')
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert body.get('notebook_status') == 'running'
    assert body.get('notebook_message') == 'Refreshing sources now...'
    assert body.get('kpis') == {}
    assert body.get('priority_questions') == []


def test_cold_actor_triggers_backfill_and_inserts_source(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Cold-Backfill', 'Cold backfill scope')
    calls: list[dict[str, object]] = []

    def _fake_backfill(actor_id: str, actor_name: str, actor_aliases: list[str] | None = None) -> dict[str, object]:
        calls.append({'actor_id': actor_id, 'actor_name': actor_name, 'actor_aliases': actor_aliases or []})
        with sqlite3.connect(app_module.DB_PATH) as connection:
            app_module._upsert_source_for_actor(  # noqa: SLF001
                connection=connection,
                actor_id=actor_id,
                source_name='unit42',
                source_url='https://unit42.paloaltonetworks.com/apt-cold-backfill-report',
                published_at='2026-02-24T00:00:00+00:00',
                pasted_text='APT-Cold-Backfill observed with suspicious PowerShell and vssadmin activity.',
                source_type='web_backfill',
            )
            connection.commit()
        return {'ran': True, 'inserted': 1, 'used_cache': False}

    monkeypatch.setattr(app_module, 'run_cold_actor_backfill', _fake_backfill)
    monkeypatch.setattr(app_module, 'build_notebook', lambda *_args, **_kwargs: None)

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    assert len(calls) == 1
    with sqlite3.connect(app_module.DB_PATH) as connection:
        inserted_count = connection.execute(
            'SELECT COUNT(*) FROM sources WHERE actor_id = ?',
            (actor['id'],),
        ).fetchone()[0]
    assert int(inserted_count) >= 1
    assert str(notebook.get('backfill_notice') or '') == 'Backfilled sources (cold actor)'


def test_backfill_not_run_when_recent_source_exists(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Warm', 'Warm actor scope')
    with sqlite3.connect(app_module.DB_PATH) as connection:
        app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=actor['id'],
            source_name='CISA',
            source_url='https://www.cisa.gov/example-warm',
            published_at=datetime.now(timezone.utc).isoformat(),
            pasted_text='APT-Warm actor report with fresh publication date.',
        )
        connection.commit()

    calls: list[str] = []

    def _fake_backfill(actor_id: str, actor_name: str, actor_aliases: list[str] | None = None) -> dict[str, object]:
        calls.append(actor_id)
        return {'ran': True, 'inserted': 1}

    monkeypatch.setattr(app_module, 'run_cold_actor_backfill', _fake_backfill)
    app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    assert calls == []


def test_quick_checks_after_backfill_respect_evidence_backed_citation_gate(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Citation', 'Citation gate scope')

    def _fake_backfill(actor_id: str, actor_name: str, actor_aliases: list[str] | None = None) -> dict[str, object]:
        with sqlite3.connect(app_module.DB_PATH) as connection:
            app_module._upsert_source_for_actor(  # noqa: SLF001
                connection=connection,
                actor_id=actor_id,
                source_name='Mandiant',
                source_url='https://www.mandiant.com/resources/blog/apt-citation-writeup',
                published_at='2026-02-24T00:00:00+00:00',
                pasted_text='APT-Citation actor operations observed in reporting.',
                source_type='web_backfill',
            )
            connection.commit()
        return {'ran': True, 'inserted': 1, 'used_cache': False}

    monkeypatch.setattr(app_module, 'run_cold_actor_backfill', _fake_backfill)
    monkeypatch.setattr(app_module, 'build_notebook', lambda *_args, **_kwargs: None)

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    questions = notebook.get('priority_questions', [])
    assert isinstance(questions, list)
    for question in questions:
        if bool(question.get('evidence_backed')):
            evidence_used = question.get('evidence_used', [])
            assert evidence_used
            assert 'No actor-linked evidence in last 30 days.' not in str(evidence_used[0])


def test_quick_checks_include_web_backfill_ingested_sources_in_evidence_window(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Black Basta', 'Ingested-date fallback scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at, pasted_text, source_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-ingested-1',
                actor['id'],
                'Mandiant',
                'https://www.mandiant.com/resources/blog/black-basta-ingested',
                None,
                '2026-02-24T00:00:00+00:00',
                'ingested',
                '2026-02-24T00:00:00+00:00',
                'Black Basta operators showed beaconing behavior and suspicious PowerShell execution patterns.',
                'web_backfill',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_threads (
                id, actor_id, question_text, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'thread-ingested-evidence',
                actor['id'],
                'Is Black Basta showing command-and-control beaconing from compromised hosts?',
                'open',
                '2026-02-24T00:05:00+00:00',
                '2026-02-24T00:10:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_updates (
                id, thread_id, source_id, trigger_excerpt, update_note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'upd-ingested-1',
                'thread-ingested-evidence',
                'src-ingested-1',
                'Black Basta beaconing observed with PowerShell execution.',
                '',
                '2026-02-24T00:15:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-ingested-1',
                actor['id'],
                '2026-02-24T00:20:00+00:00',
                'command_and_control',
                'Black Basta beaconing activity',
                'Recent Black Basta reporting indicates recurring beaconing and PowerShell execution.',
                'src-ingested-1',
                'Enterprise',
                '[]',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    card = next(
        (item for item in notebook.get('priority_questions', []) if item.get('id') == 'thread-ingested-evidence'),
        None,
    )
    assert card is not None
    assert bool(card.get('evidence_backed')) is True
    evidence_used = [str(item) for item in (card.get('evidence_used') or [])]
    assert any('black-basta-ingested' in item for item in evidence_used)
    assert any('(ingested)' in item for item in evidence_used)


def test_quick_checks_can_use_recent_sources_when_question_updates_are_missing(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Black Basta', 'Source-fallback quick checks scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at, pasted_text, source_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-fallback-1',
                actor['id'],
                'Unit42',
                'https://unit42.paloaltonetworks.com/black-basta-c2-profile',
                None,
                '2026-02-24T02:00:00+00:00',
                'ingested',
                '2026-02-24T02:00:00+00:00',
                'Black Basta campaign analysis documents beaconing C2 traffic and PowerShell execution behavior.',
                'web_backfill',
            ),
        )
        connection.execute(
            '''
            INSERT INTO question_threads (
                id, actor_id, question_text, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'thread-fallback-1',
                actor['id'],
                'Is Black Basta beaconing through C2 infrastructure right now?',
                'open',
                '2026-02-24T02:05:00+00:00',
                '2026-02-24T02:10:00+00:00',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    card = next(
        (item for item in notebook.get('priority_questions', []) if item.get('id') == 'thread-fallback-1'),
        None,
    )
    assert card is not None
    assert bool(card.get('evidence_backed')) is True
    evidence_used = [str(item) for item in (card.get('evidence_used') or [])]
    assert any('black-basta-c2-profile' in item for item in evidence_used)
    assert any('(ingested)' in item for item in evidence_used)

def test_source_quality_filters_scope_recent_change_inputs(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Filter', 'Source quality filter test')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        src_high = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'CISA',
            'https://www.cisa.gov/news-events/cybersecurity-advisories/example-1',
            '2026-02-20T00:00:00+00:00',
            'APT-Filter exploited CVE-2026-0001 against healthcare organizations.',
            'APT-Filter exploited CVE-2026-0001 against healthcare organizations.',
        )
        src_unrated = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Unknown Blog',
            'https://unknown-security.example/reports/apt-filter-update',
            '2026-02-19T00:00:00+00:00',
            'APT-Filter used PowerShell execution and targeted finance entities.',
            'APT-Filter used PowerShell execution and targeted finance entities.',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-high',
                actor['id'],
                '2026-02-20T00:00:00+00:00',
                'initial_access',
                'Initial access move',
                'APT-Filter exploited CVE-2026-0001 to gain access.',
                src_high,
                'Healthcare',
                '["T1190"]',
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
                'evt-unrated',
                actor['id'],
                '2026-02-19T00:00:00+00:00',
                'execution',
                'Execution move',
                'APT-Filter used PowerShell for follow-on execution.',
                src_unrated,
                'Finance',
                '["T1190"]',
            ),
        )
        connection.commit()

    notebook_all = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        min_confidence_weight=0,
        source_days=3650,
    )
    all_urls = {
        str(item.get('source_url') or '')
        for item in notebook_all.get('recent_activity_highlights', [])
    }
    assert any('cisa.gov' in value for value in all_urls)
    assert any('unknown-security.example' in value for value in all_urls)
    assert any(
        str(item.get('technique_id') or '').strip().upper() == 'T1190'
        for item in notebook_all.get('emerging_techniques', [])
    )

    notebook_high_only = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        source_tier='high',
    )
    high_urls = [
        str(item.get('source_url') or '')
        for item in notebook_high_only.get('recent_activity_highlights', [])
    ]
    assert high_urls
    assert all('cisa.gov' in value for value in high_urls)
    filters = notebook_high_only.get('source_quality_filters', {})
    assert str(filters.get('source_tier') or '') == 'high'
    assert str(filters.get('total_sources') or '') == '2'
    assert str(filters.get('applied_sources') or '') == '1'
    assert str(filters.get('filtered_out_sources') or '') == '1'
    filtered_source_urls = {
        str(item.get('url') or '')
        for item in notebook_high_only.get('sources', [])
        if str(item.get('source_tier') or '').strip().lower() == 'high'
    }
    top_signal_urls = {
        str(evidence.get('source_url') or '')
        for signal in notebook_high_only.get('top_change_signals', [])
        if isinstance(signal, dict)
        for evidence in (signal.get('validated_sources') or [])
        if isinstance(evidence, dict) and str(evidence.get('source_url') or '').strip()
    }
    assert top_signal_urls
    assert top_signal_urls.issubset(filtered_source_urls)
    assert not any(
        str(item.get('technique_id') or '').strip().upper() == 'T1190'
        for item in notebook_high_only.get('emerging_techniques', [])
    )


def test_source_quality_filters_apply_weight_and_days(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Filter-Days', 'Source quality day/weight test')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        src_recent_high = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'CISA',
            'https://www.cisa.gov/news-events/cybersecurity-advisories/example-2',
            '2026-02-21T00:00:00+00:00',
            'APT-Filter-Days activity and exploitation details.',
            'APT-Filter-Days activity and exploitation details.',
        )
        src_old_medium = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Mandiant',
            'https://www.mandiant.com/resources/blog/legacy-activity-report',
            '2025-01-10T00:00:00+00:00',
            'APT-Filter-Days legacy campaign activity.',
            'APT-Filter-Days legacy campaign activity.',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-recent-high',
                actor['id'],
                '2026-02-21T00:00:00+00:00',
                'initial_access',
                'Recent high source',
                'APT-Filter-Days exploited internet-facing services.',
                src_recent_high,
                'Government',
                '["T1190"]',
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
                'evt-old-medium',
                actor['id'],
                '2025-01-10T00:00:00+00:00',
                'execution',
                'Old medium source',
                'APT-Filter-Days executed staged tooling.',
                src_old_medium,
                'Technology',
                '["T1059"]',
            ),
        )
        connection.commit()

    notebook_filtered = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        min_confidence_weight=3,
        source_days=60,
    )
    filtered_urls = [
        str(item.get('source_url') or '')
        for item in notebook_filtered.get('recent_activity_highlights', [])
    ]
    assert filtered_urls
    assert any('cisa.gov' in value for value in filtered_urls)
    assert all('mandiant.com' not in value for value in filtered_urls)
    filters = notebook_filtered.get('source_quality_filters', {})
    assert str(filters.get('min_confidence_weight') or '') == '3'
    assert str(filters.get('source_days') or '') == '60'
    assert str(filters.get('total_sources') or '') == '2'
    assert str(filters.get('applied_sources') or '') == '1'
    assert str(filters.get('filtered_out_sources') or '') == '1'


def test_generate_actor_requirements_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_generate_core(actor_id, org_context, priority_mode, **kwargs):
        captured['actor_id'] = actor_id
        captured['org_context'] = org_context
        captured['priority_mode'] = priority_mode
        captured.update(kwargs)
        return 2

    monkeypatch.setattr(app_module, 'pipeline_generate_actor_requirements_core', _fake_generate_core)

    inserted = app_module.generate_actor_requirements('actor-1', 'finance org', 'Operational')

    assert inserted == 2
    assert captured['actor_id'] == 'actor-1'
    assert captured['org_context'] == 'finance org'
    assert captured['priority_mode'] == 'Operational'
    assert captured['db_path'] == app_module.DB_PATH
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert 'actor_exists' in deps
    assert 'build_actor_profile_from_mitre' in deps
    assert 'new_id' in deps


def test_import_default_feeds_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_import_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)
        return 5

    monkeypatch.setattr(app_module, 'pipeline_import_default_feeds_for_actor_core', _fake_import_core)

    imported = app_module.import_default_feeds_for_actor('actor-feed-wrapper')

    assert imported == 5
    assert captured['actor_id'] == 'actor-feed-wrapper'
    assert captured['db_path'] == app_module.DB_PATH
    assert captured['default_cti_feeds'] == app_module.DEFAULT_CTI_FEEDS
    assert captured['actor_feed_lookback_days'] == app_module.ACTOR_FEED_LOOKBACK_DAYS
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert 'derive_source_from_url' in deps
    assert 'upsert_source_for_actor' in deps
    assert 'duckduckgo_actor_search_urls' in deps


def test_run_actor_generation_wrapper_delegates_to_pipeline_core(monkeypatch):
    monkeypatch.setattr(app_module, '_mark_actor_generation_started', lambda _actor_id: True)  # noqa: SLF001
    monkeypatch.setattr(app_module, '_mark_actor_generation_finished', lambda _actor_id: None)  # noqa: SLF001

    captured: dict[str, object] = {}

    def _fake_run_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)

    monkeypatch.setattr(app_module, 'pipeline_run_actor_generation_core', _fake_run_core)

    app_module.run_actor_generation('actor-runner-wrapper')

    assert captured['actor_id'] == 'actor-runner-wrapper'
    assert captured['db_path'] == app_module.DB_PATH
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert deps['set_actor_notebook_status'] is app_module.set_actor_notebook_status
    assert deps['import_default_feeds_for_actor'] is app_module.import_default_feeds_for_actor
    assert deps['build_notebook'] is app_module.build_notebook


def test_derive_source_from_url_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_derive_core(source_url, **kwargs):
        captured['source_url'] = source_url
        captured.update(kwargs)
        return {'source_name': 'example.com', 'source_url': source_url, 'pasted_text': 'ok'}

    monkeypatch.setattr(app_module, 'pipeline_derive_source_from_url_core', _fake_derive_core)

    result = app_module.derive_source_from_url('https://example.com/post', fallback_source_name='Example')

    assert result['source_name'] == 'example.com'
    assert captured['source_url'] == 'https://example.com/post'
    assert captured['fallback_source_name'] == 'Example'
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert deps['safe_http_get'] is app_module._safe_http_get  # noqa: SLF001
    assert deps['extract_question_sentences'] is app_module._extract_question_sentences  # noqa: SLF001
    assert deps['first_sentences'] is app_module._first_sentences  # noqa: SLF001


def test_evidence_title_prefers_structured_title_over_pasted_text():
    source = {
        'title': 'Executive Threat Update',
        'headline': 'Should not be used',
        'pasted_text': 'First pasted sentence that would otherwise be chosen.',
        'url': 'https://example.com/article',
    }

    title = app_module._evidence_title_from_source(source)  # noqa: SLF001

    assert title == 'Executive Threat Update'


def test_priority_where_to_check_wrapper_delegates_to_priority_module(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_priority_where_to_check(guidance_items, question_text, **kwargs):
        captured['guidance_items'] = guidance_items
        captured['question_text'] = question_text
        captured.update(kwargs)
        return 'Firewall/VPN'

    monkeypatch.setattr(app_module.priority_questions, 'priority_where_to_check', _fake_priority_where_to_check)

    result = app_module._priority_where_to_check([{'platform': 'EDR'}], 'Is edge access compromised?')  # noqa: SLF001

    assert result == 'Firewall/VPN'
    assert captured['question_text'] == 'Is edge access compromised?'
    assert callable(captured['platforms_for_question'])


def test_question_org_alignment_preserves_overlap_scoring():
    score = app_module._question_org_alignment(  # noqa: SLF001
        'How should we protect finance payment systems from ransomware?',
        'Priority assets include finance payment systems and payroll operations.',
    )
    assert score >= 2


def test_platforms_for_question_wrapper_delegates_to_guidance_catalog(monkeypatch):
    monkeypatch.setattr(app_module.guidance_catalog, 'platforms_for_question', lambda _q: ['DNS/Proxy'])

    platforms = app_module._platforms_for_question('Any question')  # noqa: SLF001

    assert platforms == ['DNS/Proxy']


def test_platforms_for_question_dedupes_and_prioritizes_expected_domains():
    platforms = app_module._platforms_for_question(  # noqa: SLF001
        'Phish email with VPN exploit and DNS beacon plus process command line'
    )

    assert platforms[0] == 'M365'
    assert platforms.count('M365') == 1
    assert 'Email Gateway' in platforms
    assert 'Firewall/VPN' in platforms
    assert 'DNS/Proxy' in platforms
    assert 'EDR' in platforms


def test_extract_major_move_events_wrapper_delegates_to_timeline_module(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_extract_major_move_events(source_name, source_id, occurred_at, text, actor_terms, **kwargs):
        captured['source_name'] = source_name
        captured['source_id'] = source_id
        captured['occurred_at'] = occurred_at
        captured['text'] = text
        captured['actor_terms'] = actor_terms
        captured.update(kwargs)
        return [{'id': 'evt-1'}]

    monkeypatch.setattr(app_module.timeline_extraction, 'extract_major_move_events', _fake_extract_major_move_events)

    events = app_module._extract_major_move_events(  # noqa: SLF001
        'CISA',
        'src-1',
        '2026-02-20T00:00:00+00:00',
        'APT-Flow exploited edge devices.',
        ['apt-flow'],
    )

    assert events == [{'id': 'evt-1'}]
    assert captured['source_name'] == 'CISA'
    assert callable(captured['deps']['split_sentences'])
    assert callable(captured['deps']['extract_ttp_ids'])
    assert callable(captured['deps']['new_id'])


def test_extract_major_move_events_behavior_classifies_and_targets():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'CISA',
        'src-1',
        '2026-02-20T00:00:00+00:00',
        'APT-Flow targeted Acme Hospital and used PowerShell execution for access.',
        ['apt-flow'],
    )

    assert events
    assert events[0]['category'] == 'execution'
    assert 'Acme Hospital' in str(events[0]['target_text'])


def test_extract_major_move_events_ransomware_live_keeps_full_structured_synthesis():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-1',
        '2026-02-23T11:57:55.045635+00:00',
        (
            'Who: Qilin ransomware operators.\n'
            'What: 15 public victim disclosures in the last 90 days.\n'
            'When: Latest listed disclosure date is 2026-02-22.\n'
            'Where: US (6), FR (1), NZ (1).\n'
            'How/Targets: Manufacturing (3), Healthcare (2).'
        ),
        ['qilin'],
    )

    assert len(events) == 1
    assert events[0]['category'] == 'impact'
    assert '90d disclosures: 15' in str(events[0]['summary'])
    assert 'Top geographies: US (6), FR (1), NZ (1)' in str(events[0]['summary'])
    assert 'Top sectors: Manufacturing (3), Healthcare (2)' in str(events[0]['summary'])
    assert '\n' in str(events[0]['summary'])
    assert 'Who:' not in str(events[0]['summary'])
    assert 'What:' not in str(events[0]['summary'])
    assert 'When:' not in str(events[0]['summary'])


def test_extract_major_move_events_ransomware_live_normalizes_legacy_trend_blob():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-legacy',
        '2026-02-17T09:37:10+00:00',
        (
            'qilin ransomware activity synthesis (tempo, geography, and target examples) from ransomware.live. '
            'Ransomware.live trend for qilin: 1469 total public victim disclosures, 15 in the last 90 days. '
            'Latest listed activity: 2026-02-16. '
            'Most frequent victim geographies in the current sample: US (8), CL (2), IT (1). '
            'Recently observed targets include: 2026-02-16 - Casartigiani (IT).'
        ),
        ['qilin'],
    )

    assert len(events) == 1
    assert events[0]['title'] == 'Qilin ransomware disclosure and targeting update'
    summary = str(events[0]['summary'])
    assert '90d disclosures: 15' in summary
    assert 'Total listed: 1469' in summary
    assert 'Top geographies: US (8), CL (2), IT (1)' in summary
    assert 'Ransomware.live trend for' not in summary
    assert 'Latest listed activity date:' not in summary


def test_extract_major_move_events_ransomware_live_uses_full_prose_summary():
    text = (
        'Qilin ransomware operators have 15 public victim disclosures in the last 90 days '
        '(1498 total listed disclosures in this ransomware.live sample). '
        'Latest listed disclosure date is 2026-02-22. '
        'Most frequently listed victim geographies in this sample are US (6), FR (1), NZ (1). '
        'Most frequently listed victim sectors are Not Found (5), Manufacturing (3), Healthcare (2). '
        'Recent listed victim examples: Example One; Example Two; Example Three. '
        'Analyst use: Treat this as trend context.'
    )
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-prose',
        '2026-02-23T12:23:57+00:00',
        text,
        ['qilin'],
    )

    assert len(events) == 1
    summary = str(events[0]['summary'])
    assert '90d disclosures: 15' in summary
    assert 'Total listed: 1498' in summary
    assert 'Top geographies: US (6), FR (1), NZ (1)' in summary
    assert 'Top sectors: Not Found (5), Manufacturing (3), Healthcare (2)' in summary
    assert 'Analyst use:' not in summary


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

    def _should_not_fetch(*_args, **_kwargs):
        raise AssertionError('derive_source_from_url should not be called for manual text imports')

    monkeypatch.setattr(app_module, 'derive_source_from_url', _should_not_fetch)

    with TestClient(app_module.app) as client:
        response = client.post(
            f"/actors/{actor['id']}/sources",
            data={
                'source_url': 'https://example.com/report',
                'pasted_text': 'Manual analyst text about APT-Manual operations and observed tactics.',
                'published_at': '2026-02-10',
            },
            follow_redirects=False,
        )

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


def test_capability_category_from_technique_id_uses_mitre_dataset(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--1',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1566'},
                    ],
                    'kill_chain_phases': [{'phase_name': 'initial-access'}],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_TECHNIQUE_PHASE_CACHE', None)

    assert app_module._capability_category_from_technique_id('T1566') == 'initial_access'  # noqa: SLF001
    assert app_module._capability_category_from_technique_id('T1566.001') == 'initial_access'  # noqa: SLF001
    assert app_module._capability_category_from_technique_id('T9999') is None  # noqa: SLF001


def test_create_observation_generates_delta_from_mitre_tactic_mapping(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Mitre', 'MITRE-mapped observation scope')
    app_module.initialize_actor_state(actor['id'])

    with TestClient(app_module.app) as client:
        monkeypatch.setattr(
            app_module,
            'MITRE_DATASET_CACHE',
            {
                'objects': [
                    {
                        'type': 'attack-pattern',
                        'id': 'attack-pattern--2',
                        'external_references': [
                            {'source_name': 'mitre-attack', 'external_id': 'T1071'},
                        ],
                        'kill_chain_phases': [{'phase_name': 'command-and-control'}],
                    }
                ]
            },
        )
        monkeypatch.setattr(app_module, 'MITRE_TECHNIQUE_PHASE_CACHE', None)

        obs_response = client.post(
            f"/actors/{actor['id']}/state/observations",
            json={
                'source_type': 'report',
                'source_ref': 'unit-test',
                'ttp_list': ['t1071.001'],
                'tools_list': [],
                'infra_list': [],
                'target_list': [],
            },
        )
        assert obs_response.status_code == 200

        deltas_response = client.get(f"/actors/{actor['id']}/deltas")
        assert deltas_response.status_code == 200
        deltas = deltas_response.json()

    assert deltas
    assert deltas[0]['affected_category'] == 'command_and_control'
    assert deltas[0]['status'] == 'pending'


def test_match_mitre_group_uses_x_mitre_aliases_and_attack_id(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'intrusion-set',
                    'id': 'intrusion-set--unit-1',
                    'name': 'Alpha Group',
                    'aliases': ['Alpha Legacy'],
                    'x_mitre_aliases': ['Alias One'],
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'G1234'},
                    ],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_GROUP_CACHE', None)

    alias_match = app_module._match_mitre_group('Alias One')  # noqa: SLF001
    assert alias_match is not None
    assert alias_match['name'] == 'Alpha Group'

    id_match = app_module._match_mitre_group('G1234')  # noqa: SLF001
    assert id_match is not None
    assert id_match['name'] == 'Alpha Group'


def test_match_mitre_software_uses_aliases_and_attack_id(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'tool',
                    'id': 'tool--unit-1',
                    'name': 'Gamma Tool',
                    'aliases': ['Gamma Legacy'],
                    'x_mitre_aliases': ['GammaX'],
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'S9001'},
                    ],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_SOFTWARE_CACHE', None)

    alias_match = app_module._match_mitre_software('Gamma Legacy')  # noqa: SLF001
    assert alias_match is not None
    assert alias_match['name'] == 'Gamma Tool'

    id_match = app_module._match_mitre_software('S9001')  # noqa: SLF001
    assert id_match is not None
    assert id_match['name'] == 'Gamma Tool'


def test_match_mitre_software_fuzzy_uses_alias_tokens(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'malware',
                    'id': 'malware--unit-2',
                    'name': 'Unrelated Primary',
                    'x_mitre_aliases': ['Wizard Spider'],
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'S9002'},
                    ],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_SOFTWARE_CACHE', None)

    fuzzy_match = app_module._match_mitre_software('wizard spider team')  # noqa: SLF001
    assert fuzzy_match is not None
    assert fuzzy_match['name'] == 'Unrelated Primary'


def test_match_mitre_group_uses_campaign_alias_enrichment(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'intrusion-set',
                    'id': 'intrusion-set--unit-2',
                    'name': 'Canonical Group',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'G2000'},
                    ],
                },
                {
                    'type': 'campaign',
                    'id': 'campaign--unit-1',
                    'name': 'Operation Snowfall',
                    'x_mitre_aliases': ['Snowfall Cluster'],
                },
                {
                    'type': 'relationship',
                    'relationship_type': 'attributed-to',
                    'source_ref': 'campaign--unit-1',
                    'target_ref': 'intrusion-set--unit-2',
                },
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_GROUP_CACHE', None)
    monkeypatch.setattr(app_module, 'MITRE_CAMPAIGN_LINK_CACHE', None)

    match = app_module._match_mitre_group('Snowfall Cluster')  # noqa: SLF001
    assert match is not None
    assert match['name'] == 'Canonical Group'


def test_match_mitre_software_uses_campaign_alias_enrichment(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'tool',
                    'id': 'tool--unit-9',
                    'name': 'Canonical Tool',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'S2000'},
                    ],
                },
                {
                    'type': 'campaign',
                    'id': 'campaign--unit-9',
                    'name': 'Project Lantern',
                    'aliases': ['Lantern Ops'],
                },
                {
                    'type': 'relationship',
                    'relationship_type': 'uses',
                    'source_ref': 'campaign--unit-9',
                    'target_ref': 'tool--unit-9',
                },
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_SOFTWARE_CACHE', None)
    monkeypatch.setattr(app_module, 'MITRE_CAMPAIGN_LINK_CACHE', None)

    match = app_module._match_mitre_software('Lantern Ops')  # noqa: SLF001
    assert match is not None
    assert match['name'] == 'Canonical Tool'


def test_actors_ui_escapes_actor_display_name(tmp_path):
    _setup_db(tmp_path)
    app_module.create_actor_profile('APT-<script>alert(1)</script>', 'Test scope')

    response = app_module.actors_ui()

    assert '<script>alert(1)</script>' not in response
    assert 'APT-&lt;script&gt;alert(1)&lt;/script&gt;' in response


def test_resolve_startup_db_path_falls_back_on_permission_error(monkeypatch):
    original_db_path = app_module.DB_PATH
    app_module.DB_PATH = '/data/app.db'
    calls: list[str] = []

    def fake_prepare(path_value: str) -> str:
        calls.append(path_value)
        if path_value == '/data/app.db':
            raise PermissionError('denied')
        return path_value

    monkeypatch.setattr(app_module, '_prepare_db_path', fake_prepare)
    resolved = app_module._resolve_startup_db_path()  # noqa: SLF001
    app_module.DB_PATH = original_db_path

    assert calls[0] == '/data/app.db'
    assert resolved.endswith('/app.db')


def test_root_handles_notebook_load_failure(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Render', 'Render scope')
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute('UPDATE actor_profiles SET is_tracked = 1 WHERE id = ?', (actor['id'],))
        connection.commit()

    monkeypatch.setattr(
        app_module,
        '_fetch_actor_notebook',
        lambda actor_id, **_kwargs: (_ for _ in ()).throw(RuntimeError('boom')),
    )
    monkeypatch.setattr(app_module, 'get_ollama_status', lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'})

    scope = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': '/',
        'raw_path': b'/',
        'query_string': f'actor_id={actor["id"]}'.encode(),
        'headers': [],
        'client': ('127.0.0.1', 12345),
        'server': ('testserver', 80),
    }
    request = Request(scope)

    response = app_module.root(
        request=request,
        background_tasks=BackgroundTasks(),
        actor_id=str(actor['id']),
        notice=None,
    )

    assert response.status_code == 200


def test_root_renders_analyst_flow_headings(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Flow', 'Flow scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    with TestClient(app_module.app) as client:
        response = client.get(f'/?actor_id={actor["id"]}')

    assert response.status_code == 200
    assert '1) Who are they?' in response.text
    assert '2) What have they been up to recently?' in response.text
    assert 'Quick checks' in response.text


def test_route_table_has_no_duplicate_method_path_pairs():
    route_map: dict[tuple[tuple[str, ...], str], int] = {}
    for route in app_module.app.routes:
        methods = tuple(sorted(method for method in getattr(route, 'methods', set()) if method not in {'HEAD', 'OPTIONS'}))
        path = str(getattr(route, 'path', '') or '')
        if not methods or not path:
            continue
        key = (methods, path)
        route_map[key] = route_map.get(key, 0) + 1
    duplicates = [key for key, count in route_map.items() if count > 1]
    assert not duplicates


def test_observation_filters_and_exports(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Obs', 'Observation filter/export scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, title, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-ob-1',
                actor['id'],
                'Unit42',
                'https://unit42.example/report',
                '2026-02-20',
                '2026-02-20T00:00:00+00:00',
                'Unit 42 report',
                'APT-Obs activity writeup.',
            ),
        )
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref, confidence,
                source_reliability, information_credibility, updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'obs-1',
                actor['id'],
                'source',
                'src-ob-1',
                'High-confidence pattern shift.',
                'case-1',
                'high',
                'A',
                '1',
                'alice',
                '2026-02-21T11:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref, confidence,
                source_reliability, information_credibility, updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'obs-2',
                actor['id'],
                'source',
                'src-ob-2',
                'Lower-confidence duplicate note.',
                'case-2',
                'low',
                'C',
                '3',
                'bob',
                '2026-02-22T11:00:00+00:00',
            ),
        )
        connection.commit()

    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/observations?analyst=ali&confidence=high&limit=10&offset=0")
        assert response.status_code == 200
        payload = response.json()
        assert payload['actor_id'] == actor['id']
        assert payload['limit'] == 10
        assert payload['offset'] == 0
        assert len(payload['items']) == 1
        assert payload['items'][0]['updated_by'] == 'alice'
        assert payload['items'][0]['source_title'] == 'Unit 42 report'

        json_export = client.get(f"/actors/{actor['id']}/observations/export.json?confidence=high")
        assert json_export.status_code == 200
        json_payload = json_export.json()
        assert json_payload['count'] == 1
        assert json_payload['items'][0]['confidence'] == 'high'

        csv_export = client.get(f"/actors/{actor['id']}/observations/export.csv?analyst=ali")
        assert csv_export.status_code == 200
        assert 'text/csv' in (csv_export.headers.get('content-type') or '')
        assert 'High-confidence pattern shift.' in csv_export.text
        assert 'Lower-confidence duplicate note.' not in csv_export.text


def test_observation_upsert_returns_non_blocking_quality_guidance(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Obs-Q', 'Observation quality guidance scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    with TestClient(app_module.app) as client:
        response = client.post(
            f"/actors/{actor['id']}/observations/source/src-guidance",
            json={
                'updated_by': 'alice',
                'confidence': 'high',
                'note': 'Needs review',
                'source_ref': '',
                'source_reliability': '',
                'information_credibility': '',
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload['ok'] is True
        assert isinstance(payload.get('quality_guidance'), list)
        assert payload['quality_guidance']
        assert any('source reference' in item for item in payload['quality_guidance'])


def test_observation_history_endpoint_tracks_versions(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Obs-History', 'Observation history scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    with TestClient(app_module.app) as client:
        first = client.post(
            f"/actors/{actor['id']}/observations/source/src-history",
            json={
                'updated_by': 'alice',
                'confidence': 'moderate',
                'note': 'Initial baseline observation.',
                'source_ref': 'case-1',
            },
        )
        assert first.status_code == 200

        second = client.post(
            f"/actors/{actor['id']}/observations/source/src-history",
            json={
                'updated_by': 'alice',
                'confidence': 'high',
                'note': 'Second observation with stronger corroboration.',
                'source_ref': 'case-2',
                'source_reliability': 'A',
                'information_credibility': '1',
            },
        )
        assert second.status_code == 200

        history = client.get(f"/actors/{actor['id']}/observations/source/src-history/history?limit=10")
        assert history.status_code == 200
        payload = history.json()
        assert payload['actor_id'] == actor['id']
        assert payload['item_type'] == 'source'
        assert payload['item_key'] == 'src-history'
        assert payload['count'] >= 2
        assert payload['items'][0]['note'] == 'Second observation with stronger corroboration.'
        assert payload['items'][1]['note'] == 'Initial baseline observation.'


def test_auto_snapshot_and_export_analyst_pack(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Auto', 'Auto snapshot scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, title, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-auto-1',
                actor['id'],
                'CISA',
                'https://example.test/a1',
                '2026-02-22',
                '2026-02-22T00:00:00+00:00',
                'Auto source',
                'Automated snapshot content for actor.',
            ),
        )
        connection.commit()
    app_module.build_notebook(actor['id'])

    with TestClient(app_module.app) as client:
        response = client.post(
            f"/actors/{actor['id']}/observations/auto-snapshot",
            follow_redirects=False,
        )
        assert response.status_code == 303
        observations = client.get(f"/actors/{actor['id']}/observations")
        assert observations.status_code == 200
        items = observations.json().get('items', [])
        assert items
        assert any(str(item.get('updated_by') or '') == 'auto' for item in items)

        pack = client.get(
            f"/actors/{actor['id']}/export/analyst-pack.json",
            params={'source_tier': 'high', 'min_confidence_weight': '3', 'source_days': '90'},
        )
        assert pack.status_code == 200
        payload = pack.json()
        assert payload['actor_id'] == actor['id']
        quality_filters = payload.get('source_quality_filters', {})
        assert str(quality_filters.get('source_tier') or '') == 'high'
        assert str(quality_filters.get('min_confidence_weight') or '') == '3'
        assert str(quality_filters.get('source_days') or '') == '90'
        assert 'observations' in payload
        assert 'observation_history' in payload

        pdf_pack = client.get(
            f"/actors/{actor['id']}/export/analyst-pack.pdf",
            params={'source_tier': 'high', 'min_confidence_weight': '3', 'source_days': '90'},
        )
        assert pdf_pack.status_code == 200
        assert 'application/pdf' in str(pdf_pack.headers.get('content-type') or '')
        assert str(pdf_pack.headers.get('content-disposition') or '').endswith('-analyst-pack.pdf"')
        assert pdf_pack.content.startswith(b'%PDF-1.4')


def test_ioc_hunts_misc_only_includes_actor_related_unmatched_iocs(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Qilin', 'Misc IOC scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-hunt-1',
                actor['id'],
                'Qilin report',
                'https://intel.example/qilin-update',
                '2026-02-22',
                '2026-02-22T00:00:00+00:00',
                'Qilin infrastructure observed contacting beacon.qilin-test.net.',
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
                'thread-hunt-1',
                actor['id'],
                'Is Qilin beaconing via suspicious DNS domains?',
                'open',
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T01:00:00+00:00',
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
                'update-hunt-1',
                'thread-hunt-1',
                'src-hunt-1',
                'Beaconing to suspicious infrastructure observed for Qilin.',
                '',
                '2026-02-22T01:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, is_active, validation_status, confidence_score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-used-1',
                actor['id'],
                'domain',
                'beacon.qilin-test.net',
                'Qilin report',
                1,
                'valid',
                4,
                '2026-02-22T01:05:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, is_active, validation_status, confidence_score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-misc-related-1',
                actor['id'],
                'ip',
                '185.88.1.45',
                'Qilin malware note',
                1,
                'valid',
                4,
                '2026-02-22T01:06:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, is_active, validation_status, confidence_score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-misc-unrelated-1',
                actor['id'],
                'ip',
                '203.0.113.9',
                'Unrelated campaign',
                1,
                'valid',
                5,
                '2026-02-22T01:07:00+00:00',
            ),
        )
        connection.commit()

    with TestClient(app_module.app) as client:
        response = client.get(
            f"/actors/{actor['id']}/hunts/iocs",
            params={
                'quick_check_id': 'thread-hunt-1',
                'window_start': '2026-02-01T00:00:00+00:00',
                'window_end': '2026-03-01T00:00:00+00:00',
            },
        )
        assert response.status_code == 200
        assert 'Check pre-filter: thread-hunt-1' in response.text
        assert 'Actor-related unmatched IOCs' in response.text
        assert '185.88.1.45' in response.text
        assert '203.0.113.9' not in response.text


def test_actor_ioc_hunts_renders_system_specific_section_queries(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Systems', 'System-specific hunt query scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        now_iso = '2026-02-24T00:00:00+00:00'
        rows = [
            ('ioc-sys-domain', 'domain', 'systems-check.example'),
            ('ioc-sys-ip', 'ip', '198.51.100.10'),
            ('ioc-sys-hash', 'hash', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
            ('ioc-sys-email', 'email', 'analyst@example.com'),
        ]
        for ioc_id, ioc_type, ioc_value in rows:
            connection.execute(
                '''
                INSERT INTO ioc_items (
                    id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                    lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    ioc_id,
                    actor['id'],
                    ioc_type,
                    ioc_value,
                    'systems ref',
                    'valid',
                    'active',
                    0,
                    1,
                    now_iso,
                    now_iso,
                    2,
                    4,
                ),
            )
        connection.commit()

    with TestClient(app_module.app) as client:
        response = client.get(f'/actors/{actor["id"]}/hunts/iocs?window_days=30')
    assert response.status_code == 200
    html = response.text
    assert 'Generic (Vendor-neutral)' in html
    assert 'DnsEvents, DeviceNetworkEvents, CommonSecurityLog (proxy/web gateway)' in html
    assert 'CommonSecurityLog (firewall), DeviceNetworkEvents, VMConnection/Zeek if available' in html
    assert 'DeviceProcessEvents, DeviceFileEvents, SecurityEvent (4688/4104)' in html
    assert 'SigninLogs, IdentityLogonEvents, AuditLogs, SecurityEvent(4624/4625)' in html


def test_fetch_notebook_ioc_items_excludes_unvalidated_revoked_and_expired(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-IOC-Filter', 'IOC filter scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                confidence_score, lifecycle_status, revoked, valid_until, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-valid',
                actor['id'],
                'domain',
                'active.example',
                'active.example',
                'valid',
                4,
                'active',
                0,
                '2099-01-01T00:00:00+00:00',
                1,
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T00:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                confidence_score, lifecycle_status, revoked, valid_until, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-unvalidated',
                actor['id'],
                'domain',
                'unvalidated.example',
                'unvalidated.example',
                'unvalidated',
                4,
                'active',
                0,
                '2099-01-01T00:00:00+00:00',
                1,
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T00:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                confidence_score, lifecycle_status, revoked, valid_until, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-revoked',
                actor['id'],
                'domain',
                'revoked.example',
                'revoked.example',
                'valid',
                4,
                'revoked',
                1,
                '2099-01-01T00:00:00+00:00',
                0,
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T00:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                confidence_score, lifecycle_status, revoked, valid_until, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-expired',
                actor['id'],
                'domain',
                'expired.example',
                'expired.example',
                'valid',
                4,
                'active',
                0,
                '2020-01-01T00:00:00+00:00',
                1,
                '2026-02-22T00:00:00+00:00',
                '2026-02-22T00:00:00+00:00',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    values = {str(item.get('ioc_value') or '') for item in notebook.get('ioc_items', [])}
    assert 'active.example' in values
    assert 'unvalidated.example' not in values
    assert 'revoked.example' not in values
    assert 'expired.example' not in values


def test_quick_checks_are_actor_specific_not_global_template(tmp_path):
    _setup_db(tmp_path)
    actor_a = app_module.create_actor_profile('Qilin', 'Actor A scope')
    actor_b = app_module.create_actor_profile('Akira', 'Actor B scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        for actor_id, actor_name, ioc_value, thread_id, src_id in [
            (actor_a['id'], 'Qilin', 'qilin-c2.example', 'thread-qilin', 'src-qilin'),
            (actor_b['id'], 'Akira', 'akira-c2.example', 'thread-akira', 'src-akira'),
        ]:
            connection.execute(
                '''
                INSERT INTO sources (
                    id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    src_id,
                    actor_id,
                    f'{actor_name} Report',
                    f'https://intel.example/{actor_name.lower()}',
                    '2026-02-22',
                    '2026-02-22T00:00:00+00:00',
                    f'{actor_name} beaconing and suspicious DNS activity observed.',
                ),
            )
            connection.execute(
                '''
                INSERT INTO question_threads (
                    id, actor_id, question_text, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    thread_id,
                    actor_id,
                    f'Is {actor_name} beaconing via suspicious DNS domains?',
                    'open',
                    '2026-02-22T00:00:00+00:00',
                    '2026-02-22T01:00:00+00:00',
                ),
            )
            connection.execute(
                '''
                INSERT INTO question_updates (
                    id, thread_id, source_id, trigger_excerpt, update_note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    f'upd-{thread_id}',
                    thread_id,
                    src_id,
                    f'{actor_name} DNS beaconing observed.',
                    '',
                    '2026-02-22T01:00:00+00:00',
                ),
            )
            connection.execute(
                '''
                INSERT INTO timeline_events (
                    id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    f'evt-{thread_id}',
                    actor_id,
                    '2026-02-22T00:30:00+00:00',
                    'command_and_control',
                    f'{actor_name} beaconing update',
                    f'Observed {actor_name} DNS beaconing patterns in recent reporting.',
                    src_id,
                    'Enterprise',
                    '[]',
                ),
            )
            connection.execute(
                '''
                INSERT INTO ioc_items (
                    id, actor_id, ioc_type, ioc_value, normalized_value, validation_status,
                    lifecycle_status, revoked, is_active, source_ref, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    f'ioc-{thread_id}',
                    actor_id,
                    'domain',
                    ioc_value,
                    ioc_value,
                    'valid',
                    'active',
                    0,
                    1,
                    f'{actor_name} report',
                    '2026-02-22T01:05:00+00:00',
                ),
            )
        connection.commit()

    notebook_a = app_module._fetch_actor_notebook(actor_a['id'])  # noqa: SLF001
    notebook_b = app_module._fetch_actor_notebook(actor_b['id'])  # noqa: SLF001
    card_a = next((item for item in notebook_a.get('priority_questions', []) if item.get('id') == 'thread-qilin'), None)
    card_b = next((item for item in notebook_b.get('priority_questions', []) if item.get('id') == 'thread-akira'), None)
    assert card_a is not None and card_b is not None

    text_a = ' '.join(
        [
            str(card_a.get('quick_check_title') or ''),
            str(card_a.get('first_step') or ''),
            str(card_a.get('what_to_look_for') or ''),
            str(card_a.get('query_hint') or ''),
        ]
    ).lower()
    text_b = ' '.join(
        [
            str(card_b.get('quick_check_title') or ''),
            str(card_b.get('first_step') or ''),
            str(card_b.get('what_to_look_for') or ''),
            str(card_b.get('query_hint') or ''),
        ]
    ).lower()
    assert 'qilin' in text_a
    assert 'akira' in text_b
    assert 'qilin-c2.example' in text_a
    assert 'akira-c2.example' in text_b


def test_root_sidebar_shows_actor_last_updated_label(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Sidebar', 'Sidebar label scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            UPDATE actor_profiles
            SET notebook_updated_at = ?
            WHERE id = ?
            ''',
            ('2026-02-19T14:00:00+00:00', actor['id']),
        )
        connection.commit()

    with TestClient(app_module.app) as client:
        response = client.get(f'/?actor_id={actor["id"]}')
        assert response.status_code == 200
        assert 'Updated 2026-02-19' in response.text
        assert 'analyst-pack-actor-select' in response.text
        assert '/export/analyst-pack.pdf' in response.text


def test_dashboard_and_observation_load_performance_guard(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Perf', 'Perf guard scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    source_rows: list[tuple[str, str, str, str, str, str, str]] = []
    observation_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str]] = []
    for idx in range(250):
        source_id = f'src-perf-{idx}'
        source_rows.append(
            (
                source_id,
                actor['id'],
                'PerfSource',
                f'https://example.test/{idx}',
                '2026-02-20',
                '2026-02-20T00:00:00+00:00',
                f'APT-Perf item {idx}',
            )
        )
    for idx in range(900):
        key = f'src-perf-{idx}'
        observation_rows.append(
            (
                f'obs-perf-{idx}',
                actor['id'],
                'source',
                key,
                f'Observation {idx}',
                f'case-{idx}',
                'moderate' if idx % 2 == 0 else 'high',
                'B',
                '2',
                f'analyst-{idx % 6}',
                f'2026-02-{(idx % 25) + 1:02d}T12:00:00+00:00',
            )
        )

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.executemany(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            source_rows,
        )
        connection.executemany(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref, confidence,
                source_reliability, information_credibility, updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            observation_rows,
        )
        connection.commit()

    with TestClient(app_module.app) as client:
        root_start = time.perf_counter()
        root_response = client.get(f'/?actor_id={actor["id"]}')
        root_elapsed = time.perf_counter() - root_start
        assert root_response.status_code == 200
        assert root_elapsed < 3.0

        obs_start = time.perf_counter()
        obs_response = client.get(f"/actors/{actor['id']}/observations")
        obs_elapsed = time.perf_counter() - obs_start
        assert obs_response.status_code == 200
        payload = obs_response.json()
        assert payload['limit'] == 100
        assert payload['offset'] == 0
        assert len(payload['items']) == 100
        assert obs_elapsed < 2.0


def test_known_technique_ids_for_entity_collects_all_uses(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--1',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1001'},
                    ],
                },
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--2',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1002'},
                    ],
                },
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--3',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1003.001'},
                    ],
                },
                {
                    'type': 'relationship',
                    'relationship_type': 'uses',
                    'source_ref': 'intrusion-set--unit-1',
                    'target_ref': 'attack-pattern--1',
                },
                {
                    'type': 'relationship',
                    'relationship_type': 'uses',
                    'source_ref': 'intrusion-set--unit-1',
                    'target_ref': 'attack-pattern--2',
                },
                {
                    'type': 'relationship',
                    'relationship_type': 'uses',
                    'source_ref': 'intrusion-set--unit-1',
                    'target_ref': 'attack-pattern--3',
                },
            ]
        },
    )

    known = app_module._known_technique_ids_for_entity('intrusion-set--unit-1')  # noqa: SLF001

    assert known == {'T1001', 'T1002', 'T1003.001'}


def test_emerging_technique_ids_require_repeated_evidence_and_sort_by_recent():
    timeline_items = [
        {
            'occurred_at': '2026-02-05T00:00:00+00:00',
            'source_id': 'src-1',
            'ttp_ids': ['T9001'],
        },
        {
            'occurred_at': 'Tue, 04 Feb 2026 00:00:00 GMT',
            'source_id': 'src-2',
            'ttp_ids': ['T9002'],
        },
        {
            'occurred_at': '2026-02-06T00:00:00+00:00',
            'source_id': 'src-3',
            'ttp_ids': ['T9002'],
        },
        {
            'occurred_at': '2026-02-07T00:00:00+00:00',
            'source_id': 'src-4',
            'ttp_ids': ['T9003'],
        },
        {
            'occurred_at': '2026-02-08T00:00:00+00:00',
            'source_id': 'src-4',
            'ttp_ids': ['T9003'],
        },
    ]

    emerging = app_module._emerging_technique_ids_from_timeline(  # noqa: SLF001
        timeline_items,
        known_technique_ids=set(),
    )

    assert emerging == ['T9003', 'T9002']


def test_first_seen_for_techniques_handles_mixed_datetime_formats():
    timeline_items = [
        {'occurred_at': 'Tue, 04 Feb 2026 00:00:00 GMT', 'ttp_ids': ['T7001']},
        {'occurred_at': '2026-02-03T00:00:00+00:00', 'ttp_ids': ['T7001']},
    ]

    seen = app_module._first_seen_for_techniques(timeline_items, ['T7001'])  # noqa: SLF001

    assert seen == [{'technique_id': 'T7001', 'first_seen': '2026-02-03'}]


def test_extract_ttp_ids_filters_non_mitre_techniques(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--1111',
                    'name': 'Unit Technique',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1111'},
                    ],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_TECHNIQUE_INDEX_CACHE', None)

    values = app_module._extract_ttp_ids('Observed T1111 and T9999 plus t1111 again.')  # noqa: SLF001

    assert values == ['T1111']


def test_emerging_techniques_include_metadata_and_drop_unknown_ids(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--9002',
                    'name': 'Technique Nine Zero Zero Two',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T9002', 'url': 'https://attack.mitre.org/techniques/T9002/'},
                    ],
                },
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--9003',
                    'name': 'Technique Nine Zero Zero Three',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T9003', 'url': 'https://attack.mitre.org/techniques/T9003/'},
                    ],
                },
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_TECHNIQUE_INDEX_CACHE', None)

    timeline_items = [
        {'occurred_at': '2026-02-06T00:00:00+00:00', 'source_id': 'a', 'category': 'execution', 'ttp_ids': ['T9002']},
        {'occurred_at': '2026-02-07T00:00:00+00:00', 'source_id': 'b', 'category': 'execution', 'ttp_ids': ['T9002']},
        {'occurred_at': '2026-02-08T00:00:00+00:00', 'source_id': 'z', 'category': 'impact', 'ttp_ids': ['T9003']},
        {'occurred_at': '2026-02-09T00:00:00+00:00', 'source_id': 'z', 'category': 'impact', 'ttp_ids': ['T9003', 'T9999']},
    ]

    emerging = app_module._emerging_techniques_from_timeline(timeline_items, known_technique_ids=set())  # noqa: SLF001

    assert [item['technique_id'] for item in emerging] == ['T9003', 'T9002']
    assert emerging[0]['technique_name'] == 'Technique Nine Zero Zero Three'
    assert emerging[0]['source_count'] == 1
    assert emerging[0]['event_count'] == 2
    assert emerging[0]['categories'] == ['impact']


def test_build_notebook_kpis_ignores_unknown_technique_ids(monkeypatch):
    monkeypatch.setattr(
        app_module,
        'MITRE_DATASET_CACHE',
        {
            'objects': [
                {
                    'type': 'attack-pattern',
                    'id': 'attack-pattern--1111',
                    'name': 'Known Technique',
                    'external_references': [
                        {'source_name': 'mitre-attack', 'external_id': 'T1111'},
                    ],
                }
            ]
        },
    )
    monkeypatch.setattr(app_module, 'MITRE_TECHNIQUE_INDEX_CACHE', None)

    now_iso = datetime.now(timezone.utc).isoformat()
    kpis = app_module._build_notebook_kpis(  # noqa: SLF001
        timeline_items=[{'occurred_at': now_iso, 'ttp_ids': ['T9999']}],
        known_technique_ids=set(),
        open_questions_count=0,
        sources=[],
    )

    assert kpis['new_techniques_30d'] == '0'


def test_recent_activity_highlights_prioritize_corroborated_signals():
    timeline_items = [
        {
            'occurred_at': '2026-02-20T12:00:00+00:00',
            'summary': 'APT-Flow targeted VPN edge appliances to gain initial access.',
            'category': 'initial_access',
            'target_text': 'Retail',
            'ttp_ids': ['T1566'],
            'source_id': 'src-1',
        },
        {
            'occurred_at': '2026-02-19T12:00:00+00:00',
            'summary': 'APT-Flow targeted VPN edge appliances to gain initial access.',
            'category': 'initial_access',
            'target_text': 'Retail',
            'ttp_ids': ['T1566'],
            'source_id': 'src-2',
        },
        {
            'occurred_at': '2026-02-20T13:00:00+00:00',
            'summary': 'APT-Flow used phishing lures in a broad campaign.',
            'category': 'initial_access',
            'target_text': 'Retail',
            'ttp_ids': ['T1566'],
            'source_id': 'src-3',
        },
    ]
    sources = [
        {
            'id': 'src-1',
            'source_name': 'CISA',
            'url': 'https://www.cisa.gov/advisories/aaa',
            'published_at': '2026-02-20T11:30:00+00:00',
            'pasted_text': 'APT-Flow targeted VPN edge appliances.',
            'title': 'CISA advisory',
        },
        {
            'id': 'src-2',
            'source_name': 'Mandiant',
            'url': 'https://www.mandiant.com/resources/blog/bbb',
            'published_at': '2026-02-19T11:30:00+00:00',
            'pasted_text': 'APT-Flow targeted VPN edge appliances.',
            'title': 'Mandiant blog',
        },
        {
            'id': 'src-3',
            'source_name': 'CISA',
            'url': 'https://www.cisa.gov/advisories/ccc',
            'published_at': '2026-02-20T12:30:00+00:00',
            'pasted_text': 'APT-Flow used phishing lures.',
            'title': 'CISA campaign note',
        },
    ]

    highlights = app_module._build_recent_activity_highlights(  # noqa: SLF001
        timeline_items=timeline_items,
        sources=sources,
        actor_terms=['APT-Flow'],
    )

    assert highlights
    assert 'VPN edge appliances' in str(highlights[0]['text'])
    assert highlights[0]['corroboration_sources'] == '2'


def test_run_actor_generation_returns_early_when_actor_already_running(monkeypatch):
    monkeypatch.setattr(app_module, '_mark_actor_generation_started', lambda _actor_id: False)  # noqa: SLF001

    called = {'imported': 0, 'built': 0}

    def _import_feeds(_actor_id):
        called['imported'] += 1
        return 0

    def _build_notebook(_actor_id):
        called['built'] += 1

    monkeypatch.setattr(app_module, 'import_default_feeds_for_actor', _import_feeds)
    monkeypatch.setattr(app_module, 'build_notebook', _build_notebook)

    app_module.run_actor_generation('actor-1')

    assert called['imported'] == 0
    assert called['built'] == 0


def test_text_contains_actor_term_uses_token_boundaries():
    assert app_module._text_contains_actor_term('APT-Flow targeted VPN edge', ['apt-flow'])  # noqa: SLF001
    assert not app_module._text_contains_actor_term('The apartment lease was updated.', ['apt'])  # noqa: SLF001
    assert not app_module._text_contains_actor_term('Wizardly operations observed.', ['wizard'])  # noqa: SLF001


def test_run_actor_generation_builds_timeline_then_full_notebook(monkeypatch):
    monkeypatch.setattr(app_module, '_mark_actor_generation_started', lambda _actor_id: True)  # noqa: SLF001
    monkeypatch.setattr(app_module, '_mark_actor_generation_finished', lambda _actor_id: None)  # noqa: SLF001
    monkeypatch.setattr(app_module, 'import_default_feeds_for_actor', lambda _actor_id: 3)

    status_messages: list[str] = []
    build_calls: list[tuple[bool, bool]] = []

    def _status(_actor_id, _status, message):
        status_messages.append(message)

    def _build(_actor_id, *, generate_questions=True, rebuild_timeline=True):
        build_calls.append((generate_questions, rebuild_timeline))

    monkeypatch.setattr(app_module, 'set_actor_notebook_status', _status)
    monkeypatch.setattr(app_module, 'build_notebook', _build)

    class _DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def commit(self):
            return None

    monkeypatch.setattr(app_module.sqlite3, 'connect', lambda *_args, **_kwargs: _DummyConn())

    app_module.run_actor_generation('actor-2')

    assert build_calls == [(False, True), (True, False)]
    assert any('Sources collected (3). Building timeline preview...' in message for message in status_messages)
    assert any('Timeline ready. Generating question threads and guidance...' in message for message in status_messages)
