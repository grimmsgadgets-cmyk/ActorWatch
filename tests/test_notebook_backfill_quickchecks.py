import sqlite3
import time
import asyncio
from datetime import datetime, timezone
from urllib.parse import urlparse
from fastapi import BackgroundTasks

import pytest

import app as app_module
import route_paths
from tests.notebook_test_helpers import JsonRequest as _JsonRequest
from tests.notebook_test_helpers import app_endpoint as _app_endpoint
from tests.notebook_test_helpers import http_request as _http_request
from tests.notebook_test_helpers import setup_db as _setup_db


def _evidence_has_domain(item: str, domain: str) -> bool:
    """Return True if any pipe-delimited part of an evidence_used string has *domain* as its hostname."""
    for part in item.split(' | '):
        h = (urlparse(part.strip()).hostname or '').lower()
        if h == domain or h.endswith(f'.{domain}'):
            return True
    return False


def test_fetch_actor_notebook_returns_contract_payload_on_cache_miss(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Live-Running', 'Live running scope')
    app_module.set_actor_notebook_status(actor['id'], 'running', 'Refreshing sources now...')

    def _should_not_run(*_args, **_kwargs):
        raise AssertionError('pipeline fetch should be skipped when build_on_cache_miss is false')

    monkeypatch.setattr(app_module, 'pipeline_fetch_actor_notebook_core', _should_not_run)

    body = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        build_on_cache_miss=False,
        allow_stale_cache=True,
    )
    actor_meta = body.get('actor', {}) if isinstance(body.get('actor'), dict) else {}
    assert actor_meta.get('id') == actor['id']
    cards = body.get('priority_questions')
    assert isinstance(cards, list)
    assert len(cards) >= 1


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
    assert any(_evidence_has_domain(item, 'unit42.paloaltonetworks.com') for item in evidence_used)
    assert any('(ingested)' in item for item in evidence_used)

