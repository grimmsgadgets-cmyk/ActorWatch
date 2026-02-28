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

    list_observations = _app_endpoint(route_paths.ACTOR_OBSERVATIONS, 'GET')
    payload = list_observations(actor['id'], analyst='ali', confidence='high', limit=10, offset=0)
    assert payload['actor_id'] == actor['id']
    assert payload['limit'] == 10
    assert payload['offset'] == 0
    assert len(payload['items']) == 1
    assert payload['items'][0]['updated_by'] == 'alice'
    assert payload['items'][0]['source_title'] == 'Unit 42 report'

    export_json = _app_endpoint(route_paths.ACTOR_OBSERVATIONS_EXPORT_JSON, 'GET')
    json_payload = export_json(actor['id'], confidence='high')
    assert json_payload['count'] == 1
    assert json_payload['items'][0]['confidence'] == 'high'

    export_csv = _app_endpoint(route_paths.ACTOR_OBSERVATIONS_EXPORT_CSV, 'GET')
    csv_response = export_csv(actor['id'], analyst='ali')
    assert csv_response.status_code == 200
    assert 'text/csv' in str(csv_response.headers.get('content-type') or '')
    csv_text = str(csv_response.body.decode('utf-8', errors='ignore'))
    assert 'High-confidence pattern shift.' in csv_text
    assert 'Lower-confidence duplicate note.' not in csv_text


def test_observation_upsert_returns_non_blocking_quality_guidance(tmp_path, monkeypatch):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Obs-Q', 'Observation quality guidance scope')
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )

    upsert_observation = _app_endpoint(route_paths.ACTOR_OBSERVATION_UPSERT, 'POST')
    payload = asyncio.run(
        upsert_observation(
            actor['id'],
            'source',
            'src-guidance',
            _JsonRequest(
                {
                    'updated_by': 'alice',
                    'confidence': 'high',
                    'note': 'Needs review',
                    'source_ref': '',
                    'source_reliability': '',
                    'information_credibility': '',
                }
            ),
        )
    )
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

    upsert_observation = _app_endpoint(route_paths.ACTOR_OBSERVATION_UPSERT, 'POST')
    first = asyncio.run(
        upsert_observation(
            actor['id'],
            'source',
            'src-history',
            _JsonRequest(
                {
                    'updated_by': 'alice',
                    'confidence': 'moderate',
                    'note': 'Initial baseline observation.',
                    'source_ref': 'case-1',
                }
            ),
        )
    )
    assert first['ok'] is True
    second = asyncio.run(
        upsert_observation(
            actor['id'],
            'source',
            'src-history',
            _JsonRequest(
                {
                    'updated_by': 'alice',
                    'confidence': 'high',
                    'note': 'Second observation with stronger corroboration.',
                    'source_ref': 'case-2',
                    'source_reliability': 'A',
                    'information_credibility': '1',
                }
            ),
        )
    )
    assert second['ok'] is True

    history_endpoint = _app_endpoint(route_paths.ACTOR_OBSERVATION_HISTORY, 'GET')
    payload = history_endpoint(actor['id'], 'source', 'src-history', limit=10)
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

    auto_snapshot = _app_endpoint(route_paths.ACTOR_OBSERVATIONS_AUTO_SNAPSHOT, 'POST')
    response = asyncio.run(auto_snapshot(actor['id'], _JsonRequest({})))
    assert response.status_code == 303

    list_observations = _app_endpoint(route_paths.ACTOR_OBSERVATIONS, 'GET')
    observations = list_observations(actor['id'])
    items = observations.get('items', [])
    assert items
    assert any(str(item.get('updated_by') or '') == 'auto' for item in items)

    export_pack_json = _app_endpoint(route_paths.ACTOR_EXPORT_ANALYST_PACK, 'GET')
    payload = export_pack_json(
        actor['id'],
        source_tier='high',
        min_confidence_weight='3',
        source_days='90',
    )
    assert payload['actor_id'] == actor['id']
    quality_filters = payload.get('source_quality_filters', {})
    assert str(quality_filters.get('source_tier') or '') == 'high'
    assert str(quality_filters.get('min_confidence_weight') or '') == '3'
    assert str(quality_filters.get('source_days') or '') == '90'
    assert 'observations' in payload
    assert 'observation_history' in payload

    export_pack_pdf = _app_endpoint(route_paths.ACTOR_EXPORT_ANALYST_PACK_PDF, 'GET')
    pdf_pack = export_pack_pdf(
        actor['id'],
        source_tier='high',
        min_confidence_weight='3',
        source_days='90',
    )
    assert pdf_pack.status_code == 200
    assert 'application/pdf' in str(pdf_pack.headers.get('content-type') or '')
    assert str(pdf_pack.headers.get('content-disposition') or '').endswith('-analyst-pack.pdf"')
    assert bytes(pdf_pack.body).startswith(b'%PDF-1.4')


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
                'domain',
                'qilin-related.example',
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

    ioc_hunts = _app_endpoint(route_paths.ACTOR_IOC_HUNT_QUERIES, 'GET')
    response = ioc_hunts(
        _http_request(path=f"/actors/{actor['id']}/hunts/iocs"),
        actor['id'],
        quick_check_id='thread-hunt-1',
        window_start='2026-02-01T00:00:00+00:00',
        window_end='2026-03-01T00:00:00+00:00',
    )
    assert response.status_code == 200
    response_text = str(response.body.decode('utf-8', errors='ignore'))
    assert 'Check pre-filter: thread-hunt-1' in response_text
    assert 'Actor-related unmatched IOCs' in response_text
    assert '203.0.113.9' not in response_text


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

    ioc_hunts = _app_endpoint(route_paths.ACTOR_IOC_HUNT_QUERIES, 'GET')
    response = ioc_hunts(_http_request(path=f"/actors/{actor['id']}/hunts/iocs"), actor['id'], window_days=30)
    assert response.status_code == 200
    html = str(response.body.decode('utf-8', errors='ignore'))
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

