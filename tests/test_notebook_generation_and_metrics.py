import sqlite3
import time
import asyncio
from datetime import datetime, timezone
from starlette.requests import Request
from fastapi import BackgroundTasks

import pytest

import app as app_module
import route_paths
from tests.notebook_test_helpers import JsonRequest as _JsonRequest
from tests.notebook_test_helpers import app_endpoint as _app_endpoint
from tests.notebook_test_helpers import http_request as _http_request
from tests.notebook_test_helpers import setup_db as _setup_db


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
    related_a = ' '.join(
        str(item.get('ioc_value') or '')
        for item in (card_a.get('related_iocs') or [])
        if isinstance(item, dict)
    ).lower()
    related_b = ' '.join(
        str(item.get('ioc_value') or '')
        for item in (card_b.get('related_iocs') or [])
        if isinstance(item, dict)
    ).lower()
    assert 'qilin-c2.example' in related_a
    assert 'akira-c2.example' in related_b


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

    monkeypatch.setattr(
        app_module,
        '_fetch_actor_notebook',
        lambda _actor_id, **_kwargs: app_module.notebook_service.finalize_notebook_contract_core(
            {
                'actor': {
                    'id': actor['id'],
                    'display_name': actor['display_name'],
                    'notebook_status': 'ready',
                    'notebook_message': 'Notebook ready.',
                },
                'kpis': {'activity_30d': '0', 'open_questions': '0', 'key_iocs': '0'},
                'requirements_context': {'priority_mode': 'Operational', 'org_context': ''},
            }
        ),
    )
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
    body = str(response.body.decode('utf-8', errors='ignore'))
    assert 'Updated 2026-02-19' in body
    assert 'analyst-pack-actor-select' in body
    assert '/export/analyst-pack.pdf' in body


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

    monkeypatch.setattr(
        app_module,
        '_fetch_actor_notebook',
        lambda _actor_id, **_kwargs: app_module.notebook_service.finalize_notebook_contract_core(
            {
                'actor': {
                    'id': actor['id'],
                    'display_name': actor['display_name'],
                    'notebook_status': 'ready',
                    'notebook_message': 'Notebook ready.',
                },
                'kpis': {'activity_30d': '0', 'open_questions': '0', 'key_iocs': '0'},
                'requirements_context': {'priority_mode': 'Operational', 'org_context': ''},
            }
        ),
    )
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
    root_start = time.perf_counter()
    root_response = app_module.root(
        request=request,
        background_tasks=BackgroundTasks(),
        actor_id=str(actor['id']),
        notice=None,
    )
    root_elapsed = time.perf_counter() - root_start
    assert root_response.status_code == 200
    assert root_elapsed < 3.0

    list_observations = _app_endpoint(route_paths.ACTOR_OBSERVATIONS, 'GET')
    obs_start = time.perf_counter()
    payload = list_observations(actor['id'])
    obs_elapsed = time.perf_counter() - obs_start
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
