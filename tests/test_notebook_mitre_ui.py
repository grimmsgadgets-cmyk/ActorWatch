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
    from routes.routes_evolution import create_evolution_router

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

    router = create_evolution_router(
        deps={
            'enforce_request_size': lambda _request, _limit: asyncio.sleep(0),
            'observation_body_limit_bytes': 1024 * 1024,
            'default_body_limit_bytes': 1024 * 1024,
            'db_path': lambda: app_module.DB_PATH,
            'actor_exists': app_module.actor_exists,
            'normalize_technique_id': app_module._normalize_technique_id,  # noqa: SLF001
            'normalize_string_list': app_module.normalize_string_list,
            'utc_now_iso': app_module.utc_now_iso,
            'capability_category_from_technique_id': app_module._capability_category_from_technique_id,  # noqa: SLF001
            'generate_validation_template': app_module.generate_validation_template,
            'baseline_entry': app_module.baseline_entry,
            'resolve_delta_action': app_module.resolve_delta_action,
        }
    )

    create_observation_endpoint = next(
        route.endpoint
        for route in router.routes
        if getattr(route, 'path', '') == route_paths.ACTOR_STATE_OBSERVATIONS
        and 'POST' in getattr(route, 'methods', set())
    )
    list_deltas_endpoint = next(
        route.endpoint
        for route in router.routes
        if getattr(route, 'path', '') == '/actors/{actor_id}/deltas'
        and 'GET' in getattr(route, 'methods', set())
    )

    class _FakeJsonRequest:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload

        async def json(self):
            return self._payload

    observation = asyncio.run(
        create_observation_endpoint(
            actor['id'],
            _FakeJsonRequest(
                {
                    'source_type': 'report',
                    'source_ref': 'unit-test',
                    'ttp_list': ['t1071.001'],
                    'tools_list': [],
                    'infra_list': [],
                    'target_list': [],
                }
            ),
        )
    )
    assert str(observation.get('actor_id') or '') == actor['id']
    deltas = list_deltas_endpoint(actor['id'])

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
    monkeypatch.setattr(app_module, 'run_actor_generation', lambda _actor_id: None)
    monkeypatch.setattr(
        app_module,
        'get_ollama_status',
        lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'},
    )
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
                'priority_questions': [{'id': 'q1', 'question_text': 'Sample quick check question'}],
                'top_change_signals': [{'change_summary': 'Sample change'}],
                'recent_activity_synthesis': [{'label': 'What changed', 'text': 'Sample synthesis'}],
                'kpis': {},
                'counts': {},
                'recent_activity_highlights': [],
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
    assert '1) Who are they?' in body
    assert '2) What have they been up to recently?' in body
    assert 'Quick checks' in body


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
