import sqlite3

from fastapi.testclient import TestClient

import app as app_module


def _setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()


def test_environment_profile_round_trip(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Learn', None)
    client = TestClient(app_module.app)

    update_resp = client.post(
        f"/actors/{actor['id']}/environment-profile",
        json={
            'query_dialect': 'kql',
            'field_mapping': {'domain': 'DnsQuery'},
            'default_time_window_hours': 36,
        },
    )
    assert update_resp.status_code == 200
    payload = update_resp.json()
    assert payload['query_dialect'] == 'kql'
    assert int(payload['default_time_window_hours']) == 36

    get_resp = client.get(f"/actors/{actor['id']}/environment-profile")
    assert get_resp.status_code == 200
    assert get_resp.json()['query_dialect'] == 'kql'


def test_feedback_updates_source_reliability(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Feedback', None)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-1',
                actor['id'],
                'Test Intel',
                'https://intel.example/report',
                '2026-01-01',
                '2026-01-01T00:00:00Z',
                'Intel text',
            ),
        )
        connection.commit()

    client = TestClient(app_module.app)
    resp = client.post(
        f"/actors/{actor['id']}/feedback",
        json={
            'item_type': 'hunt_query',
            'item_id': 'qhash-1',
            'feedback': 'useful',
            'metadata': {'evidence_source_ids': ['src-1']},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert int(body['source_reliability_updates']) == 1

    summary = client.get(f"/actors/{actor['id']}/feedback/summary?item_type=hunt_query")
    assert summary.status_code == 200
    items = summary.json()['items'].get('hunt_query', [])
    assert len(items) == 1
