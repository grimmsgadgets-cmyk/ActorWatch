from fastapi.testclient import TestClient

import app as app_module


def _setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()


def test_health_contract(tmp_path):
    _setup_db(tmp_path)
    with TestClient(app_module.app) as client:
        response = client.get('/health')
    assert response.status_code == 200
    body = response.json()
    assert body == {'status': 'ok'}


def test_actor_create_and_list_contract(tmp_path):
    _setup_db(tmp_path)
    with TestClient(app_module.app) as client:
        created = client.post('/actors', json={'display_name': 'Contract Actor'})
        assert created.status_code == 200
        payload = created.json()
        assert 'id' in payload and payload['id']
        assert payload['display_name'] == 'Contract Actor'
        listed = client.get('/actors')
    assert listed.status_code == 200
    rows = listed.json()
    assert isinstance(rows, list)
    assert any(isinstance(item, dict) and item.get('display_name') == 'Contract Actor' for item in rows)


def test_refresh_stats_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Stats Actor', None)
    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/refresh/stats")
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert isinstance(body.get('feed_state'), dict)
    assert isinstance(body.get('source_state'), dict)


def test_refresh_timeline_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Timeline Actor', None)
    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/refresh/timeline")
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert isinstance(body.get('recent_generation_runs'), list)
    assert 'eta_seconds' in body


def test_refresh_job_submit_and_detail_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Job Actor', None)
    with TestClient(app_module.app) as client:
        submit = client.post(f"/actors/{actor['id']}/refresh/jobs")
        assert submit.status_code == 200
        payload = submit.json()
        assert payload.get('actor_id') == actor['id']
        assert isinstance(payload.get('queued'), bool)
        job_id = str(payload.get('job_id') or '')
        if job_id:
            detail = client.get(f"/actors/{actor['id']}/refresh/jobs/{job_id}")
            assert detail.status_code == 200
            detail_body = detail.json()
            assert detail_body.get('actor_id') == actor['id']
            assert detail_body.get('job_id') == job_id
            assert isinstance(detail_body.get('phases'), list)


def test_stix_export_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Stix Contract Actor', None)
    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/stix/export")
    assert response.status_code == 200
    bundle = response.json()
    assert bundle.get('type') == 'bundle'
    assert isinstance(bundle.get('objects'), list)


def test_environment_profile_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Env Contract Actor', None)
    with TestClient(app_module.app) as client:
        post_resp = client.post(
            f"/actors/{actor['id']}/environment-profile",
            json={
                'query_dialect': 'kql',
                'field_mapping': {'domain': 'DnsQuery'},
                'default_time_window_hours': 12,
            },
        )
        assert post_resp.status_code == 200
        get_resp = client.get(f"/actors/{actor['id']}/environment-profile")
    assert get_resp.status_code == 200
    profile = get_resp.json()
    assert profile.get('actor_id') == actor['id']
    assert profile.get('query_dialect') == 'kql'
    assert isinstance(profile.get('field_mapping'), dict)


def test_feedback_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Feedback Contract Actor', None)
    with TestClient(app_module.app) as client:
        response = client.post(
            f"/actors/{actor['id']}/feedback",
            json={
                'item_type': 'priority_question',
                'item_id': 'thread-1',
                'feedback': 'useful',
                'reason': 'clear and actionable',
            },
        )
        assert response.status_code == 200
        summary = client.get(f"/actors/{actor['id']}/feedback/summary?item_type=priority_question")
    assert summary.status_code == 200
    body = summary.json()
    assert body.get('actor_id') == actor['id']
    assert isinstance(body.get('items'), dict)
