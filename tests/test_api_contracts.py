from fastapi.testclient import TestClient
import sqlite3

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


def test_ingest_diagnostics_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Diagnostics Actor', None)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ingest_decisions (
                id, source_id, actor_id, stage, decision, reason_code, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'dec-1',
                None,
                actor['id'],
                'resolve',
                'rejected',
                'missing_published_at',
                '{}',
                '2026-02-27T00:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO ingest_decisions (
                id, source_id, actor_id, stage, decision, reason_code, details_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'dec-2',
                None,
                actor['id'],
                'acquire_feed',
                'accepted',
                'source_upserted',
                '{}',
                '2026-02-27T00:01:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at,
                pasted_text, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-diag-1',
                actor['id'],
                'Trusted Feed',
                'https://example.test/trusted',
                '2026-02-27T00:00:00+00:00',
                '2026-02-27T00:00:00+00:00',
                'published',
                '2026-02-27T00:00:00+00:00',
                'trusted evidence',
                'feed_partial_match',
                'trusted',
                2,
            ),
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at,
                pasted_text, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-diag-2',
                actor['id'],
                'Context Feed',
                'https://example.test/context',
                '2026-02-27T00:00:00+00:00',
                '2026-02-27T00:00:00+00:00',
                'published',
                '2026-02-27T00:00:00+00:00',
                'context evidence',
                'feed_soft_match',
                'context',
                1,
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-diag-1',
                actor['id'],
                '2026-02-27T00:00:00+00:00',
                'execution',
                'Trusted event',
                'Trusted summary',
                'src-diag-1',
                '',
                '[]',
            ),
        )
        connection.commit()
    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/ingest/diagnostics")
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert isinstance(body.get('funnel_totals'), dict)
    assert isinstance(body.get('stage_breakdown'), dict)
    assert isinstance(body.get('top_rejection_reasons'), list)
    assert isinstance(body.get('recent_decisions'), list)
    assert isinstance(body.get('quality_mix'), list)
    assert isinstance(body.get('default_surface_estimate'), dict)
    assert isinstance(body.get('totals_snapshot'), dict)
    assert int(body['funnel_totals'].get('accepted', 0)) >= 1
    assert int(body['funnel_totals'].get('rejected', 0)) >= 1
    assert int(body['default_surface_estimate'].get('eligible_sources', 0)) >= 1
    assert int(body['default_surface_estimate'].get('eligible_timeline_events', 0)) >= 1


def test_ranked_evidence_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Evidence Actor', None)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at,
                pasted_text, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-ev-1',
                actor['id'],
                'Example Intel',
                'https://example.test/report',
                '2026-02-27T00:00:00+00:00',
                '2026-02-27T00:00:00+00:00',
                'published',
                '2026-02-27T00:00:00+00:00',
                'evidence text',
                'feed_partial_match',
                'trusted',
                2,
            ),
        )
        connection.execute(
            '''
            INSERT INTO source_scoring (
                source_id, relevance_score, trust_score, recency_score, novelty_score, final_score, scored_at, features_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'src-ev-1',
                0.8,
                1.0,
                0.9,
                0.6,
                0.84,
                '2026-02-27T00:00:00+00:00',
                '{}',
            ),
        )
        connection.execute(
            '''
            INSERT INTO actor_resolution (
                id, source_id, actor_id, match_type, matched_term, confidence, explanation_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'res-ev-1',
                'src-ev-1',
                actor['id'],
                'exact_actor_term',
                'Evidence Actor',
                0.8,
                '{}',
                '2026-02-27T00:00:00+00:00',
            ),
        )
        connection.execute(
            '''
            INSERT INTO source_entities (
                id, source_id, entity_type, entity_value, normalized_value, confidence, extractor, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ent-ev-1',
                'src-ev-1',
                'domain',
                'bad.example',
                'bad.example',
                0.8,
                'test',
                '2026-02-27T00:00:00+00:00',
            ),
        )
        connection.commit()
    with TestClient(app_module.app) as client:
        response = client.get(
            f"/actors/{actor['id']}/evidence/ranked?limit=10&min_final_score=0.7&source_tier=trusted&require_corroboration=0"
        )
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert int(body.get('count') or 0) >= 1
    assert isinstance(body.get('items'), list)
    assert float(body['items'][0]['scores']['final']) >= 0.8
    assert 'corroboration_sources' in body['items'][0]


def test_taxii_sync_contract_requires_collection_url_when_not_configured(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Taxii Actor', None)
    with TestClient(app_module.app) as client:
        response = client.post(f"/actors/{actor['id']}/taxii/sync", json={})
    assert response.status_code == 400
    body = response.json()
    assert isinstance(body.get('detail'), str)


def test_taxii_runs_contract(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('Taxii Runs Actor', None)
    with TestClient(app_module.app) as client:
        response = client.get(f"/actors/{actor['id']}/taxii/runs")
    assert response.status_code == 200
    body = response.json()
    assert body.get('actor_id') == actor['id']
    assert isinstance(body.get('runs'), list)


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
