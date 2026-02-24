from fastapi.testclient import TestClient

import app as app_module
from services import metrics_service


def _setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()
    metrics_service.reset_metrics_core()


def test_metrics_endpoint_reports_counters(tmp_path):
    _setup_db(tmp_path)
    with TestClient(app_module.app) as client:
        actor_resp = client.post('/actors', json={'display_name': 'Metrics Actor'})
        assert actor_resp.status_code == 200
        actor_id = actor_resp.json().get('id')
        assert actor_id

        feedback_resp = client.post(
            f'/actors/{actor_id}/feedback',
            json={
                'item_type': 'priority_question',
                'item_id': 'thread-1',
                'feedback': 'useful',
            },
        )
        assert feedback_resp.status_code == 200

        metrics = client.get('/metrics')
        assert metrics.status_code == 200
        payload = metrics.json()
        counters = payload.get('counters', {})
        assert int(counters.get('requests_total') or 0) >= 2
        by_route = payload.get('requests_by_route', {})
        assert any(
            key.startswith('POST /actors/{actor_id}/feedback')
            or key.startswith('POST /actors/:id/feedback')
            for key in by_route
        )
