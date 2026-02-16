import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

import app as app_module


@pytest.mark.skipif(
    os.environ.get('ACTORTRACKER_ONLINE_TESTS') != '1',
    reason='Set ACTORTRACKER_ONLINE_TESTS=1 to run online integration tests.',
)
def test_safe_http_get_online_example_com():
    response = app_module._safe_http_get('https://example.com', timeout=20.0)  # noqa: SLF001
    assert response.status_code == 200


@pytest.mark.skipif(
    os.environ.get('ACTORTRACKER_ONLINE_TESTS') != '1',
    reason='Set ACTORTRACKER_ONLINE_TESTS=1 to run online integration tests.',
)
def test_root_renders_actor_name_online(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()
    app_module.run_actor_generation = lambda actor_id: None  # type: ignore[assignment]
    app_module.get_ollama_status = lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'}  # type: ignore[assignment]
    actor = app_module.create_actor_profile('APT-Render', 'Render scope')
    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute('UPDATE actor_profiles SET is_tracked = 1 WHERE id = ?', (actor['id'],))
        connection.commit()
    with TestClient(app_module.app) as client:
        response = client.get('/')
    assert response.status_code == 200
    assert 'APT-Render' in response.text


@pytest.mark.skipif(
    os.environ.get('ACTORTRACKER_ONLINE_TESTS') != '1',
    reason='Set ACTORTRACKER_ONLINE_TESTS=1 to run online integration tests.',
)
def test_add_actor_ui_redirect_sets_generation_notice_online(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()
    app_module.run_actor_generation = lambda actor_id: None  # type: ignore[assignment]
    app_module.get_ollama_status = lambda: {'available': False, 'base_url': 'http://offline', 'model': 'none'}  # type: ignore[assignment]
    with TestClient(app_module.app) as client:
        response = client.post(
            '/actors/new',
            data={'display_name': 'APT-New'},
            follow_redirects=True,
        )
    assert response.status_code == 200
    assert 'Tracking started. Building notebook in the background.' in response.text
