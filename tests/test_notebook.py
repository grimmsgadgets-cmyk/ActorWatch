import sqlite3
from starlette.requests import Request
from fastapi import BackgroundTasks

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

    monkeypatch.setattr(app_module, '_fetch_actor_notebook', lambda actor_id: (_ for _ in ()).throw(RuntimeError('boom')))
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
