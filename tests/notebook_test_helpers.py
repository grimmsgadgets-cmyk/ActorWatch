import json

from starlette.requests import Request

import app as app_module


def setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()


def app_endpoint(path: str, method: str):
    method_upper = method.upper()
    for route in app_module.app.routes:
        route_path = str(getattr(route, 'path', '') or '')
        methods = set(getattr(route, 'methods', set()) or set())
        if route_path == path and method_upper in methods:
            return route.endpoint
    raise AssertionError(f'Endpoint not found for {method_upper} {path}')


class JsonRequest:
    def __init__(self, payload: dict[str, object] | None = None):
        self._payload = payload if isinstance(payload, dict) else {}
        self._body = json.dumps(self._payload).encode('utf-8')
        self.headers = {'content-length': str(len(self._body))}

    async def body(self):
        return self._body

    async def json(self):
        return self._payload


def http_request(*, path: str = '/', query: str = '') -> Request:
    scope = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': query.encode(),
        'headers': [],
        'client': ('127.0.0.1', 12345),
        'server': ('testserver', 80),
    }
    return Request(scope)
