import time
from collections import deque


def request_body_limit_bytes_core(
    method: str,
    path: str,
    *,
    deps: dict[str, object],
) -> int:
    _source_upload_body_limit_bytes = deps['source_upload_body_limit_bytes']
    _observation_body_limit_bytes = deps['observation_body_limit_bytes']
    _default_body_limit_bytes = deps['default_body_limit_bytes']

    method_upper = method.upper()
    if method_upper not in {'POST', 'PUT', 'PATCH'}:
        return 0
    if path.startswith('/actors/') and path.endswith('/sources'):
        return _source_upload_body_limit_bytes
    if path.startswith('/actors/') and path.endswith('/observations'):
        return _observation_body_limit_bytes
    return _default_body_limit_bytes


def rate_limit_bucket_core(method: str, path: str, *, deps: dict[str, object]) -> tuple[str, int] | None:
    _rate_limit_heavy_per_minute = deps['rate_limit_heavy_per_minute']
    _rate_limit_default_per_minute = deps['rate_limit_default_per_minute']

    method_upper = method.upper()
    if method_upper not in {'POST', 'PUT', 'PATCH', 'DELETE'}:
        return None
    heavy = (
        path.startswith('/actors/') and (
            path.endswith('/sources')
            or path.endswith('/sources/import-feeds')
            or path.endswith('/refresh')
            or path.endswith('/observations')
        )
    )
    if heavy:
        return ('write_heavy', _rate_limit_heavy_per_minute)
    return ('write_default', _rate_limit_default_per_minute)


def request_client_id_core(request) -> str:
    forwarded_for = request.headers.get('x-forwarded-for', '').strip()
    if forwarded_for:
        first_hop = forwarded_for.split(',', 1)[0].strip()
        if first_hop:
            return first_hop
    if request.client and request.client.host:
        return request.client.host
    return 'unknown'


def prune_rate_limit_state_core(*, now: float, rate_limit_state: dict[str, deque[float]], rate_limit_window_seconds: int) -> None:
    stale_keys: list[str] = []
    for key, timestamps in rate_limit_state.items():
        while timestamps and now - timestamps[0] >= rate_limit_window_seconds:
            timestamps.popleft()
        if not timestamps:
            stale_keys.append(key)
    for key in stale_keys:
        rate_limit_state.pop(key, None)


def check_rate_limit_core(request, *, deps: dict[str, object]) -> tuple[bool, int, int]:
    _rate_limit_enabled = deps['rate_limit_enabled']
    _rate_limit_window_seconds = deps['rate_limit_window_seconds']
    _rate_limit_state = deps['rate_limit_state']
    _rate_limit_lock = deps['rate_limit_lock']
    _rate_limit_cleanup_every = deps['rate_limit_cleanup_every']
    _rate_limit_request_counter_ref = deps['rate_limit_request_counter_ref']
    _rate_limit_bucket = deps['rate_limit_bucket']
    _request_client_id = deps['request_client_id']
    _prune_rate_limit_state = deps['prune_rate_limit_state']

    bucket = _rate_limit_bucket(request.method, request.url.path)
    if not _rate_limit_enabled or bucket is None:
        return (False, 0, 0)
    bucket_name, limit = bucket
    client_id = _request_client_id(request)
    key = f'{bucket_name}:{client_id}'
    now = time.monotonic()
    with _rate_limit_lock:
        _rate_limit_request_counter_ref['value'] += 1
        if _rate_limit_request_counter_ref['value'] % _rate_limit_cleanup_every == 0:
            _prune_rate_limit_state(now)
        timestamps = _rate_limit_state[key]
        while timestamps and now - timestamps[0] >= _rate_limit_window_seconds:
            timestamps.popleft()
        if len(timestamps) >= limit:
            retry_after = max(1, int(_rate_limit_window_seconds - (now - timestamps[0])) + 1)
            return (True, retry_after, limit)
        timestamps.append(now)
    return (False, 0, limit)
