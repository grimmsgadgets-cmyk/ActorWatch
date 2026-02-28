from urllib.parse import urlparse


async def enforce_request_size_core(*, request, limit: int, http_exception_cls) -> None:
    if limit <= 0:
        return
    content_length = request.headers.get('content-length', '').strip()
    if content_length.isdigit() and int(content_length) > limit:
        raise http_exception_cls(
            status_code=413,
            detail=f'Request body too large. Limit for this endpoint is {limit} bytes.',
        )
    body = await request.body()
    if len(body) > limit:
        raise http_exception_cls(
            status_code=413,
            detail=f'Request body too large. Limit for this endpoint is {limit} bytes.',
        )


def rate_limit_bucket_core(*, method: str, path: str, deps: dict[str, object]) -> tuple[str, int] | None:
    _rate_limit_service = deps['rate_limit_service']
    return _rate_limit_service.rate_limit_bucket_core(
        method,
        path,
        deps['rate_limit_heavy_per_minute'],
        deps['rate_limit_default_per_minute'],
    )


def request_client_id_core(*, request, deps: dict[str, object]) -> str:
    _rate_limit_service = deps['rate_limit_service']
    return _rate_limit_service.request_client_id_core(
        request,
        trust_proxy_headers=deps['trust_proxy_headers'],
    )


def prune_rate_limit_state_core(*, now: float, deps: dict[str, object]) -> None:
    _rate_limit_service = deps['rate_limit_service']
    _rate_limit_service.prune_rate_limit_state_core(
        now=now,
        rate_limit_state=deps['rate_limit_state'],
        rate_limit_window_seconds=deps['rate_limit_window_seconds'],
    )


def check_rate_limit_core(*, request, deps: dict[str, object]) -> tuple[bool, int, int]:
    _rate_limit_service = deps['rate_limit_service']
    _rate_limit_state = deps['rate_limit_state']
    _rate_limit_lock = deps['rate_limit_lock']
    counter_ref = {'value': int(deps['rate_limit_request_counter'])}

    limited, retry_after, limit = _rate_limit_service.check_rate_limit_core(
        request,
        rate_limit_enabled=deps['rate_limit_enabled'],
        rate_limit_window_seconds=deps['rate_limit_window_seconds'],
        rate_limit_state=_rate_limit_state,
        rate_limit_lock=_rate_limit_lock,
        rate_limit_cleanup_every=deps['rate_limit_cleanup_every'],
        rate_limit_request_counter_ref=counter_ref,
        rate_limit_bucket=deps['rate_limit_bucket'],
        request_client_id=deps['request_client_id'],
        prune_rate_limit_state=deps['prune_rate_limit_state'],
    )
    return (limited, retry_after, limit, int(counter_ref['value']))


def csrf_request_allowed_core(*, request) -> bool:
    if request.method.upper() not in {'POST', 'PUT', 'PATCH', 'DELETE'}:
        return True
    host = request.headers.get('host', '').strip().lower()
    if not host:
        return True

    sec_fetch_site = request.headers.get('sec-fetch-site', '').strip().lower()
    if sec_fetch_site in {'cross-site'}:
        return False

    origin = request.headers.get('origin', '').strip()
    if origin:
        parsed_origin = urlparse(origin)
        if (parsed_origin.netloc or '').strip().lower() != host:
            return False

    referer = request.headers.get('referer', '').strip()
    if referer:
        parsed_referer = urlparse(referer)
        if (parsed_referer.netloc or '').strip().lower() != host:
            return False

    return True
