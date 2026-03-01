import time


async def add_security_headers_core(*, request, call_next, deps: dict[str, object]):
    _metrics_service = deps['metrics_service']
    _log_event = deps['log_event']
    _csrf_request_allowed = deps['csrf_request_allowed']
    _request_body_limit_bytes = deps['request_body_limit_bytes']
    _check_rate_limit = deps['check_rate_limit']
    _json_response_cls = deps['json_response_cls']

    started = time.perf_counter()

    def _finalize(response):
        duration_ms = int((time.perf_counter() - started) * 1000)
        route = request.scope.get('route')
        route_path = str(getattr(route, 'path', '') or request.url.path)
        _metrics_service.record_request_core(
            method=request.method,
            path=route_path,
            status_code=int(response.status_code),
        )
        _log_event(
            'request_complete',
            method=request.method.upper(),
            path=route_path,
            status_code=int(response.status_code),
            duration_ms=duration_ms,
        )
        return response

    if not _csrf_request_allowed(request):
        return _finalize(_json_response_cls(
            status_code=403,
            content={'detail': 'cross-site request blocked'},
        ))

    limit = _request_body_limit_bytes(request.method, request.url.path)
    if limit > 0:
        content_length = request.headers.get('content-length', '').strip()
        if content_length.isdigit() and int(content_length) > limit:
            return _finalize(_json_response_cls(
                status_code=413,
                content={
                    'detail': (
                        f'Request body too large. Limit for this endpoint is {limit} bytes.'
                    )
                },
            ))

    limited, retry_after, rate_limit = _check_rate_limit(request)
    if limited:
        return _finalize(_json_response_cls(
            status_code=429,
            content={
                'detail': (
                    f'Rate limit exceeded for write requests. Try again in {retry_after} seconds.'
                )
            },
            headers={
                'Retry-After': str(retry_after),
                'X-RateLimit-Limit': str(rate_limit),
            },
        ))

    try:
        response = await call_next(request)
    except Exception as exc:
        error_response = _json_response_cls(status_code=500, content={'detail': 'internal server error'})
        _log_event('request_exception', method=request.method.upper(), path=request.url.path, error=str(exc))
        _finalize(error_response)
        raise

    csp_policy = (
        "default-src 'self'; "
        # unsafe-inline retained for inline <script>/<style> blocks in the single-page template.
        # Removing it requires nonce-based CSP refactor (future hardening task).
        # External sources scoped to the specific CDNs in use only.
        "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
        "img-src 'self' data: https:; "
        "font-src 'self' data:; "
        "connect-src 'self' https://nominatim.openstreetmap.org; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    )
    response.headers.setdefault('Content-Security-Policy', csp_policy)
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
    # HSTS: tell browsers to always use HTTPS. max-age=1 year.
    # Only effective when the app is served over TLS (behind a reverse proxy in production).
    response.headers.setdefault('Strict-Transport-Security', 'max-age=31536000; includeSubDomains')
    return _finalize(response)
