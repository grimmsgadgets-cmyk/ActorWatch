import socket

import httpx

from network_safety import safe_http_get, validate_outbound_url


def validate_outbound_url_core(
    source_url: str,
    *,
    allowed_domains: set[str] | None,
    deps: dict[str, object],
) -> str:
    _outbound_allowed_domains = deps['outbound_allowed_domains']
    _resolve_host = deps.get('resolve_host', socket.getaddrinfo)
    _ipproto_tcp = int(deps.get('ipproto_tcp', socket.IPPROTO_TCP))

    effective_allowlist = _outbound_allowed_domains if allowed_domains is None else allowed_domains
    return validate_outbound_url(
        source_url,
        allowed_domains=effective_allowlist,
        resolve_host=_resolve_host,
        ipproto_tcp=_ipproto_tcp,
    )


def safe_http_get_core(
    source_url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None,
    allowed_domains: set[str] | None,
    max_redirects: int,
    deps: dict[str, object],
) -> httpx.Response:
    _validate_url = deps['validate_url']

    return safe_http_get(
        source_url,
        timeout=timeout,
        headers=headers,
        allowed_domains=allowed_domains,
        max_redirects=max_redirects,
        validate_url=_validate_url,
        http_get=httpx.get,
    )
