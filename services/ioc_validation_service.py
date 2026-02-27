import ipaddress
import re
from urllib.parse import urlparse


def _normalize_ioc_type(raw_type: str | None) -> str:
    value = str(raw_type or '').strip().lower()
    if value in {'ip', 'ipv4', 'ipv6'}:
        return 'ip'
    if value in {'domain', 'fqdn', 'host'}:
        return 'domain'
    if value in {'url', 'uri'}:
        return 'url'
    if value in {'hash', 'md5', 'sha1', 'sha256'}:
        return 'hash'
    if value in {'email', 'mail'}:
        return 'email'
    return 'indicator'


def _detect_ioc_type(value: str) -> str:
    if re.fullmatch(r'(?:[0-9]{1,3}\.){3}[0-9]{1,3}', value):
        return 'ip'
    if re.fullmatch(r'[a-fA-F0-9]{32}|[a-fA-F0-9]{40}|[a-fA-F0-9]{64}', value):
        return 'hash'
    if re.fullmatch(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', value):
        return 'email'
    try:
        parsed = urlparse(value)
    except Exception:
        parsed = None
    if parsed is not None and parsed.scheme.lower() in {'http', 'https'} and parsed.netloc:
        return 'url'
    if re.fullmatch(r'(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}', value):
        return 'domain'
    return 'indicator'


def _normalize_domain(value: str) -> str:
    normalized = value.strip().lower().rstrip('.')
    return normalized


def _suppress_benign(ioc_type: str, normalized_value: str) -> tuple[bool, str]:
    if ioc_type == 'ip':
        try:
            ip_obj = ipaddress.ip_address(normalized_value)
        except Exception:
            return False, ''
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_multicast or ip_obj.is_reserved or ip_obj.is_link_local:
            return True, 'private/reserved ip'
    if ioc_type == 'domain':
        lower = normalized_value.lower()
        if lower in {'example.com', 'example.net', 'example.org', 'localhost'}:
            return True, 'example/local domain'
        if lower.endswith('.local') or lower.endswith('.lan'):
            return True, 'internal/local domain'
    return False, ''


def validate_ioc_candidate_core(
    *,
    raw_value: str,
    raw_type: str | None,
    source_tier: str | None = None,
    extraction_method: str = 'manual',
) -> dict[str, object]:
    value = str(raw_value or '').strip()
    if not value:
        return {
            'valid': False,
            'ioc_type': '',
            'ioc_value': '',
            'normalized_value': '',
            'validation_status': 'invalid',
            'validation_reason': 'empty value',
            'confidence_score': 0,
            'is_active': 0,
        }

    requested_type = _normalize_ioc_type(raw_type)
    detected_type = _detect_ioc_type(value)
    ioc_type = detected_type if requested_type == 'indicator' else requested_type

    normalized_value = value
    reason = ''
    valid = False

    if ioc_type == 'ip':
        try:
            ip_obj = ipaddress.ip_address(value)
            normalized_value = str(ip_obj)
            valid = True
        except Exception:
            reason = 'invalid ip format'
    elif ioc_type == 'domain':
        normalized_value = _normalize_domain(value)
        if re.fullmatch(r'(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}', normalized_value) and len(normalized_value) <= 253:
            valid = True
        else:
            reason = 'invalid domain format'
    elif ioc_type == 'url':
        try:
            parsed = urlparse(value)
        except Exception:
            parsed = None
        if parsed is None:
            reason = 'invalid url format'
            parsed = urlparse('')
        scheme = parsed.scheme.lower()
        host = str(parsed.hostname or '').strip()
        if scheme in {'http', 'https'} and host:
            normalized_value = f'{scheme}://{host}{parsed.path or ""}'
            if parsed.query:
                normalized_value += f'?{parsed.query}'
            valid = True
        else:
            reason = 'invalid url format'
    elif ioc_type == 'hash':
        lowered = value.lower()
        if re.fullmatch(r'[a-f0-9]{32}|[a-f0-9]{40}|[a-f0-9]{64}', lowered):
            normalized_value = lowered
            valid = True
        else:
            reason = 'invalid hash format'
    elif ioc_type == 'email':
        lowered = value.lower()
        if re.fullmatch(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', lowered):
            normalized_value = lowered
            valid = True
        else:
            reason = 'invalid email format'
    else:
        reason = 'unsupported indicator type'

    if not valid:
        return {
            'valid': False,
            'ioc_type': ioc_type,
            'ioc_value': value,
            'normalized_value': normalized_value,
            'validation_status': 'invalid',
            'validation_reason': reason,
            'confidence_score': 0,
            'is_active': 0,
        }

    suppressed, suppress_reason = _suppress_benign(ioc_type, normalized_value)
    if suppressed:
        return {
            'valid': True,
            'ioc_type': ioc_type,
            'ioc_value': value,
            'normalized_value': normalized_value,
            'validation_status': 'suppressed_benign',
            'validation_reason': suppress_reason,
            'confidence_score': 1,
            'is_active': 0,
        }

    tier = str(source_tier or '').strip().lower()
    if extraction_method == 'manual':
        confidence_score = 4
    elif tier == 'high':
        confidence_score = 5
    elif tier == 'medium':
        confidence_score = 4
    elif tier == 'trusted':
        confidence_score = 3
    elif tier == 'context':
        confidence_score = 1
    else:
        confidence_score = 2

    return {
        'valid': True,
        'ioc_type': ioc_type,
        'ioc_value': value,
        'normalized_value': normalized_value,
        'validation_status': 'valid',
        'validation_reason': '',
        'confidence_score': confidence_score,
        'is_active': 1,
    }
