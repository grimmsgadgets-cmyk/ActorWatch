import re
from datetime import datetime, timedelta, timezone
from typing import Callable

import services.ioc_store_service as ioc_store_service
import services.ioc_validation_service as ioc_validation_service

def _quick_check_ioc_type_hints(text: str) -> set[str]:
    normalized = str(text or '').lower()
    hints: set[str] = set()
    matched_specific_signal = False

    def _contains(pattern: str) -> bool:
        return bool(re.search(pattern, normalized))

    if _contains(r'\b(dns|domain|domains|fqdn|url|uri|proxy)\b'):
        hints.update({'domain', 'url', 'indicator'})
        matched_specific_signal = True
    if _contains(r'\b(ip|ipv4|ipv6|network|c2|beacon|egress|vpn|firewall|address)\b'):
        hints.update({'ip', 'domain', 'url', 'indicator'})
        matched_specific_signal = True
    if _contains(r'\b(email|phish|sender|mailbox|m365|exchange)\b'):
        hints.update({'email', 'domain', 'url', 'indicator'})
        matched_specific_signal = True
    if _contains(r'\b(hash|sha256|sha1|md5|binary|payload|malware)\b'):
        hints.update({'hash', 'indicator'})
        matched_specific_signal = True
    if (not matched_specific_signal) and _contains(r'\b(ioc|iocs|indicator|indicators|artifact|artifacts)\b'):
        hints.update({'ip', 'domain', 'url', 'hash', 'email', 'indicator'})
    return hints


def _quick_check_allows_ioc_fallback(text: str) -> bool:
    normalized = str(text or '').lower()

    def _contains(pattern: str) -> bool:
        return bool(re.search(pattern, normalized))

    event_or_process_focused = _contains(
        r'\b(event\s*id|eventid|sysmon|powershell|script\s*block|4688|4104|4624|4698|'
        r'process|scheduled\s*task|task\s*scheduler|logon|security\s*log)\b'
    )
    explicit_ioc_signal = _contains(
        r'\b(ioc|indicator|artifact|domain|dns|url|ip|hash|email|beacon|c2|callback)\b'
    )
    if event_or_process_focused and not explicit_ioc_signal:
        return False
    return True


def _ioc_value_is_hunt_relevant(ioc_type: str, ioc_value: str) -> bool:
    normalized_type = str(ioc_type or '').strip().lower()
    normalized_value = str(ioc_value or '').strip().lower()
    if not normalized_type or not normalized_value:
        return False
    if len(normalized_value) < 4:
        return False
    if normalized_type == 'domain':
        if re.fullmatch(r'^[a-z0-9-]+\.(js|json|css|html|xml|yaml|yml|md|txt|jsx|tsx)$', normalized_value):
            return False
    return True


def _ioc_semantic_tokens(value: str) -> set[str]:
    ignored_tokens = {
        'domain',
        'domains',
        'http',
        'https',
        'www',
        'com',
        'net',
        'org',
        'info',
        'local',
        'internal',
        'update',
        'report',
        'campaign',
    }
    tokens = {
        token
        for token in re.split(r'[^a-z0-9]+', str(value or '').lower())
        if len(token) >= 4 and not token.isdigit() and token not in ignored_tokens
    }
    return tokens


def _ioc_semantically_links_to_card(*, card_text: str, ioc_value: str, source_ref: str) -> bool:
    text = str(card_text or '').lower()
    value = str(ioc_value or '').strip().lower()
    ref = str(source_ref or '').strip().lower()
    if not text or not value:
        return False
    if value in text:
        return True
    for token in _ioc_semantic_tokens(value):
        if token in text:
            return True
    if ref:
        if ref in text:
            return True
        for token in _ioc_semantic_tokens(ref):
            if token in text:
                return True
    return False


def _relevant_iocs_for_quick_check(
    card: dict[str, object],
    ioc_items: list[dict[str, object]],
    *,
    limit: int = 5,
) -> list[dict[str, str]]:
    if not ioc_items:
        return []
    card_text = ' '.join(
        [
            str(card.get('question_text') or ''),
            str(card.get('first_step') or ''),
            str(card.get('what_to_look_for') or ''),
            str(card.get('query_hint') or ''),
            str(card.get('telemetry_anchor') or ''),
        ]
    ).lower()
    if not card_text.strip():
        return []

    hinted_types = _quick_check_ioc_type_hints(card_text)
    fallback_allowed = _quick_check_allows_ioc_fallback(card_text)
    relevant: list[tuple[int, dict[str, str]]] = []
    fallback_ranked: list[tuple[int, int, int, dict[str, str]]] = []
    type_priority = {'domain': 0, 'ip': 1, 'url': 2, 'hash': 3, 'email': 4, 'indicator': 5}
    for item in ioc_items:
        ioc_type = str(item.get('ioc_type') or '').strip().lower()
        ioc_value = str(item.get('ioc_value') or '').strip().lower()
        if not ioc_type or not ioc_value:
            continue
        if not _ioc_value_is_hunt_relevant(ioc_type, ioc_value):
            continue
        try:
            confidence_score = int(item.get('confidence_score') or 0)
        except Exception:
            confidence_score = 0
        source_ref = str(item.get('source_ref') or '')
        semantic_link = _ioc_semantically_links_to_card(
            card_text=card_text,
            ioc_value=ioc_value,
            source_ref=source_ref,
        )
        if fallback_allowed:
            fallback_ranked.append(
                (
                    1 if (confidence_score >= 3 and semantic_link) else 0,
                    confidence_score,
                    -type_priority.get(ioc_type, 99),
                    {
                        'ioc_type': str(item.get('ioc_type') or ''),
                        'ioc_value': str(item.get('ioc_value') or ''),
                        'source_ref': source_ref,
                    },
                )
            )

        score = 0
        if ioc_type in hinted_types and semantic_link:
            score += 3
        elif not hinted_types and fallback_allowed and confidence_score >= 3 and semantic_link:
            score += 1
        if ioc_value in card_text:
            score += 5
        if score > 0 and confidence_score >= 4:
            score += 1
        if score <= 0:
            continue
        relevant.append(
            (
                score,
                {
                    'ioc_type': str(item.get('ioc_type') or ''),
                    'ioc_value': str(item.get('ioc_value') or ''),
                    'source_ref': str(item.get('source_ref') or ''),
                },
            )
        )

    if not relevant:
        if not fallback_ranked:
            return []
        ranked_fallback = sorted(fallback_ranked, key=lambda row: (row[0], row[1], row[2]), reverse=True)
        return [row[3] for row in ranked_fallback[:limit]]
    ranked = sorted(relevant, key=lambda row: int(row[0]), reverse=True)
    return [row[1] for row in ranked[:limit]]


def _extract_ioc_candidates_from_text(
    text: str,
    *,
    ignored_domains: set[str] | None = None,
) -> list[tuple[str, str]]:
    raw = str(text or '')
    if not raw.strip():
        return []
    ignored = {str(item).lower().strip() for item in (ignored_domains or set()) if str(item).strip()}
    found: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _add(ioc_type: str, value: str) -> None:
        normalized_type = str(ioc_type or '').strip().lower()
        normalized_value = str(value or '').strip()
        if not normalized_type or not normalized_value:
            return
        key = (normalized_type, normalized_value.lower())
        if key in seen:
            return
        seen.add(key)
        found.append((normalized_type, normalized_value))

    url_matches = re.findall(r'https?://[^\s<>"\')]+', raw, flags=re.IGNORECASE)
    for value in url_matches:
        clean = value.rstrip('.,;:)')
        host_match = re.match(r'^https?://([^/:?#]+)', clean, flags=re.IGNORECASE)
        host = str(host_match.group(1) if host_match else '').strip().lower()
        if host and host in ignored:
            continue
        _add('url', clean)
    for value in re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', raw):
        _add('email', value)
    for value in re.findall(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', raw):
        octets = value.split('.')
        if len(octets) == 4 and all(part.isdigit() and 0 <= int(part) <= 255 for part in octets):
            _add('ip', value)
    for value in re.findall(r'\b[a-fA-F0-9]{64}\b', raw):
        _add('hash', value.lower())
    for value in re.findall(r'\b[a-fA-F0-9]{40}\b', raw):
        _add('hash', value.lower())
    for value in re.findall(r'\b[a-fA-F0-9]{32}\b', raw):
        _add('hash', value.lower())
    raw_without_urls = raw
    for value in url_matches:
        raw_without_urls = raw_without_urls.replace(value, ' ')
    ioc_context_pattern = re.compile(
        r'\b(ioc|indicator|artifact|domain|dns|beacon|c2|callback|malicious|suspicious|'
        r'phish|ransom|payload|host|url|ip|hash)\b',
        flags=re.IGNORECASE,
    )
    software_like_domain_pattern = re.compile(
        r'^[a-z0-9-]+\.(js|json|css|html|xml|yaml|yml)$',
        flags=re.IGNORECASE,
    )
    for match in re.finditer(r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b', raw_without_urls):
        value = str(match.group(0) or '')
        if value.lower().startswith('http'):
            continue
        normalized = value.lower()
        if normalized in ignored:
            continue
        if software_like_domain_pattern.fullmatch(normalized):
            continue
        window_start = max(0, match.start() - 90)
        window_end = min(len(raw_without_urls), match.end() + 90)
        context_window = raw_without_urls[window_start:window_end]
        if not ioc_context_pattern.search(context_window):
            continue
        _add('domain', normalized)
    return found


def _derived_ioc_items_from_sources(
    source_items: list[dict[str, object]],
    *,
    max_items: int = 40,
) -> list[dict[str, str]]:
    if not source_items:
        return []
    context_only_domains = {
        'ransomware.live',
        'api.ransomware.live',
        'therecord.media',
        'bleepingcomputer.com',
        'thehackernews.com',
        'darkreading.com',
        'krebsonsecurity.com',
        'isc.sans.edu',
    }
    high_quality_source_domains = {
        'blog.talosintelligence.com',
        'talosintelligence.com',
        'unit42.paloaltonetworks.com',
        'mandiant.com',
        'cloud.google.com',
        'crowdstrike.com',
        'securelist.com',
        'microsoft.com',
        'security.microsoft.com',
        'www.cisa.gov',
        'cisa.gov',
        'symantec-enterprise-blogs.security.com',
        'trellix.com',
        'sentinelone.com',
        'proofpoint.com',
        'rapid7.com',
    }

    def _host_from_url(raw_url: str) -> str:
        match = re.match(r'^https?://([^/:?#]+)', str(raw_url or '').strip(), flags=re.IGNORECASE)
        host = str(match.group(1) if match else '').strip().lower()
        if host.startswith('www.') and len(host) > 4:
            host = host[4:]
        return host

    def _looks_ioc_capable(source: dict[str, object]) -> bool:
        source_url = str(source.get('url') or '').strip()
        source_name = str(source.get('source_name') or '').strip().lower()
        source_tier = str(source.get('source_tier') or '').strip().lower()
        host = _host_from_url(source_url)
        if host in context_only_domains:
            return False
        text = str(source.get('pasted_text') or '')
        if len(text.strip()) < 80:
            return False
        has_strong_ioc_pattern = bool(
            re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', text)
            or re.search(r'https?://[^\s<>"\')]+', text, flags=re.IGNORECASE)
            or re.search(r'\b[a-fA-F0-9]{32}\b', text)
            or re.search(r'\b[a-fA-F0-9]{40}\b', text)
            or re.search(r'\b[a-fA-F0-9]{64}\b', text)
            or re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', text)
        )
        if has_strong_ioc_pattern:
            return True
        if source_tier in {'high', 'medium'}:
            return True
        ioc_likely_name_markers = (
            'advisory',
            'psirt',
            'cert',
            'security',
            'threat',
            'research',
            'intel',
            'mandiant',
            'talos',
            'unit 42',
            'crowdstrike',
            'sentinelone',
            'proofpoint',
            'rapid7',
        )
        return any(marker in source_name for marker in ioc_likely_name_markers)

    source_order = {id(source): index for index, source in enumerate(source_items)}

    def _source_priority(source: dict[str, object]) -> tuple[int, int]:
        source_url = str(source.get('url') or '').strip()
        host = _host_from_url(source_url)
        source_tier = str(source.get('source_tier') or '').strip().lower()
        if host in high_quality_source_domains:
            tier_rank = 0
        elif source_tier == 'high':
            tier_rank = 1
        elif source_tier == 'medium':
            tier_rank = 2
        elif source_tier == 'trusted':
            tier_rank = 3
        else:
            tier_rank = 4
        recency_rank = -int(source_order.get(id(source), 0))
        return (tier_rank, recency_rank)

    ignored_domains: set[str] = set()
    for source in source_items:
        source_url = str(source.get('url') or '').strip()
        match = re.match(r'^https?://([^/:?#]+)', source_url, flags=re.IGNORECASE)
        host = str(match.group(1) if match else '').strip().lower()
        if host:
            ignored_domains.add(host)
            if host.startswith('www.') and len(host) > 4:
                ignored_domains.add(host[4:])
    derived: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    prioritized_sources = sorted(source_items, key=_source_priority)
    for source in prioritized_sources:
        if not _looks_ioc_capable(source):
            continue
        source_name = str(source.get('source_name') or '').strip()
        source_url = str(source.get('url') or '').strip()
        source_ref = source_name or source_url
        source_text = ' '.join(
            [
                str(source.get('title') or ''),
                str(source.get('headline') or ''),
                str(source.get('og_title') or ''),
                str(source.get('html_title') or ''),
                str(source.get('pasted_text') or ''),
            ]
        )
        if not source_text.strip():
            continue
        for ioc_type, ioc_value in _extract_ioc_candidates_from_text(
            source_text,
            ignored_domains=ignored_domains,
        ):
            key = (ioc_type, ioc_value.lower())
            if key in seen:
                continue
            seen.add(key)
            derived.append(
                {
                    'ioc_type': ioc_type,
                    'ioc_value': ioc_value,
                    'source_ref': source_ref,
                    'source_id': str(source.get('id') or ''),
                    'source_tier': str(source.get('source_tier') or ''),
                    'observed_at': str(source.get('published_at') or source.get('retrieved_at') or ''),
                }
            )
            if len(derived) >= max_items:
                return derived
    return derived


def _ioc_seen_within_days(
    item: dict[str, object],
    *,
    days: int,
    parse_published_datetime: Callable[[str], datetime | None],
) -> bool:
    if days <= 0:
        return True
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=days)
    last_seen = parse_published_datetime(str(item.get('last_seen_at') or ''))
    created = parse_published_datetime(str(item.get('created_at') or ''))
    seen_dt = last_seen or created
    if seen_dt is None:
        return False
    return seen_dt >= cutoff


def latest_reporting_recency_label(
    timeline_recent_items: list[dict[str, object]],
    *,
    parse_published_datetime: Callable[[str], datetime | None],
) -> str:
    parsed_dates: list[datetime] = []
    for item in timeline_recent_items:
        dt = parse_published_datetime(str(item.get('occurred_at') or ''))
        if dt is not None:
            parsed_dates.append(dt)
    if not parsed_dates:
        return 'recency unclear'
    newest = max(parsed_dates)
    days_old = max(0, (datetime.now(timezone.utc) - newest).days)
    if days_old <= 7:
        return 'latest reporting in the last 7 days'
    if days_old <= 30:
        return 'latest reporting in the last 30 days'
    return 'latest reporting in the last 90 days'

