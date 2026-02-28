import html
import json
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, quote_plus, unquote, urlparse, urlunparse

import services.source_evidence_service as source_evidence_service


PRIMARY_BACKFILL_DOMAINS = [
    'cisa.gov',
    'ncsc.gov.uk',
    'mitre.org',
    'unit42.paloaltonetworks.com',
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'securelist.com',
    'blog.talosintelligence.com',
    'welivesecurity.com',
    'trendmicro.com',
    'microsoft.com',
    'thedfirreport.com',
    'recordedfuture.com',
    'intel471.com',
    'proofpoint.com',
    'redcanary.com',
    'sygnia.co',
    'checkpoint.com',
    'malwarebytes.com',
    'zerodayinitiative.com',
    'netwitness.com',
    'corelight.com',
    'eclecticiq.com',
    'levelblue.com',
    'cert.ssi.gouv.fr',
    'jpcert.or.jp',
]
FALLBACK_BACKFILL_DOMAINS = [
    'thehackernews.com',
    'bleepingcomputer.com',
    'securityweek.com',
    'therecord.media',
    'darkreading.com',
]
BACKFILL_QUERY_SUFFIXES = ['report', 'ransomware', 'malware analysis', 'ttp', 'advisory', 'attack campaign']
BACKFILL_MAX_SECONDS = 120.0
BACKFILL_SEARCH_TIMEOUT_SECONDS = 15.0
BACKFILL_FETCH_TIMEOUT_SECONDS = 15.0
BACKFILL_QUERY_BUDGET = 30
PREFILTER_EVAL_CAP_PER_PROVIDER = 50
PRIMARY_ALLOWLIST_REGISTRABLE = {
    'cisa.gov',
    'ncsc.gov.uk',
    'mitre.org',
    'paloaltonetworks.com',
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'securelist.com',
    'talosintelligence.com',
    'welivesecurity.com',
    'trendmicro.com',
    'microsoft.com',
    'thedfirreport.com',
    'recordedfuture.com',
    'intel471.com',
    'proofpoint.com',
    'redcanary.com',
    'sygnia.co',
    'checkpoint.com',
    'malwarebytes.com',
    'zerodayinitiative.com',
    'netwitness.com',
    'corelight.com',
    'eclecticiq.com',
    'levelblue.com',
    'gouv.fr',
    'jpcert.or.jp',
    'bleepingcomputer.com',
    'thehackernews.com',
    'securityweek.com',
    'therecord.media',
    'darkreading.com',
}
SECONDARY_ALLOWLIST_HOSTS = {
    'learn.microsoft.com',
    'docs.google.com',
    'storage.googleapis.com',
    'github.com',
}

RSS_PROVIDER_FEEDS = [
    ('cisa.gov', 'https://www.cisa.gov/cybersecurity-advisories/all.xml'),
    ('ncsc.gov.uk', 'https://www.ncsc.gov.uk/api/1/services/v1/all-rss-feed.xml'),
    ('unit42.paloaltonetworks.com', 'https://unit42.paloaltonetworks.com/feed/'),
    ('blog.talosintelligence.com', 'https://blog.talosintelligence.com/rss/'),
    ('microsoft.com', 'https://www.microsoft.com/en-us/security/blog/feed/'),
    ('mandiant.com', 'https://www.mandiant.com/resources/blog/rss.xml'),
    ('crowdstrike.com', 'https://www.crowdstrike.com/blog/feed/'),
    ('sentinelone.com', 'https://www.sentinelone.com/blog/feed/'),
    ('welivesecurity.com', 'https://www.welivesecurity.com/en/rss/feed'),
    ('trendmicro.com', 'https://www.trendmicro.com/en_us/research.html/rss.xml'),
    ('proofpoint.com', 'https://www.proofpoint.com/us/blog/feed'),
    ('redcanary.com', 'https://redcanary.com/feed/'),
    ('thedfirreport.com', 'https://thedfirreport.com/feed/'),
    ('recordedfuture.com', 'https://www.recordedfuture.com/feed'),
    ('intel471.com', 'https://www.intel471.com/blog/feed'),
    ('sygnia.co', 'https://www.sygnia.co/blog/feed/'),
    ('research.checkpoint.com', 'https://research.checkpoint.com/feed/'),
    ('malwarebytes.com', 'https://www.malwarebytes.com/blog/feed/index.xml'),
    ('zerodayinitiative.com', 'https://www.zerodayinitiative.com/blog?format=rss'),
    ('netwitness.com', 'https://www.netwitness.com/en-us/blog/feed/'),
    ('corelight.com', 'https://corelight.com/blog/rss.xml'),
    ('blog.eclecticiq.com', 'https://blog.eclecticiq.com/rss.xml'),
    ('levelblue.com', 'https://www.levelblue.com/en-us/resources/blogs/spiderlabs-blog/rss.xml'),
    ('cert.ssi.gouv.fr', 'https://www.cert.ssi.gouv.fr/feed/'),
    ('jpcert.or.jp', 'https://www.jpcert.or.jp/english/rss/rss.xml'),
]

FAIL_DNS = 'dns_error'
FAIL_TIMEOUT = 'timeout'
FAIL_403 = '403'
FAIL_PARSE = 'parse_failed'
FAIL_NO_DATE = 'no_date'
FAIL_NO_TEXT = 'no_text'
FAIL_ALLOWLIST = 'rejected_allowlist'
FAIL_SCORE = 'score_below_threshold'
FAIL_CANDIDATE_LOW_RELEVANCE = 'candidate_low_relevance'
FAIL_CANDIDATE_INVALID_URL = 'candidate_invalid_url'

MATCHER_VERSION = 'v2_scored_linking'
MATCH_THRESHOLD = 3
MAX_MATCH_TERMS = 24
MAX_MATCH_TERM_LEN = 80


def _parse_iso(value: str | None) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonicalize_url(raw_url: str) -> str:
    raw = str(raw_url or '').strip()
    if not raw:
        return ''
    try:
        parsed = urlparse(raw)
    except Exception:
        return ''
    if parsed.scheme not in {'http', 'https'}:
        return ''
    query_pairs = parse_qs(parsed.query, keep_blank_values=False)
    filtered_query = '&'.join(
        f'{key}={quote_plus(str(values[0]))}'
        for key, values in sorted(query_pairs.items())
        if values and not key.lower().startswith('utm_')
    )
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment='',
        query=filtered_query,
    )
    return urlunparse(normalized).rstrip('/')


def _registrable_domain(host: str) -> str:
    value = str(host or '').strip('.').lower()
    if not value:
        return ''
    if re.fullmatch(r'[0-9.]+', value):
        return value
    labels = [part for part in value.split('.') if part]
    if len(labels) <= 2:
        return '.'.join(labels)
    multi_part_suffixes = {
        'co.uk',
        'org.uk',
        'gov.uk',
        'ac.uk',
        'com.au',
        'net.au',
        'org.au',
        'co.jp',
    }
    suffix2 = '.'.join(labels[-2:])
    suffix3 = '.'.join(labels[-3:])
    if suffix2 in multi_part_suffixes and len(labels) >= 3:
        return suffix3
    return '.'.join(labels[-2:])


def _url_host(url_value: str) -> str:
    try:
        return (urlparse(url_value).hostname or '').strip('.').lower()
    except Exception:
        return ''


def _is_allowed_host(host: str) -> bool:
    if not host:
        return False
    registrable = _registrable_domain(host)
    if registrable in PRIMARY_ALLOWLIST_REGISTRABLE:
        return True
    if any(host == allowed or host.endswith(f'.{allowed}') for allowed in SECONDARY_ALLOWLIST_HOSTS):
        return True
    return False


def _domain_matches(url_value: str, allowed_domain: str) -> bool:
    try:
        host = (urlparse(url_value).hostname or '').strip('.').lower()
    except Exception:
        return False
    domain = str(allowed_domain or '').strip('.').lower()
    if not host or not domain:
        return False
    return host == domain or host.endswith(f'.{domain}')


def _is_allowed(url_value: str) -> bool:
    return _is_allowed_host(_url_host(url_value))


def _candidate_source_value(source_type: str, source_label: str | None = None) -> str:
    normalized_type = str(source_type or '').strip().lower() or 'search'
    label = str(source_label or '').strip()
    if normalized_type == 'rss' and label:
        return f'rss:{label}'
    return normalized_type


def _candidate_from_url(
    *,
    url_value: str,
    source_type: str,
    source_label: str | None,
) -> dict[str, str] | None:
    canonical = _canonicalize_url(url_value)
    if not canonical:
        return None
    return {
        'candidate_url': canonical,
        'candidate_registrable_domain': _registrable_domain(_url_host(canonical)) or 'unknown',
        'candidate_source': _candidate_source_value(source_type, source_label),
    }


def _split_terms(raw_terms: list[str]) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for raw in raw_terms:
        for token in re.split(r'[,/;|]+', str(raw or '').strip()):
            value = token.strip().lower()
            if len(value) < 3:
                continue
            if value in seen:
                continue
            seen.add(value)
            terms.append(value)
    return terms


def _url_path_contains_any_term(url_value: str, terms: list[str]) -> bool:
    try:
        path = (urlparse(url_value).path or '').strip('/').lower()
    except Exception:
        return False
    if not path:
        return False
    for term in terms:
        if str(term).strip().lower() in path:
            return True
    return False


def _extract_cluster_labels(text_blob: str) -> list[str]:
    text = str(text_blob or '')
    if not text.strip():
        return []
    labels: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(
        r'\b(?:UNC[0-9]{3,5}|DEV-[0-9]{3,5}|TA[0-9]{4}|APT[0-9]{1,2}|FIN[0-9]{1,2})\b',
        text,
        flags=re.IGNORECASE,
    ):
        label = str(match.group(0) or '').upper()
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return labels


def _is_mitre_structured_url(url_value: str) -> bool:
    try:
        parsed = urlparse(url_value)
    except Exception:
        return False
    host = (parsed.hostname or '').strip('.').lower()
    path = (parsed.path or '').strip()
    if host != 'attack.mitre.org':
        return False
    return bool(re.search(r'^/(software/S[0-9]{4}|groups/G[0-9]{4})/?$', path, flags=re.IGNORECASE))


def _is_cisa_structured_url(url_value: str) -> bool:
    try:
        parsed = urlparse(url_value)
    except Exception:
        return False
    host = (parsed.hostname or '').strip('.').lower()
    if _registrable_domain(host) != 'cisa.gov':
        return False
    path = (parsed.path or '').lower()
    return ('/news-events/' in path) or ('/cybersecurity-advisories/' in path)


def _is_authoritative_mapping_url(url_value: str) -> bool:
    return _is_mitre_structured_url(url_value) or _is_cisa_structured_url(url_value)


def _text_blob_matches_actor_terms(text_blob: str, actor_terms: list[str]) -> bool:
    blob = str(text_blob or '').lower()
    if not blob.strip():
        return False
    return any(str(term or '').strip().lower() in blob for term in actor_terms if str(term or '').strip())


def _collect_matched_terms(*values: str) -> list[str]:
    matched: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or '').strip()
        if not normalized:
            continue
        clipped = normalized[:MAX_MATCH_TERM_LEN]
        key = clipped.lower()
        if key in seen:
            continue
        seen.add(key)
        matched.append(clipped)
        if len(matched) >= MAX_MATCH_TERMS:
            break
    return matched


def _score_linkage(
    *,
    actor_terms: list[str],
    context_terms: list[str],
    title_text: str,
    summary_text: str,
    source_text: str,
    final_url: str,
) -> dict[str, object]:
    score = 0
    reasons: list[str] = []
    matched_terms: list[str] = []

    title_blob = str(title_text or '').lower()
    summary_blob = str(summary_text or '').lower()
    source_blob = str(source_text or '').lower()

    actor_hits = [
        term for term in actor_terms
        if term and (term in title_blob or term in summary_blob or term in source_blob)
    ]
    if actor_hits:
        score += 3
        reasons.append('actor_term')
        matched_terms.extend(_collect_matched_terms(*actor_hits))

    cluster_labels = _extract_cluster_labels(source_text)
    if cluster_labels:
        score += 2
        reasons.append('cluster_label')
        matched_terms.extend(_collect_matched_terms(*cluster_labels))

    normalized_context_terms = [str(item).strip().lower() for item in context_terms if str(item).strip()]
    context_hits = [term for term in normalized_context_terms if term in source_blob]
    if context_hits:
        score += 2
        reasons.append('context_term')
        matched_terms.extend(_collect_matched_terms(*context_hits))

    if _is_mitre_structured_url(final_url):
        score += 3
        reasons.append('mitre_structured')
    elif _is_cisa_structured_url(final_url):
        score += 2
        reasons.append('cisa_structured')

    return {
        'score': int(score),
        'reasons': reasons,
        'matched_terms': _collect_matched_terms(*matched_terms),
    }


def _classify_error(exc: Exception | None = None, *, status_code: int | None = None) -> str:
    if status_code == 403:
        return FAIL_403
    if exc is None:
        return FAIL_PARSE
    text = str(exc).lower()
    if 'timed out' in text or 'timeout' in text:
        return FAIL_TIMEOUT
    if 'name or service not known' in text or 'temporary failure in name resolution' in text or 'gaierror' in text:
        return FAIL_DNS
    return FAIL_PARSE


def _record_error(error_counts: dict[str, int], reason: str) -> None:
    key = str(reason or FAIL_PARSE).strip() or FAIL_PARSE
    error_counts[key] = int(error_counts.get(key, 0)) + 1


def _record_rejected_domain(rejected_domain_counts: dict[tuple[str, str], int], *, url_value: str) -> None:
    host = _url_host(url_value)
    registrable = _registrable_domain(host)
    key = (host or 'unknown', registrable or 'unknown')
    rejected_domain_counts[key] = int(rejected_domain_counts.get(key, 0)) + 1


def _record_domain_count(domain_counts: dict[str, int], *, url_value: str) -> None:
    registrable = _registrable_domain(_url_host(url_value)) or 'unknown'
    domain_counts[registrable] = int(domain_counts.get(registrable, 0)) + 1


def _record_domain_reason_count(
    domain_reason_counts: dict[tuple[str, str], int],
    *,
    url_value: str,
    reason: str,
) -> None:
    registrable = _registrable_domain(_url_host(url_value)) or 'unknown'
    key = (registrable, str(reason or '').strip() or 'unknown')
    domain_reason_counts[key] = int(domain_reason_counts.get(key, 0)) + 1


def _summarize_rejected_domains(
    rejected_domain_counts: dict[tuple[str, str], int],
) -> tuple[list[list[object]], list[list[object]]]:
    top_by_host = sorted(
        [(host, registrable, int(count)) for (host, registrable), count in rejected_domain_counts.items()],
        key=lambda item: item[2],
        reverse=True,
    )[:10]
    by_registrable: dict[str, int] = {}
    for (_host, registrable), count in rejected_domain_counts.items():
        key = str(registrable or 'unknown')
        by_registrable[key] = int(by_registrable.get(key, 0)) + int(count)
    top_by_registrable = sorted(
        [(registrable, int(count)) for registrable, count in by_registrable.items()],
        key=lambda item: item[1],
        reverse=True,
    )[:10]
    return (
        [[str(host), str(registrable), int(count)] for host, registrable, count in top_by_host],
        [[str(registrable), int(count)] for registrable, count in top_by_registrable],
    )


def _summarize_domain_counts(domain_counts: dict[str, int], *, limit: int = 10) -> list[list[object]]:
    return [
        [str(domain), int(count)]
        for domain, count in sorted(domain_counts.items(), key=lambda item: int(item[1]), reverse=True)[:limit]
    ]


def _summarize_domain_reason_counts(
    domain_reason_counts: dict[tuple[str, str], int],
    *,
    limit: int = 10,
) -> list[list[object]]:
    return [
        [str(domain), str(reason), int(count)]
        for (domain, reason), count in sorted(domain_reason_counts.items(), key=lambda item: int(item[1]), reverse=True)[:limit]
    ]


def _prefetch_score_candidate(
    *,
    actor_terms: list[str],
    title_text: str,
    summary_text: str,
    candidate_url: str,
) -> dict[str, object]:
    score = 0
    reasons: list[str] = []
    title_blob = str(title_text or '').lower()
    summary_blob = str(summary_text or '').lower()
    path_blob = ''
    try:
        path_blob = (urlparse(candidate_url).path or '').lower()
    except Exception:
        path_blob = ''

    if any(term for term in actor_terms if term and (term in title_blob or term in summary_blob)):
        score += 3
        reasons.append('actor_term_title_summary')

    if re.search(r'\b(?:UNC[0-9]{3,5}|DEV-[0-9]{3,5}|TA[0-9]{4}|APT[0-9]{1,2}|FIN[0-9]{1,2})\b', title_text, flags=re.IGNORECASE):
        score += 2
        reasons.append('cluster_pattern_title')

    if re.search(r'\b(ransomware|apt|intrusion|campaign|group|threat actor)\b', title_text, flags=re.IGNORECASE):
        score += 1
        reasons.append('keyword_title')

    if re.search(r'(ransomware|apt|threat|campaign|group)', path_blob, flags=re.IGNORECASE):
        score += 1
        reasons.append('keyword_url_path')

    return {'score': int(score), 'reasons': reasons}


def _extract_search_result_urls(search_html: str, *, allowed_domain: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'href="([^"]+)"', str(search_html or ''), flags=re.IGNORECASE):
        href = html.unescape(str(match.group(1) or '').strip())
        if not href:
            continue
        if href.startswith('/l/?'):
            try:
                query = parse_qs(urlparse(href).query)
                href = str(query.get('uddg', [''])[0] or '')
            except Exception:
                href = ''
        href = unquote(href)
        canonical = _canonicalize_url(href)
        if not canonical:
            continue
        host = _url_host(canonical)
        registrable = _registrable_domain(host)
        if not (host == allowed_domain or host.endswith(f'.{allowed_domain}') or registrable == _registrable_domain(allowed_domain)):
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        urls.append(canonical)
        if len(urls) >= 12:
            break
    return urls


def _extract_feed_entries(feed_text: str) -> list[dict[str, str]]:
    body = str(feed_text or '')
    entries: list[dict[str, str]] = []
    item_blocks = re.findall(r'<item\b[^>]*>(.*?)</item>', body, flags=re.IGNORECASE | re.DOTALL)
    if not item_blocks:
        item_blocks = re.findall(r'<entry\b[^>]*>(.*?)</entry>', body, flags=re.IGNORECASE | re.DOTALL)
    for block in item_blocks:
        title_match = re.search(r'<title[^>]*>(.*?)</title>', block, flags=re.IGNORECASE | re.DOTALL)
        summary_match = re.search(r'<description[^>]*>(.*?)</description>', block, flags=re.IGNORECASE | re.DOTALL)
        if summary_match is None:
            summary_match = re.search(r'<summary[^>]*>(.*?)</summary>', block, flags=re.IGNORECASE | re.DOTALL)
        link_match = re.search(r'<link>([^<]+)</link>', block, flags=re.IGNORECASE)
        if link_match is None:
            link_match = re.search(r'<link[^>]+href="([^"]+)"', block, flags=re.IGNORECASE)
        entries.append(
            {
                'title': html.unescape(re.sub(r'<[^>]+>', ' ', str(title_match.group(1) if title_match else '')).strip()),
                'summary': html.unescape(re.sub(r'<[^>]+>', ' ', str(summary_match.group(1) if summary_match else '')).strip()),
                'link': html.unescape(str(link_match.group(1) if link_match else '').strip()),
            }
        )
    return entries


def _provider_rss_candidates(
    *,
    actor_terms: list[str],
    http_get,
    deadline_ts: float,
    metrics: dict[str, int],
    error_counts: dict[str, int],
    rejected_domain_counts: dict[tuple[str, str], int],
    rejected_allowlist_domain_counts: dict[str, int],
    low_relevance_domain_counts: dict[str, int],
    dropped_domain_reason_counts: dict[tuple[str, str], int],
) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    user_agent = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    )
    for feed_name, feed_url in RSS_PROVIDER_FEEDS:
        if time.perf_counter() >= deadline_ts:
            break
        try:
            timeout_value = max(1.0, min(BACKFILL_SEARCH_TIMEOUT_SECONDS, deadline_ts - time.perf_counter()))
            response = http_get(
                feed_url,
                timeout=timeout_value,
                follow_redirects=True,
                headers={'User-Agent': user_agent},
            )
            code = int(getattr(response, 'status_code', 0) or 0)
            if code != 200:
                _record_error(error_counts, _classify_error(None, status_code=code))
                continue
            entries = _extract_feed_entries(str(getattr(response, 'text', '') or ''))
        except Exception as exc:
            _record_error(error_counts, _classify_error(exc))
            continue

        deduped_feed_entries: list[dict[str, str]] = []
        seen_links: set[str] = set()
        for entry in entries:
            link = str(entry.get('link') or '')
            canonical = _canonicalize_url(link)
            if not canonical:
                _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                continue
            if canonical in seen_links:
                continue
            seen_links.add(canonical)
            deduped_feed_entries.append(
                {
                    'candidate_url': canonical,
                    'title': str(entry.get('title') or ''),
                    'summary': str(entry.get('summary') or ''),
                }
            )

        for entry in deduped_feed_entries[:PREFILTER_EVAL_CAP_PER_PROVIDER]:
            link = str(entry.get('candidate_url') or '')
            host = _url_host(link)
            registrable = _registrable_domain(host)
            if not (registrable in PRIMARY_ALLOWLIST_REGISTRABLE or _is_allowed_host(host)):
                _record_error(error_counts, FAIL_ALLOWLIST)
                _record_rejected_domain(rejected_domain_counts, url_value=link)
                _record_domain_count(rejected_allowlist_domain_counts, url_value=link)
                continue

            if _is_authoritative_mapping_url(link):
                prefetch_score = 99
            else:
                prefetch = _prefetch_score_candidate(
                    actor_terms=actor_terms,
                    title_text=str(entry.get('title') or ''),
                    summary_text=str(entry.get('summary') or ''),
                    candidate_url=link,
                )
                prefetch_score = int(prefetch.get('score') or 0)
            if prefetch_score < 2:
                _record_error(error_counts, FAIL_CANDIDATE_LOW_RELEVANCE)
                _record_domain_count(low_relevance_domain_counts, url_value=link)
                _record_domain_reason_count(
                    dropped_domain_reason_counts,
                    url_value=link,
                    reason='prefetch_score_below_2',
                )
                metrics['prefetch_dropped'] = int(metrics.get('prefetch_dropped', 0)) + 1
                continue
            if link in seen:
                continue
            seen.add(link)
            candidate = _candidate_from_url(
                url_value=link,
                source_type='rss',
                source_label=feed_name,
            )
            if candidate is None:
                _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                continue
            candidates.append(candidate)
            metrics['prefetch_kept'] = int(metrics.get('prefetch_kept', 0)) + 1
            metrics['candidates_found'] = int(metrics.get('candidates_found', 0)) + 1
            if len(candidates) >= 24:
                return candidates
    return candidates


def _provider_authoritative_candidates(
    *,
    actor_terms: list[str],
    build_actor_profile_from_mitre,
    http_get,
    deadline_ts: float,
    metrics: dict[str, int],
    error_counts: dict[str, int],
    rejected_domain_counts: dict[tuple[str, str], int],
    rejected_allowlist_domain_counts: dict[str, int],
) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    actor_name = actor_terms[0] if actor_terms else ''
    if callable(build_actor_profile_from_mitre):
        try:
            profile = build_actor_profile_from_mitre(actor_name)
            profile_url = _canonicalize_url(str(profile.get('source_url') or '')) if isinstance(profile, dict) else ''
            if profile_url and _is_mitre_structured_url(profile_url) and _is_allowed(profile_url):
                candidate = _candidate_from_url(
                    url_value=profile_url,
                    source_type='authoritative',
                    source_label='mitre_profile',
                )
                if candidate is None:
                    _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                else:
                    candidates.append(candidate)
                seen.add(profile_url)
                metrics['candidates_found'] = int(metrics.get('candidates_found', 0)) + 1
            elif profile_url:
                if not _is_mitre_structured_url(profile_url):
                    _record_error(error_counts, FAIL_CANDIDATE_LOW_RELEVANCE)
                else:
                    _record_error(error_counts, FAIL_ALLOWLIST)
                    _record_rejected_domain(rejected_domain_counts, url_value=profile_url)
                    _record_domain_count(rejected_allowlist_domain_counts, url_value=profile_url)
        except Exception:
            _record_error(error_counts, FAIL_PARSE)
    # CISA deterministic source
    if time.perf_counter() < deadline_ts:
        try:
            timeout_value = max(1.0, min(BACKFILL_SEARCH_TIMEOUT_SECONDS, deadline_ts - time.perf_counter()))
            response = http_get(
                'https://www.cisa.gov/cybersecurity-advisories/all.xml',
                timeout=timeout_value,
                follow_redirects=True,
                headers={'User-Agent': 'Mozilla/5.0'},
            )
            code = int(getattr(response, 'status_code', 0) or 0)
            if code == 200:
                entries = _extract_feed_entries(str(getattr(response, 'text', '') or ''))
                for entry in entries[:60]:
                    blob = f"{entry.get('title') or ''} {entry.get('summary') or ''}"
                    if not _text_blob_matches_actor_terms(blob, actor_terms):
                        continue
                    link = _canonicalize_url(str(entry.get('link') or ''))
                    if not link or _registrable_domain(_url_host(link)) != 'cisa.gov':
                        continue
                    if link in seen:
                        continue
                    seen.add(link)
                    candidate = _candidate_from_url(
                        url_value=link,
                        source_type='authoritative',
                        source_label='cisa_feed',
                    )
                    if candidate is None:
                        _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                        continue
                    candidates.append(candidate)
                    metrics['candidates_found'] = int(metrics.get('candidates_found', 0)) + 1
                    if len(candidates) >= 8:
                        break
            else:
                _record_error(error_counts, _classify_error(None, status_code=code))
        except Exception as exc:
            _record_error(error_counts, _classify_error(exc))
    return candidates


def _provider_search_candidates(
    *,
    actor_terms: list[str],
    domains: list[str],
    http_get,
    deadline_ts: float,
    metrics: dict[str, int],
    error_counts: dict[str, int],
    rejected_domain_counts: dict[tuple[str, str], int],
    rejected_allowlist_domain_counts: dict[str, int],
    query_budget: int,
) -> list[dict[str, str]]:
    user_agent = (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    )
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    budget = max(1, int(query_budget))
    terms = [str(item).strip() for item in actor_terms if str(item).strip()][:3]
    for domain in domains:
        if time.perf_counter() >= deadline_ts or budget <= 0:
            break
        for term in terms:
            for suffix in BACKFILL_QUERY_SUFFIXES[:3]:
                if time.perf_counter() >= deadline_ts or budget <= 0:
                    break
                budget -= 1
                metrics['queries_attempted'] = int(metrics.get('queries_attempted', 0)) + 1
                query = f'site:{domain} "{term}" {suffix}'
                search_url = f'https://html.duckduckgo.com/html/?q={quote_plus(query)}'
                try:
                    timeout_value = max(1.0, min(BACKFILL_SEARCH_TIMEOUT_SECONDS, deadline_ts - time.perf_counter()))
                    response = http_get(
                        search_url,
                        timeout=timeout_value,
                        follow_redirects=True,
                        headers={'User-Agent': user_agent},
                    )
                    code = int(getattr(response, 'status_code', 0) or 0)
                    if code != 200:
                        _record_error(error_counts, _classify_error(None, status_code=code))
                        continue
                    body = str(getattr(response, 'text', '') or '')
                except Exception as exc:
                    _record_error(error_counts, _classify_error(exc))
                    continue
                for url_value in _extract_search_result_urls(body, allowed_domain=domain):
                    if not _is_allowed(url_value):
                        _record_error(error_counts, FAIL_ALLOWLIST)
                        _record_rejected_domain(rejected_domain_counts, url_value=url_value)
                        _record_domain_count(rejected_allowlist_domain_counts, url_value=url_value)
                        continue
                    if url_value in seen:
                        continue
                    seen.add(url_value)
                    candidate = _candidate_from_url(
                        url_value=url_value,
                        source_type='search',
                        source_label=domain,
                    )
                    if candidate is None:
                        _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                        continue
                    candidates.append(candidate)
                    metrics['candidates_found'] = int(metrics.get('candidates_found', 0)) + 1
                    if len(candidates) >= 16:
                        return candidates
    return candidates


def _load_cache_row(connection: sqlite3.Connection, actor_id: str) -> dict[str, object] | None:
    row = connection.execute(
        '''
        SELECT queried_at, result_urls_json, inserted_count
        FROM web_backfill_cache
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchone()
    if row is None:
        return None
    try:
        result_urls = json.loads(str(row[1] or '[]'))
    except Exception:
        result_urls = []
    return {
        'queried_at': str(row[0] or ''),
        'result_urls': result_urls if isinstance(result_urls, list) else [],
        'inserted_count': int(row[2] or 0),
    }


def _store_cache_row(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    queried_at: str,
    result_urls: list[str],
    inserted_count: int,
) -> None:
    connection.execute(
        '''
        INSERT INTO web_backfill_cache (actor_id, queried_at, result_urls_json, inserted_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(actor_id) DO UPDATE SET
            queried_at = excluded.queried_at,
            result_urls_json = excluded.result_urls_json,
            inserted_count = excluded.inserted_count
        ''',
        (actor_id, queried_at, json.dumps(result_urls), int(inserted_count)),
    )


def _insert_backfill_run_row(connection: sqlite3.Connection, *, actor_id: str, started_at: str, mode: str) -> str:
    run_id = str(uuid.uuid4())
    connection.execute(
        '''
        INSERT INTO backfill_runs (
            id, actor_id, started_at, mode, queries_attempted, candidates_found,
            pages_fetched, pages_parsed_ok, sources_inserted, error_summary_json
        )
        VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, '{}')
        ''',
        (run_id, actor_id, started_at, mode),
    )
    return run_id


def _ensure_backfill_linkage_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS backfill_source_linkage (
            actor_id TEXT NOT NULL,
            source_id TEXT,
            source_url TEXT NOT NULL,
            matcher_version TEXT NOT NULL,
            match_score INTEGER NOT NULL DEFAULT 0,
            match_reasons_json TEXT NOT NULL DEFAULT '[]',
            matched_terms_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            PRIMARY KEY (actor_id, source_url, matcher_version)
        )
        '''
    )


def _store_backfill_linkage(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    source_id: str | None,
    source_url: str,
    match_score: int,
    match_reasons: list[str],
    matched_terms: list[str],
    now_iso: str,
) -> None:
    connection.execute(
        '''
        INSERT INTO backfill_source_linkage (
            actor_id, source_id, source_url, matcher_version,
            match_score, match_reasons_json, matched_terms_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_id, source_url, matcher_version) DO UPDATE SET
            source_id = excluded.source_id,
            match_score = excluded.match_score,
            match_reasons_json = excluded.match_reasons_json,
            matched_terms_json = excluded.matched_terms_json,
            created_at = excluded.created_at
        ''',
        (
            actor_id,
            str(source_id or '').strip() or None,
            str(source_url or '').strip(),
            MATCHER_VERSION,
            int(match_score),
            json.dumps([str(item) for item in (match_reasons or [])]),
            json.dumps([str(item) for item in (matched_terms or [])]),
            now_iso,
        ),
    )


def _finish_backfill_run_row(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    finished_at: str,
    metrics: dict[str, int],
    error_counts: dict[str, int],
    rejected_domain_counts: dict[tuple[str, str], int],
    rejected_allowlist_domain_counts: dict[str, int],
    low_relevance_domain_counts: dict[str, int],
    dropped_domain_reason_counts: dict[tuple[str, str], int],
) -> None:
    top_rejected, top_rejected_registrable = _summarize_rejected_domains(rejected_domain_counts)
    enriched_errors = dict(error_counts)
    for key in (
        FAIL_NO_TEXT,
        FAIL_SCORE,
        FAIL_CANDIDATE_LOW_RELEVANCE,
        FAIL_CANDIDATE_INVALID_URL,
        FAIL_ALLOWLIST,
        FAIL_TIMEOUT,
        FAIL_DNS,
        FAIL_403,
        FAIL_PARSE,
        FAIL_NO_DATE,
    ):
        enriched_errors[key] = int(enriched_errors.get(key, 0))
    enriched_errors['not_in_allowlist_domains'] = top_rejected
    enriched_errors['not_in_allowlist_registrable_domains'] = top_rejected_registrable
    enriched_errors['rejected_allowlist_domains'] = _summarize_domain_counts(rejected_allowlist_domain_counts, limit=10)
    enriched_errors['candidate_low_relevance_domains'] = _summarize_domain_counts(low_relevance_domain_counts, limit=10)
    enriched_errors['candidate_low_relevance_domain_reasons'] = _summarize_domain_reason_counts(
        dropped_domain_reason_counts,
        limit=10,
    )
    connection.execute(
        '''
        UPDATE backfill_runs
        SET finished_at = ?,
            queries_attempted = ?,
            candidates_found = ?,
            pages_fetched = ?,
            pages_parsed_ok = ?,
            sources_inserted = ?,
            error_summary_json = ?
        WHERE id = ?
        ''',
        (
            finished_at,
            int(metrics.get('queries_attempted', 0)),
            int(metrics.get('candidates_found', 0)),
            int(metrics.get('pages_fetched', 0)),
            int(metrics.get('pages_parsed_ok', 0)),
            int(metrics.get('sources_inserted', 0)),
            json.dumps(enriched_errors),
            run_id,
        ),
    )


def run_cold_actor_backfill_core(
    *,
    actor_id: str,
    actor_name: str,
    actor_aliases: list[str],
    deps: dict[str, object],
) -> dict[str, object]:
    _db_path = deps['db_path']
    _sqlite_connect = deps.get('sqlite_connect', sqlite3.connect)
    _utc_now_iso = deps['utc_now_iso']
    _http_get = deps['http_get']
    _derive_source_from_url = deps['derive_source_from_url']
    _upsert_source_for_actor = deps['upsert_source_for_actor']
    _search_candidates = deps.get('search_candidates')
    _build_actor_profile_from_mitre = deps.get('build_actor_profile_from_mitre')
    _context_terms_raw = deps.get('context_terms', [])

    now_iso = _utc_now_iso()
    now_dt = _parse_iso(now_iso) or datetime.now(timezone.utc)
    recent_cutoff = now_dt - timedelta(days=30)
    cache_cutoff = now_dt - timedelta(hours=24)
    started_ts = time.perf_counter()
    deadline_ts = started_ts + max(8.0, float(deps.get('backfill_max_seconds', BACKFILL_MAX_SECONDS)))

    actor_terms: list[str] = []
    for raw in [actor_name] + list(actor_aliases or []):
        value = str(raw or '').strip()
        if len(value) < 2:
            continue
        if value.lower() not in {item.lower() for item in actor_terms}:
            actor_terms.append(value)
    if not actor_terms:
        actor_terms = [str(actor_id)]
    context_terms = _split_terms(
        _context_terms_raw if isinstance(_context_terms_raw, list) else []
    )

    metrics = {
        'queries_attempted': 0,
        'candidates_found': 0,
        'pages_fetched': 0,
        'pages_parsed_ok': 0,
        'sources_inserted': 0,
        'prefetch_kept': 0,
        'prefetch_dropped': 0,
    }
    error_counts: dict[str, int] = {}
    rejected_domain_counts: dict[tuple[str, str], int] = {}
    rejected_allowlist_domain_counts: dict[str, int] = {}
    low_relevance_domain_counts: dict[str, int] = {}
    dropped_domain_reason_counts: dict[tuple[str, str], int] = {}
    candidates: list[dict[str, str]] = []
    inserted = 0
    used_cache = False
    mode = 'rss+authoritative+search'

    with _sqlite_connect(_db_path()) as connection:
        _ensure_backfill_linkage_schema(connection)
        max_row = connection.execute(
            '''
            SELECT MAX(COALESCE(published_at, ingested_at, retrieved_at))
            FROM sources
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        max_source_dt = _parse_iso(str(max_row[0] or '')) if max_row else None
        is_cold = max_source_dt is None or max_source_dt < recent_cutoff
        if not is_cold:
            return {'ran': False, 'is_cold': False, 'used_cache': False, 'inserted': 0, 'urls': [], 'telemetry': metrics}

        run_id = _insert_backfill_run_row(connection, actor_id=actor_id, started_at=now_iso, mode=mode)

        cache_row = _load_cache_row(connection, actor_id)
        if cache_row is not None:
            cache_dt = _parse_iso(str(cache_row.get('queried_at') or ''))
            if cache_dt is not None and cache_dt >= cache_cutoff:
                used_cache = True
                cached_candidates: list[dict[str, str]] = []
                for item in (cache_row.get('result_urls') if isinstance(cache_row.get('result_urls'), list) else []):
                    candidate = _candidate_from_url(
                        url_value=str(item),
                        source_type='search',
                        source_label='cache',
                    )
                    if candidate is None:
                        _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                        continue
                    cached_candidates.append(candidate)
                candidates = cached_candidates

        if not candidates:
            candidates.extend(
                _provider_rss_candidates(
                    actor_terms=actor_terms,
                    http_get=_http_get,
                    deadline_ts=deadline_ts,
                    metrics=metrics,
                    error_counts=error_counts,
                    rejected_domain_counts=rejected_domain_counts,
                    rejected_allowlist_domain_counts=rejected_allowlist_domain_counts,
                    low_relevance_domain_counts=low_relevance_domain_counts,
                    dropped_domain_reason_counts=dropped_domain_reason_counts,
                )
            )
            candidates.extend(
                _provider_authoritative_candidates(
                    actor_terms=actor_terms,
                    build_actor_profile_from_mitre=_build_actor_profile_from_mitre,
                    http_get=_http_get,
                    deadline_ts=deadline_ts,
                    metrics=metrics,
                    error_counts=error_counts,
                    rejected_domain_counts=rejected_domain_counts,
                    rejected_allowlist_domain_counts=rejected_allowlist_domain_counts,
                )
            )
            if len(candidates) < 4:
                if callable(_search_candidates):
                    found = _search_candidates(actor_terms, PRIMARY_BACKFILL_DOMAINS + FALLBACK_BACKFILL_DOMAINS)
                    for item in found:
                        candidate = _candidate_from_url(
                            url_value=str(item),
                            source_type='search',
                            source_label='custom',
                        )
                        if candidate is None:
                            _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                            continue
                        candidates.append(candidate)
                    metrics['queries_attempted'] = int(metrics.get('queries_attempted', 0)) + 1
                    metrics['candidates_found'] = int(metrics.get('candidates_found', 0)) + len(found)
                else:
                    candidates.extend(
                        _provider_search_candidates(
                            actor_terms=actor_terms,
                            domains=PRIMARY_BACKFILL_DOMAINS + FALLBACK_BACKFILL_DOMAINS,
                            http_get=_http_get,
                            deadline_ts=deadline_ts,
                            metrics=metrics,
                            error_counts=error_counts,
                            rejected_domain_counts=rejected_domain_counts,
                            rejected_allowlist_domain_counts=rejected_allowlist_domain_counts,
                            query_budget=BACKFILL_QUERY_BUDGET,
                        )
                    )

        deduped_candidates: list[dict[str, str]] = []
        seen_candidates: set[str] = set()
        for candidate in candidates:
            canonical = _canonicalize_url(str(candidate.get('candidate_url') or ''))
            if not canonical:
                _record_error(error_counts, FAIL_CANDIDATE_INVALID_URL)
                continue
            if _url_host(canonical) == 'attack.mitre.org' and not _is_mitre_structured_url(canonical):
                _record_error(error_counts, FAIL_CANDIDATE_LOW_RELEVANCE)
                _record_domain_count(low_relevance_domain_counts, url_value=canonical)
                _record_domain_reason_count(
                    dropped_domain_reason_counts,
                    url_value=canonical,
                    reason='mitre_non_structured',
                )
                metrics['prefetch_dropped'] = int(metrics.get('prefetch_dropped', 0)) + 1
                continue
            if not _is_allowed(canonical):
                _record_error(error_counts, FAIL_ALLOWLIST)
                _record_rejected_domain(rejected_domain_counts, url_value=canonical)
                _record_domain_count(rejected_allowlist_domain_counts, url_value=canonical)
                continue
            if canonical in seen_candidates:
                continue
            seen_candidates.add(canonical)
            deduped_candidates.append(
                {
                    'candidate_url': canonical,
                    'candidate_registrable_domain': _registrable_domain(_url_host(canonical)) or 'unknown',
                    'candidate_source': str(candidate.get('candidate_source') or 'search'),
                }
            )
        candidates = deduped_candidates[:20]
        metrics['prefetch_kept'] = int(max(int(metrics.get('prefetch_kept', 0)), len(candidates)))

        existing_urls = {
            _canonicalize_url(str(row[0] or ''))
            for row in connection.execute('SELECT url FROM sources WHERE actor_id = ?', (actor_id,)).fetchall()
            if _canonicalize_url(str(row[0] or ''))
        }

        candidate_index = 0
        while candidate_index < len(candidates):
            candidate = candidates[candidate_index]
            candidate_index += 1
            if time.perf_counter() >= deadline_ts:
                break
            canonical = _canonicalize_url(str(candidate.get('candidate_url') or ''))
            if not canonical or canonical in existing_urls:
                continue
            metrics['pages_fetched'] = int(metrics.get('pages_fetched', 0)) + 1
            try:
                timeout_value = max(2.0, min(BACKFILL_FETCH_TIMEOUT_SECONDS, deadline_ts - time.perf_counter()))
                derived = _derive_source_from_url(
                    canonical,
                    fallback_source_name=(urlparse(canonical).hostname or ''),
                    published_hint=None,
                    fetch_timeout_seconds=timeout_value,
                )
            except Exception as exc:
                _record_error(error_counts, _classify_error(exc))
                continue
            if not isinstance(derived, dict):
                _record_error(error_counts, FAIL_PARSE)
                continue
            final_url = _canonicalize_url(str(derived.get('source_url') or canonical))
            if final_url and not _is_allowed(final_url):
                _record_error(error_counts, FAIL_ALLOWLIST)
                _record_rejected_domain(rejected_domain_counts, url_value=final_url)
                continue
            source_text = str(derived.get('pasted_text') or '').strip()
            if len(source_text) < 120:
                _record_error(error_counts, FAIL_NO_TEXT)
                continue
            title_text = str(derived.get('title') or derived.get('headline') or '')
            summary_text = str(derived.get('trigger_excerpt') or '')
            linkage = _score_linkage(
                actor_terms=actor_terms,
                context_terms=context_terms,
                title_text=title_text,
                summary_text=summary_text,
                source_text=source_text,
                final_url=final_url or canonical,
            )
            match_score = int(linkage.get('score') or 0)
            match_reasons = [str(item) for item in linkage.get('reasons', []) if str(item).strip()]
            matched_terms = [str(item) for item in linkage.get('matched_terms', []) if str(item).strip()]
            if match_score < MATCH_THRESHOLD:
                _record_error(error_counts, FAIL_SCORE)
                continue
            published_at = str(derived.get('published_at') or '').strip() or None
            if not published_at:
                _record_error(error_counts, FAIL_NO_DATE)
            source_name = str(derived.get('source_name') or urlparse(canonical).hostname or 'web').strip()
            source_id = _upsert_source_for_actor(
                connection=connection,
                actor_id=actor_id,
                source_name=source_name,
                source_url=final_url or canonical,
                published_at=published_at,
                pasted_text=source_text,
                trigger_excerpt=None,
                title=str(derived.get('title') or '').strip() or None,
                headline=str(derived.get('headline') or '').strip() or None,
                og_title=str(derived.get('og_title') or '').strip() or None,
                html_title=str(derived.get('html_title') or '').strip() or None,
                publisher=str(derived.get('publisher') or '').strip() or None,
                site_name=str(derived.get('site_name') or '').strip() or None,
                source_tier=str(derived.get('source_tier') or '').strip() or None,
                confidence_weight=(
                    int(derived.get('confidence_weight') or 0)
                    if derived.get('confidence_weight') is not None
                    else None
                ),
                source_type='web_backfill',
            )
            _store_backfill_linkage(
                connection,
                actor_id=actor_id,
                source_id=str(source_id or '').strip() or None,
                source_url=(final_url or canonical),
                match_score=match_score,
                match_reasons=match_reasons,
                matched_terms=matched_terms,
                now_iso=now_iso,
            )
            tier_label = str(derived.get('source_tier') or '').strip().lower()
            trust_score = 0
            if tier_label == 'high':
                trust_score = 4
            elif tier_label == 'medium':
                trust_score = 3
            elif tier_label == 'trusted':
                trust_score = 2
            elif tier_label == 'context':
                trust_score = 1
            source_evidence_service.persist_source_evidence_core(
                connection,
                source_id=str(source_id),
                actor_id=actor_id,
                source_url=(final_url or canonical),
                source_text=source_text,
                raw_html=str(derived.get('raw_html') or ''),
                fetched_at=now_iso,
                published_at=published_at,
                http_status=(
                    int(derived.get('http_status'))
                    if str(derived.get('http_status') or '').strip().isdigit()
                    else None
                ),
                content_type=str(derived.get('content_type') or ''),
                parse_status=str(derived.get('parse_status') or 'parsed'),
                parse_error=str(derived.get('parse_error') or ''),
                actor_terms=actor_terms,
                relevance_score=min(1.0, max(0.0, float(match_score) / 8.0)),
                match_type='backfill_linkage',
                match_reasons=match_reasons,
                matched_terms=matched_terms,
                source_trust_score=trust_score,
                novelty_score=0.6,
                extractor='web_backfill_v2',
            )
            metrics['pages_parsed_ok'] = int(metrics.get('pages_parsed_ok', 0)) + 1
            inserted += 1
            existing_urls.add(canonical)

        metrics['sources_inserted'] = inserted
        _store_cache_row(
            connection,
            actor_id=actor_id,
            queried_at=now_iso,
            result_urls=[str(item.get('candidate_url') or '') for item in candidates if str(item.get('candidate_url') or '').strip()],
            inserted_count=inserted,
        )
        _finish_backfill_run_row(
            connection,
            run_id=run_id,
            finished_at=_utc_now_iso(),
            metrics=metrics,
            error_counts=error_counts,
            rejected_domain_counts=rejected_domain_counts,
            rejected_allowlist_domain_counts=rejected_allowlist_domain_counts,
            low_relevance_domain_counts=low_relevance_domain_counts,
            dropped_domain_reason_counts=dropped_domain_reason_counts,
        )
        connection.commit()

    top_error_reason = ''
    if error_counts:
        top_error_reason = sorted(error_counts.items(), key=lambda item: int(item[1]), reverse=True)[0][0]
    return {
        'ran': True,
        'is_cold': True,
        'used_cache': used_cache,
        'inserted': inserted,
        'urls': [str(item.get('candidate_url') or '') for item in candidates if str(item.get('candidate_url') or '').strip()],
        'candidates': candidates,
        'telemetry': metrics,
        'error_counts': error_counts,
        'dropped_domains': _summarize_domain_reason_counts(dropped_domain_reason_counts, limit=5),
        'top_error_reason': top_error_reason,
    }
