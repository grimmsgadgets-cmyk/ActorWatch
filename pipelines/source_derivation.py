import html
import re
from typing import Callable
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import HTTPException


def strip_html(value: str) -> str:
    value = re.sub(r'<script[\s\S]*?</script>', ' ', value, flags=re.IGNORECASE)
    value = re.sub(r'<style[\s\S]*?</style>', ' ', value, flags=re.IGNORECASE)
    value = re.sub(r'<[^>]+>', ' ', value)
    value = html.unescape(value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value


def extract_meta(content: str, key_patterns: list[str]) -> str | None:
    for pattern in key_patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE)
        if match:
            return html.unescape(match.group(1)).strip()
    return None


def fallback_title_from_url(source_url: str) -> str:
    _ = source_url
    return 'Untitled article'


def evidence_title_from_source(
    source: dict[str, object],
    *,
    split_sentences: Callable[[str], list[str]],
    fallback_title: Callable[[str], str],
) -> str:
    for key in ('title', 'headline', 'og_title', 'html_title'):
        value = str(source.get(key) or '').strip()
        if value:
            if value.startswith(('http://', 'https://')) or (value.count('/') >= 2 and ' ' not in value):
                continue
            return value
    pasted_text = str(source.get('pasted_text') or '').strip()
    if pasted_text:
        split = split_sentences(pasted_text)
        first_sentence = split[0] if split else pasted_text
        first_sentence = ' '.join(first_sentence.split()).strip()
        if (
            first_sentence
            and not first_sentence.lower().startswith('actor-matched feed item from')
            and not first_sentence.startswith(('http://', 'https://'))
            and not (first_sentence.count('/') >= 2 and ' ' not in first_sentence)
        ):
            return first_sentence[:120]
    return fallback_title(str(source.get('url') or ''))


def evidence_source_label_from_source(
    source: dict[str, object],
    *,
    evidence_title: Callable[[dict[str, object]], str],
) -> str:
    source_url = str(source.get('url') or '').strip()
    parsed_source = urlparse(source_url)
    source_host = (parsed_source.netloc or '').lower()
    if source_host.endswith('news.google.com'):
        title_hint = evidence_title(source)
        if ' - ' in title_hint:
            publisher_hint = title_hint.rsplit(' - ', 1)[-1].strip()
            if publisher_hint and publisher_hint.lower() not in {'google news', 'news'}:
                return publisher_hint
    for key in ('publisher', 'site_name'):
        value = str(source.get(key) or '').strip()
        if value:
            return value
    parsed = urlparse(source_url)
    hostname = (parsed.netloc or '').strip()
    if hostname:
        return hostname
    return str(source.get('source_name') or 'Unknown source').strip() or 'Unknown source'


def canonical_group_domain(
    source: dict[str, object],
    *,
    evidence_source_label: Callable[[dict[str, object]], str],
) -> str:
    source_url = str(source.get('url') or '').strip()
    parsed = urlparse(source_url)
    host = (parsed.netloc or '').lower()
    if host.endswith('news.google.com'):
        query_params = parse_qs(parsed.query)
        for key in ('url', 'u', 'q'):
            candidate = str((query_params.get(key) or [''])[0]).strip()
            if not candidate.startswith(('http://', 'https://')):
                continue
            candidate_host = (urlparse(candidate).netloc or '').lower()
            if candidate_host and not candidate_host.endswith('news.google.com'):
                return candidate_host
        source_label = evidence_source_label(source)
        source_label_lower = source_label.lower().strip()
        if re.match(r'^[a-z0-9.-]+\.[a-z]{2,}$', source_label_lower):
            return source_label_lower
        normalized = re.sub(r'[^a-z0-9]+', '-', source_label_lower).strip('-')
        if normalized:
            return f'publisher:{normalized}'
    return host or 'unknown-source'


def _extract_structured_blocks(content: str, *, host: str) -> tuple[list[str], str]:
    body = str(content or '')
    lowered_host = str(host or '').strip('.').lower()
    parse_status = 'parsed'
    containers: list[str] = []
    if lowered_host == 'attack.mitre.org':
        parse_status = 'parsed_structured_mitre'
        containers = re.findall(r'<main[^>]*>(.*?)</main>', body, flags=re.IGNORECASE | re.DOTALL)
        if not containers:
            containers = re.findall(r'<article[^>]*>(.*?)</article>', body, flags=re.IGNORECASE | re.DOTALL)
    elif lowered_host.endswith('cisa.gov'):
        parse_status = 'parsed_structured_cisa'
        containers = re.findall(r'<main[^>]*>(.*?)</main>', body, flags=re.IGNORECASE | re.DOTALL)
        if not containers:
            containers = re.findall(r'<article[^>]*>(.*?)</article>', body, flags=re.IGNORECASE | re.DOTALL)
    else:
        return ([], parse_status)

    if not containers:
        return ([], parse_status)

    scoped = ' '.join(containers[:2])
    paragraph_matches = re.findall(r'<p[^>]*>(.*?)</p>', scoped, flags=re.IGNORECASE | re.DOTALL)
    list_matches = re.findall(r'<li[^>]*>(.*?)</li>', scoped, flags=re.IGNORECASE | re.DOTALL)
    heading_matches = re.findall(r'<h[12][^>]*>(.*?)</h[12]>', scoped, flags=re.IGNORECASE | re.DOTALL)
    cleaned = [
        strip_html(part)
        for part in (heading_matches[:4] + paragraph_matches[:20] + list_matches[:20])
    ]
    cleaned = [part for part in cleaned if len(part) >= 25]
    return (cleaned, parse_status)


def derive_source_from_url_core(
    source_url: str,
    *,
    fallback_source_name: str | None = None,
    published_hint: str | None = None,
    fetch_timeout_seconds: float = 20.0,
    deps: dict[str, object],
) -> dict[str, str | None]:
    _safe_http_get = deps['safe_http_get']
    _extract_question_sentences = deps['extract_question_sentences']
    _first_sentences = deps['first_sentences']

    try:
        response = _safe_http_get(source_url, timeout=max(1.0, float(fetch_timeout_seconds)))
        response.raise_for_status()
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=400, detail=f'failed to fetch source URL: {exc}') from exc

    content = response.text
    parsed = urlparse(str(response.url))
    domain = parsed.netloc or 'unknown'
    host = (parsed.hostname or '').strip('.').lower()

    site_name = extract_meta(
        content,
        [
            r'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']application-name["\'][^>]+content=["\']([^"\']+)["\']',
        ],
    )
    publisher = extract_meta(
        content,
        [
            r'<meta[^>]+property=["\']article:publisher["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']publisher["\'][^>]+content=["\']([^"\']+)["\']',
        ],
    )
    source_name = site_name or fallback_source_name or domain

    og_title = extract_meta(
        content,
        [
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        ],
    )
    html_title = extract_meta(
        content,
        [
            r'<title[^>]*>([^<]+)</title>',
        ],
    )
    headline = extract_meta(
        content,
        [
            r'<meta[^>]+name=["\']headline["\'][^>]+content=["\']([^"\']+)["\']',
            r'<h1[^>]*>([^<]+)</h1>',
        ],
    )
    title = (
        extract_meta(
            content,
            [
                r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
            ],
        )
        or headline
        or og_title
        or html_title
    )

    published_at = extract_meta(
        content,
        [
            r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']pubdate["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+name=["\']date["\'][^>]+content=["\']([^"\']+)["\']',
            r'<time[^>]+datetime=["\']([^"\']+)["\']',
        ],
    )
    if not published_at:
        published_at = published_hint
    if not published_at and (parsed.hostname or '').strip('.').lower() == 'attack.mitre.org':
        published_at = str(response.headers.get('Last-Modified') or '').strip() or None

    structured_blocks, parse_status = _extract_structured_blocks(content, host=host)
    paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', content, flags=re.IGNORECASE | re.DOTALL)
    cleaned_paragraphs = [strip_html(paragraph) for paragraph in paragraphs]
    cleaned_paragraphs = [paragraph for paragraph in cleaned_paragraphs if len(paragraph) > 40]
    if structured_blocks:
        cleaned_paragraphs = structured_blocks + cleaned_paragraphs

    if cleaned_paragraphs:
        pasted_text = ' '.join(cleaned_paragraphs[:10])
    else:
        pasted_text = strip_html(content)[:5000]

    if title and title not in pasted_text:
        pasted_text = f'{title}. {pasted_text}'

    if len(pasted_text) < 80:
        raise HTTPException(status_code=400, detail='unable to derive sufficient text from source URL')

    excerpts = _extract_question_sentences(pasted_text)
    trigger_excerpt = excerpts[0] if excerpts else _first_sentences(pasted_text, count=1)

    return {
        'source_name': source_name,
        'site_name': site_name,
        'publisher': publisher,
        'title': title,
        'headline': headline,
        'og_title': og_title,
        'html_title': html_title,
        'source_url': str(response.url),
        'published_at': published_at,
        'pasted_text': pasted_text,
        'trigger_excerpt': trigger_excerpt,
        'raw_html': content,
        'http_status': int(response.status_code),
        'content_type': str(response.headers.get('content-type') or ''),
        'parse_status': parse_status,
        'parse_error': '',
    }
