import html
import re
from urllib.parse import quote_plus, urlparse


def actor_terms_core(actor_name: str, mitre_group_name: str, aliases_csv: str, *, deps: dict[str, object]) -> list[str]:
    _dedupe_actor_terms = deps['dedupe_actor_terms']

    raw_terms = [actor_name, mitre_group_name] + aliases_csv.split(',')
    generic_terms = {
        'apt',
        'group',
        'team',
        'actor',
        'threat actor',
        'intrusion set',
        'cluster',
    }
    terms: list[str] = []
    for raw in raw_terms:
        value = raw.strip().lower()
        if len(value) < 3:
            continue
        if value in generic_terms:
            continue
        if value not in terms:
            terms.append(value)
    return _dedupe_actor_terms(terms)


def text_contains_actor_term_core(text: str, actor_terms: list[str], *, deps: dict[str, object]) -> bool:
    _sentence_mentions_actor_terms = deps['sentence_mentions_actor_terms']
    return _sentence_mentions_actor_terms(text, actor_terms)


def actor_query_feeds_core(actor_terms: list[str]) -> list[tuple[str, str]]:
    feeds: list[tuple[str, str]] = []
    added: set[str] = set()
    for term in actor_terms:
        compact = term.strip()
        if len(compact) < 3 or len(compact) > 40:
            continue
        if compact in added:
            continue
        added.add(compact)
        q = quote_plus(f'"{compact}" cybersecurity OR ransomware OR threat actor')
        feeds.append(('Google News Actor Query', f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en'))
        if len(feeds) >= 3:
            break
    return feeds


def actor_search_queries_core(actor_terms: list[str]) -> list[str]:
    queries: list[str] = []
    for term in actor_terms:
        compact = term.strip()
        if len(compact) < 3 or len(compact) > 60:
            continue
        queries.extend(
            [
                f'"{compact}" threat intelligence report',
                f'"{compact}" malware analysis',
                f'"{compact}" ransomware attack',
            ]
        )
        if len(queries) >= 9:
            break
    return queries[:9]


def domain_allowed_for_actor_search_core(url: str, *, domains: list[str]) -> bool:
    try:
        hostname = (urlparse(url).hostname or '').strip('.').lower()
    except Exception:
        return False
    if not hostname:
        return False
    return any(hostname == domain or hostname.endswith(f'.{domain}') for domain in domains)


def duckduckgo_actor_search_urls_core(actor_terms: list[str], *, limit: int, deps: dict[str, object]) -> list[str]:
    _actor_search_queries = deps['actor_search_queries']
    _http_get = deps['http_get']
    _domain_allowed_for_actor_search = deps['domain_allowed_for_actor_search']

    urls: list[str] = []
    seen: set[str] = set()
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        )
    }
    for query in _actor_search_queries(actor_terms):
        search_url = f'https://html.duckduckgo.com/html/?q={quote_plus(query)}'
        try:
            response = _http_get(search_url, timeout=20.0, follow_redirects=True, headers=headers)
            if response.status_code != 200:
                continue
            body = response.text
        except Exception:
            continue

        # Two-pass extraction: find all <a> tags that carry the result__a class,
        # then pull href from each tag. This handles any attribute ordering
        # (DDG renders both class-before-href and href-before-class).
        for tag_match in re.finditer(r'<a\b([^>]*)>', body):
            attrs = tag_match.group(1)
            if 'result__a' not in attrs:
                continue
            href_match = re.search(r'\bhref="([^"]+)"', attrs)
            if not href_match:
                continue
            candidate = html.unescape(href_match.group(1)).strip()
            if not candidate.startswith('http'):
                continue
            if candidate in seen:
                continue
            if not _domain_allowed_for_actor_search(candidate):
                continue
            seen.add(candidate)
            urls.append(candidate)
            if len(urls) >= limit:
                return urls
    return urls
