import sqlite3
import re
from datetime import datetime, timedelta, timezone
from typing import Callable

from fastapi import HTTPException
import services.ioc_store_service as ioc_store_service
import services.ioc_validation_service as ioc_validation_service


def _quick_check_ioc_type_hints(text: str) -> set[str]:
    normalized = str(text or '').lower()
    hints: set[str] = set()
    matched_specific_signal = False

    def _contains(pattern: str) -> bool:
        return bool(re.search(pattern, normalized))

    if _contains(r'\b(dns|domain|domains|fqdn|host|url|uri|proxy)\b'):
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
    relevant: list[tuple[int, dict[str, str]]] = []
    for item in ioc_items:
        ioc_type = str(item.get('ioc_type') or '').strip().lower()
        ioc_value = str(item.get('ioc_value') or '').strip().lower()
        if not ioc_type or not ioc_value:
            continue

        score = 0
        if ioc_type in hinted_types:
            score += 3
        if ioc_value in card_text:
            score += 5
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
        return []
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
    for value in re.findall(r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b', raw_without_urls):
        if value.lower().startswith('http'):
            continue
        normalized = value.lower()
        if normalized in ignored:
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
    for source in source_items:
        if not _looks_ioc_capable(source):
            continue
        source_name = str(source.get('source_name') or '').strip()
        source_url = str(source.get('url') or '').strip()
        source_ref = source_name or source_url
        source_text = str(source.get('pasted_text') or '')
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
                }
            )
            if len(derived) >= max_items:
                return derived
    return derived


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


def build_environment_checks(
    timeline_recent_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    top_techniques: list[dict[str, str]],
    *,
    recency_label: str,
) -> list[dict[str, str]]:
    categories = {str(item.get('category') or '').lower() for item in timeline_recent_items}
    text_blob = ' '.join(
        [str(item.get('title') or '') for item in timeline_recent_items]
        + [str(item.get('summary') or '') for item in timeline_recent_items]
        + [str(item.get('text') or '') for item in recent_activity_highlights]
    ).lower()
    recent_ttps: set[str] = set()
    for item in timeline_recent_items:
        for ttp in item.get('ttp_ids', []):
            token = str(ttp or '').upper().strip()
            if token:
                recent_ttps.add(token)
    for item in recent_activity_highlights:
        csv_ids = str(item.get('ttp_ids') or '')
        for token in csv_ids.split(','):
            token_norm = token.strip().upper()
            if token_norm:
                recent_ttps.add(token_norm)
    known_ttps = {
        str(item.get('technique_id') or '').upper().strip()
        for item in top_techniques
        if str(item.get('technique_id') or '').strip()
    }
    source_ids = {
        str(item.get('source_id') or '').strip()
        for item in timeline_recent_items
        if str(item.get('source_id') or '').strip()
    }
    source_urls = {
        str(item.get('source_url') or '').strip()
        for item in recent_activity_highlights
        if str(item.get('source_url') or '').strip()
    }
    source_count = len(source_ids | source_urls)

    theme_defs = [
        {
            'id': 'remote_access',
            'check': 'Unusual remote access and edge logins',
            'primary_area': 'Firewall/VPN',
            'short_cue': 'Look for unusual remote logins and edge access activity',
            'where': 'Firewall/VPN logs, identity sign-in logs, EDR',
            'look_for': 'Repeated failed VPN logins followed by success; sign-ins from new geographies or devices.',
            'why': 'Recent reporting links this actor to external-access paths before follow-on activity.',
            'keyword_tags': ['vpn', 'edge', 'remote access', 'login', 'external authentication', 'exploit'],
            'category_tags': {'initial_access', 'lateral_movement', 'command_and_control'},
            'ttp_tags': {'T1133', 'T1078', 'T1190'},
        },
        {
            'id': 'endpoint_activity',
            'check': 'Suspicious endpoint command activity',
            'primary_area': 'Endpoint',
            'short_cue': 'Look for unusual script execution and startup persistence changes',
            'where': 'EDR, Windows Event Logs, PowerShell logs',
            'look_for': 'PowerShell or command shell launched by unusual parent processes; new scheduled tasks or startup entries.',
            'why': 'Recent actor-linked behavior includes host execution and persistence techniques.',
            'keyword_tags': ['powershell', 'cmd.exe', 'wmi', 'scheduled task', 'execution', 'persistence'],
            'category_tags': {'execution', 'persistence', 'defense_evasion'},
            'ttp_tags': {'T1059', 'T1547', 'T1053'},
        },
        {
            'id': 'early_impact',
            'check': 'Early signs of data theft or disruption',
            'primary_area': 'DNS/Proxy',
            'short_cue': 'Look for early data movement and disruptive file behavior',
            'where': 'DNS/Proxy logs, EDR file activity, storage/backup audit logs',
            'look_for': 'Large outbound transfers to new domains; unusual mass file changes or rapid archive creation.',
            'why': 'Recent reporting references ransomware and data-theft style outcomes tied to this actor.',
            'keyword_tags': ['ransom', 'data theft', 'exfil', 'encrypt', 'disrupt', 'leak'],
            'category_tags': {'exfiltration', 'impact', 'command_and_control'},
            'ttp_tags': {'T1041', 'T1486', 'T1567'},
        },
    ]

    candidates: list[dict[str, object]] = []
    for theme in theme_defs:
        matched_tags: list[str] = []
        score = 0

        for cat in theme['category_tags']:
            if cat in categories:
                score += 2
                matched_tags.append(cat.replace('_', ' '))

        for keyword in theme['keyword_tags']:
            if keyword in text_blob:
                score += 1
                if keyword not in matched_tags:
                    matched_tags.append(keyword)

        ttp_hits = (recent_ttps | known_ttps).intersection(theme['ttp_tags'])
        if ttp_hits:
            score += 2
            for ttp in sorted(ttp_hits):
                if ttp not in matched_tags:
                    matched_tags.append(ttp)

        if score > 0:
            based_tags = ', '.join(matched_tags[:3]) if matched_tags else 'actor activity evidence'
            source_label = f'{source_count} sources' if source_count > 0 else 'limited source coverage'
            candidates.append(
                {
                    'score': score,
                    'primary_where': str(theme['where']).split(',')[0].strip().lower(),
                    'card': {
                        'check': str(theme['check']),
                        'primary_area': str(theme['primary_area']),
                        'short_cue': str(theme['short_cue']),
                        'where_to_look': str(theme['where']),
                        'what_to_look_for': str(theme['look_for']),
                        'why_this_matters': str(theme['why']),
                        'based_on': f'Based on: {based_tags} mentioned in {source_label} ({recency_label}).',
                    },
                }
            )

    if not candidates:
        return [
            {
                'check': 'Start with unusual remote access and logins',
                'primary_area': 'Firewall/VPN',
                'short_cue': 'Start with unusual remote access and login patterns',
                'where_to_look': 'Firewall/VPN logs, identity sign-in logs',
                'what_to_look_for': 'Repeated failed logins followed by success; sign-ins from new geographies or devices.',
                'why_this_matters': 'This gives a reliable first pass when recent actor reporting is limited or ambiguous.',
                'based_on': 'Based on: limited recent reporting.',
            }
        ]

    deduped: list[dict[str, str]] = []
    seen_primary_where: set[str] = set()
    for candidate in sorted(candidates, key=lambda item: int(item['score']), reverse=True):
        primary_where = str(candidate['primary_where'])
        if primary_where in seen_primary_where:
            continue
        seen_primary_where.add(primary_where)
        deduped.append(candidate['card'])  # type: ignore[arg-type]
        if len(deduped) >= 3:
            break

    return deduped[:3]


def recent_change_summary(
    timeline_recent_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    source_items: list[dict[str, object]],
) -> dict[str, str]:
    new_reports = len({str(item.get('source_id') or '') for item in timeline_recent_items if str(item.get('source_id') or '').strip()})
    source_by_id = {str(item.get('id') or ''): item for item in source_items}
    related_source_ids = {str(item.get('source_id') or '').strip() for item in timeline_recent_items if str(item.get('source_id') or '').strip()}

    industry_markers: dict[str, tuple[str, ...]] = {
        'Healthcare': ('healthcare', 'hospital', 'clinic', 'medical', 'patient'),
        'Government': ('government', 'public sector', 'ministry', 'state agency', 'municipal'),
        'Financial services': ('bank', 'financial', 'credit union', 'insurance', 'fintech'),
        'Technology': ('technology', 'software', 'saas', 'cloud provider', 'it services'),
        'Manufacturing': ('manufacturing', 'industrial', 'factory', 'automotive', 'semiconductor'),
        'Energy': ('energy', 'oil', 'gas', 'utility', 'power grid'),
        'Telecom': ('telecom', 'telecommunications', 'mobile operator', 'isp', 'broadband'),
        'Education': ('education', 'university', 'school', 'college', 'academic'),
        'Retail': ('retail', 'ecommerce', 'merchant', 'point of sale', 'consumer brand'),
        'Transportation': ('transportation', 'logistics', 'shipping', 'aviation', 'rail'),
        'Defense': ('defense', 'military', 'aerospace', 'armed forces', 'national security'),
    }
    industry_scores = {name: 0 for name in industry_markers}
    for item in recent_activity_highlights:
        target = str(item.get('target_text') or '').strip().lower()
        text = str(item.get('text') or '').strip().lower()
        joined = f'{target} {text}'
        if not joined.strip():
            continue
        for industry, markers in industry_markers.items():
            if any(marker in joined for marker in markers):
                industry_scores[industry] += 1
    for source_id in related_source_ids:
        source = source_by_id.get(source_id)
        if not source:
            continue
        text = str(source.get('pasted_text') or '').lower()
        if not text:
            continue
        for industry, markers in industry_markers.items():
            if any(marker in text for marker in markers):
                industry_scores[industry] += 1

    top_industries = [name for name, score in sorted(industry_scores.items(), key=lambda x: x[1], reverse=True) if score > 0][:3]
    if top_industries:
        targets_text = ', '.join(top_industries)
    else:
        explicit_targets: list[str] = []
        for item in recent_activity_highlights:
            target = str(item.get('target_text') or '').strip()
            if target and target not in explicit_targets:
                explicit_targets.append(target)
            if len(explicit_targets) >= 3:
                break
        targets_text = ', '.join(explicit_targets) if explicit_targets else 'Not clear yet'

    damage_markers: dict[str, tuple[str, ...]] = {
        'data theft': ('exfil', 'data theft', 'stolen data', 'data leak', 'data breach'),
        'ransomware/extortion': ('ransom', 'extortion', 'encrypt', 'lockbit', 'leak site'),
        'service disruption': ('outage', 'disrupt', 'downtime', 'service interruption', 'unavailable'),
        'credential theft/account abuse': ('credential theft', 'password spray', 'account takeover', 'stolen credential'),
        'destructive impact': ('wiper', 'data destruction', 'sabotage', 'destructive'),
    }
    damage_scores = {name: 0 for name in damage_markers}
    summary_blob = ' '.join(str(item.get('summary') or '') for item in timeline_recent_items).lower()
    highlight_blob = ' '.join(str(item.get('text') or '') for item in recent_activity_highlights).lower()
    source_blob_parts: list[str] = []
    for source_id in related_source_ids:
        source = source_by_id.get(source_id)
        if not source:
            continue
        pasted = str(source.get('pasted_text') or '').strip()
        if pasted:
            source_blob_parts.append(pasted)
    source_blob = ' '.join(source_blob_parts).lower()
    damage_text = f'{summary_blob}\n{highlight_blob}\n{source_blob}'
    for damage_type, markers in damage_markers.items():
        damage_scores[damage_type] = sum(damage_text.count(marker) for marker in markers)
    top_damage = [name for name, score in sorted(damage_scores.items(), key=lambda x: x[1], reverse=True) if score > 0][:2]
    if len(top_damage) >= 2:
        damage = f'{top_damage[0].capitalize()} and {top_damage[1]}'
    elif len(top_damage) == 1:
        damage = top_damage[0].capitalize()
    else:
        damage = 'No clear damage outcome reported yet'

    return {
        'new_reports': str(new_reports),
        'targets': targets_text,
        'damage': damage,
    }


def build_top_change_signals(
    recent_activity_highlights: list[dict[str, object]],
    *,
    limit: int = 3,
) -> list[dict[str, object]]:
    def _norm_text(value: object) -> str:
        return ' '.join(str(value or '').strip().lower().split())

    def _is_generic_noise(item: dict[str, object]) -> bool:
        title = _norm_text(item.get('evidence_title'))
        text = _norm_text(item.get('text'))
        source_label = _norm_text(item.get('evidence_source_label') or item.get('source_name'))
        category = _norm_text(item.get('category'))
        has_structured_signal = bool(str(item.get('ttp_ids') or '').strip() or str(item.get('target_text') or '').strip())
        combined = f'{title} {text}'
        if (
            'victim disclosures in the last 90 days' in combined
            or 'total public victim disclosures' in combined
            or 'activity synthesis' in combined
            or 'trend for' in combined
            or 'tempo, geography' in combined
        ):
            return True
        if (
            ('ransomware.live' in source_label or 'ransomware.live' in _norm_text(item.get('source_url')))
            and category in {'impact', 'activity synthesis'}
            and not has_structured_signal
        ):
            return True
        return False

    def _int_value(value: object) -> int:
        try:
            return int(str(value or '0').strip() or '0')
        except Exception:
            return 0

    def _is_change_signal(item: dict[str, object]) -> bool:
        if _is_generic_noise(item):
            return False
        ttp_csv = str(item.get('ttp_ids') or '').strip()
        if ttp_csv:
            return True
        target_text = str(item.get('target_text') or '').strip()
        if target_text:
            return True
        if _int_value(item.get('corroboration_sources')) >= 2:
            return True
        text = str(item.get('text') or '').strip().lower()
        if any(
            token in text
            for token in (
                'first observed',
                'newly observed',
                'new campaign',
                'shift',
                'pivot',
                'lateral movement',
                'beacon',
                'c2',
                'command and control',
                'exploit',
                'ransom',
                'encrypt',
            )
        ):
            return True
        return False

    def _signal_score(item: dict[str, object]) -> int:
        score = _int_value(item.get('corroboration_sources'))
        if str(item.get('ttp_ids') or '').strip():
            score += 3
        if str(item.get('target_text') or '').strip():
            score += 2
        if str(item.get('category') or '').strip().lower() not in {'', 'activity synthesis'}:
            score += 1
        return score

    if limit <= 0:
        return []

    filtered = [
        item
        for item in recent_activity_highlights
        if isinstance(item, dict) and _is_change_signal(item)
    ]
    filtered.sort(key=_signal_score, reverse=True)

    picked: list[dict[str, object]] = []
    seen_fingerprints: set[str] = set()
    seen_domains: set[str] = set()
    for item in filtered:
        fingerprint = (
            _norm_text(item.get('source_url'))
            + '|'
            + _norm_text(item.get('evidence_title'))
            + '|'
            + _norm_text(item.get('text'))
        )
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        domain_key = str(
            item.get('evidence_group_domain')
            or item.get('evidence_source_label')
            or item.get('source_name')
            or 'unknown'
        ).strip().lower()
        if domain_key in seen_domains:
            continue
        seen_domains.add(domain_key)
        picked.append(item)
        if len(picked) >= limit:
            break

    return picked[:limit]


def build_recent_activity_highlights(
    timeline_items: list[dict[str, object]],
    sources: list[dict[str, object]],
    actor_terms: list[str],
    *,
    trusted_activity_domains: set[str],
    source_domain: Callable[[str], str],
    canonical_group_domain: Callable[[dict[str, object]], str],
    looks_like_activity_sentence: Callable[[str], bool],
    sentence_mentions_actor_terms: Callable[[str, list[str]], bool],
    text_contains_actor_term: Callable[[str, list[str]], bool],
    normalize_text: Callable[[str], str],
    parse_published_datetime: Callable[[str], datetime | None],
    freshness_badge: Callable[[str | None], tuple[str, str]],
    evidence_title_from_source: Callable[[dict[str, object] | None], str],
    fallback_title_from_url: Callable[[str], str],
    evidence_source_label_from_source: Callable[[dict[str, object] | None], str],
    extract_ttp_ids: Callable[[str], list[str]],
    split_sentences: Callable[[str], list[str]],
    looks_like_navigation_noise: Callable[[str], bool],
    source_trust_score: Callable[[str], int] | None = None,
) -> list[dict[str, str | None]]:
    def _host_from_domain(domain: str) -> str:
        return (domain or '').strip().split(':', 1)[0].strip('.').lower()

    def _domain_matches_trusted(domain: str) -> bool:
        host = _host_from_domain(domain)
        if not host:
            return False
        for trusted in trusted_activity_domains:
            candidate = _host_from_domain(str(trusted))
            if not candidate:
                continue
            if host == candidate or host.endswith(f'.{candidate}'):
                return True
        return False

    def _is_trusted_domain(url: str) -> bool:
        domain = source_domain(url)
        return _domain_matches_trusted(domain)

    def _domain_trust_points(url: str) -> int:
        if callable(source_trust_score):
            try:
                return max(0, int(source_trust_score(url)))
            except Exception:
                return 0
        return 2 if _is_trusted_domain(url) else 0

    def _actor_specific_text(text: str, terms: list[str]) -> bool:
        return bool(text and terms and sentence_mentions_actor_terms(text, terms))

    def _candidate_signal_key(
        summary: str,
        category: str,
        target_text: str,
        ttp_values: list[str],
    ) -> str:
        normalized_summary = normalize_text(summary)
        key_summary = ' '.join(normalized_summary.split()[:14])
        key_target = normalize_text(target_text)
        key_ttps = ','.join(sorted(str(value).upper() for value in ttp_values[:4]))
        return f'{normalize_text(category)}|{key_target}|{key_ttps}|{key_summary}'

    def _recency_points(value: str | None) -> int:
        dt = parse_published_datetime(str(value or ''))
        if dt is None:
            return 0
        days_old = max(0, (datetime.now(timezone.utc) - dt).days)
        if days_old <= 1:
            return 4
        if days_old <= 7:
            return 3
        if days_old <= 30:
            return 2
        return 1

    source_by_id = {str(source['id']): source for source in sources}
    highlights: list[dict[str, str | None]] = []
    terms = [term.lower() for term in actor_terms if term]
    if not terms:
        return highlights

    candidates: list[dict[str, object]] = []
    for item in timeline_items:
        source = source_by_id.get(str(item['source_id']))
        summary = str(item.get('summary') or '')
        source_text = str(source.get('pasted_text') if source else '')
        source_url = str(source.get('url') if source else '')
        if not looks_like_activity_sentence(summary):
            continue
        if not (_actor_specific_text(summary, terms) or _actor_specific_text(source_text, terms)):
            continue
        if source_url and not _is_trusted_domain(source_url) and not _actor_specific_text(summary, terms):
            continue

        ttp_list = [str(t) for t in item.get('ttp_ids', [])]
        signal_key = _candidate_signal_key(
            summary,
            str(item.get('category') or ''),
            str(item.get('target_text') or ''),
            ttp_list,
        )
        candidates.append(
            {
                'item': item,
                'source': source,
                'source_url': source_url,
                'summary': summary,
                'signal_key': signal_key,
                'date_value': str(source.get('published_at') if source else '') or str(item.get('occurred_at') or ''),
            }
        )

    signal_domains: dict[str, set[str]] = {}
    for candidate in candidates:
        signal_key = str(candidate.get('signal_key') or '')
        source_obj = candidate.get('source')
        source_domain_value = (
            canonical_group_domain(source_obj)
            if isinstance(source_obj, dict)
            else source_domain(str(candidate.get('source_url') or ''))
        )
        if not signal_key or not source_domain_value:
            continue
        signal_domains.setdefault(signal_key, set()).add(source_domain_value)

    ranked_candidates: list[dict[str, object]] = []
    for candidate in candidates:
        signal_key = str(candidate.get('signal_key') or '')
        source_url = str(candidate.get('source_url') or '')
        corroboration_sources = len(signal_domains.get(signal_key, set()))
        score = _recency_points(str(candidate.get('date_value') or ''))
        score += _domain_trust_points(source_url)
        if corroboration_sources >= 3:
            score += 3
        elif corroboration_sources == 2:
            score += 2
        elif corroboration_sources == 1:
            score += 1

        date_dt = parse_published_datetime(str(candidate.get('date_value') or ''))
        ranked = dict(candidate)
        ranked['score'] = score
        ranked['corroboration_sources'] = corroboration_sources
        ranked['date_dt'] = date_dt or datetime.min.replace(tzinfo=timezone.utc)
        ranked_candidates.append(ranked)

    ranked_candidates.sort(
        key=lambda entry: (
            int(entry.get('score') or 0),
            int(entry.get('corroboration_sources') or 0),
            entry.get('date_dt') or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )

    for candidate in ranked_candidates:
        item = candidate.get('item')
        source = candidate.get('source')
        if not isinstance(item, dict):
            continue
        freshness_value = str(source['published_at']) if source and source.get('published_at') else str(item.get('occurred_at') or '')
        freshness_label, freshness_class = freshness_badge(freshness_value)
        source_url = str(candidate.get('source_url') or '')
        ttp_values = [str(t) for t in item.get('ttp_ids', [])]
        highlights.append(
            {
                'timeline_event_id': str(item.get('id') or ''),
                'source_id': str(item.get('source_id') or ''),
                'date': str(item.get('occurred_at') or ''),
                'text': str(candidate.get('summary') or ''),
                'category': str(item['category']).replace('_', ' '),
                'target_text': str(item.get('target_text') or ''),
                'ttp_ids': ', '.join(ttp_values),
                'source_name': str(source['source_name']) if source else None,
                'source_url': source_url if source else None,
                'evidence_title': evidence_title_from_source(source) if source else fallback_title_from_url(source_url),
                'evidence_source_label': evidence_source_label_from_source(source) if source else (source_domain(source_url) or 'Unknown source'),
                'evidence_group_domain': canonical_group_domain(source) if source else (source_domain(source_url) or 'unknown-source'),
                'source_published_at': str(source['published_at']) if source and source.get('published_at') else None,
                'corroboration_sources': str(candidate.get('corroboration_sources') or '0'),
                'source_trust_score': str(_domain_trust_points(source_url)),
                'freshness_label': freshness_label,
                'freshness_class': freshness_class,
            }
        )
        if len(highlights) >= 8:
            break

    if highlights:
        return highlights

    def _activity_synthesis_sentence(text: str, terms: list[str]) -> str | None:
        for sentence in split_sentences(text):
            normalized = ' '.join(sentence.split())
            if len(normalized) < 35:
                continue
            if looks_like_navigation_noise(normalized):
                continue
            if not sentence_mentions_actor_terms(normalized, terms):
                continue
            if not looks_like_activity_sentence(normalized):
                continue
            return normalized
        for sentence in split_sentences(text):
            normalized = ' '.join(sentence.split())
            if len(normalized) < 35:
                continue
            if looks_like_navigation_noise(normalized):
                continue
            if not sentence_mentions_actor_terms(normalized, terms):
                continue
            return normalized
        return None

    for source in sorted(
        sources,
        key=lambda item: str(item.get('published_at') or item.get('retrieved_at') or ''),
        reverse=True,
    ):
        text = str(source.get('pasted_text') or '').strip()
        if not text:
            continue
        combined = f'{source.get("source_name") or ""} {source.get("url") or ""} {text}'
        if actor_terms and not text_contains_actor_term(combined, actor_terms):
            continue
        if not _is_trusted_domain(str(source.get('url') or '')):
            continue
        synthesized = _activity_synthesis_sentence(text, actor_terms)
        if not synthesized:
            continue
        freshness_label, freshness_class = freshness_badge(str(source.get('published_at') or source.get('retrieved_at') or ''))
        highlights.append(
            {
                'timeline_event_id': '',
                'source_id': str(source.get('id') or ''),
                'date': str(source.get('published_at') or source.get('retrieved_at') or ''),
                'text': synthesized,
                'category': 'activity synthesis',
                'target_text': '',
                'ttp_ids': ', '.join(extract_ttp_ids(synthesized)[:4]),
                'source_name': str(source['source_name']) if source else None,
                'source_url': str(source['url']) if source else None,
                'evidence_title': evidence_title_from_source(source) if source else fallback_title_from_url(str(source.get('url') or '')),
                'evidence_source_label': evidence_source_label_from_source(source) if source else (source_domain(str(source.get('url') or '')) or 'Unknown source'),
                'evidence_group_domain': canonical_group_domain(source) if source else (source_domain(str(source.get('url') or '')) or 'unknown-source'),
                'source_published_at': str(source['published_at']) if source and source.get('published_at') else None,
                'corroboration_sources': '1',
                'freshness_label': freshness_label,
                'freshness_class': freshness_class,
            }
        )
        if len(highlights) >= 6:
            break
    return highlights


def fetch_actor_notebook_core(
    actor_id: str,
    *,
    db_path: str,
    source_tier: str | None = None,
    min_confidence_weight: int | None = None,
    source_days: int | None = None,
    deps: dict[str, object],
) -> dict[str, object]:
    _parse_published_datetime = deps['parse_published_datetime']
    _safe_json_string_list = deps['safe_json_string_list']
    _actor_signal_categories = deps['actor_signal_categories']
    _question_actor_relevance = deps['question_actor_relevance']
    _priority_update_evidence_dt = deps['priority_update_evidence_dt']
    _question_org_alignment = deps['question_org_alignment']
    _priority_rank_score = deps['priority_rank_score']
    _phase_label_for_question = deps['phase_label_for_question']
    _priority_where_to_check = deps['priority_where_to_check']
    _priority_confidence_label = deps['priority_confidence_label']
    _quick_check_title = deps['quick_check_title']
    _short_decision_trigger = deps['short_decision_trigger']
    _telemetry_anchor_line = deps['telemetry_anchor_line']
    _priority_next_best_action = deps['priority_next_best_action']
    _guidance_line = deps['guidance_line']
    _guidance_query_hint = deps['guidance_query_hint']
    _priority_disconfirming_signal = deps['priority_disconfirming_signal']
    _confidence_change_threshold_line = deps.get('confidence_change_threshold_line', deps['escalation_threshold_line'])
    _expected_output_line = deps.get('expected_output_line', _short_decision_trigger)
    _priority_update_recency_label = deps['priority_update_recency_label']
    _org_alignment_label = deps['org_alignment_label']
    _fallback_priority_questions = deps['fallback_priority_questions']
    _token_overlap = deps['token_overlap']
    _build_actor_profile_from_mitre = deps['build_actor_profile_from_mitre']
    _group_top_techniques = deps['group_top_techniques']
    _favorite_attack_vectors = deps['favorite_attack_vectors']
    _known_technique_ids_for_entity = deps['known_technique_ids_for_entity']
    _emerging_techniques_from_timeline = deps['emerging_techniques_from_timeline']
    _build_timeline_graph = deps['build_timeline_graph']
    _compact_timeline_rows = deps['compact_timeline_rows']
    _actor_terms = deps['actor_terms']
    _build_recent_activity_highlights = deps['build_recent_activity_highlights']
    _build_top_change_signals = deps.get('build_top_change_signals', build_top_change_signals)
    _ollama_review_change_signals = deps.get('ollama_review_change_signals', lambda *_args, **_kwargs: [])
    _build_recent_activity_synthesis = deps['build_recent_activity_synthesis']
    _recent_change_summary = deps['recent_change_summary']
    _build_environment_checks = deps['build_environment_checks']
    _build_notebook_kpis = deps['build_notebook_kpis']
    _format_date_or_unknown = deps['format_date_or_unknown']
    _recent_change_max_days = int(deps.get('recent_change_max_days', 45))
    _load_quick_check_overrides = deps.get('load_quick_check_overrides')
    _load_source_reliability_map = deps.get(
        'load_source_reliability_map',
        lambda _connection, *, actor_id: {},
    )
    _domain_from_url = deps.get('domain_from_url', lambda _url: '')
    _confidence_weight_adjustment = deps.get('confidence_weight_adjustment', lambda _score: 0)

    quick_check_overrides: dict[str, dict[str, str]] = {}
    question_feedback: dict[str, dict[str, int]] = {}
    source_reliability_map: dict[str, dict[str, object]] = {}
    with sqlite3.connect(db_path) as connection:
        actor_row = connection.execute(
            '''
            SELECT
                id, display_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, notebook_updated_at,
                last_refresh_duration_ms, last_refresh_sources_processed
            FROM actor_profiles
            WHERE id = ?
            ''',
            (actor_id,),
        ).fetchone()
        if actor_row is None:
            raise HTTPException(status_code=404, detail='actor not found')

        sources = connection.execute(
            '''
            SELECT
                id, source_name, url, published_at, retrieved_at, pasted_text,
                title, headline, og_title, html_title, publisher, site_name,
                source_tier, confidence_weight
            FROM sources
            WHERE actor_id = ?
            ORDER BY COALESCE(published_at, retrieved_at) DESC
            ''',
            (actor_id,),
        ).fetchall()
        source_items_for_ioc = [
            {
                'id': row[0],
                'source_name': row[1],
                'url': row[2],
                'published_at': row[3],
                'retrieved_at': row[4],
                'pasted_text': row[5],
                'title': row[6],
                'headline': row[7],
                'og_title': row[8],
                'html_title': row[9],
                'publisher': row[10],
                'site_name': row[11],
                'source_tier': row[12],
                'confidence_weight': row[13],
            }
            for row in sources
        ]
        derived_ioc_candidates = _derived_ioc_items_from_sources(source_items_for_ioc, max_items=80)
        if derived_ioc_candidates:
            now_iso = datetime.now(timezone.utc).isoformat()
            for candidate in derived_ioc_candidates:
                ioc_store_service.upsert_ioc_item_core(
                    connection,
                    actor_id=actor_id,
                    raw_ioc_type=str(candidate.get('ioc_type') or 'indicator'),
                    raw_ioc_value=str(candidate.get('ioc_value') or ''),
                    source_ref=str(candidate.get('source_ref') or '') or None,
                    source_id=str(candidate.get('source_id') or '') or None,
                    source_tier=str(candidate.get('source_tier') or '') or None,
                    extraction_method='auto_source_regex',
                    now_iso=now_iso,
                    deps={
                        'validate_ioc_candidate': ioc_validation_service.validate_ioc_candidate_core,
                    },
                )

        timeline_rows = connection.execute(
            '''
            SELECT id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            FROM timeline_events
            WHERE actor_id = ?
            ORDER BY occurred_at ASC
            ''',
            (actor_id,),
        ).fetchall()

        thread_rows = connection.execute(
            '''
            SELECT id, question_text, status, created_at, updated_at
            FROM question_threads
            WHERE actor_id = ?
            ORDER BY updated_at DESC
            ''',
            (actor_id,),
        ).fetchall()

        updates_by_thread: dict[str, list[dict[str, object]]] = {}
        for thread_row in thread_rows:
            thread_id = thread_row[0]
            update_rows = connection.execute(
                '''
                SELECT
                    qu.id,
                    qu.trigger_excerpt,
                    qu.update_note,
                    qu.created_at,
                    s.source_name,
                    s.url,
                    s.published_at
                FROM question_updates qu
                JOIN sources s ON s.id = qu.source_id
                WHERE qu.thread_id = ?
                ORDER BY qu.created_at DESC
                ''',
                (thread_id,),
            ).fetchall()
            updates_by_thread[thread_id] = [
                {
                    'id': update_row[0],
                    'trigger_excerpt': update_row[1],
                    'update_note': update_row[2],
                    'created_at': update_row[3],
                    'source_name': update_row[4],
                    'source_url': update_row[5],
                    'source_published_at': update_row[6],
                }
                for update_row in update_rows
            ]

        guidance_rows = connection.execute(
            '''
            SELECT id, thread_id, platform, what_to_look_for, where_to_look, query_hint, created_at
            FROM environment_guidance
            WHERE actor_id = ?
            ORDER BY created_at ASC
            ''',
            (actor_id,),
        ).fetchall()
        ioc_rows = connection.execute(
            '''
            SELECT
                id, ioc_type, ioc_value, source_ref, created_at,
                lifecycle_status, handling_tlp, seen_count, last_seen_at, confidence_score
            FROM ioc_items
            WHERE actor_id = ? AND is_active = 1 AND validation_status IN ('valid', 'unvalidated', 'legacy')
            ORDER BY last_seen_at DESC, created_at DESC
            ''',
            (actor_id,),
        ).fetchall()
        ioc_items = [
            {
                'id': row[0],
                'ioc_type': row[1],
                'ioc_value': row[2],
                'source_ref': row[3],
                'created_at': row[4],
                'lifecycle_status': row[5],
                'handling_tlp': row[6],
                'seen_count': row[7],
                'last_seen_at': row[8],
                'confidence_score': row[9],
            }
            for row in ioc_rows
        ]
        context_row = connection.execute(
            '''
            SELECT org_context, priority_mode, updated_at
            FROM requirement_context
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        requirement_rows = connection.execute(
            '''
            SELECT id, req_type, requirement_text, rationale_text,
                   source_name, source_url, source_published_at,
                   validation_score, validation_notes,
                   status, created_at
            FROM requirement_items
            WHERE actor_id = ?
            ORDER BY created_at DESC
            ''',
            (actor_id,),
        ).fetchall()

        guidance_by_thread: dict[str, list[dict[str, object]]] = {}
        for row in guidance_rows:
            guidance_by_thread.setdefault(row[1], []).append(
                {
                    'id': row[0],
                    'platform': row[2],
                    'what_to_look_for': row[3],
                    'where_to_look': row[4],
                    'query_hint': row[5],
                    'created_at': row[6],
                }
            )
        if callable(_load_quick_check_overrides):
            try:
                loaded = _load_quick_check_overrides(connection, actor_id)
                if isinstance(loaded, dict):
                    quick_check_overrides = {
                        str(key): value
                        for key, value in loaded.items()
                        if isinstance(value, dict)
                    }
            except Exception:
                quick_check_overrides = {}
        feedback_rows = connection.execute(
            '''
            SELECT item_id, COUNT(*), SUM(rating_score)
            FROM analyst_feedback_events
            WHERE actor_id = ? AND item_type = 'priority_question'
            GROUP BY item_id
            ''',
            (actor_id,),
        ).fetchall()
        question_feedback = {
            str(row[0]): {
                'votes': int(row[1] or 0),
                'score': int(row[2] or 0),
            }
            for row in feedback_rows
            if str(row[0] or '').strip()
        }
        try:
            loaded_reliability = _load_source_reliability_map(connection, actor_id=actor_id)
            if isinstance(loaded_reliability, dict):
                source_reliability_map = loaded_reliability
        except Exception:
            source_reliability_map = {}

    actor = {
        'id': actor_row[0],
        'display_name': actor_row[1],
        'scope_statement': actor_row[2],
        'created_at': actor_row[3],
        'is_tracked': bool(actor_row[4]),
        'notebook_status': actor_row[5],
        'notebook_message': actor_row[6],
        'notebook_updated_at': actor_row[7],
        'last_refresh_duration_ms': actor_row[8],
        'last_refresh_sources_processed': actor_row[9],
    }
    timeline_items: list[dict[str, object]] = [
        {
            'id': row[0],
            'occurred_at': row[1],
            'category': row[2],
            'title': row[3],
            'summary': row[4],
            'source_id': row[5],
            'target_text': row[6],
            'ttp_ids': _safe_json_string_list(row[7]),
        }
        for row in timeline_rows
    ]
    cutoff_90 = datetime.now(timezone.utc) - timedelta(days=90)
    timeline_recent_items = [
        item
        for item in timeline_items
        if (
            (dt := _parse_published_datetime(str(item.get('occurred_at') or ''))) is not None
            and dt >= cutoff_90
        )
    ]

    thread_items: list[dict[str, object]] = []
    for row in thread_rows:
        thread_items.append(
            {
                'id': row[0],
                'question_text': row[1],
                'status': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'updates': updates_by_thread.get(row[0], []),
            }
        )

    open_thread_ids = [thread['id'] for thread in thread_items if thread['status'] == 'open']
    guidance_for_open = [
        {
            'thread_id': thread_id,
            'question_text': next(item['question_text'] for item in thread_items if item['id'] == thread_id),
            'guidance_items': guidance_by_thread.get(thread_id, []),
        }
        for thread_id in open_thread_ids
    ]
    priority_questions: list[dict[str, object]] = []
    open_threads = [thread for thread in thread_items if thread['status'] == 'open']
    actor_categories = _actor_signal_categories(timeline_recent_items)
    signal_text = ' '.join([str(item.get('summary') or '') for item in timeline_recent_items]).lower()
    org_context_text = str(context_row[0]) if context_row and context_row[0] else ''
    scored_threads: list[dict[str, object]] = []
    for thread in open_threads:
        question_text = str(thread.get('question_text') or '')
        relevance = _question_actor_relevance(question_text, actor_categories, signal_text)
        if relevance <= 0:
            continue
        updates = thread.get('updates', [])
        updates_list = updates if isinstance(updates, list) else []
        evidence_dts = [
            dt
            for dt in (
                _priority_update_evidence_dt(update)
                for update in updates_list
                if isinstance(update, dict)
            )
            if dt is not None
        ]
        latest_evidence_dt = max(evidence_dts) if evidence_dts else None
        corroborating_sources = len(
            {
                str(update.get('source_url') or update.get('source_name') or '').strip().lower()
                for update in updates_list
                if isinstance(update, dict)
                and str(update.get('source_url') or update.get('source_name') or '').strip()
            }
        )
        org_alignment = _question_org_alignment(question_text, org_context_text)
        rank_score = _priority_rank_score(
            thread,
            relevance,
            latest_evidence_dt,
            corroborating_sources,
            org_alignment,
        )
        feedback = question_feedback.get(str(thread.get('id') or ''), {'score': 0, 'votes': 0})
        feedback_score = int(feedback.get('score') or 0)
        rank_score += max(-3, min(3, feedback_score))
        scored_threads.append(
            {
                'thread': thread,
                'relevance': relevance,
                'rank_score': rank_score,
                'latest_evidence_dt': latest_evidence_dt,
                'corroborating_sources': corroborating_sources,
                'org_alignment': org_alignment,
                'feedback_score': feedback_score,
                'feedback_votes': int(feedback.get('votes') or 0),
            }
        )

    sorted_scored_threads = sorted(
        scored_threads,
        key=lambda item: (
            int(item['rank_score']),
            item['latest_evidence_dt'] or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    for scored in sorted_scored_threads:
        thread = scored['thread']
        question_text = str(thread.get('question_text') or '')
        relevance = int(scored['relevance'])
        rank_score = int(scored['rank_score'])
        updates = thread.get('updates', [])
        updates_list = updates if isinstance(updates, list) else []
        latest_update = updates_list[0] if updates_list and isinstance(updates_list[0], dict) else None
        latest_excerpt = str(latest_update.get('trigger_excerpt') or '') if isinstance(latest_update, dict) else ''
        latest_excerpt = ' '.join(latest_excerpt.split())
        if len(latest_excerpt) > 180:
            latest_excerpt = latest_excerpt[:180].rsplit(' ', 1)[0] + '...'
        if rank_score >= 10:
            priority = 'High'
        elif rank_score >= 7:
            priority = 'Medium'
        else:
            priority = 'Low'
        guidance_items = guidance_by_thread.get(str(thread['id']), [])
        updates_count = len(updates_list)
        phase_label = _phase_label_for_question(question_text)
        where_to_check = _priority_where_to_check(guidance_items, question_text)
        confidence = _priority_confidence_label(updates_count, relevance, latest_excerpt)
        priority_questions.append(
            {
                'id': thread['id'],
                'question_text': question_text,
                'phase_label': phase_label,
                'quick_check_title': _quick_check_title(question_text, phase_label),
                'decision_trigger': _short_decision_trigger(question_text),
                'telemetry_anchor': _telemetry_anchor_line(guidance_items, question_text),
                'first_step': _priority_next_best_action(question_text, where_to_check),
                'what_to_look_for': _guidance_line(guidance_items, 'what_to_look_for'),
                'query_hint': _guidance_query_hint(guidance_items, question_text),
                'success_condition': _priority_disconfirming_signal(question_text),
                'confidence_change_threshold': _confidence_change_threshold_line(question_text),
                'escalation_threshold': _confidence_change_threshold_line(question_text),
                'expected_output': _expected_output_line(question_text),
                'priority': priority,
                'confidence': confidence,
                'evidence_recency': _priority_update_recency_label(
                    scored['latest_evidence_dt'] if isinstance(scored['latest_evidence_dt'], datetime) else None
                ),
                'corroborating_sources': int(scored['corroborating_sources']),
                'org_alignment': _org_alignment_label(int(scored.get('org_alignment') or 0)),
                'analyst_feedback_score': int(scored.get('feedback_score') or 0),
                'analyst_feedback_votes': int(scored.get('feedback_votes') or 0),
                'updates_count': updates_count,
                'updated_at': thread['updated_at'],
            }
        )
        if len(priority_questions) >= 5:
            break

    if len(priority_questions) < 3:
        fallback_items = _fallback_priority_questions(str(actor['display_name']), actor_categories)
        for idx, item in enumerate(fallback_items, start=1):
            fallback_question_text = str(item['question_text'])
            if any(
                _token_overlap(str(existing.get('question_text') or ''), fallback_question_text) >= 0.7
                for existing in priority_questions
            ):
                continue
            priority_questions.append(
                {
                    'id': f'fallback-{idx}',
                    'question_text': fallback_question_text,
                    'phase_label': _phase_label_for_question(fallback_question_text),
                    'quick_check_title': _quick_check_title(
                        fallback_question_text,
                        _phase_label_for_question(fallback_question_text),
                    ),
                    'decision_trigger': _short_decision_trigger(fallback_question_text),
                    'telemetry_anchor': f'Anchor: {str(item["where_to_check"])}.',
                    'first_step': _priority_next_best_action(fallback_question_text, str(item['where_to_check'])),
                    'what_to_look_for': str(item.get('hunt_focus') or ''),
                    'query_hint': f'Start in: {str(item["where_to_check"])}.',
                    'success_condition': str(item.get('disconfirming_signal') or ''),
                    'confidence_change_threshold': _confidence_change_threshold_line(fallback_question_text),
                    'escalation_threshold': _confidence_change_threshold_line(fallback_question_text),
                    'expected_output': _expected_output_line(fallback_question_text),
                    'priority': str(item['priority']),
                    'confidence': str(item.get('confidence') or 'Low'),
                    'evidence_recency': 'Evidence recency unknown',
                    'corroborating_sources': 0,
                    'org_alignment': 'Unknown',
                    'updates_count': 0,
                    'updated_at': '',
                }
            )
            if len(priority_questions) >= 5:
                break

    if quick_check_overrides:
        for card in priority_questions:
            card_id = str(card.get('id') or '').strip()
            if not card_id:
                continue
            override = quick_check_overrides.get(card_id)
            if not override:
                continue
            first_step = str(override.get('first_step') or '').strip()
            what_to_look_for = str(override.get('what_to_look_for') or '').strip()
            expected_output = str(override.get('expected_output') or '').strip()
            if first_step:
                card['first_step'] = first_step
            if what_to_look_for:
                card['what_to_look_for'] = what_to_look_for
            if expected_output:
                card['expected_output'] = expected_output
    phase_group_order: list[str] = []
    phase_groups_map: dict[str, list[dict[str, object]]] = {}
    for card in priority_questions:
        phase = str(card.get('phase_label') or 'Operational Signal')
        if phase not in phase_groups_map:
            phase_groups_map[phase] = []
            phase_group_order.append(phase)
        phase_groups_map[phase].append(card)
    priority_phase_groups = [{'phase': phase, 'cards': phase_groups_map[phase]} for phase in phase_group_order]

    source_items = [
        {
            'id': row[0],
            'source_name': row[1],
            'url': row[2],
            'published_at': row[3],
            'retrieved_at': row[4],
            'pasted_text': row[5],
            'title': row[6],
            'headline': row[7],
            'og_title': row[8],
            'html_title': row[9],
            'publisher': row[10],
            'site_name': row[11],
            'source_tier': row[12],
            'confidence_weight': row[13],
        }
        for row in sources
    ]
    for source in source_items:
        source_url = str(source.get('url') or '').strip()
        domain = _domain_from_url(source_url)
        reliability = source_reliability_map.get(domain, {}) if domain else {}
        reliability_score = float(reliability.get('reliability_score') or 0.5)
        weight_adjust = int(_confidence_weight_adjustment(reliability_score))
        try:
            base_weight = int(source.get('confidence_weight') or 0)
        except Exception:
            base_weight = 0
        source['confidence_weight'] = max(0, min(4, base_weight + weight_adjust))
        source['source_reliability_score'] = reliability_score
        source['source_reliability_votes'] = int(reliability.get('helpful_count') or 0) + int(
            reliability.get('unhelpful_count') or 0
        )
    quick_check_ioc_pool: list[dict[str, str]] = []
    seen_ioc_pairs: set[tuple[str, str]] = set()
    for item in ioc_items:
        ioc_type = str(item.get('ioc_type') or '').strip().lower()
        ioc_value = str(item.get('ioc_value') or '').strip()
        if not ioc_type or not ioc_value:
            continue
        key = (ioc_type, ioc_value.lower())
        if key in seen_ioc_pairs:
            continue
        seen_ioc_pairs.add(key)
        quick_check_ioc_pool.append(
            {
                'ioc_type': ioc_type,
                'ioc_value': ioc_value,
                'source_ref': str(item.get('source_ref') or ''),
            }
        )
    for card in priority_questions:
        related = _relevant_iocs_for_quick_check(card, quick_check_ioc_pool, limit=4)
        card['related_iocs'] = related
    allowed_tiers = {'high', 'medium', 'trusted', 'context', 'unrated'}
    normalized_source_tier = str(source_tier or '').strip().lower() or None
    if normalized_source_tier not in allowed_tiers:
        normalized_source_tier = None

    normalized_min_confidence: int | None = None
    if min_confidence_weight is not None:
        try:
            normalized_min_confidence = max(0, min(4, int(min_confidence_weight)))
        except Exception:
            normalized_min_confidence = None

    normalized_source_days: int | None = None
    if source_days is not None:
        try:
            parsed_days = int(source_days)
            normalized_source_days = parsed_days if parsed_days > 0 else None
        except Exception:
            normalized_source_days = None
    strict_default_mode = (
        normalized_source_tier is None
        and normalized_min_confidence is None
        and normalized_source_days is None
    )
    if strict_default_mode:
        normalized_min_confidence = 3
        normalized_source_days = 90

    source_cutoff_dt = (
        datetime.now(timezone.utc) - timedelta(days=normalized_source_days)
        if normalized_source_days is not None
        else None
    )

    source_items_for_changes = source_items
    if (
        normalized_source_tier is not None
        or normalized_min_confidence is not None
        or source_cutoff_dt is not None
    ):
        filtered_sources: list[dict[str, object]] = []
        for source in source_items:
            source_tier_value = str(source.get('source_tier') or '').strip().lower() or 'unrated'
            if normalized_source_tier is not None and source_tier_value != normalized_source_tier:
                continue
            try:
                source_weight = int(source.get('confidence_weight') or 0)
            except Exception:
                source_weight = 0
            if normalized_min_confidence is not None and source_weight < normalized_min_confidence:
                continue
            raw_date = str(source.get('published_at') or source.get('retrieved_at') or '')
            source_dt = _parse_published_datetime(raw_date)
            # Undated sources remain available as context but are excluded from change scoring.
            if source_dt is None:
                continue
            if source_cutoff_dt is not None:
                if source_dt < source_cutoff_dt:
                    continue
            filtered_sources.append(source)
        source_items_for_changes = filtered_sources

    allowed_source_ids_for_changes = {
        str(source.get('id') or '').strip()
        for source in source_items_for_changes
        if str(source.get('id') or '').strip()
    }
    timeline_items_for_changes = (
        [
            item
            for item in timeline_items
            if str(item.get('source_id') or '').strip() in allowed_source_ids_for_changes
        ]
        if (
            normalized_source_tier is not None
            or normalized_min_confidence is not None
            or source_cutoff_dt is not None
        )
        else timeline_items
    )
    timeline_recent_items_for_changes = [
        item
        for item in timeline_items_for_changes
        if (
            (dt := _parse_published_datetime(str(item.get('occurred_at') or ''))) is not None
            and dt >= cutoff_90
        )
    ]
    mitre_profile = _build_actor_profile_from_mitre(str(actor['display_name']))
    actor_profile_summary = str(mitre_profile['summary'])
    top_techniques = _group_top_techniques(str(mitre_profile.get('stix_id') or ''))
    favorite_vectors = _favorite_attack_vectors(top_techniques)
    known_technique_ids = _known_technique_ids_for_entity(str(mitre_profile.get('stix_id') or ''))
    if not known_technique_ids:
        known_technique_ids = {
            str(item.get('technique_id') or '').upper()
            for item in top_techniques
            if item.get('technique_id')
        }
    emerging_techniques = _emerging_techniques_from_timeline(
        timeline_recent_items_for_changes,
        known_technique_ids,
    )
    emerging_technique_ids = [str(item.get('technique_id') or '') for item in emerging_techniques]
    emerging_techniques_with_dates = [
        {
            'technique_id': str(item.get('technique_id') or ''),
            'first_seen': str(item.get('first_seen') or ''),
        }
        for item in emerging_techniques
    ]
    timeline_graph = _build_timeline_graph(timeline_recent_items)
    timeline_compact_rows = _compact_timeline_rows(timeline_items, known_technique_ids)
    actor_terms = _actor_terms(
        str(actor['display_name']),
        str(mitre_profile.get('group_name') or ''),
        str(mitre_profile.get('aliases_csv') or ''),
    )
    recent_activity_highlights = _build_recent_activity_highlights(
        timeline_items_for_changes,
        source_items_for_changes,
        actor_terms,
    )
    llm_change_signals_raw = _ollama_review_change_signals(
        str(actor.get('display_name') or ''),
        source_items_for_changes,
        recent_activity_highlights,
    )
    llm_change_signals = (
        [item for item in llm_change_signals_raw if isinstance(item, dict)]
        if isinstance(llm_change_signals_raw, list)
        else []
    )

    # Carry over freshness metadata from known sources referenced in validated evidence.
    known_source_urls = {
        str(source.get('url') or '').strip()
        for source in source_items_for_changes
        if str(source.get('url') or '').strip()
    }
    highlight_by_url = {
        str(item.get('source_url') or '').strip(): item
        for item in recent_activity_highlights
        if str(item.get('source_url') or '').strip()
    }
    now_utc = datetime.now(timezone.utc)
    min_recent_dt = now_utc - timedelta(days=max(1, _recent_change_max_days))
    for item in llm_change_signals:
        evidence_values = item.get('validated_sources')
        if not isinstance(evidence_values, list):
            item['validated_sources'] = []
            continue
        validated_recent: list[dict[str, object]] = []
        for evidence in evidence_values:
            if not isinstance(evidence, dict):
                continue
            source_url = str(evidence.get('source_url') or '').strip()
            if not source_url:
                continue
            if source_url not in known_source_urls and source_url not in highlight_by_url:
                continue
            original = highlight_by_url.get(source_url)
            evidence_date_raw = str(evidence.get('source_date') or (original.get('date') if original else '') or '').strip()
            evidence_dt = _parse_published_datetime(evidence_date_raw)
            if evidence_dt is None and original is not None:
                evidence_dt = _parse_published_datetime(str(original.get('date') or ''))
            if evidence_dt is None or evidence_dt < min_recent_dt:
                continue
            if original is not None:
                evidence.setdefault('freshness_label', str(original.get('freshness_label') or ''))
                evidence.setdefault('freshness_class', str(original.get('freshness_class') or 'badge'))
                evidence.setdefault('source_date', str(evidence.get('source_date') or original.get('date') or ''))
            validated_recent.append(evidence)
        item['validated_sources'] = validated_recent

    top_change_signals = [
        item
        for item in llm_change_signals[:3]
        if str(item.get('change_summary') or '').strip()
        and isinstance(item.get('validated_sources'), list)
        and len(item.get('validated_sources') or []) > 0
    ]
    if not top_change_signals:
        deterministic_signals = _build_top_change_signals(recent_activity_highlights, limit=3)
        for item in deterministic_signals:
            evidence_url = str(item.get('source_url') or '').strip()
            evidence_date = str(item.get('source_published_at') or item.get('date') or '').strip()
            evidence_dt = _parse_published_datetime(evidence_date)
            if evidence_dt is None or evidence_dt < min_recent_dt:
                continue
            evidence_label = str(item.get('evidence_source_label') or item.get('source_name') or evidence_url).strip()
            proof = ' '.join(str(item.get('text') or '').split()).strip()[:220]
            corroboration = int(str(item.get('corroboration_sources') or '0') or '0')
            confidence = 'high' if corroboration >= 3 else 'medium' if corroboration >= 2 else 'low'

            window_days = '90'
            observed_dt = _parse_published_datetime(evidence_date)
            if observed_dt is not None:
                age_days = max(0, (datetime.now(timezone.utc) - observed_dt).days)
                if age_days <= 30:
                    window_days = '30'
                elif age_days <= 60:
                    window_days = '60'

            top_change_signals.append(
                {
                    'change_summary': str(item.get('evidence_title') or item.get('text') or '').strip()[:180],
                    'change_why_new': str(item.get('text') or '').strip()[:300],
                    'category': str(item.get('category') or ''),
                    'ttp_ids': str(item.get('ttp_ids') or ''),
                    'target_text': str(item.get('target_text') or ''),
                    'change_window_days': window_days,
                    'change_confidence': confidence,
                    'validated_source_count': str(max(1, corroboration)),
                    'validated_sources': [
                        {
                            'source_url': evidence_url,
                            'source_label': evidence_label,
                            'source_date': evidence_date,
                            'proof': proof,
                            'freshness_label': str(item.get('freshness_label') or ''),
                            'freshness_class': str(item.get('freshness_class') or 'badge'),
                        }
                    ]
                    if evidence_url
                    else [],
                }
            )
    recent_activity_synthesis = _build_recent_activity_synthesis(recent_activity_highlights)
    recent_change_summary = _recent_change_summary(
        timeline_recent_items_for_changes,
        recent_activity_highlights,
        source_items_for_changes,
    )
    environment_checks = _build_environment_checks(
        timeline_recent_items_for_changes,
        recent_activity_highlights,
        top_techniques,
    )
    notebook_kpis = _build_notebook_kpis(
        timeline_items,
        known_technique_ids,
        len(open_thread_ids),
        source_items,
    )

    return {
        'actor': actor,
        'sources': source_items,
        'timeline_items': timeline_items,
        'timeline_recent_items': timeline_recent_items,
        'timeline_window_label': 'Last 90 days',
        'threads': thread_items,
        'guidance_for_open': guidance_for_open,
        'actor_profile_summary': actor_profile_summary,
        'actor_profile_source_label': str(mitre_profile['source_label']),
        'actor_profile_source_url': str(mitre_profile['source_url']),
        'actor_profile_group_name': str(mitre_profile['group_name']),
        'actor_created_date': _format_date_or_unknown(str(actor.get('created_at') or '')),
        'favorite_vectors': favorite_vectors,
        'top_techniques': top_techniques,
        'emerging_techniques': emerging_techniques,
        'emerging_technique_ids': emerging_technique_ids,
        'emerging_techniques_with_dates': emerging_techniques_with_dates,
        'timeline_graph': timeline_graph,
        'timeline_compact_rows': timeline_compact_rows,
        'recent_activity_highlights': recent_activity_highlights,
        'top_change_signals': top_change_signals,
        'recent_activity_synthesis': recent_activity_synthesis,
        'recent_change_summary': recent_change_summary,
        'source_quality_filters': {
            'source_tier': normalized_source_tier or '',
            'min_confidence_weight': str(normalized_min_confidence) if normalized_min_confidence is not None else '',
            'source_days': str(normalized_source_days) if normalized_source_days is not None else '',
            'total_sources': str(len(source_items)),
            'applied_sources': len(source_items_for_changes),
            'filtered_out_sources': max(0, len(source_items) - len(source_items_for_changes)),
            'undated_excluded': '1',
            'strict_default_mode': '1' if strict_default_mode else '0',
        },
        'environment_checks': environment_checks,
        'kpis': notebook_kpis,
        'ioc_items': ioc_items,
        'requirements_context': {
            'org_context': str(context_row[0]) if context_row else '',
            'priority_mode': str(context_row[1]) if context_row else 'Operational',
            'updated_at': str(context_row[2]) if context_row and context_row[2] else '',
        },
        'requirements': [
            {
                'id': row[0],
                'req_type': row[1],
                'requirement_text': row[2],
                'rationale_text': row[3],
                'source_name': row[4],
                'source_url': row[5],
                'source_published_at': row[6],
                'validation_score': row[7],
                'validation_notes': row[8],
                'status': row[9],
                'created_at': row[10],
            }
            for row in requirement_rows
        ],
        'priority_questions': priority_questions,
        'priority_phase_groups': priority_phase_groups,
        'counts': {
            'sources': len(sources),
            'timeline_events': len(timeline_rows),
            'open_questions': len(open_thread_ids),
        },
    }
