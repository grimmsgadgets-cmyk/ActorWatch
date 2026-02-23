import re
from typing import Callable


def extract_target_hint(sentence: str) -> str:
    patterns = [
        r'\btarget(?:ed|ing)?\s+([A-Z][A-Za-z0-9&\-/ ]{3,80})',
        r'\bagainst\s+([A-Z][A-Za-z0-9&\-/ ]{3,80})',
        r'\bvictims?\s+include\s+([A-Z][A-Za-z0-9&\-/ ,]{3,100})',
    ]
    for pattern in patterns:
        match = re.search(pattern, sentence)
        if not match:
            continue
        target = ' '.join(match.group(1).split())
        target = re.sub(r'[.,;:]+$', '', target)
        if len(target) >= 4:
            return target[:90]
    return ''


def sentence_mentions_actor_terms(sentence: str, actor_terms: list[str]) -> bool:
    lowered = sentence.lower()
    for term in actor_terms:
        value = term.strip().lower()
        if not value:
            continue
        escaped = re.escape(value).replace(r'\ ', r'\s+')
        pattern = rf'(?<![a-z0-9]){escaped}(?![a-z0-9])'
        if re.search(pattern, lowered):
            return True
    return False


def looks_like_activity_sentence(sentence: str) -> bool:
    lowered = sentence.lower()
    verbs = (
        'target', 'attack', 'exploit', 'compromise', 'phish', 'deploy',
        'ransom', 'encrypt', 'exfiltrat', 'move laterally', 'beacon',
        'used', 'leveraged', 'abused', 'campaign', 'operation',
        'activity', 'incident', 'disclosure', 'victim',
    )
    return any(token in lowered for token in verbs)


def extract_target_from_activity_text(text: str) -> str:
    patterns = [
        r'\bstrikes?\s+([A-Z][A-Za-z0-9&\-/ ]{3,90})',
        r'\battack(?:ed)?\s+on\s+([A-Z][A-Za-z0-9&\-/ ]{3,90})',
        r'\bagainst\s+([A-Z][A-Za-z0-9&\-/ ]{3,90})',
        r'\btarget(?:ed|ing)?\s+([A-Z][A-Za-z0-9&\-/ ]{3,90})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        target = ' '.join(match.group(1).split())
        target = re.sub(r'[.,;:|]+$', '', target).strip()
        if len(target) >= 4:
            return target[:90]
    return ''


def timeline_category_from_sentence(sentence: str) -> str | None:
    lowered = sentence.lower()
    if any(token in lowered for token in ('phish', 'email', 'exploit', 'initial access', 'cve-')):
        return 'initial_access'
    if any(token in lowered for token in ('powershell', 'wmi', 'command', 'execution')):
        return 'execution'
    if any(token in lowered for token in ('scheduled task', 'startup', 'registry run key', 'persistence')):
        return 'persistence'
    if any(token in lowered for token in ('lateral movement', 'remote service', 'rdp', 'smb', 'pivot')):
        return 'lateral_movement'
    if any(token in lowered for token in ('dns', 'beacon', 'c2', 'command and control')):
        return 'command_and_control'
    if any(token in lowered for token in ('exfiltrat', 'stolen data', 'collection')):
        return 'exfiltration'
    if any(token in lowered for token in ('ransom', 'encrypt', 'wiper', 'impact')):
        return 'impact'
    if any(token in lowered for token in ('defense evasion', 'disable', 'tamper', 'obfuscat')):
        return 'defense_evasion'
    return None


def compact_ransomware_snapshot_summary(text: str, actor_hint: str) -> str | None:
    normalized = ' '.join(str(text or '').split())
    if not normalized:
        return None

    recent_match = re.search(
        r'(\d+)\s+(?:public\s+)?victim disclosures in the last 90 days',
        normalized,
        flags=re.IGNORECASE,
    )
    trend_match = re.search(
        r'trend for\s+[^:]+:\s*(\d+)\s+total public victim disclosures,\s*(\d+)\s+in the last 90 days',
        normalized,
        flags=re.IGNORECASE,
    )
    total_match = re.search(
        r'\((\d+)\s+total listed disclosures',
        normalized,
        flags=re.IGNORECASE,
    ) or re.search(
        r'(\d+)\s+total public victim disclosures',
        normalized,
        flags=re.IGNORECASE,
    )
    geographies_match = re.search(
        r'(?:most frequently listed victim geographies in this sample are|most frequent victim geographies in the current sample:)\s*([^\.]+)\.',
        normalized,
        flags=re.IGNORECASE,
    ) or re.search(
        r'where:\s*([^\.]+)\.',
        normalized,
        flags=re.IGNORECASE,
    )
    sectors_match = re.search(
        r'(?:most frequently listed victim sectors are|most frequent exposed sectors in the current sample:)\s*([^\.]+)\.',
        normalized,
        flags=re.IGNORECASE,
    ) or re.search(
        r'how/targets:\s*([^\.]+)\.',
        normalized,
        flags=re.IGNORECASE,
    )
    examples_match = re.search(
        r'(?:recent listed victim examples|recently observed targets include|recent disclosures include):\s*([^\.]+)\.',
        normalized,
        flags=re.IGNORECASE,
    )

    recent_90 = str(recent_match.group(1) or '').strip() if recent_match else ''
    if not recent_90 and trend_match:
        recent_90 = str(trend_match.group(2) or '').strip()
    total_listed = str(total_match.group(1) or '').strip() if total_match else ''
    if not total_listed and trend_match:
        total_listed = str(trend_match.group(1) or '').strip()
    geographies = str(geographies_match.group(1) or '').strip() if geographies_match else ''
    sectors = str(sectors_match.group(1) or '').strip() if sectors_match else ''
    examples = str(examples_match.group(1) or '').strip() if examples_match else ''

    if not any((recent_90, total_listed, geographies, sectors, examples)):
        return None

    lines: list[str] = []
    if geographies:
        lines.append(f'Top geographies: {geographies}')
    if sectors:
        lines.append(f'Top sectors: {sectors}')
    if recent_90:
        lines.append(f'90d disclosures: {recent_90}')
    if total_listed:
        lines.append(f'Total listed: {total_listed}')
    if examples:
        lines.append(f'Recent examples: {examples}')

    summary = '\n'.join(lines)
    if len(summary) > 420:
        summary = summary[:420].rsplit(' ', 1)[0] + '...'
    return summary


def extract_major_move_events(
    source_name: str,
    source_id: str,
    occurred_at: str,
    text: str,
    actor_terms: list[str],
    source_title: str | None = None,
    *,
    deps: dict[str, object],
) -> list[dict[str, object]]:
    _split_sentences = deps['split_sentences']
    _extract_ttp_ids = deps['extract_ttp_ids']
    _new_id = deps['new_id']

    source_name_lower = str(source_name or '').strip().lower()
    if 'ransomware.live' in source_name_lower:
        actor_hint = ''
        if actor_terms:
            actor_hint = str(actor_terms[0]).strip().title()
        clean_source_title = (
            f'{actor_hint} ransomware disclosure and targeting update'
            if actor_hint
            else 'Ransomware disclosure and targeting update'
        )
        compact_snapshot = compact_ransomware_snapshot_summary(text, actor_hint)
        if compact_snapshot:
            return [
                {
                    'id': _new_id(),
                    'occurred_at': occurred_at,
                    'category': 'impact',
                    'title': clean_source_title,
                    'summary': compact_snapshot,
                    'source_id': source_id,
                    'source_name': source_name,
                    'target_text': '',
                    'ttp_ids': [],
                }
            ]

        synthesis_fields: dict[str, str] = {}
        for label in ('Who:', 'What:', 'When:', 'Where:', 'How/Targets:'):
            pattern = rf'{re.escape(label)}\s*([^\n]+)'
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = ' '.join(match.group(1).split()).strip()
                if value:
                    synthesis_fields[label] = value
        if synthesis_fields:
            prose_parts: list[str] = []
            who = synthesis_fields.get('Who:', '')
            what = synthesis_fields.get('What:', '')
            when = synthesis_fields.get('When:', '')
            where = synthesis_fields.get('Where:', '')
            how_targets = synthesis_fields.get('How/Targets:', '')
            if who and what:
                prose_parts.append(f'{who} {what}')
            elif who:
                prose_parts.append(who)
            elif what:
                prose_parts.append(what)
            if when:
                prose_parts.append(when)
            if where:
                prose_parts.append(f'Geographies: {where}')
            if how_targets:
                prose_parts.append(f'Sectors/targets: {how_targets}')
            summary = ' '.join(part.rstrip('. ') + '.' for part in prose_parts if part.strip())
            if len(summary) < 80:
                group_name = actor_hint or 'This ransomware group'
                summary = (
                    f'{group_name} has recent victim disclosure activity tracked in ransomware.live. '
                    'Detailed geography and sector breakdown is unavailable in this cached record.'
                )
            if len(summary) > 420:
                summary = summary[:420].rsplit(' ', 1)[0] + '...'
            return [
                {
                    'id': _new_id(),
                    'occurred_at': occurred_at,
                    'category': 'impact',
                    'title': clean_source_title or 'Ransomware activity update',
                    'summary': summary,
                    'source_id': source_id,
                    'source_name': source_name,
                    'target_text': '',
                    'ttp_ids': [],
                }
            ]

        normalized_full = ' '.join(str(text or '').split())
        has_legacy_trend_blob = bool(
            re.search(
                r'trend for\s+[^:]+:\s*\d+\s+total public victim disclosures,\s*\d+\s+in the last 90 days',
                normalized_full,
                flags=re.IGNORECASE,
            )
        )
        if normalized_full:
            prose_summary = normalized_full
            if 'Analyst use:' in prose_summary:
                prose_summary = prose_summary.split('Analyst use:', 1)[0].strip()
            if len(prose_summary) > 420:
                prose_summary = prose_summary[:420].rsplit(' ', 1)[0] + '...'
            if len(prose_summary) >= 120 and not has_legacy_trend_blob:
                return [
                    {
                        'id': _new_id(),
                        'occurred_at': occurred_at,
                        'category': 'impact',
                        'title': clean_source_title,
                        'summary': prose_summary,
                        'source_id': source_id,
                        'source_name': source_name,
                        'target_text': '',
                        'ttp_ids': [],
                    }
                ]

        normalized_blob = ' '.join(str(text or '').split())
        trend_match = re.search(
            r'trend for\s+([^:]+):\s*(\d+)\s+total public victim disclosures,\s*(\d+)\s+in the last 90 days',
            normalized_blob,
            flags=re.IGNORECASE,
        )
        latest_match = re.search(r'latest listed activity:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', normalized_blob, flags=re.IGNORECASE)
        geo_match = re.search(
            r'most frequent victim geographies in the current sample:\s*([^\.]+)\.',
            normalized_blob,
            flags=re.IGNORECASE,
        )
        sector_match = re.search(
            r'most frequent exposed sectors in the current sample:\s*([^\.]+)\.',
            normalized_blob,
            flags=re.IGNORECASE,
        )
        examples_match = re.search(
            r'(recently observed targets include|recent disclosures include):\s*([^\.]+)\.',
            normalized_blob,
            flags=re.IGNORECASE,
        )
        if trend_match:
            group_name = actor_hint or str(trend_match.group(1) or '').strip().title() or 'Ransomware group'
            total_disclosures = str(trend_match.group(2) or '').strip()
            recent_disclosures = str(trend_match.group(3) or '').strip()
            latest_value = str(latest_match.group(1) or '').strip() if latest_match else 'unknown'
            geographies = str(geo_match.group(1) or '').strip() if geo_match else 'not specified'
            sectors = str(sector_match.group(1) or '').strip() if sector_match else 'not specified'
            examples = str(examples_match.group(2) or '').strip() if examples_match else ''
            summary_parts = [
                f'{group_name} has {recent_disclosures} listed victim disclosures in the last 90 days ({total_disclosures} total listed disclosures).',
                f'Latest listed activity date: {latest_value}.',
                f'Frequent listed victim geographies: {geographies}.',
                f'Frequent listed victim sectors: {sectors}.',
            ]
            if examples:
                summary_parts.append(f'Recent listed victim examples: {examples}.')
            summary = ' '.join(summary_parts)
            if len(summary) > 420:
                summary = summary[:420].rsplit(' ', 1)[0] + '...'
            clean_source_title = (
                f'{group_name} ransomware disclosure and targeting update'
                if group_name
                else 'Ransomware disclosure and targeting update'
            )
            return [
                {
                    'id': _new_id(),
                    'occurred_at': occurred_at,
                    'category': 'impact',
                    'title': clean_source_title,
                    'summary': summary,
                    'source_id': source_id,
                    'source_name': source_name,
                    'target_text': '',
                    'ttp_ids': [],
                }
            ]

    events: list[dict[str, object]] = []
    for sentence in _split_sentences(text):
        if not sentence_mentions_actor_terms(sentence, actor_terms):
            continue
        if not looks_like_activity_sentence(sentence):
            continue
        category = timeline_category_from_sentence(sentence)
        if category is None:
            continue
        summary = ' '.join(sentence.split())
        if len(summary) > 260:
            summary = summary[:260].rsplit(' ', 1)[0] + '...'
        target_hint = extract_target_hint(sentence)
        ttp_ids = _extract_ttp_ids(sentence)
        clean_source_title = ' '.join(str(source_title or '').split()).strip()
        if clean_source_title.startswith(('http://', 'https://')):
            clean_source_title = ''
        title = clean_source_title or summary[:120]
        events.append(
            {
                'id': _new_id(),
                'occurred_at': occurred_at,
                'category': category,
                'title': title,
                'summary': summary,
                'source_id': source_id,
                'source_name': source_name,
                'target_text': target_hint,
                'ttp_ids': ttp_ids,
            }
        )
    return events
