from datetime import datetime, timedelta, timezone
import re


_GENERIC_TARGET_TERMS = {
    'this threat',
    'threat actor',
    'actor',
    'actors',
    'threat',
    'threats',
    'victim',
    'victims',
    'targets',
    'targets include',
    'organizations',
    'entities',
    'organization',
    'entity',
    'high-impact sectors',
    'high impact sectors',
    'multiple sectors',
    'various sectors',
}

_SECTOR_HINT_PATTERNS: dict[str, tuple[str, ...]] = {
    'Healthcare': ('healthcare', 'hospital', 'medical', 'clinic', 'pharma', 'nhs'),
    'Finance': ('bank', 'banking', 'financial', 'finance', 'insurance', 'fintech'),
    'Government': ('government', 'federal', 'state agency', 'ministry', 'public sector', 'municipal'),
    'Education': ('education', 'university', 'college', 'school', 'academic'),
    'Retail': ('retail', 'e-commerce', 'ecommerce', 'merchant'),
    'Manufacturing': ('manufacturing', 'industrial', 'factory', 'automotive'),
    'Energy': ('energy', 'oil', 'gas', 'utility', 'power grid', 'electric'),
    'Technology': ('technology', 'tech company', 'software vendor', 'saas'),
    'Telecommunications': ('telecom', 'telecommunications', 'isp', 'mobile carrier'),
    'Transportation': ('transportation', 'logistics', 'shipping', 'aviation', 'rail'),
}

_GENERIC_ACTIVITY_PATTERNS = (
    'provides protection against this threat',
    'provides protection against',
    'threat emulation',
    'threat prevention',
    'ips provides protection',
    'signature update',
    'security vendor advisory',
)


def _clean_target_fragment(raw_value: str) -> str:
    value = re.sub(r'\s+', ' ', str(raw_value or '')).strip(" \t\r\n.,;:-")
    if not value:
        return ''
    value = re.sub(
        r'^(?:recently\s+)?(?:affected|targeted|impacted)\s+(?:organizations?|entities|sectors?)\s+(?:include|including)\s+',
        '',
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r'^(?:high[- ]impact\s+)?(?:sectors?|industr(?:y|ies))\s+such\s+as\s+', '', value, flags=re.IGNORECASE)
    return value.strip(" \t\r\n.,;:-")


def _is_generic_target_fragment(value: str) -> bool:
    token = value.strip().lower()
    if not token:
        return True
    if token in _GENERIC_TARGET_TERMS:
        return True
    if token.startswith('this threat'):
        return True
    return False


def _normalize_targets(raw_value: str) -> list[str]:
    cleaned = _clean_target_fragment(raw_value)
    if not cleaned:
        return []
    parts = re.split(r',|;|/|\band\b', cleaned, flags=re.IGNORECASE)
    normalized: list[str] = []
    for part in parts:
        candidate = _clean_target_fragment(part)
        if not candidate:
            continue
        if _is_generic_target_fragment(candidate):
            continue
        if candidate not in normalized:
            normalized.append(candidate)
    return normalized


def _extract_sector_hints(raw_value: str) -> list[str]:
    haystack = str(raw_value or '').lower()
    if not haystack:
        return []
    hits: list[str] = []
    for sector, markers in _SECTOR_HINT_PATTERNS.items():
        if any(marker in haystack for marker in markers):
            hits.append(sector)
    return hits


def build_recent_activity_synthesis_core(
    highlights: list[dict[str, str | None]],
    *,
    deps: dict[str, object],
) -> list[dict[str, str]]:
    _extract_target_from_activity_text = deps['extract_target_from_activity_text']
    _parse_published_datetime = deps['parse_published_datetime']

    if not highlights:
        return []

    filtered_highlights = [
        item
        for item in highlights
        if not any(
            pattern in (
                ' '.join(
                    [
                        str(item.get('title') or ''),
                        str(item.get('summary') or ''),
                        str(item.get('text') or ''),
                    ]
                ).lower()
            )
            for pattern in _GENERIC_ACTIVITY_PATTERNS
        )
    ]
    if filtered_highlights:
        highlights = filtered_highlights

    category_counts: dict[str, int] = {}
    targets: list[str] = []
    techniques: list[str] = []
    parsed_dates: list[datetime] = []
    recent_90 = 0
    cutoff_90 = datetime.now(timezone.utc) - timedelta(days=90)

    for item in highlights:
        category = str(item.get('category') or '').strip().lower()
        if category:
            category_counts[category] = category_counts.get(category, 0) + 1

        target = str(item.get('target_text') or '').strip() or _extract_target_from_activity_text(
            str(item.get('text') or '')
        )
        for normalized_target in _normalize_targets(target):
            if normalized_target not in targets:
                targets.append(normalized_target)
        if not target:
            combined = ' '.join(
                [
                    str(item.get('title') or ''),
                    str(item.get('summary') or ''),
                    str(item.get('text') or ''),
                ]
            )
            for sector in _extract_sector_hints(combined):
                if sector not in targets:
                    targets.append(sector)

        ttp_csv = str(item.get('ttp_ids') or '').strip()
        if ttp_csv:
            for part in ttp_csv.split(','):
                token = part.strip().upper()
                if token and token not in techniques:
                    techniques.append(token)

        dt = _parse_published_datetime(str(item.get('date') or ''))
        if dt is not None:
            parsed_dates.append(dt)
            if dt >= cutoff_90:
                recent_90 += 1

    unique_sources = {
        str(item.get('source_url') or '').strip()
        for item in highlights
        if str(item.get('source_url') or '').strip()
    }
    lineage_count = len(unique_sources)
    if lineage_count >= 4 and recent_90 >= 2:
        confidence_label = 'High'
    elif lineage_count >= 2:
        confidence_label = 'Medium'
    else:
        confidence_label = 'Low'

    if parsed_dates:
        newest = max(parsed_dates).date().isoformat()
        oldest = min(parsed_dates).date().isoformat()
        what_changed = (
            f'Observed {len(highlights)} actor-linked signals between {oldest} and {newest}, '
            f'with {recent_90} in the last 90 days.'
        )
    else:
        what_changed = f'Observed {len(highlights)} actor-linked activity signals in current source coverage.'

    if category_counts:
        top_categories = sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[:2]
        category_text = ', '.join(
            f'{name.replace("_", " ")} ({count})' for name, count in top_categories
        )
        what_changed = f'{what_changed} Primary behavior clusters: {category_text}.'

    who_affected = 'Affected organizations/entities are not clearly named in current reporting.'
    if targets:
        who_affected = f'Recently affected organizations/entities include: {", ".join(targets[:4])}.'
    elif highlights:
        who_affected = (
            'Recently affected organizations/entities are not explicitly named in-source; '
            'review the linked reports below for victim details.'
        )

    action_parts: list[str] = []
    if techniques:
        action_parts.append(f'Prioritize detections for {", ".join(techniques[:5])}')
    if category_counts:
        dominant = sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[0][0]
        action_parts.append(f'focus hunt workflows on {dominant.replace("_", " ")} behavior')
    if not action_parts:
        action_parts.append('continue actor-specific source collection and validate new events')
    what_to_do_next = 'Next analyst action: ' + '; '.join(action_parts) + '.'

    return [
        {
            'label': 'What changed',
            'text': what_changed,
            'confidence': confidence_label,
            'lineage': f'{lineage_count} sources',
        },
        {
            'label': 'Who is affected',
            'text': who_affected,
            'confidence': confidence_label,
            'lineage': f'{lineage_count} sources',
        },
        {
            'label': 'What to do next',
            'text': what_to_do_next,
            'confidence': confidence_label,
            'lineage': f'{lineage_count} sources',
        },
    ]
