from datetime import datetime, timedelta, timezone


def build_recent_activity_synthesis_core(
    highlights: list[dict[str, str | None]],
    *,
    deps: dict[str, object],
) -> list[dict[str, str]]:
    _extract_target_from_activity_text = deps['extract_target_from_activity_text']
    _parse_published_datetime = deps['parse_published_datetime']

    if not highlights:
        return []

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
        if target and target not in targets:
            targets.append(target)

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
