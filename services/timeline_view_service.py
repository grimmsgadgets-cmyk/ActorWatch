from datetime import datetime, timezone


def short_date_core(value: str, *, deps: dict[str, object]) -> str:
    _parse_published_datetime = deps['parse_published_datetime']
    dt = _parse_published_datetime(value)
    if dt is None:
        return value[:10]
    return dt.date().isoformat()


def format_date_or_unknown_core(value: str, *, deps: dict[str, object]) -> str:
    _parse_published_datetime = deps['parse_published_datetime']
    dt = _parse_published_datetime(value)
    if dt is None:
        return 'Unknown'
    return dt.date().isoformat()


def freshness_badge_core(value: str | None, *, deps: dict[str, object]) -> tuple[str, str]:
    _parse_published_datetime = deps['parse_published_datetime']
    dt = _parse_published_datetime(value)
    if dt is None:
        return ('unknown', 'freshness-unknown')
    days_old = max(0, (datetime.now(timezone.utc) - dt).days)
    if days_old <= 1:
        return ('<=24h', 'freshness-new')
    if days_old <= 7:
        return (f'{days_old}d', 'freshness-recent')
    if days_old <= 30:
        return (f'{days_old}d stale', 'freshness-stale')
    return (f'{days_old}d old', 'freshness-old')


def bucket_label_core(value: str, *, deps: dict[str, object]) -> str:
    _parse_iso_for_sort = deps['parse_iso_for_sort']
    dt = _parse_iso_for_sort(value)
    if dt == datetime.min.replace(tzinfo=timezone.utc):
        return value[:7]
    return dt.strftime('%Y-%m')


def timeline_category_color_core(category: str) -> str:
    palette = {
        'initial_access': '#5b8def',
        'execution': '#49a078',
        'persistence': '#8a6adf',
        'lateral_movement': '#2e8bcb',
        'command_and_control': '#d48a2f',
        'exfiltration': '#c44f4f',
        'impact': '#9f2d2d',
        'defense_evasion': '#6f7d8c',
        'report': '#7b8a97',
    }
    return palette.get(category, '#7b8a97')
