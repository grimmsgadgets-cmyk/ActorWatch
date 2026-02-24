import json
from urllib.parse import urlparse


def normalize_environment_profile(payload: dict[str, object]) -> dict[str, object]:
    dialect = str(payload.get('query_dialect') or 'generic').strip().lower()
    if dialect not in {'generic', 'kql', 'splunk', 'lucene'}:
        dialect = 'generic'
    field_mapping_raw = payload.get('field_mapping')
    field_mapping: dict[str, str] = {}
    if isinstance(field_mapping_raw, dict):
        for key, value in field_mapping_raw.items():
            k = str(key or '').strip().lower()[:40]
            v = str(value or '').strip()[:80]
            if k and v:
                field_mapping[k] = v
    try:
        window = max(1, min(168, int(payload.get('default_time_window_hours') or 24)))
    except Exception:
        window = 24
    return {
        'query_dialect': dialect,
        'field_mapping': field_mapping,
        'default_time_window_hours': window,
    }


def upsert_environment_profile_core(connection, *, actor_id: str, profile: dict[str, object], now_iso: str) -> dict[str, object]:
    connection.execute(
        '''
        INSERT INTO actor_environment_profiles (
            actor_id, query_dialect, field_mapping_json, default_time_window_hours, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(actor_id) DO UPDATE SET
            query_dialect = excluded.query_dialect,
            field_mapping_json = excluded.field_mapping_json,
            default_time_window_hours = excluded.default_time_window_hours,
            updated_at = excluded.updated_at
        ''',
        (
            actor_id,
            str(profile.get('query_dialect') or 'generic'),
            json.dumps(profile.get('field_mapping') or {}),
            int(profile.get('default_time_window_hours') or 24),
            now_iso,
        ),
    )
    return {
        'actor_id': actor_id,
        **profile,
        'updated_at': now_iso,
    }


def load_environment_profile_core(connection, *, actor_id: str) -> dict[str, object]:
    row = connection.execute(
        '''
        SELECT query_dialect, field_mapping_json, default_time_window_hours, updated_at
        FROM actor_environment_profiles
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchone()
    if row is None:
        return {
            'actor_id': actor_id,
            'query_dialect': 'generic',
            'field_mapping': {},
            'default_time_window_hours': 24,
            'updated_at': '',
        }
    try:
        field_mapping = json.loads(str(row[1] or '{}'))
    except Exception:
        field_mapping = {}
    if not isinstance(field_mapping, dict):
        field_mapping = {}
    return {
        'actor_id': actor_id,
        'query_dialect': str(row[0] or 'generic'),
        'field_mapping': {str(k): str(v) for k, v in field_mapping.items()},
        'default_time_window_hours': int(row[2] or 24),
        'updated_at': str(row[3] or ''),
    }


def personalize_query_core(query: str, *, ioc_value: str, profile: dict[str, object]) -> str:
    personalized = ' '.join(str(query or '').split()).strip()
    mapping = profile.get('field_mapping')
    fields = mapping if isinstance(mapping, dict) else {}
    replacements = {
        'domain': str(fields.get('domain') or ''),
        'ip': str(fields.get('ip') or ''),
        'process': str(fields.get('process') or ''),
    }
    for generic, target in replacements.items():
        if target:
            personalized = personalized.replace(generic, target)

    dialect = str(profile.get('query_dialect') or 'generic').strip().lower()
    window_hours = int(profile.get('default_time_window_hours') or 24)
    if dialect == 'kql':
        suffix = f" | where TimeGenerated >= ago({window_hours}h)"
    elif dialect == 'splunk':
        suffix = f" earliest=-{window_hours}h"
    elif dialect == 'lucene':
        suffix = f" AND @timestamp:[now-{window_hours}h TO now]"
    else:
        suffix = f" time_window_hours:{window_hours}"
    if ioc_value and ioc_value not in personalized:
        personalized = f'{personalized} "{ioc_value}"'
    return f'{personalized}{suffix}'.strip()


def domain_from_url_core(url: str) -> str:
    try:
        host = (urlparse(str(url)).hostname or '').strip().lower()
    except Exception:
        host = ''
    return host.strip('.')
