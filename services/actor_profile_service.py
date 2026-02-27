import json
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException


def normalize_actor_name_core(value: str) -> str:
    return ' '.join(str(value or '').strip().lower().split())


def actor_exists_core(connection: sqlite3.Connection, actor_id: str) -> bool:
    row = connection.execute('SELECT id FROM actor_profiles WHERE id = ?', (actor_id,)).fetchone()
    return row is not None


def set_actor_notebook_status_core(*, actor_id: str, status: str, message: str, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']

    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            UPDATE actor_profiles
            SET notebook_status = ?, notebook_message = ?, notebook_updated_at = ?
            WHERE id = ?
            ''',
            (status, message, _utc_now_iso(), actor_id),
        )
        connection.commit()


def list_actor_profiles_core(*, deps: dict[str, object]) -> list[dict[str, object]]:
    _db_path = deps['db_path']

    with sqlite3.connect(_db_path()) as connection:
        rows = connection.execute(
            '''
            SELECT
                id, display_name, scope_statement, created_at, is_tracked, aliases_csv,
                notebook_status, notebook_message, notebook_updated_at,
                last_refresh_duration_ms, last_refresh_sources_processed,
                last_confirmed_at, last_confirmed_by, last_confirmed_note
            FROM actor_profiles
            ORDER BY created_at DESC
            '''
        ).fetchall()
    return [
        {
            'id': row[0],
            'display_name': row[1],
            'scope_statement': row[2],
            'created_at': row[3],
            'is_tracked': bool(row[4]),
            'aliases_csv': str(row[5] or ''),
            'notebook_status': row[6],
            'notebook_message': row[7],
            'notebook_updated_at': row[8],
            'last_refresh_duration_ms': row[9],
            'last_refresh_sources_processed': row[10],
            'last_confirmed_at': row[11],
            'last_confirmed_by': row[12],
            'last_confirmed_note': row[13],
        }
        for row in rows
    ]


def load_tracking_intent_core(connection: sqlite3.Connection, actor_id: str) -> dict[str, object]:
    row = connection.execute(
        '''
        SELECT
            why_track, mission_impact, intelligence_focus, key_questions_json,
            priority, impact, review_cadence_days,
            confirmation_min_sources, confirmation_max_age_days,
            confirmation_criteria, updated_by, updated_at
        FROM tracking_intent_register
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchone()
    if row is None:
        return {
            'why_track': '',
            'mission_impact': '',
            'intelligence_focus': '',
            'key_questions': [],
            'priority': 'medium',
            'impact': 'medium',
            'review_cadence_days': 30,
            'confirmation_min_sources': 2,
            'confirmation_max_age_days': 90,
            'confirmation_criteria': '',
            'updated_by': '',
            'updated_at': '',
        }
    try:
        key_questions = json.loads(str(row[3] or '[]'))
        if not isinstance(key_questions, list):
            key_questions = []
    except Exception:
        key_questions = []
    return {
        'why_track': str(row[0] or ''),
        'mission_impact': str(row[1] or ''),
        'intelligence_focus': str(row[2] or ''),
        'key_questions': [str(item).strip() for item in key_questions if str(item).strip()],
        'priority': str(row[4] or 'medium').lower(),
        'impact': str(row[5] or 'medium').lower(),
        'review_cadence_days': int(row[6] or 30),
        'confirmation_min_sources': int(row[7] or 2),
        'confirmation_max_age_days': int(row[8] or 90),
        'confirmation_criteria': str(row[9] or ''),
        'updated_by': str(row[10] or ''),
        'updated_at': str(row[11] or ''),
    }


def upsert_tracking_intent_core(
    *,
    actor_id: str,
    why_track: str,
    mission_impact: str,
    intelligence_focus: str,
    key_questions: list[str],
    priority: str,
    impact: str,
    review_cadence_days: int,
    confirmation_min_sources: int,
    confirmation_max_age_days: int,
    confirmation_criteria: str,
    updated_by: str,
    deps: dict[str, object],
) -> dict[str, object]:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _actor_exists = deps['actor_exists']

    normalized_priority = str(priority or '').strip().lower()
    if normalized_priority not in {'low', 'medium', 'high', 'critical'}:
        normalized_priority = 'medium'
    normalized_impact = str(impact or '').strip().lower()
    if normalized_impact not in {'low', 'medium', 'high', 'critical'}:
        normalized_impact = 'medium'
    safe_cadence = max(1, min(365, int(review_cadence_days)))
    safe_min_sources = max(1, min(20, int(confirmation_min_sources)))
    safe_max_age = max(1, min(3650, int(confirmation_max_age_days)))
    safe_questions = [
        ' '.join(str(item).split()).strip()[:220]
        for item in (key_questions or [])
        if ' '.join(str(item).split()).strip()
    ][:12]
    now_iso = _utc_now_iso()
    with sqlite3.connect(_db_path()) as connection:
        if not _actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')
        connection.execute(
            '''
            INSERT INTO tracking_intent_register (
                actor_id, why_track, mission_impact, intelligence_focus, key_questions_json,
                priority, impact, review_cadence_days,
                confirmation_min_sources, confirmation_max_age_days,
                confirmation_criteria, updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(actor_id) DO UPDATE SET
                why_track = excluded.why_track,
                mission_impact = excluded.mission_impact,
                intelligence_focus = excluded.intelligence_focus,
                key_questions_json = excluded.key_questions_json,
                priority = excluded.priority,
                impact = excluded.impact,
                review_cadence_days = excluded.review_cadence_days,
                confirmation_min_sources = excluded.confirmation_min_sources,
                confirmation_max_age_days = excluded.confirmation_max_age_days,
                confirmation_criteria = excluded.confirmation_criteria,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at
            ''',
            (
                actor_id,
                str(why_track or '').strip()[:2000],
                str(mission_impact or '').strip()[:2000],
                str(intelligence_focus or '').strip()[:2000],
                json.dumps(safe_questions),
                normalized_priority,
                normalized_impact,
                safe_cadence,
                safe_min_sources,
                safe_max_age,
                str(confirmation_criteria or '').strip()[:2000],
                str(updated_by or '').strip()[:120],
                now_iso,
            ),
        )
        connection.commit()
        return load_tracking_intent_core(connection, actor_id)


def confirm_actor_assessment_core(
    *,
    actor_id: str,
    analyst: str,
    note: str,
    deps: dict[str, object],
) -> dict[str, object]:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _actor_exists = deps['actor_exists']

    analyst_name = str(analyst or '').strip()[:120]
    if not analyst_name:
        raise HTTPException(status_code=400, detail='analyst is required')
    confirm_note = str(note or '').strip()[:1000]
    now_iso = _utc_now_iso()
    with sqlite3.connect(_db_path()) as connection:
        if not _actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')
        intent = load_tracking_intent_core(connection, actor_id)
        min_sources = max(1, int(intent.get('confirmation_min_sources') or 2))
        max_age_days = max(1, int(intent.get('confirmation_max_age_days') or 90))
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
        source_count_row = connection.execute(
            '''
            SELECT COUNT(DISTINCT id)
            FROM sources
            WHERE actor_id = ?
              AND COALESCE(published_at, ingested_at, retrieved_at, '') >= ?
            ''',
            (actor_id, cutoff_iso),
        ).fetchone()
        qualifying_sources = int(source_count_row[0] or 0) if source_count_row else 0
        if qualifying_sources < min_sources:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'confirmation criteria not met: need at least {min_sources} '
                    f'sources in the last {max_age_days} days (found {qualifying_sources})'
                ),
            )
        connection.execute(
            '''
            UPDATE actor_profiles
            SET last_confirmed_at = ?, last_confirmed_by = ?, last_confirmed_note = ?
            WHERE id = ?
            ''',
            (now_iso, analyst_name, confirm_note, actor_id),
        )
        connection.commit()
        return {
            'actor_id': actor_id,
            'last_confirmed_at': now_iso,
            'last_confirmed_by': analyst_name,
            'last_confirmed_note': confirm_note,
            'criteria': {
                'confirmation_min_sources': min_sources,
                'confirmation_max_age_days': max_age_days,
                'qualifying_sources': qualifying_sources,
            },
        }


def seed_actor_profiles_from_mitre_groups_core(*, deps: dict[str, object]) -> dict[str, int]:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _new_id = deps['new_id']
    _normalize_actor_name = deps.get('normalize_actor_name', normalize_actor_name_core)
    _load_mitre_groups = deps.get('load_mitre_groups')

    if not callable(_load_mitre_groups):
        return {'total': 0, 'seeded': 0, 'existing': 0}

    groups_raw = _load_mitre_groups()
    groups = groups_raw if isinstance(groups_raw, list) else []
    if not groups:
        return {'total': 0, 'seeded': 0, 'existing': 0}

    seeded = 0
    existing = 0
    now_iso = _utc_now_iso()
    with sqlite3.connect(_db_path()) as connection:
        existing_rows = connection.execute(
            'SELECT canonical_name, display_name FROM actor_profiles'
        ).fetchall()
        existing_canonical: set[str] = set()
        for row in existing_rows:
            canonical = str(row[0] or '').strip()
            if canonical:
                existing_canonical.add(canonical)
                continue
            existing_canonical.add(_normalize_actor_name(str(row[1] or '')))

        for group in groups:
            if not isinstance(group, dict):
                continue
            name = ' '.join(str(group.get('name') or '').split()).strip()
            if not name:
                continue
            canonical = _normalize_actor_name(name)
            if not canonical:
                continue
            if canonical in existing_canonical:
                existing += 1
                continue

            aliases_raw = group.get('aliases')
            aliases = aliases_raw if isinstance(aliases_raw, list) else []
            alias_values = [
                ' '.join(str(alias).split()).strip()
                for alias in aliases
                if str(alias).strip()
            ]
            aliases_csv = ', '.join(alias_values)

            scope_statement = str(group.get('description') or '').strip() or None
            connection.execute(
                '''
                INSERT INTO actor_profiles (
                    id, display_name, canonical_name, aliases_csv, scope_statement,
                    created_at, is_tracked, notebook_status, notebook_message, notebook_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 'idle', 'Waiting for tracking action.', ?)
                ''',
                (
                    _new_id(),
                    name,
                    canonical,
                    aliases_csv,
                    scope_statement,
                    now_iso,
                    now_iso,
                ),
            )
            existing_canonical.add(canonical)
            seeded += 1
        connection.commit()

    return {'total': len(groups), 'seeded': seeded, 'existing': existing}


def create_actor_profile_core(
    *,
    display_name: str,
    scope_statement: str | None,
    is_tracked: bool,
    deps: dict[str, object],
) -> dict[str, str | None]:
    _db_path = deps['db_path']
    _new_id = deps['new_id']
    _utc_now_iso = deps['utc_now_iso']
    _normalize_actor_name = deps.get('normalize_actor_name', normalize_actor_name_core)

    cleaned_display_name = ' '.join(str(display_name or '').split()).strip()
    if not cleaned_display_name:
        raise HTTPException(status_code=400, detail='display_name is required')
    canonical_name = _normalize_actor_name(cleaned_display_name)

    actor_profile = {
        'id': _new_id(),
        'display_name': cleaned_display_name,
        'scope_statement': scope_statement,
        'created_at': _utc_now_iso(),
    }
    with sqlite3.connect(_db_path()) as connection:
        duplicate = connection.execute(
            '''
            SELECT id, display_name
            FROM actor_profiles
            WHERE canonical_name = ?
            ORDER BY created_at ASC
            LIMIT 1
            ''',
            (canonical_name,),
        ).fetchone()
        if duplicate is None:
            rows = connection.execute(
                'SELECT id, display_name FROM actor_profiles WHERE COALESCE(canonical_name, \'\') = \'\''
            ).fetchall()
            for row in rows:
                existing_canonical = _normalize_actor_name(str(row[1] or ''))
                if existing_canonical == canonical_name:
                    duplicate = row
                    break
        if duplicate is not None:
            existing_row = connection.execute(
                '''
                SELECT id, display_name, scope_statement, created_at, is_tracked
                FROM actor_profiles
                WHERE id = ?
                ''',
                (str(duplicate[0]),),
            ).fetchone()
            if existing_row is None:
                raise HTTPException(status_code=500, detail='duplicate actor resolution failed')
            if is_tracked and not bool(existing_row[4]):
                connection.execute(
                    '''
                    UPDATE actor_profiles
                    SET is_tracked = 1,
                        notebook_status = 'running',
                        notebook_message = 'Preparing notebook generation...',
                        notebook_updated_at = ?
                    WHERE id = ?
                    ''',
                    (_utc_now_iso(), str(existing_row[0])),
                )
                connection.commit()
            return {
                'id': str(existing_row[0]),
                'display_name': str(existing_row[1]),
                'scope_statement': existing_row[2],
                'created_at': str(existing_row[3]),
            }

        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, aliases_csv, scope_statement, created_at, is_tracked)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                actor_profile['id'],
                actor_profile['display_name'],
                canonical_name,
                '',
                actor_profile['scope_statement'],
                actor_profile['created_at'],
                1 if is_tracked else 0,
            ),
        )
        connection.execute(
            '''
            UPDATE actor_profiles
            SET notebook_status = ?,
                notebook_message = ?,
                notebook_updated_at = ?
            WHERE id = ?
            ''',
            (
                'running' if is_tracked else 'idle',
                'Preparing notebook generation...' if is_tracked else 'Waiting for tracking action.',
                _utc_now_iso(),
                actor_profile['id'],
            ),
        )
        connection.commit()
    return actor_profile


def merge_actor_profiles_core(
    *,
    target_actor_id: str,
    source_actor_id: str,
    deps: dict[str, object],
) -> dict[str, object]:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _new_id = deps['new_id']

    if target_actor_id == source_actor_id:
        raise HTTPException(status_code=400, detail='source and target actor ids must differ')

    with sqlite3.connect(_db_path()) as connection:
        target_row = connection.execute(
            '''
            SELECT id, display_name, scope_statement, is_tracked, canonical_name
            FROM actor_profiles
            WHERE id = ?
            ''',
            (target_actor_id,),
        ).fetchone()
        source_row = connection.execute(
            '''
            SELECT id, display_name, scope_statement, is_tracked, canonical_name
            FROM actor_profiles
            WHERE id = ?
            ''',
            (source_actor_id,),
        ).fetchone()
        if target_row is None or source_row is None:
            raise HTTPException(status_code=404, detail='source or target actor not found')

        moved_counts: dict[str, int] = {}

        def move_table(table_name: str) -> None:
            cursor = connection.execute(
                f'UPDATE {table_name} SET actor_id = ? WHERE actor_id = ?',
                (target_actor_id, source_actor_id),
            )
            moved_counts[table_name] = int(cursor.rowcount or 0)

        for table in (
            'sources',
            'timeline_events',
            'observation_records',
            'delta_proposals',
            'state_transition_log',
            'question_threads',
            'environment_guidance',
            'ioc_items',
            'requirement_items',
            'analyst_observation_history',
        ):
            move_table(table)

        actor_state_source = connection.execute(
            'SELECT 1 FROM actor_state WHERE actor_id = ?',
            (source_actor_id,),
        ).fetchone()
        actor_state_target = connection.execute(
            'SELECT 1 FROM actor_state WHERE actor_id = ?',
            (target_actor_id,),
        ).fetchone()
        if actor_state_source is not None and actor_state_target is None:
            connection.execute(
                'UPDATE actor_state SET actor_id = ? WHERE actor_id = ?',
                (target_actor_id, source_actor_id),
            )
            moved_counts['actor_state'] = 1
        else:
            moved_counts['actor_state'] = 0
            connection.execute('DELETE FROM actor_state WHERE actor_id = ?', (source_actor_id,))

        requirement_context_source = connection.execute(
            'SELECT 1 FROM requirement_context WHERE actor_id = ?',
            (source_actor_id,),
        ).fetchone()
        requirement_context_target = connection.execute(
            'SELECT 1 FROM requirement_context WHERE actor_id = ?',
            (target_actor_id,),
        ).fetchone()
        if requirement_context_source is not None and requirement_context_target is None:
            connection.execute(
                'UPDATE requirement_context SET actor_id = ? WHERE actor_id = ?',
                (target_actor_id, source_actor_id),
            )
            moved_counts['requirement_context'] = 1
        else:
            moved_counts['requirement_context'] = 0
            connection.execute('DELETE FROM requirement_context WHERE actor_id = ?', (source_actor_id,))

        feed_rows = connection.execute(
            '''
            SELECT
                feed_name,
                feed_url,
                last_checked_at,
                last_success_at,
                last_success_published_at,
                last_imported_count,
                total_imported,
                consecutive_failures,
                total_failures,
                last_error
            FROM actor_feed_state
            WHERE actor_id = ?
            ''',
            (source_actor_id,),
        ).fetchall()
        merged_feed_rows = 0
        for row in feed_rows:
            existing = connection.execute(
                '''
                SELECT
                    total_imported,
                    total_failures,
                    consecutive_failures
                FROM actor_feed_state
                WHERE actor_id = ? AND feed_name = ? AND feed_url = ?
                ''',
                (target_actor_id, row[0], row[1]),
            ).fetchone()
            if existing is None:
                connection.execute(
                    '''
                    UPDATE actor_feed_state
                    SET actor_id = ?
                    WHERE actor_id = ? AND feed_name = ? AND feed_url = ?
                    ''',
                    (target_actor_id, source_actor_id, row[0], row[1]),
                )
                merged_feed_rows += 1
                continue
            connection.execute(
                '''
                UPDATE actor_feed_state
                SET
                    last_checked_at = COALESCE(?, last_checked_at),
                    last_success_at = COALESCE(?, last_success_at),
                    last_success_published_at = COALESCE(?, last_success_published_at),
                    last_imported_count = MAX(last_imported_count, ?),
                    total_imported = ?,
                    consecutive_failures = MAX(consecutive_failures, ?),
                    total_failures = ?,
                    last_error = COALESCE(?, last_error)
                WHERE actor_id = ? AND feed_name = ? AND feed_url = ?
                ''',
                (
                    row[2],
                    row[3],
                    row[4],
                    int(row[5] or 0),
                    int(existing[0] or 0) + int(row[6] or 0),
                    int(row[7] or 0),
                    int(existing[1] or 0) + int(row[8] or 0),
                    row[9],
                    target_actor_id,
                    row[0],
                    row[1],
                ),
            )
            connection.execute(
                'DELETE FROM actor_feed_state WHERE actor_id = ? AND feed_name = ? AND feed_url = ?',
                (source_actor_id, row[0], row[1]),
            )
            merged_feed_rows += 1
        moved_counts['actor_feed_state'] = merged_feed_rows

        source_observations = connection.execute(
            '''
            SELECT
                item_type,
                item_key,
                note,
                source_ref,
                confidence,
                source_reliability,
                information_credibility,
                updated_by,
                updated_at
            FROM analyst_observations
            WHERE actor_id = ?
            ''',
            (source_actor_id,),
        ).fetchall()
        merged_observations = 0
        for row in source_observations:
            connection.execute(
                '''
                INSERT INTO analyst_observations (
                    id,
                    actor_id,
                    item_type,
                    item_key,
                    note,
                    source_ref,
                    confidence,
                    source_reliability,
                    information_credibility,
                    updated_by,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(actor_id, item_type, item_key) DO UPDATE SET
                    note = excluded.note,
                    source_ref = excluded.source_ref,
                    confidence = excluded.confidence,
                    source_reliability = excluded.source_reliability,
                    information_credibility = excluded.information_credibility,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                ''',
                (
                    _new_id(),
                    target_actor_id,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                ),
            )
            merged_observations += 1
        connection.execute(
            'DELETE FROM analyst_observations WHERE actor_id = ?',
            (source_actor_id,),
        )
        moved_counts['analyst_observations'] = merged_observations

        target_scope = str(target_row[2] or '').strip()
        source_scope = str(source_row[2] or '').strip()
        merged_scope = target_scope or source_scope or None
        merged_is_tracked = 1 if (bool(target_row[3]) or bool(source_row[3])) else 0
        merged_notebook_status = 'running' if merged_is_tracked else 'idle'
        merged_notebook_message = (
            'Preparing notebook generation...' if merged_is_tracked else 'Waiting for tracking action.'
        )
        connection.execute(
            '''
            UPDATE actor_profiles
            SET
                scope_statement = ?,
                is_tracked = ?,
                notebook_status = ?,
                notebook_message = ?,
                notebook_updated_at = ?
            WHERE id = ?
            ''',
            (
                merged_scope,
                merged_is_tracked,
                merged_notebook_status,
                merged_notebook_message,
                _utc_now_iso(),
                target_actor_id,
            ),
        )
        connection.execute('DELETE FROM actor_profiles WHERE id = ?', (source_actor_id,))
        connection.commit()

    return {
        'target_actor_id': target_actor_id,
        'source_actor_id': source_actor_id,
        'moved_counts': moved_counts,
    }


def auto_merge_duplicate_actors_core(*, deps: dict[str, object]) -> int:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _new_id = deps['new_id']
    merged_count = 0
    with sqlite3.connect(_db_path()) as connection:
        dup_keys = connection.execute(
            '''
            SELECT canonical_name
            FROM actor_profiles
            WHERE COALESCE(TRIM(canonical_name), '') <> ''
            GROUP BY canonical_name
            HAVING COUNT(*) > 1
            ORDER BY canonical_name ASC
            '''
        ).fetchall()
        duplicate_sets: list[tuple[str, list[str]]] = []
        for row in dup_keys:
            canonical = str(row[0] or '').strip()
            members = connection.execute(
                '''
                SELECT a.id
                FROM actor_profiles a
                LEFT JOIN sources s ON s.actor_id = a.id
                WHERE a.canonical_name = ?
                GROUP BY a.id, a.created_at
                ORDER BY COUNT(s.id) DESC, a.created_at ASC
                ''',
                (canonical,),
            ).fetchall()
            ids = [str(item[0]) for item in members if str(item[0] or '').strip()]
            if len(ids) > 1:
                duplicate_sets.append((canonical, ids))

    for _canonical, ids in duplicate_sets:
        target_id = ids[0]
        for source_id in ids[1:]:
            merge_actor_profiles_core(
                target_actor_id=target_id,
                source_actor_id=source_id,
                deps={
                    'db_path': _db_path,
                    'utc_now_iso': _utc_now_iso,
                    'new_id': _new_id,
                },
            )
            merged_count += 1
    return merged_count
