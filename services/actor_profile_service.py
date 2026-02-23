import sqlite3

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
                id, display_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, notebook_updated_at,
                last_refresh_duration_ms, last_refresh_sources_processed
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
            'notebook_status': row[5],
            'notebook_message': row[6],
            'notebook_updated_at': row[7],
            'last_refresh_duration_ms': row[8],
            'last_refresh_sources_processed': row[9],
        }
        for row in rows
    ]


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
            raise HTTPException(
                status_code=409,
                detail=f'actor already exists: {duplicate[1]} ({duplicate[0]})',
            )

        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                actor_profile['id'],
                actor_profile['display_name'],
                canonical_name,
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
