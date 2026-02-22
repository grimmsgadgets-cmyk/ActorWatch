import sqlite3


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

    actor_profile = {
        'id': _new_id(),
        'display_name': display_name,
        'scope_statement': scope_statement,
        'created_at': _utc_now_iso(),
    }
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, scope_statement, created_at, is_tracked)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (
                actor_profile['id'],
                actor_profile['display_name'],
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
