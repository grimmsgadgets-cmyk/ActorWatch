import sqlite3
from datetime import datetime, timedelta, timezone


def create_generation_job_core(
    *,
    actor_id: str,
    trigger_type: str,
    initial_status: str = 'running',
    deps: dict[str, object],
) -> str:
    _db_path = deps['db_path']
    _new_id = deps['new_id']
    _utc_now_iso = deps['utc_now_iso']

    job_id = _new_id()
    now_iso = _utc_now_iso()
    normalized_status = str(initial_status or 'running').strip().lower()
    if normalized_status not in {'queued', 'running', 'completed', 'error', 'skipped'}:
        normalized_status = 'running'
    started_at = now_iso if normalized_status == 'running' else None
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            INSERT INTO notebook_generation_jobs (
                id, actor_id, trigger_type, status, created_at, started_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                job_id,
                actor_id,
                str(trigger_type or 'manual_refresh'),
                normalized_status,
                now_iso,
                started_at,
            ),
        )
        connection.commit()
    return str(job_id)


def mark_generation_job_started_core(*, job_id: str, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    now_iso = _utc_now_iso()
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            UPDATE notebook_generation_jobs
            SET status = 'running',
                started_at = COALESCE(NULLIF(started_at, ''), ?)
            WHERE id = ?
            ''',
            (now_iso, str(job_id)),
        )
        connection.commit()


def finalize_generation_job_core(
    *,
    job_id: str,
    status: str,
    imported_sources: int,
    duration_ms: int,
    final_message: str = '',
    error_message: str = '',
    deps: dict[str, object],
) -> None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            UPDATE notebook_generation_jobs
            SET status = ?,
                finished_at = ?,
                duration_ms = ?,
                imported_sources = ?,
                final_message = ?,
                error_message = ?
            WHERE id = ?
            ''',
            (
                str(status or 'completed'),
                _utc_now_iso(),
                int(duration_ms),
                max(0, int(imported_sources)),
                str(final_message or ''),
                str(error_message or ''),
                str(job_id),
            ),
        )
        connection.commit()


def start_generation_phase_core(
    *,
    job_id: str,
    actor_id: str,
    phase_key: str,
    phase_label: str,
    attempt: int,
    message: str,
    deps: dict[str, object],
) -> str:
    _db_path = deps['db_path']
    _new_id = deps['new_id']
    _utc_now_iso = deps['utc_now_iso']
    phase_id = _new_id()
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            INSERT INTO notebook_generation_phases (
                id, job_id, actor_id, phase_key, phase_label, attempt,
                status, message, started_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?)
            ''',
            (
                phase_id,
                str(job_id),
                str(actor_id),
                str(phase_key),
                str(phase_label),
                max(1, int(attempt)),
                str(message or ''),
                _utc_now_iso(),
            ),
        )
        connection.commit()
    return str(phase_id)


def finish_generation_phase_core(
    *,
    phase_id: str,
    status: str,
    message: str = '',
    error_detail: str = '',
    duration_ms: int | None = None,
    deps: dict[str, object],
) -> None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            UPDATE notebook_generation_phases
            SET status = ?,
                message = ?,
                error_detail = ?,
                finished_at = ?,
                duration_ms = ?
            WHERE id = ?
            ''',
            (
                str(status or 'completed'),
                str(message or ''),
                str(error_detail or ''),
                _utc_now_iso(),
                None if duration_ms is None else max(0, int(duration_ms)),
                str(phase_id),
            ),
        )
        connection.commit()


def recent_generation_timeline_for_actor_core(
    *,
    actor_id: str,
    job_limit: int,
    phase_limit: int,
    deps: dict[str, object],
) -> list[dict[str, object]]:
    _db_path = deps['db_path']
    rows: list[sqlite3.Row]
    with sqlite3.connect(_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            '''
            SELECT
                p.id,
                p.job_id,
                p.phase_key,
                p.phase_label,
                p.attempt,
                p.status,
                p.message,
                p.error_detail,
                p.started_at,
                p.finished_at,
                p.duration_ms,
                j.status AS job_status,
                j.trigger_type,
                j.created_at AS job_created_at
            FROM notebook_generation_phases p
            JOIN notebook_generation_jobs j ON j.id = p.job_id
            WHERE p.actor_id = ?
              AND p.job_id IN (
                  SELECT id
                  FROM notebook_generation_jobs
                  WHERE actor_id = ?
                  ORDER BY created_at DESC
                  LIMIT ?
              )
            ORDER BY p.started_at DESC
            LIMIT ?
            ''',
            (str(actor_id), str(actor_id), max(1, int(job_limit)), max(1, int(phase_limit))),
        ).fetchall()
    return [
        {
            'phase_id': str(row['id'] or ''),
            'job_id': str(row['job_id'] or ''),
            'phase_key': str(row['phase_key'] or ''),
            'phase_label': str(row['phase_label'] or ''),
            'attempt': int(row['attempt'] or 1),
            'status': str(row['status'] or ''),
            'message': str(row['message'] or ''),
            'error_detail': str(row['error_detail'] or ''),
            'started_at': str(row['started_at'] or ''),
            'finished_at': str(row['finished_at'] or ''),
            'duration_ms': row['duration_ms'],
            'job_status': str(row['job_status'] or ''),
            'trigger_type': str(row['trigger_type'] or ''),
            'job_created_at': str(row['job_created_at'] or ''),
        }
        for row in rows
    ]


def active_generation_job_for_actor_core(*, actor_id: str, deps: dict[str, object]) -> dict[str, object] | None:
    _db_path = deps['db_path']
    with sqlite3.connect(_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            '''
            SELECT
                id,
                actor_id,
                trigger_type,
                status,
                created_at,
                started_at,
                finished_at,
                duration_ms,
                imported_sources,
                final_message,
                error_message
            FROM notebook_generation_jobs
            WHERE actor_id = ?
              AND status IN ('queued', 'running')
            ORDER BY created_at DESC
            LIMIT 1
            ''',
            (str(actor_id),),
        ).fetchone()
    if row is None:
        return None
    return {
        'job_id': str(row['id'] or ''),
        'actor_id': str(row['actor_id'] or ''),
        'trigger_type': str(row['trigger_type'] or ''),
        'status': str(row['status'] or ''),
        'created_at': str(row['created_at'] or ''),
        'started_at': str(row['started_at'] or ''),
        'finished_at': str(row['finished_at'] or ''),
        'duration_ms': row['duration_ms'],
        'imported_sources': int(row['imported_sources'] or 0),
        'final_message': str(row['final_message'] or ''),
        'error_message': str(row['error_message'] or ''),
        'phases': [],
    }


def expire_stale_generation_jobs_for_actor_core(
    *,
    actor_id: str,
    stale_after_minutes: int,
    deps: dict[str, object],
) -> int:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(minutes=max(5, int(stale_after_minutes)))
    expired_job_ids: list[str] = []

    def _parse_iso(value: object) -> datetime | None:
        raw = str(value or '').strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    with sqlite3.connect(_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            '''
            SELECT id, created_at, started_at
            FROM notebook_generation_jobs
            WHERE actor_id = ?
              AND status IN ('queued', 'running')
            ''',
            (str(actor_id),),
        ).fetchall()

        for row in rows:
            reference_dt = _parse_iso(row['started_at']) or _parse_iso(row['created_at'])
            if reference_dt is None:
                continue
            if reference_dt > cutoff:
                continue
            expired_job_ids.append(str(row['id'] or ''))

        now_iso = _utc_now_iso()
        for job_id in expired_job_ids:
            connection.execute(
                '''
                UPDATE notebook_generation_jobs
                SET status = 'error',
                    finished_at = ?,
                    duration_ms = COALESCE(duration_ms, 0),
                    error_message = CASE
                        WHEN TRIM(COALESCE(error_message, '')) = '' THEN 'stale_generation_job_recovered'
                        ELSE error_message
                    END
                WHERE id = ?
                ''',
                (now_iso, job_id),
            )
        if expired_job_ids:
            connection.commit()
    return len(expired_job_ids)


def generation_job_detail_core(*, actor_id: str, job_id: str, deps: dict[str, object]) -> dict[str, object] | None:
    _db_path = deps['db_path']
    with sqlite3.connect(_db_path()) as connection:
        connection.row_factory = sqlite3.Row
        job_row = connection.execute(
            '''
            SELECT
                id,
                actor_id,
                trigger_type,
                status,
                created_at,
                started_at,
                finished_at,
                duration_ms,
                imported_sources,
                final_message,
                error_message
            FROM notebook_generation_jobs
            WHERE actor_id = ? AND id = ?
            ''',
            (str(actor_id), str(job_id)),
        ).fetchone()
        if job_row is None:
            return None
        phase_rows = connection.execute(
            '''
            SELECT
                id,
                phase_key,
                phase_label,
                attempt,
                status,
                message,
                error_detail,
                started_at,
                finished_at,
                duration_ms
            FROM notebook_generation_phases
            WHERE actor_id = ? AND job_id = ?
            ORDER BY started_at ASC
            ''',
            (str(actor_id), str(job_id)),
        ).fetchall()
    return {
        'job_id': str(job_row['id'] or ''),
        'actor_id': str(job_row['actor_id'] or ''),
        'trigger_type': str(job_row['trigger_type'] or ''),
        'status': str(job_row['status'] or ''),
        'created_at': str(job_row['created_at'] or ''),
        'started_at': str(job_row['started_at'] or ''),
        'finished_at': str(job_row['finished_at'] or ''),
        'duration_ms': job_row['duration_ms'],
        'imported_sources': int(job_row['imported_sources'] or 0),
        'final_message': str(job_row['final_message'] or ''),
        'error_message': str(job_row['error_message'] or ''),
        'phases': [
            {
                'phase_id': str(row['id'] or ''),
                'phase_key': str(row['phase_key'] or ''),
                'phase_label': str(row['phase_label'] or ''),
                'attempt': int(row['attempt'] or 1),
                'status': str(row['status'] or ''),
                'message': str(row['message'] or ''),
                'error_detail': str(row['error_detail'] or ''),
                'started_at': str(row['started_at'] or ''),
                'finished_at': str(row['finished_at'] or ''),
                'duration_ms': row['duration_ms'],
            }
            for row in phase_rows
        ],
    }
