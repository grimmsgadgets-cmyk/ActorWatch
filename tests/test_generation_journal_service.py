import sqlite3
from pathlib import Path

from services import db_schema_service
from services import generation_journal_service


def _init_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as connection:
        db_schema_service.ensure_schema(connection)
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            ('actor-1', 'APT Demo', 'apt demo', None, '2026-02-27T10:00:00+00:00', 1),
        )
        connection.commit()


def test_expire_stale_generation_jobs_for_actor_marks_old_running_job_error(tmp_path: Path) -> None:
    db_path = tmp_path / 'app.db'
    _init_db(db_path)
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            INSERT INTO notebook_generation_jobs (
                id, actor_id, trigger_type, status, created_at, started_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                'job-1',
                'actor-1',
                'manual_refresh',
                'running',
                '2026-02-27T10:00:00+00:00',
                '2026-02-27T10:00:00+00:00',
            ),
        )
        connection.commit()

    expired = generation_journal_service.expire_stale_generation_jobs_for_actor_core(
        actor_id='actor-1',
        stale_after_minutes=30,
        deps={
            'db_path': lambda: str(db_path),
            'utc_now_iso': lambda: '2026-02-27T12:00:00+00:00',
        },
    )
    assert expired == 1

    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            '''
            SELECT status, finished_at, error_message
            FROM notebook_generation_jobs
            WHERE id = ?
            ''',
            ('job-1',),
        ).fetchone()
    assert row is not None
    assert str(row[0]) == 'error'
    assert str(row[1]).startswith('2026-02-27T12:00:00')
    assert str(row[2]) == 'stale_generation_job_recovered'
