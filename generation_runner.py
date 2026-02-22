import sqlite3
import time


def run_actor_generation_core(
    actor_id: str,
    *,
    db_path: str,
    deps: dict[str, object],
) -> None:
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _import_default_feeds_for_actor = deps['import_default_feeds_for_actor']
    _build_notebook = deps['build_notebook']

    started_at = time.perf_counter()
    try:
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Collecting sources...',
        )
        imported = _import_default_feeds_for_actor(actor_id)
        _set_actor_notebook_status(
            actor_id,
            'running',
            f'Sources collected ({imported}). Building timeline preview...',
        )
        _build_notebook(actor_id, generate_questions=False, rebuild_timeline=True)
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Timeline ready. Generating question threads and guidance...',
        )
        _build_notebook(actor_id, generate_questions=True, rebuild_timeline=False)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                '''
                UPDATE actor_profiles
                SET last_refresh_duration_ms = ?, last_refresh_sources_processed = ?
                WHERE id = ?
                ''',
                (elapsed_ms, imported, actor_id),
            )
            connection.commit()
        _set_actor_notebook_status(
            actor_id,
            'ready',
            f'Notebook ready. Imported {imported} feed source(s).',
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        try:
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    '''
                    UPDATE actor_profiles
                    SET last_refresh_duration_ms = ?, last_refresh_sources_processed = ?
                    WHERE id = ?
                    ''',
                    (elapsed_ms, 0, actor_id),
                )
                connection.commit()
        except Exception:
            pass
        _set_actor_notebook_status(actor_id, 'error', f'Notebook generation failed: {exc}')
