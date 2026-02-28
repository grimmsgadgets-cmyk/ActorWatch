import json
import sqlite3
from datetime import datetime, timedelta, timezone
from threading import Event


def log_event_core(*, event: str, fields: dict[str, object], utc_now_iso, logger) -> None:
    payload = {'event': event, **fields, 'ts': utc_now_iso()}
    try:
        logger.info(json.dumps(payload, separators=(',', ':'), default=str))
    except Exception:
        logger.info(str(payload))


def run_tracked_actor_auto_refresh_once_core(*, limit: int, deps: dict[str, object]) -> int:
    _refresh_ops_service = deps['refresh_ops_service']
    _db_path = deps['db_path']
    _auto_refresh_min_interval_hours = deps['auto_refresh_min_interval_hours']
    _parse_published_datetime = deps['parse_published_datetime']
    _enqueue_actor_generation = deps['enqueue_actor_generation']
    _submit_actor_refresh_job = deps['submit_actor_refresh_job']
    _metrics_service = deps['metrics_service']
    _log_event = deps['log_event']
    queued = _refresh_ops_service.run_tracked_actor_auto_refresh_once_core(
        db_path=_db_path,
        min_interval_hours=_auto_refresh_min_interval_hours,
        limit=limit,
        deps={
            'parse_published_datetime': _parse_published_datetime,
            'enqueue_actor_generation': _enqueue_actor_generation,
            'submit_actor_refresh_job': _submit_actor_refresh_job,
            'on_actor_queued': lambda actor_id: _log_event('auto_refresh_actor_queued', actor_id=actor_id),
        },
    )
    _metrics_service.record_refresh_queue_core(queued_count=queued)
    _log_event('auto_refresh_run', queued_count=queued, limit=limit)
    return queued


def auto_refresh_loop_core(*, stop_event: Event, deps: dict[str, object]) -> None:
    _refresh_ops_service = deps['refresh_ops_service']
    _auto_refresh_loop_seconds = deps['auto_refresh_loop_seconds']
    _recover_stale_running_states = deps['recover_stale_running_states']
    _run_tracked_actor_auto_refresh_once = deps['run_tracked_actor_auto_refresh_once']
    _auto_refresh_batch_size = deps['auto_refresh_batch_size']
    _refresh_ops_service.auto_refresh_loop_core(
        stop_event=stop_event,
        loop_seconds=_auto_refresh_loop_seconds,
        run_once=lambda: (
            _recover_stale_running_states(),
            _run_tracked_actor_auto_refresh_once(limit=_auto_refresh_batch_size),
        ),
    )


def recover_stale_running_states_core(*, deps: dict[str, object]) -> int:
    _generation_service = deps['generation_service']
    _generation_journal_service = deps.get('generation_journal_service')
    _running_stale_recovery_minutes = deps['running_stale_recovery_minutes']
    _db_path = deps['db_path']
    _parse_published_datetime = deps['parse_published_datetime']
    _utc_now_iso = deps.get('utc_now_iso')
    running_ids = _generation_service.running_actor_ids_snapshot_core()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(5, _running_stale_recovery_minutes))
    recovered_ids: list[str] = []
    with sqlite3.connect(_db_path) as connection:
        rows = connection.execute(
            '''
            SELECT id, notebook_updated_at
            FROM actor_profiles
            WHERE notebook_status = 'running'
            '''
        ).fetchall()
        for row in rows:
            actor_id = str(row[0] or '')
            if not actor_id or actor_id in running_ids:
                continue
            updated_raw = str(row[1] or '').strip()
            updated_dt = _parse_published_datetime(updated_raw) if updated_raw else None
            if updated_dt is not None and updated_dt > cutoff:
                continue
            connection.execute(
                '''
                UPDATE actor_profiles
                SET notebook_status = 'error',
                    notebook_message = 'Previous refresh stalled and was recovered. Refresh again.',
                    auto_refresh_last_status = 'error'
                WHERE id = ?
                ''',
                (actor_id,),
            )
            recovered_ids.append(actor_id)
        connection.commit()
    # Also expire the corresponding generation journal jobs so that the next
    # submit_actor_refresh_job call is not blocked by a stuck 'running' record.
    if _generation_journal_service is not None and _utc_now_iso is not None and recovered_ids:
        journal_deps = {'db_path': lambda: _db_path, 'utc_now_iso': _utc_now_iso}
        for actor_id in recovered_ids:
            try:
                _generation_journal_service.expire_stale_generation_jobs_for_actor_core(
                    actor_id=actor_id,
                    stale_after_minutes=_running_stale_recovery_minutes,
                    deps=journal_deps,
                )
            except Exception:
                pass
    return len(recovered_ids)
