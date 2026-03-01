import sqlite3
from datetime import datetime, timedelta, timezone
from threading import Event


def submit_actor_refresh_job_core(
    actor_id: str,
    *,
    trigger_type: str = 'manual_refresh',
    deps: dict[str, object],
) -> dict[str, object]:
    _generation_journal_service = deps['generation_journal_service']
    _generation_job_stale_minutes = int(deps['generation_job_stale_minutes'])
    _generation_journal_deps = deps['generation_journal_deps']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _create_generation_job = deps['create_generation_job']
    _enqueue_actor_generation = deps['enqueue_actor_generation']
    _finalize_generation_job = deps['finalize_generation_job']

    try:
        _generation_journal_service.expire_stale_generation_jobs_for_actor_core(
            actor_id=actor_id,
            stale_after_minutes=_generation_job_stale_minutes,
            deps=_generation_journal_deps(),
        )
    except Exception:
        pass

    active = _generation_journal_service.active_generation_job_for_actor_core(
        actor_id=actor_id,
        deps=_generation_journal_deps(),
    )
    if isinstance(active, dict) and str(active.get('job_id') or '').strip():
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Refresh is already in progress for this actor.',
        )
        return {
            'actor_id': actor_id,
            'job_id': str(active.get('job_id') or ''),
            'status': str(active.get('status') or 'running'),
            'queued': False,
            'message': 'A refresh job is already in progress for this actor.',
        }

    job_id = _create_generation_job(actor_id=actor_id, trigger_type=trigger_type, initial_status='queued')
    _set_actor_notebook_status(
        actor_id,
        'running',
        'Refresh queued. Waiting for worker slot...',
    )
    queue_priority = 2 if str(trigger_type or '').strip().lower() == 'auto_refresh' else 0
    enqueued = _enqueue_actor_generation(
        actor_id,
        trigger_type=trigger_type,
        job_id=job_id,
        priority=queue_priority,
    )
    if not enqueued:
        _finalize_generation_job(
            job_id=job_id,
            status='skipped',
            imported_sources=0,
            duration_ms=0,
            final_message='Skipped because another refresh was already queued.',
            error_message='',
        )
        active = _generation_journal_service.active_generation_job_for_actor_core(
            actor_id=actor_id,
            deps=_generation_journal_deps(),
        )
        return {
            'actor_id': actor_id,
            'job_id': str((active or {}).get('job_id') or job_id),
            'status': str((active or {}).get('status') or 'queued'),
            'queued': False,
            'message': 'Refresh already queued for this actor.',
        }
    return {
        'actor_id': actor_id,
        'job_id': str(job_id),
        'status': 'queued',
        'queued': True,
        'message': 'Refresh job queued.',
    }


def run_tracked_actor_auto_refresh_once_core(
    *,
    db_path: str,
    min_interval_hours: int,
    limit: int,
    deps: dict[str, object],
) -> int:
    _parse_published_datetime = deps['parse_published_datetime']
    _enqueue_actor_generation = deps['enqueue_actor_generation']
    _submit_actor_refresh_job = deps.get('submit_actor_refresh_job')
    _on_actor_queued = deps.get('on_actor_queued')
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=max(1, int(min_interval_hours)))
    queued_actor_ids: list[str] = []
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            '''
            SELECT id, notebook_status, auto_refresh_last_run_at
            FROM actor_profiles
            WHERE is_tracked = 1
            ORDER BY COALESCE(auto_refresh_last_run_at, created_at) ASC
            '''
        ).fetchall()
        for row in rows:
            actor_id = str(row[0])
            notebook_status = str(row[1] or '')
            if notebook_status == 'running':
                continue
            last_run_raw = str(row[2] or '').strip()
            last_run_dt = _parse_published_datetime(last_run_raw) if last_run_raw else None
            if last_run_dt is not None and last_run_dt > cutoff:
                continue
            queued_actor_ids.append(actor_id)
            if len(queued_actor_ids) >= max(1, int(limit)):
                break
        for actor_id in queued_actor_ids:
            connection.execute(
                '''
                UPDATE actor_profiles
                SET auto_refresh_last_run_at = ?, auto_refresh_last_status = ?
                WHERE id = ?
                ''',
                (now_utc.isoformat(), 'queued', actor_id),
            )
        connection.commit()
    for actor_id in queued_actor_ids:
        if callable(_submit_actor_refresh_job):
            _submit_actor_refresh_job(actor_id, trigger_type='auto_refresh')
        else:
            _enqueue_actor_generation(actor_id, trigger_type='auto_refresh')
        if _on_actor_queued is not None:
            try:
                _on_actor_queued(actor_id)
            except Exception:
                pass
    return len(queued_actor_ids)


def auto_refresh_loop_core(
    *,
    stop_event: Event,
    loop_seconds: int,
    run_once,
) -> None:
    while not stop_event.is_set():
        try:
            run_once()
        except Exception:
            pass
        stop_event.wait(max(1, int(loop_seconds)))


def actor_refresh_stats_core(
    *,
    actor_id: str,
    db_path: str,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    def _parse_iso(raw: str) -> datetime | None:
        value = str(raw or '').strip()
        if not value:
            return None
        normalized = value.replace('Z', '+00:00')
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    reference_now = now_utc or datetime.now(timezone.utc)
    freshness_cutoff = reference_now - timedelta(hours=24)
    eta_seconds: int | None = None
    avg_duration_ms: int | None = None
    with sqlite3.connect(db_path) as connection:
        actor_row = connection.execute(
            '''
            SELECT display_name, is_tracked, notebook_status, auto_refresh_last_run_at, auto_refresh_last_status
            FROM actor_profiles
            WHERE id = ?
            ''',
            (actor_id,),
        ).fetchone()
        if actor_row is None:
            raise ValueError('actor not found')
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
            ORDER BY total_imported DESC, feed_name ASC
            ''',
            (actor_id,),
        ).fetchall()
        source_stats = connection.execute(
            '''
            SELECT
                COUNT(*),
                SUM(CASE WHEN COALESCE(confidence_weight, 0) >= 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN COALESCE(confidence_weight, 0) >= 3
                          AND COALESCE(published_at, retrieved_at) >= ? THEN 1 ELSE 0 END)
            FROM sources
            WHERE actor_id = ?
            ''',
            (freshness_cutoff.isoformat(), actor_id),
        ).fetchone()
        actor_key = ' '.join(str(actor_row[0] or '').strip().lower().split())
        llm_cache_row = connection.execute(
            '''
            SELECT
                COALESCE(SUM(hit_count), 0),
                COALESCE(SUM(saved_ms_total), 0)
            FROM llm_synthesis_cache
            WHERE actor_key = ?
            ''',
            (actor_key,),
        ).fetchone()
        recent_job_rows = connection.execute(
            '''
            SELECT
                id,
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
            ORDER BY created_at DESC
            LIMIT 5
            ''',
            (actor_id,),
        ).fetchall()
        completed_duration_rows = connection.execute(
            '''
            SELECT duration_ms
            FROM notebook_generation_jobs
            WHERE actor_id = ?
              AND status = 'completed'
              AND duration_ms IS NOT NULL
              AND duration_ms > 0
            ORDER BY created_at DESC
            LIMIT 10
            ''',
            (actor_id,),
        ).fetchall()
        recent_job_ids = [str(row[0] or '') for row in recent_job_rows if str(row[0] or '').strip()]
        phase_rows = []
        if recent_job_ids:
            # {placeholders} is only '?,?,...' — no user data — safe from SQL injection.  # nosec B608
            placeholders = ','.join('?' for _ in recent_job_ids)
            phase_rows = connection.execute(  # nosec B608
                f'''
                SELECT
                    id,
                    job_id,
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
                WHERE actor_id = ?
                  AND job_id IN ({placeholders})
                ORDER BY started_at DESC
                ''',
                (actor_id, *recent_job_ids),
            ).fetchall()
    failing = [row for row in feed_rows if int(row[7] or 0) > 0]
    backoff = [row for row in feed_rows if int(row[7] or 0) >= 3]
    top_failures = [
        {
            'feed_name': str(row[0]),
            'feed_url': str(row[1]),
            'consecutive_failures': int(row[7] or 0),
            'total_failures': int(row[8] or 0),
            'last_error': str(row[9] or '') or None,
        }
        for row in sorted(feed_rows, key=lambda item: int(item[7] or 0), reverse=True)[:5]
        if int(row[7] or 0) > 0
    ]
    latest_success = next(
        (str(row[3]) for row in feed_rows if str(row[3] or '').strip()),
        None,
    )
    latest_checked = next(
        (str(row[2]) for row in feed_rows if str(row[2] or '').strip()),
        None,
    )
    phases_by_job: dict[str, list[dict[str, object]]] = {}
    for row in phase_rows:
        job_id = str(row[1] or '')
        if not job_id:
            continue
        phases_by_job.setdefault(job_id, []).append(
            {
                'phase_id': str(row[0] or ''),
                'phase_key': str(row[2] or ''),
                'phase_label': str(row[3] or ''),
                'attempt': int(row[4] or 1),
                'status': str(row[5] or ''),
                'message': str(row[6] or ''),
                'error_detail': str(row[7] or ''),
                'started_at': str(row[8] or ''),
                'finished_at': str(row[9] or ''),
                'duration_ms': row[10],
            }
        )
    recent_runs = [
        {
            'job_id': str(row[0] or ''),
            'trigger_type': str(row[1] or ''),
            'status': str(row[2] or ''),
            'created_at': str(row[3] or ''),
            'started_at': str(row[4] or ''),
            'finished_at': str(row[5] or ''),
            'duration_ms': row[6],
            'imported_sources': int(row[7] or 0),
            'final_message': str(row[8] or ''),
            'error_message': str(row[9] or ''),
            'phases': phases_by_job.get(str(row[0] or ''), []),
        }
        for row in recent_job_rows
    ]
    duration_values = [int(row[0] or 0) for row in completed_duration_rows if int(row[0] or 0) > 0]
    if duration_values:
        avg_duration_ms = max(1000, int(sum(duration_values) / len(duration_values)))
    running_run = next((run for run in recent_runs if str(run.get('status') or '') == 'running'), None)
    if running_run is not None and avg_duration_ms is not None:
        started_dt = _parse_iso(str(running_run.get('started_at') or running_run.get('created_at') or ''))
        if started_dt is not None:
            elapsed_ms = max(0, int((reference_now - started_dt).total_seconds() * 1000))
            running_run['elapsed_ms'] = elapsed_ms
            eta_seconds = max(0, int((avg_duration_ms - elapsed_ms) / 1000))
    return {
        'actor_id': actor_id,
        'actor_name': str(actor_row[0]),
        'is_tracked': bool(actor_row[1]),
        'notebook_status': str(actor_row[2]),
        'auto_refresh_last_run_at': str(actor_row[3] or '') or None,
        'auto_refresh_last_status': str(actor_row[4] or '') or None,
        'feed_state': {
            'total_feeds': len(feed_rows),
            'failing_feeds': len(failing),
            'backoff_feeds': len(backoff),
            'latest_checked_at': latest_checked,
            'latest_success_at': latest_success,
            'top_failures': top_failures,
        },
        'source_state': {
            'total_sources': int(source_stats[0] or 0) if source_stats else 0,
            'high_confidence_sources': int(source_stats[1] or 0) if source_stats else 0,
            'recent_high_confidence_sources_24h': int(source_stats[2] or 0) if source_stats else 0,
        },
        'llm_cache_state': {
            'cache_hits': int(llm_cache_row[0] or 0) if llm_cache_row else 0,
            'saved_ms_total': int(llm_cache_row[1] or 0) if llm_cache_row else 0,
        },
        'eta_seconds': eta_seconds,
        'avg_duration_ms': avg_duration_ms,
        'recent_generation_runs': recent_runs,
    }
