import sqlite3
from datetime import datetime, timedelta, timezone
from threading import Event


def run_tracked_actor_auto_refresh_once_core(
    *,
    db_path: str,
    min_interval_hours: int,
    limit: int,
    deps: dict[str, object],
) -> int:
    _parse_published_datetime = deps['parse_published_datetime']
    _enqueue_actor_generation = deps['enqueue_actor_generation']
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
        _enqueue_actor_generation(actor_id)
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
    reference_now = now_utc or datetime.now(timezone.utc)
    freshness_cutoff = reference_now - timedelta(hours=24)
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
    }
