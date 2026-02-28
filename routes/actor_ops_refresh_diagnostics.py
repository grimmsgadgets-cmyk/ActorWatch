import sqlite3

import route_paths
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse


def register_actor_refresh_and_diagnostic_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _get_actor_refresh_stats = deps['get_actor_refresh_stats']
    _get_actor_refresh_timeline = deps.get('get_actor_refresh_timeline')
    _submit_actor_refresh_job = deps.get('submit_actor_refresh_job')
    _get_actor_refresh_job = deps.get('get_actor_refresh_job')
    _enqueue_actor_generation = deps['enqueue_actor_generation']

    @router.post('/actors/{actor_id}/refresh')
    def refresh_notebook(actor_id: str, background_tasks: BackgroundTasks) -> RedirectResponse:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Refreshing sources, questions, and timeline entries...',
        )
        if callable(_submit_actor_refresh_job):
            _submit_actor_refresh_job(actor_id, trigger_type='manual_refresh')
        else:
            _enqueue_actor_generation(actor_id)
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Notebook refresh started',
            status_code=303,
        )

    @router.post(route_paths.ACTOR_REFRESH_JOBS, response_class=JSONResponse)
    def submit_refresh_job(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        if callable(_submit_actor_refresh_job):
            return _submit_actor_refresh_job(actor_id, trigger_type='manual_refresh')
        queued = bool(_enqueue_actor_generation(actor_id))
        return {
            'actor_id': actor_id,
            'job_id': '',
            'status': 'queued' if queued else 'running',
            'queued': queued,
            'message': 'Refresh job queued.' if queued else 'Refresh already in progress.',
        }

    @router.get('/actors/{actor_id}/refresh/stats')
    def actor_refresh_stats(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        return _get_actor_refresh_stats(actor_id)

    @router.get(route_paths.ACTOR_REFRESH_TIMELINE, response_class=JSONResponse)
    def actor_refresh_timeline(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        if callable(_get_actor_refresh_timeline):
            return _get_actor_refresh_timeline(actor_id)
        stats = _get_actor_refresh_stats(actor_id)
        return {
            'actor_id': actor_id,
            'recent_generation_runs': stats.get('recent_generation_runs', []),
            'eta_seconds': stats.get('eta_seconds'),
            'avg_duration_ms': stats.get('avg_duration_ms'),
            'llm_cache_state': stats.get('llm_cache_state', {}),
            'queue_state': {},
        }

    @router.get(route_paths.ACTOR_REFRESH_JOB_DETAIL, response_class=JSONResponse)
    def actor_refresh_job_detail(actor_id: str, job_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        if not callable(_get_actor_refresh_job):
            raise HTTPException(status_code=404, detail='refresh job endpoint unavailable')
        return _get_actor_refresh_job(actor_id, job_id)

    @router.get(route_paths.ACTOR_INGEST_DIAGNOSTICS, response_class=JSONResponse)
    def actor_ingest_diagnostics(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            try:
                stage_rows = connection.execute(
                    '''
                    SELECT stage, decision, COUNT(*)
                    FROM ingest_decisions
                    WHERE actor_id = ?
                    GROUP BY stage, decision
                    ''',
                    (actor_id,),
                ).fetchall()
                rejection_rows = connection.execute(
                    '''
                    SELECT reason_code, COUNT(*)
                    FROM ingest_decisions
                    WHERE actor_id = ? AND decision = 'rejected'
                    GROUP BY reason_code
                    ORDER BY COUNT(*) DESC, reason_code ASC
                    LIMIT 8
                    ''',
                    (actor_id,),
                ).fetchall()
                recent_rows = connection.execute(
                    '''
                    SELECT stage, decision, reason_code, details_json, created_at
                    FROM ingest_decisions
                    WHERE actor_id = ?
                    ORDER BY created_at DESC
                    LIMIT 20
                    ''',
                    (actor_id,),
                ).fetchall()
                source_quality_rows = connection.execute(
                    '''
                    SELECT
                        COALESCE(NULLIF(TRIM(source_tier), ''), 'unrated'),
                        COALESCE(confidence_weight, 0),
                        COALESCE(NULLIF(TRIM(source_type), ''), 'unknown'),
                        COUNT(*)
                    FROM sources
                    WHERE actor_id = ?
                    GROUP BY
                        COALESCE(NULLIF(TRIM(source_tier), ''), 'unrated'),
                        COALESCE(confidence_weight, 0),
                        COALESCE(NULLIF(TRIM(source_type), ''), 'unknown')
                    ORDER BY COUNT(*) DESC, source_tier ASC, source_type ASC
                    ''',
                    (actor_id,),
                ).fetchall()
                total_sources_row = connection.execute(
                    'SELECT COUNT(*) FROM sources WHERE actor_id = ?',
                    (actor_id,),
                ).fetchone()
                total_timeline_row = connection.execute(
                    'SELECT COUNT(*) FROM timeline_events WHERE actor_id = ?',
                    (actor_id,),
                ).fetchone()
                eligible_sources_row = connection.execute(
                    '''
                    SELECT COUNT(*)
                    FROM sources
                    WHERE actor_id = ?
                      AND LOWER(COALESCE(source_type, '')) <> 'feed_soft_match'
                      AND COALESCE(confidence_weight, 0) >= 2
                    ''',
                    (actor_id,),
                ).fetchone()
                eligible_timeline_row = connection.execute(
                    '''
                    SELECT COUNT(*)
                    FROM timeline_events te
                    JOIN sources s ON s.id = te.source_id
                    WHERE te.actor_id = ?
                      AND LOWER(COALESCE(s.source_type, '')) <> 'feed_soft_match'
                      AND COALESCE(s.confidence_weight, 0) >= 2
                    ''',
                    (actor_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                stage_rows = []
                rejection_rows = []
                recent_rows = []
                source_quality_rows = []
                total_sources_row = (0,)
                total_timeline_row = (0,)
                eligible_sources_row = (0,)
                eligible_timeline_row = (0,)

        stage_breakdown: dict[str, dict[str, int]] = {}
        for row in stage_rows:
            stage = str(row[0] or '').strip() or 'unknown'
            decision = str(row[1] or '').strip() or 'unknown'
            count = int(row[2] or 0)
            stage_bucket = stage_breakdown.setdefault(stage, {'accepted': 0, 'rejected': 0, 'other': 0, 'total': 0})
            if decision == 'accepted':
                stage_bucket['accepted'] += count
            elif decision == 'rejected':
                stage_bucket['rejected'] += count
            else:
                stage_bucket['other'] += count
            stage_bucket['total'] += count

        totals = {
            'accepted': sum(values.get('accepted', 0) for values in stage_breakdown.values()),
            'rejected': sum(values.get('rejected', 0) for values in stage_breakdown.values()),
            'other': sum(values.get('other', 0) for values in stage_breakdown.values()),
        }
        totals['all'] = totals['accepted'] + totals['rejected'] + totals['other']

        top_rejections = [
            {
                'reason_code': str(row[0] or '').strip() or 'unknown',
                'count': int(row[1] or 0),
            }
            for row in rejection_rows
        ]
        recent = [
            {
                'stage': str(row[0] or '').strip() or 'unknown',
                'decision': str(row[1] or '').strip() or 'unknown',
                'reason_code': str(row[2] or '').strip() or '',
                'details_json': str(row[3] or '{}'),
                'created_at': str(row[4] or ''),
            }
            for row in recent_rows
        ]
        quality_mix = [
            {
                'source_tier': str(row[0] or 'unrated'),
                'confidence_weight': int(row[1] or 0),
                'source_type': str(row[2] or 'unknown'),
                'count': int(row[3] or 0),
            }
            for row in source_quality_rows
        ]
        default_surface_estimate = {
            'eligible_sources': int(eligible_sources_row[0] or 0) if eligible_sources_row else 0,
            'eligible_timeline_events': int(eligible_timeline_row[0] or 0) if eligible_timeline_row else 0,
        }
        totals_snapshot = {
            'sources': int(total_sources_row[0] or 0) if total_sources_row else 0,
            'timeline_events': int(total_timeline_row[0] or 0) if total_timeline_row else 0,
        }

        return {
            'actor_id': actor_id,
            'funnel_totals': totals,
            'stage_breakdown': stage_breakdown,
            'top_rejection_reasons': top_rejections,
            'recent_decisions': recent,
            'quality_mix': quality_mix,
            'default_surface_estimate': default_surface_estimate,
            'totals_snapshot': totals_snapshot,
        }
