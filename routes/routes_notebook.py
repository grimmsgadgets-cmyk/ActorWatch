import html
import io
import sqlite3
import uuid
import csv

import services.observation_service as observation_service
import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response


def create_notebook_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _generate_actor_requirements = deps['generate_actor_requirements']
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _safe_json_string_list = deps['safe_json_string_list']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _templates = deps['templates']
    _actor_exists = deps['actor_exists']

    def _upsert_observation_with_history(
        connection: sqlite3.Connection,
        *,
        actor_id: str,
        item_type: str,
        item_key: str,
        note: str,
        source_ref: str,
        confidence: str,
        source_reliability: str,
        information_credibility: str,
        updated_by: str,
        updated_at: str,
    ) -> None:
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(actor_id, item_type, item_key)
            DO UPDATE SET
                note = excluded.note,
                source_ref = excluded.source_ref,
                confidence = excluded.confidence,
                source_reliability = excluded.source_reliability,
                information_credibility = excluded.information_credibility,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at
            ''',
            (
                str(uuid.uuid4()),
                actor_id,
                item_type,
                item_key,
                note,
                source_ref,
                confidence,
                source_reliability,
                information_credibility,
                updated_by,
                updated_at,
            ),
        )
        connection.execute(
            '''
            INSERT INTO analyst_observation_history (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(uuid.uuid4()),
                actor_id,
                item_type,
                item_key,
                note,
                source_ref,
                confidence,
                source_reliability,
                information_credibility,
                updated_by,
                updated_at,
            ),
        )

    def _fetch_analyst_observations(
        actor_id: str,
        *,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        normalized_filters = observation_service.normalize_observation_filters_core(
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
        )
        where_sql, params = observation_service.build_observation_where_clause_core(
            actor_id,
            filters=normalized_filters,
        )

        safe_limit: int | None = None
        if limit is not None:
            try:
                safe_limit = max(1, min(500, int(limit)))
            except Exception:
                safe_limit = 100
        try:
            safe_offset = max(0, int(offset))
        except Exception:
            safe_offset = 0

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            query = (
                '''
                SELECT item_type, item_key, note, source_ref, confidence,
                       source_reliability, information_credibility, updated_by, updated_at
                FROM analyst_observations
                WHERE '''
                + where_sql
                + '\nORDER BY updated_at DESC'
            )
            query_params: list[object] = list(params)
            if safe_limit is not None:
                query += '\nLIMIT ? OFFSET ?'
                query_params.extend([safe_limit, safe_offset])
            rows = connection.execute(query, query_params).fetchall()
            source_keys = observation_service.observation_source_keys_core(rows)
            source_lookup: dict[str, dict[str, str]] = {}
            if source_keys:
                for key_chunk in observation_service.source_lookup_chunks_core(source_keys, chunk_size=800):
                    placeholders = ','.join('?' for _ in key_chunk)
                    source_rows = connection.execute(
                        f'''
                        SELECT id, source_name, url, title, published_at, retrieved_at
                        FROM sources
                        WHERE actor_id = ? AND id IN ({placeholders})
                        ''',
                        (actor_id, *key_chunk),
                    ).fetchall()
                    source_lookup.update(
                        {
                            str(source_row[0]): {
                                'source_name': str(source_row[1] or ''),
                                'source_url': str(source_row[2] or ''),
                                'source_title': str(source_row[3] or ''),
                                'source_date': str(source_row[4] or source_row[5] or ''),
                            }
                            for source_row in source_rows
                        }
                    )
        return observation_service.map_observation_rows_core(rows, source_lookup=source_lookup)

    @router.post(route_paths.ACTOR_NOTEBOOK_REQUIREMENTS_GENERATE)
    async def generate_requirements(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        org_context = str(form_data.get('org_context', '')).strip()
        priority_mode = str(form_data.get('priority_mode', 'Operational')).strip()
        if priority_mode not in {'Strategic', 'Operational', 'Tactical'}:
            priority_mode = 'Operational'
        count = _generate_actor_requirements(actor_id, org_context, priority_mode)
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Generated+{count}+requirements',
            status_code=303,
        )

    @router.post('/requirements/{requirement_id}/resolve')
    async def resolve_requirement(requirement_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        actor_id = str(form_data.get('actor_id', '')).strip()
        with sqlite3.connect(_db_path()) as connection:
            row = connection.execute(
                'SELECT actor_id FROM requirement_items WHERE id = ?',
                (requirement_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='requirement not found')
            resolved_actor_id = str(row[0])
            connection.execute(
                'UPDATE requirement_items SET status = ? WHERE id = ?',
                ('resolved', requirement_id),
            )
            connection.commit()
        return RedirectResponse(url=f'/?actor_id={actor_id or resolved_actor_id}', status_code=303)

    @router.post('/questions/{thread_id}/resolve')
    async def resolve_question_thread(thread_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        actor_id = str(form_data.get('actor_id', '')).strip()

        with sqlite3.connect(_db_path()) as connection:
            row = connection.execute(
                'SELECT actor_id, status FROM question_threads WHERE id = ?',
                (thread_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='question thread not found')
            db_actor_id = row[0]
            if row[1] != 'resolved':
                connection.execute(
                    'UPDATE question_threads SET status = ?, updated_at = ? WHERE id = ?',
                    ('resolved', _utc_now_iso(), thread_id),
                )
            connection.commit()

        return RedirectResponse(url=f'/?actor_id={actor_id or db_actor_id}', status_code=303)

    @router.get(route_paths.ACTOR_TIMELINE_DETAILS, response_class=HTMLResponse)
    def actor_timeline_details(actor_id: str) -> HTMLResponse:
        with sqlite3.connect(_db_path()) as connection:
            actor_row = connection.execute(
                'SELECT id, display_name FROM actor_profiles WHERE id = ?',
                (actor_id,),
            ).fetchone()
            if actor_row is None:
                raise HTTPException(status_code=404, detail='actor not found')

            rows = connection.execute(
                '''
                SELECT
                    te.occurred_at, te.category, te.title, te.summary, te.target_text, te.ttp_ids_json,
                    s.source_name, s.url, s.published_at, s.title, s.headline, s.og_title, s.html_title
                FROM timeline_events te
                LEFT JOIN sources s ON s.id = te.source_id
                WHERE te.actor_id = ?
                ORDER BY te.occurred_at DESC
                ''',
                (actor_id,),
            ).fetchall()

        detail_rows: list[dict[str, object]] = []
        for row in rows:
            detail_rows.append(
                {
                    'occurred_at': row[0],
                    'category': str(row[1]).replace('_', ' '),
                    'title': row[2],
                    'summary': row[3],
                    'target_text': row[4] or '',
                    'ttp_ids': _safe_json_string_list(row[5]),
                    'source_name': row[6] or '',
                    'source_url': row[7] or '',
                    'source_published_at': row[8] or '',
                    'source_title': row[9] or row[10] or row[11] or row[12] or '',
                }
            )

        content = ['<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">']
        content.append('<title>Timeline Details</title>')
        content.append(
            '<style>'
            'body{font-family:Arial,sans-serif;background:#f7f7f7;color:#111;margin:0;padding:16px;}'
            '.wrap{max-width:980px;margin:0 auto;}'
            '.top{margin-bottom:12px;}'
            '.card{background:#fff;border:1px solid #ddd;border-radius:10px;padding:10px;margin-bottom:10px;}'
            '.meta{font-size:12px;color:#334155;margin:4px 0 8px;}'
            '.summary{white-space:pre-line;}'
            '.badge{display:inline-block;padding:2px 8px;border-radius:999px;border:1px solid #888;font-size:12px;}'
            '.muted{color:#4b5563;font-size:12px;}'
            'a{color:#2255aa;text-decoration:none;}a:hover{text-decoration:underline;}'
            '</style>'
        )
        content.append('</head><body><div class="wrap">')
        content.append(
            f'<div class="top"><a href="/?actor_id={actor_id}">← Back to dashboard</a>'
            f'<h1>Timeline Details: {html.escape(str(actor_row[1]))}</h1>'
            f'<div class="muted">Full activity evidence view for this actor.</div></div>'
        )

        if not detail_rows:
            content.append('<div class="card">No timeline entries yet.</div>')
        else:
            for item in detail_rows:
                ttp_text = ', '.join(item['ttp_ids']) if item['ttp_ids'] else ''
                source_block = ''
                event_title = str(item.get('title') or item.get('source_title') or item.get('summary') or '').strip()
                if event_title.startswith(('http://', 'https://')):
                    event_title = str(item.get('summary') or item.get('title') or 'Untitled report').strip()
                if 'who/what/when/where/how' in event_title.lower():
                    fallback_title = str(item.get('title') or '').strip()
                    if fallback_title and 'who/what/when/where/how' not in fallback_title.lower():
                        event_title = fallback_title
                    else:
                        event_title = 'Ransomware disclosure and targeting update'
                if item['source_url']:
                    source_name = html.escape(str(item['source_name']) or str(item['source_url']))
                    source_url = html.escape(str(item['source_url']))
                    source_pub = html.escape(str(item['source_published_at'] or 'unknown'))
                    source_block = (
                        f'<div class="meta">Source: <a href="{source_url}" target="_blank" rel="noreferrer">{source_name}</a> '
                        f'| Published: {source_pub}</div>'
                    )
                content.append('<div class="card">')
                content.append(
                    f'<div><span class="badge">{html.escape(str(item["category"]))}</span> '
                    f'<span class="muted">{html.escape(str(item["occurred_at"]))}</span></div>'
                )
                content.append(f'<h3>{html.escape(event_title)}</h3>')
                content.append(f'<div class="summary">{html.escape(str(item["summary"]))}</div>')
                if item['target_text']:
                    content.append(f'<div class="meta"><strong>Target:</strong> {html.escape(str(item["target_text"]))}</div>')
                if ttp_text:
                    content.append(f'<div class="meta"><strong>Techniques:</strong> {html.escape(ttp_text)}</div>')
                content.append(source_block)
                content.append('</div>')

        content.append('</div></body></html>')
        return HTMLResponse(''.join(content))

    @router.get(route_paths.ACTOR_QUESTIONS_WORKSPACE, response_class=HTMLResponse)
    def actor_questions_workspace(
        request: Request,
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> HTMLResponse:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        return _templates.TemplateResponse(
            request,
            'questions.html',
            {
                'actor_id': actor_id,
                'notebook': notebook,
            },
        )

    @router.get(route_paths.ACTOR_UI_LIVE, response_class=JSONResponse)
    def actor_live_state(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        return {
            'actor_id': actor_id,
            'notebook_status': str(notebook.get('actor', {}).get('notebook_status') or 'idle'),
            'notebook_message': str(notebook.get('actor', {}).get('notebook_message') or ''),
            'kpis': notebook.get('kpis', {}),
            'recent_change_summary': notebook.get('recent_change_summary', {}),
            'priority_questions': notebook.get('priority_questions', []),
            'timeline_compact_rows': notebook.get('timeline_compact_rows', []),
            'timeline_window_label': notebook.get('timeline_window_label', ''),
        }

    @router.get(route_paths.ACTOR_OBSERVATIONS, response_class=JSONResponse)
    def list_observations(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=limit,
            offset=offset,
        )
        return {
            'actor_id': actor_id,
            'limit': max(1, min(500, int(limit))),
            'offset': max(0, int(offset)),
            'items': items,
        }

    @router.get(route_paths.ACTOR_OBSERVATIONS_EXPORT_JSON, response_class=JSONResponse)
    def export_observations_json(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
    ) -> dict[str, object]:
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=None,
            offset=0,
        )
        return {
            'actor_id': actor_id,
            'count': len(items),
            'filters': {
                'analyst': analyst or '',
                'confidence': confidence or '',
                'updated_from': updated_from or '',
                'updated_to': updated_to or '',
            },
            'items': items,
        }

    @router.get(route_paths.ACTOR_EXPORT_ANALYST_PACK, response_class=JSONResponse)
    def export_analyst_pack(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        observations = _fetch_analyst_observations(actor_id, limit=None, offset=0)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            history_rows = connection.execute(
                '''
                SELECT item_type, item_key, note, source_ref, confidence,
                       source_reliability, information_credibility, updated_by, updated_at
                FROM analyst_observation_history
                WHERE actor_id = ?
                ORDER BY updated_at DESC
                LIMIT 1000
                ''',
                (actor_id,),
            ).fetchall()
        history_items = [
            {
                'item_type': str(row[0] or ''),
                'item_key': str(row[1] or ''),
                'note': str(row[2] or ''),
                'source_ref': str(row[3] or ''),
                'confidence': str(row[4] or 'moderate'),
                'source_reliability': str(row[5] or ''),
                'information_credibility': str(row[6] or ''),
                'updated_by': str(row[7] or ''),
                'updated_at': str(row[8] or ''),
            }
            for row in history_rows
        ]
        quality_filters = notebook.get('source_quality_filters', {})
        quality_filters_dict = quality_filters if isinstance(quality_filters, dict) else {}
        source_scope_active = any(
            str(quality_filters_dict.get(key) or '').strip()
            for key in ('source_tier', 'min_confidence_weight', 'source_days')
        )
        if source_scope_active:
            allowed_source_ids = {
                str(item.get('id') or '').strip()
                for item in (notebook.get('sources', []) if isinstance(notebook.get('sources', []), list) else [])
                if isinstance(item, dict) and str(item.get('id') or '').strip()
            }
            observations = [
                item
                for item in observations
                if str(item.get('item_type') or '').strip().lower() != 'source'
                or str(item.get('item_key') or '').strip() in allowed_source_ids
            ]
            history_items = [
                item
                for item in history_items
                if str(item.get('item_type') or '').strip().lower() != 'source'
                or str(item.get('item_key') or '').strip() in allowed_source_ids
            ]
        return {
            'actor_id': actor_id,
            'exported_at': _utc_now_iso(),
            'source_quality_filters': quality_filters_dict,
            'actor': notebook.get('actor', {}),
            'recent_change_summary': notebook.get('recent_change_summary', {}),
            'priority_questions': notebook.get('priority_questions', [])[:3],
            'observations': observations,
            'observation_history': history_items,
        }

    @router.get(route_paths.ACTOR_OBSERVATIONS_EXPORT_CSV)
    def export_observations_csv(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
    ) -> Response:
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=None,
            offset=0,
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                'actor_id',
                'item_type',
                'item_key',
                'note',
                'source_ref',
                'confidence',
                'source_reliability',
                'information_credibility',
                'updated_by',
                'updated_at',
                'source_name',
                'source_title',
                'source_url',
                'source_date',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    actor_id,
                    item.get('item_type', ''),
                    item.get('item_key', ''),
                    item.get('note', ''),
                    item.get('source_ref', ''),
                    item.get('confidence', ''),
                    item.get('source_reliability', ''),
                    item.get('information_credibility', ''),
                    item.get('updated_by', ''),
                    item.get('updated_at', ''),
                    item.get('source_name', ''),
                    item.get('source_title', ''),
                    item.get('source_url', ''),
                    item.get('source_date', ''),
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename="{actor_id}-observations.csv"',
            },
        )

    @router.post(route_paths.ACTOR_OBSERVATION_UPSERT, response_class=JSONResponse)
    async def upsert_observation(actor_id: str, item_type: str, item_key: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()

        note = str(payload.get('note') or '').strip()[:4000]
        source_ref = str(payload.get('source_ref') or '').strip()[:500]
        confidence = str(payload.get('confidence') or 'moderate').strip().lower()
        if confidence not in {'low', 'moderate', 'high'}:
            confidence = 'moderate'
        source_reliability = str(payload.get('source_reliability') or '').strip().upper()[:1]
        if source_reliability and source_reliability not in {'A', 'B', 'C', 'D', 'E', 'F'}:
            source_reliability = ''
        information_credibility = str(payload.get('information_credibility') or '').strip()[:1]
        if information_credibility and information_credibility not in {'1', '2', '3', '4', '5', '6'}:
            information_credibility = ''
        updated_by = str(payload.get('updated_by') or '').strip()[:120]
        updated_at = _utc_now_iso()
        quality_guidance = observation_service.observation_quality_guidance_core(
            note=note,
            source_ref=source_ref,
            confidence=confidence,
            source_reliability=source_reliability,
            information_credibility=information_credibility,
        )

        safe_item_type = item_type.strip().lower()[:40]
        safe_item_key = item_key.strip()[:200]
        if not safe_item_type or not safe_item_key:
            raise HTTPException(status_code=400, detail='invalid observation key')

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            _upsert_observation_with_history(
                connection,
                actor_id=actor_id,
                item_type=safe_item_type,
                item_key=safe_item_key,
                note=note,
                source_ref=source_ref,
                confidence=confidence,
                source_reliability=source_reliability,
                information_credibility=information_credibility,
                updated_by=updated_by,
                updated_at=updated_at,
            )
            connection.commit()

        return {
            'ok': True,
            'item_type': safe_item_type,
            'item_key': safe_item_key,
            'note': note,
            'source_ref': source_ref,
            'confidence': confidence,
            'source_reliability': source_reliability,
            'information_credibility': information_credibility,
            'updated_by': updated_by,
            'updated_at': updated_at,
            'quality_guidance': quality_guidance,
        }

    @router.post(route_paths.ACTOR_OBSERVATIONS_AUTO_SNAPSHOT)
    async def auto_snapshot_observations(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        notebook = _fetch_actor_notebook(actor_id)
        highlights = notebook.get('recent_activity_highlights', [])
        entries = highlights if isinstance(highlights, list) else []
        saved = 0
        updated_at = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            for item in entries[:5]:
                if not isinstance(item, dict):
                    continue
                item_key = str(item.get('source_id') or item.get('timeline_event_id') or '').strip()[:200]
                if not item_key:
                    continue
                title = str(item.get('evidence_title') or item.get('source_name') or 'source').strip()
                date = str(item.get('date') or '').strip()
                summary = str(item.get('text') or '').strip()
                note_parts = [part for part in [f'{date} {title}'.strip(), summary] if part]
                note = 'Auto: ' + ' | '.join(note_parts)
                _upsert_observation_with_history(
                    connection,
                    actor_id=actor_id,
                    item_type='source',
                    item_key=item_key,
                    note=note[:4000],
                    source_ref=f'auto-snapshot:{updated_at[:10]}',
                    confidence='moderate',
                    source_reliability='',
                    information_credibility='',
                    updated_by='auto',
                    updated_at=updated_at,
                )
                saved += 1
            if saved == 0:
                source_rows = connection.execute(
                    '''
                    SELECT id, title, source_name, published_at
                    FROM sources
                    WHERE actor_id = ?
                    ORDER BY COALESCE(published_at, retrieved_at) DESC
                    LIMIT 5
                    ''',
                    (actor_id,),
                ).fetchall()
                for row in source_rows:
                    source_id = str(row[0] or '').strip()[:200]
                    if not source_id:
                        continue
                    title = str(row[1] or row[2] or 'source').strip()
                    published_at = str(row[3] or '').strip()
                    note = f'Auto: {published_at} {title}'.strip()
                    _upsert_observation_with_history(
                        connection,
                        actor_id=actor_id,
                        item_type='source',
                        item_key=source_id,
                        note=note[:4000],
                        source_ref=f'auto-snapshot:{updated_at[:10]}',
                        confidence='moderate',
                        source_reliability='',
                        information_credibility='',
                        updated_by='auto',
                        updated_at=updated_at,
                    )
                    saved += 1
            connection.commit()
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Auto-noted+{saved}+recent+changes',
            status_code=303,
        )

    @router.get(route_paths.ACTOR_OBSERVATION_HISTORY, response_class=JSONResponse)
    def observation_history(actor_id: str, item_type: str, item_key: str, limit: int = 25) -> dict[str, object]:
        safe_item_type = item_type.strip().lower()[:40]
        safe_item_key = item_key.strip()[:200]
        if not safe_item_type or not safe_item_key:
            raise HTTPException(status_code=400, detail='invalid observation key')
        safe_limit = max(1, min(100, int(limit)))

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            rows = connection.execute(
                '''
                SELECT note, source_ref, confidence, source_reliability,
                       information_credibility, updated_by, updated_at
                FROM analyst_observation_history
                WHERE actor_id = ? AND item_type = ? AND item_key = ?
                ORDER BY updated_at DESC
                LIMIT ?
                ''',
                (actor_id, safe_item_type, safe_item_key, safe_limit),
            ).fetchall()
            if not rows:
                latest_row = connection.execute(
                    '''
                    SELECT note, source_ref, confidence, source_reliability,
                           information_credibility, updated_by, updated_at
                    FROM analyst_observations
                    WHERE actor_id = ? AND item_type = ? AND item_key = ?
                    ''',
                    (actor_id, safe_item_type, safe_item_key),
                ).fetchone()
                if latest_row is not None:
                    rows = [latest_row]

        items = [
            {
                'note': str(row[0] or ''),
                'source_ref': str(row[1] or ''),
                'confidence': str(row[2] or 'moderate'),
                'source_reliability': str(row[3] or ''),
                'information_credibility': str(row[4] or ''),
                'updated_by': str(row[5] or ''),
                'updated_at': str(row[6] or ''),
            }
            for row in rows
        ]
        return {
            'actor_id': actor_id,
            'item_type': safe_item_type,
            'item_key': safe_item_key,
            'count': len(items),
            'items': items,
        }

    return router
