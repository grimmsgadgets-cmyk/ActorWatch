import html
import sqlite3

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse


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

    @router.post('/actors/{actor_id}/requirements/generate')
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

    @router.get('/actors/{actor_id}/timeline/details', response_class=HTMLResponse)
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
                    s.source_name, s.url, s.published_at
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
                content.append(f'<h3>{html.escape(str(item["title"]))}</h3>')
                content.append(f'<div>{html.escape(str(item["summary"]))}</div>')
                if item['target_text']:
                    content.append(f'<div class="meta"><strong>Target:</strong> {html.escape(str(item["target_text"]))}</div>')
                if ttp_text:
                    content.append(f'<div class="meta"><strong>Techniques:</strong> {html.escape(ttp_text)}</div>')
                content.append(source_block)
                content.append('</div>')

        content.append('</div></body></html>')
        return HTMLResponse(''.join(content))

    @router.get('/actors/{actor_id}/questions', response_class=HTMLResponse)
    def actor_questions_workspace(request: Request, actor_id: str) -> HTMLResponse:
        notebook = _fetch_actor_notebook(actor_id)
        return _templates.TemplateResponse(
            request,
            'questions.html',
            {
                'actor_id': actor_id,
                'notebook': notebook,
            },
        )

    return router
