import sqlite3
import uuid
from urllib.parse import urlparse

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import RedirectResponse


def create_actor_ops_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    _enforce_request_size = deps['enforce_request_size']
    _source_upload_body_limit_bytes = deps['source_upload_body_limit_bytes']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _derive_source_from_url = deps['derive_source_from_url']
    _upsert_source_for_actor = deps['upsert_source_for_actor']
    _import_default_feeds_for_actor = deps['import_default_feeds_for_actor']
    _parse_ioc_values = deps['parse_ioc_values']
    _utc_now_iso = deps['utc_now_iso']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _enqueue_actor_generation = deps.get('enqueue_actor_generation', deps['run_actor_generation'])

    @router.post('/actors/{actor_id}/sources')
    async def add_source(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _source_upload_body_limit_bytes)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')

        form_data = await request.form()
        source_url = str(form_data.get('source_url', '')).strip()

        source_name = str(form_data.get('source_name', '')).strip()
        published_at = str(form_data.get('published_at', '')).strip() or None
        pasted_text = str(form_data.get('pasted_text', '')).strip()
        trigger_excerpt = str(form_data.get('trigger_excerpt', '')).strip() or None
        source_title: str | None = None
        source_headline: str | None = None
        source_og_title: str | None = None
        source_html_title: str | None = None
        source_publisher: str | None = None
        source_site_name: str | None = None

        if not source_url:
            raise HTTPException(status_code=400, detail='source_url is required')

        if not pasted_text:
            derived = _derive_source_from_url(source_url)
            source_name = str(derived['source_name'])
            source_url = str(derived['source_url'])
            published_at = str(derived['published_at']) if derived['published_at'] else published_at
            pasted_text = str(derived['pasted_text'])
            trigger_excerpt = str(derived['trigger_excerpt']) if derived['trigger_excerpt'] else trigger_excerpt
            source_title = str(derived.get('title') or '') or None
            source_headline = str(derived.get('headline') or '') or None
            source_og_title = str(derived.get('og_title') or '') or None
            source_html_title = str(derived.get('html_title') or '') or None
            source_publisher = str(derived.get('publisher') or '') or None
            source_site_name = str(derived.get('site_name') or '') or None
        elif not source_name:
            parsed = urlparse(source_url)
            source_name = (parsed.hostname or parsed.netloc or 'Manual source').strip()

        with sqlite3.connect(_db_path()) as connection:
            _upsert_source_for_actor(
                connection,
                actor_id,
                source_name,
                source_url,
                published_at,
                pasted_text,
                trigger_excerpt,
                source_title,
                source_headline,
                source_og_title,
                source_html_title,
                source_publisher,
                source_site_name,
            )
            connection.commit()

        return RedirectResponse(url=f'/?actor_id={actor_id}', status_code=303)

    @router.post('/actors/{actor_id}/sources/import-feeds')
    def import_feeds(actor_id: str) -> RedirectResponse:
        _import_default_feeds_for_actor(actor_id)
        return RedirectResponse(url=f'/?actor_id={actor_id}', status_code=303)

    @router.post('/actors/{actor_id}/iocs')
    async def add_iocs(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')

        form_data = await request.form()
        ioc_type = str(form_data.get('ioc_type', 'indicator')).strip() or 'indicator'
        ioc_values_raw = str(form_data.get('ioc_values', '')).strip()
        source_ref = str(form_data.get('source_ref', '')).strip() or None

        values = _parse_ioc_values(ioc_values_raw)
        if not values:
            raise HTTPException(status_code=400, detail='ioc_values is required')

        with sqlite3.connect(_db_path()) as connection:
            for value in values:
                connection.execute(
                    '''
                    INSERT INTO ioc_items (id, actor_id, ioc_type, ioc_value, source_ref, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (str(uuid.uuid4()), actor_id, ioc_type, value, source_ref, _utc_now_iso()),
                )
            connection.commit()

        return RedirectResponse(url=f'/?actor_id={actor_id}', status_code=303)

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
        _enqueue_actor_generation(actor_id)
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Notebook refresh started',
            status_code=303,
        )

    return router
