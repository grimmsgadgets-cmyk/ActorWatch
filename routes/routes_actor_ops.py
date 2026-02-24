import sqlite3
import uuid
from urllib.parse import urlparse

import route_paths
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse


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
    _upsert_ioc_item = deps['upsert_ioc_item']
    _export_actor_stix_bundle = deps['export_actor_stix_bundle']
    _import_actor_stix_bundle = deps['import_actor_stix_bundle']
    _utc_now_iso = deps['utc_now_iso']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _get_actor_refresh_stats = deps['get_actor_refresh_stats']
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
        handling_tlp = str(form_data.get('handling_tlp', 'TLP:CLEAR')).strip().upper() or 'TLP:CLEAR'
        if handling_tlp not in {'TLP:CLEAR', 'TLP:GREEN', 'TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:RED'}:
            handling_tlp = 'TLP:CLEAR'

        values = _parse_ioc_values(ioc_values_raw)
        if not values:
            raise HTTPException(status_code=400, detail='ioc_values is required')

        inserted = 0
        skipped: list[str] = []
        with sqlite3.connect(_db_path()) as connection:
            for value in values:
                result = _upsert_ioc_item(
                    connection,
                    actor_id=actor_id,
                    raw_ioc_type=ioc_type,
                    raw_ioc_value=value,
                    source_ref=source_ref,
                    source_id=None,
                    source_tier=None,
                    extraction_method='manual',
                    now_iso=_utc_now_iso(),
                    lifecycle_status='active',
                    handling_tlp=handling_tlp,
                )
                if bool(result.get('stored')):
                    inserted += 1
                else:
                    skipped.append(str(result.get('reason') or 'invalid IOC'))
            if inserted == 0:
                raise HTTPException(status_code=400, detail=f'No valid IOCs saved: {", ".join(skipped[:3])}')
            connection.commit()
        notice = f'Saved+{inserted}+IOC(s)'
        if skipped:
            notice += f'.+Skipped+{len(skipped)}+invalid/suppressed'
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice={notice}', status_code=303)

    @router.post('/actors/{actor_id}/iocs/{ioc_id}/status')
    async def update_ioc_status(actor_id: str, ioc_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        lifecycle_status = str(form_data.get('lifecycle_status', 'active')).strip().lower() or 'active'
        if lifecycle_status not in {'active', 'monitor', 'superseded', 'revoked', 'false_positive'}:
            lifecycle_status = 'active'
        handling_tlp = str(form_data.get('handling_tlp', 'TLP:CLEAR')).strip().upper() or 'TLP:CLEAR'
        if handling_tlp not in {'TLP:CLEAR', 'TLP:GREEN', 'TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:RED'}:
            handling_tlp = 'TLP:CLEAR'
        status_reason = str(form_data.get('status_reason', '')).strip()[:240]
        updated_at = _utc_now_iso()

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            row = connection.execute(
                '''
                SELECT id, ioc_type, ioc_value, normalized_value, confidence_score, source_id, source_ref, extraction_method
                FROM ioc_items
                WHERE id = ? AND actor_id = ?
                ''',
                (ioc_id, actor_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='ioc not found')
            is_active = 1 if lifecycle_status in {'active', 'monitor'} else 0
            connection.execute(
                '''
                UPDATE ioc_items
                SET lifecycle_status = ?, handling_tlp = ?, is_active = ?, updated_at = ?, validation_reason = ?
                WHERE id = ? AND actor_id = ?
                ''',
                (
                    lifecycle_status,
                    handling_tlp,
                    is_active,
                    updated_at,
                    status_reason or '',
                    ioc_id,
                    actor_id,
                ),
            )
            connection.execute(
                '''
                INSERT INTO ioc_history (
                    id, ioc_item_id, actor_id, event_type, ioc_type, ioc_value, normalized_value,
                    validation_status, validation_reason, confidence_score, source_id, source_ref,
                    extraction_method, lifecycle_status, handling_tlp, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    str(uuid.uuid4()),
                    str(row[0]),
                    actor_id,
                    'status_change',
                    str(row[1] or ''),
                    str(row[2] or ''),
                    str(row[3] or ''),
                    'valid',
                    status_reason or '',
                    int(row[4] or 0),
                    str(row[5] or ''),
                    str(row[6] or ''),
                    str(row[7] or ''),
                    lifecycle_status,
                    handling_tlp,
                    updated_at,
                ),
            )
            connection.commit()
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=IOC+status+updated', status_code=303)

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

    @router.get('/actors/{actor_id}/refresh/stats')
    def actor_refresh_stats(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
        return _get_actor_refresh_stats(actor_id)

    @router.get(route_paths.ACTOR_STIX_EXPORT, response_class=JSONResponse)
    def export_stix_bundle(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            row = connection.execute(
                'SELECT display_name FROM actor_profiles WHERE id = ?',
                (actor_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='actor not found')
            return _export_actor_stix_bundle(
                connection,
                actor_id=actor_id,
                actor_name=str(row[0] or actor_id),
            )

    @router.post(route_paths.ACTOR_STIX_IMPORT, response_class=JSONResponse)
    async def import_stix_bundle(actor_id: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail='invalid STIX bundle payload')
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            result = _import_actor_stix_bundle(
                connection,
                actor_id=actor_id,
                bundle=payload,
            )
            connection.commit()
        return {
            'actor_id': actor_id,
            **result,
        }

    return router
