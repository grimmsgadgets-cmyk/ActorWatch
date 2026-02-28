import sqlite3

import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


def register_actor_stix_and_taxii_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _export_actor_stix_bundle = deps['export_actor_stix_bundle']
    _import_actor_stix_bundle = deps['import_actor_stix_bundle']
    _list_ranked_evidence = deps.get('list_ranked_evidence')
    _sync_taxii_collection = deps.get('sync_taxii_collection')
    _list_taxii_sync_runs = deps.get('list_taxii_sync_runs')
    _taxii_collection_url = deps.get('taxii_collection_url', lambda: '')
    _taxii_auth_token = deps.get('taxii_auth_token', lambda: '')
    _taxii_lookback_hours = int(deps.get('taxii_lookback_hours', 72))
    _utc_now_iso = deps['utc_now_iso']

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

    @router.get(route_paths.ACTOR_EVIDENCE_RANKED, response_class=JSONResponse)
    def ranked_evidence(actor_id: str, limit: int = 100, entity_type: str = '') -> dict[str, object]:
        if not callable(_list_ranked_evidence):
            raise HTTPException(status_code=404, detail='ranked evidence endpoint unavailable')
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            items = _list_ranked_evidence(
                connection,
                actor_id=actor_id,
                limit=limit,
                entity_type=entity_type,
            )
        return {
            'actor_id': actor_id,
            'count': len(items),
            'items': items,
        }

    @router.post(route_paths.ACTOR_TAXII_SYNC, response_class=JSONResponse)
    async def taxii_sync(actor_id: str, request: Request) -> dict[str, object]:
        if not callable(_sync_taxii_collection):
            raise HTTPException(status_code=404, detail='taxii sync endpoint unavailable')
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        collection_url = str(payload.get('collection_url') or _taxii_collection_url() or '').strip()
        auth_token = str(payload.get('auth_token') or _taxii_auth_token() or '').strip()
        lookback_hours = int(payload.get('lookback_hours') or _taxii_lookback_hours or 72)
        if not collection_url:
            raise HTTPException(
                status_code=400,
                detail='collection_url required (set TAXII_COLLECTION_URL or provide in request body)',
            )
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            result = _sync_taxii_collection(
                connection,
                actor_id=actor_id,
                collection_url=collection_url,
                auth_token=auth_token,
                now_iso=_utc_now_iso(),
                lookback_hours=lookback_hours,
            )
            connection.commit()
        if not bool(result.get('ok')):
            raise HTTPException(status_code=502, detail=str(result.get('error') or 'taxii sync failed'))
        return result

    @router.get(route_paths.ACTOR_TAXII_RUNS, response_class=JSONResponse)
    def taxii_runs(actor_id: str, limit: int = 20) -> dict[str, object]:
        if not callable(_list_taxii_sync_runs):
            raise HTTPException(status_code=404, detail='taxii runs endpoint unavailable')
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            runs = _list_taxii_sync_runs(connection, actor_id=actor_id, limit=limit)
        return {
            'actor_id': actor_id,
            'count': len(runs),
            'runs': runs,
        }

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
