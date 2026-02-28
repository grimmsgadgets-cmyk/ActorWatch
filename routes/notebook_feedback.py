import sqlite3

import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


def register_notebook_feedback_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _normalize_environment_profile = deps['normalize_environment_profile']
    _upsert_environment_profile = deps['upsert_environment_profile']
    _load_environment_profile = deps['load_environment_profile']
    _store_feedback_event = deps['store_feedback_event']
    _feedback_summary_for_actor = deps['feedback_summary_for_actor']
    _apply_feedback_to_source_domains = deps['apply_feedback_to_source_domains']

    @router.get(route_paths.ACTOR_ENVIRONMENT_PROFILE, response_class=JSONResponse)
    def actor_environment_profile(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            return _load_environment_profile(connection, actor_id=actor_id)

    @router.post(route_paths.ACTOR_ENVIRONMENT_PROFILE, response_class=JSONResponse)
    async def upsert_actor_environment_profile(actor_id: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail='invalid profile payload')
        profile = _normalize_environment_profile(payload)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            response = _upsert_environment_profile(connection, actor_id=actor_id, profile=profile)
            connection.commit()
        return response

    @router.post(route_paths.ACTOR_FEEDBACK, response_class=JSONResponse)
    async def submit_feedback(actor_id: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail='invalid feedback payload')
        item_type = str(payload.get('item_type') or '').strip()
        item_id = str(payload.get('item_id') or '').strip()
        feedback_label = str(payload.get('feedback') or payload.get('feedback_label') or 'partial').strip()
        reason = str(payload.get('reason') or '').strip()
        source_id = str(payload.get('source_id') or '').strip() or None
        metadata_raw = payload.get('metadata')
        metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
        source_reliability_updates = 0
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            stored = _store_feedback_event(
                connection,
                actor_id=actor_id,
                item_type=item_type,
                item_id=item_id,
                feedback_label=feedback_label,
                reason=reason,
                source_id=source_id,
                metadata=metadata,
            )
            if not bool(stored.get('stored')):
                raise HTTPException(status_code=400, detail=str(stored.get('reason') or 'failed to store feedback'))
            evidence_ids_raw = metadata.get('evidence_source_ids')
            evidence_ids = [str(item).strip() for item in evidence_ids_raw if str(item).strip()] if isinstance(evidence_ids_raw, list) else []
            if evidence_ids:
                placeholders = ','.join('?' for _ in evidence_ids)
                rows = connection.execute(
                    f'''
                    SELECT url
                    FROM sources
                    WHERE actor_id = ? AND id IN ({placeholders})
                    ''',
                    (actor_id, *evidence_ids),
                ).fetchall()
                urls = [str(row[0] or '').strip() for row in rows if str(row[0] or '').strip()]
                source_reliability_updates = _apply_feedback_to_source_domains(
                    connection,
                    actor_id=actor_id,
                    source_urls=urls,
                    rating_score=int(stored.get('rating_score') or 0),
                )
            connection.commit()
        return {
            'actor_id': actor_id,
            **stored,
            'source_reliability_updates': source_reliability_updates,
        }

    @router.get(route_paths.ACTOR_FEEDBACK_SUMMARY, response_class=JSONResponse)
    def feedback_summary(actor_id: str, item_type: str | None = None) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            return _feedback_summary_for_actor(connection, actor_id=actor_id, item_type=item_type)

