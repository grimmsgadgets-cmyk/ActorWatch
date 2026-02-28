import csv
import io
import sqlite3
import uuid
from datetime import datetime

import route_paths
import services.observation_service as observation_service
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response


def register_notebook_observation_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _utc_now_iso = deps['utc_now_iso']
    _upsert_observation_with_history = deps['upsert_observation_with_history']
    _fetch_analyst_observations = deps['fetch_analyst_observations']

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
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, object]:
        safe_limit = max(1, min(5000, int(limit)))
        safe_offset = max(0, int(offset))
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=safe_limit,
            offset=safe_offset,
        )
        return {
            'actor_id': actor_id,
            'count': len(items),
            'limit': safe_limit,
            'offset': safe_offset,
            'filters': {
                'analyst': analyst or '',
                'confidence': confidence or '',
                'updated_from': updated_from or '',
                'updated_to': updated_to or '',
            },
            'items': items,
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
                'claim_type',
                'citation_url',
                'observed_on',
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
                    item.get('claim_type', 'assessment'),
                    item.get('citation_url', ''),
                    item.get('observed_on', ''),
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
        claim_type = str(payload.get('claim_type') or 'assessment').strip().lower()
        if claim_type not in {'evidence', 'assessment'}:
            claim_type = 'assessment'
        citation_url = str(payload.get('citation_url') or '').strip()[:500]
        observed_on = str(payload.get('observed_on') or '').strip()[:10]
        observed_on_normalized = ''
        if observed_on:
            try:
                observed_on_normalized = datetime.fromisoformat(observed_on).date().isoformat()
            except Exception:
                observed_on_normalized = ''
        updated_by = str(payload.get('updated_by') or '').strip()[:120]
        updated_at = _utc_now_iso()
        contract_errors: list[str] = []
        if not updated_by:
            contract_errors.append('analyst is required')
        if claim_type == 'evidence':
            if not citation_url:
                contract_errors.append('citation_url is required for evidence-backed claims')
            if not observed_on_normalized:
                contract_errors.append('observed_on (YYYY-MM-DD) is required for evidence-backed claims')
        if contract_errors:
            raise HTTPException(status_code=400, detail='; '.join(contract_errors))
        quality_guidance = observation_service.observation_quality_guidance_core(
            note=note,
            source_ref=source_ref,
            confidence=confidence,
            source_reliability=source_reliability,
            information_credibility=information_credibility,
            claim_type=claim_type,
            citation_url=citation_url,
            observed_on=observed_on_normalized,
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
                claim_type=claim_type,
                citation_url=citation_url,
                observed_on=observed_on_normalized,
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
            'claim_type': claim_type,
            'citation_url': citation_url,
            'observed_on': observed_on_normalized,
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
                    claim_type='assessment',
                    citation_url='',
                    observed_on='',
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
                        claim_type='assessment',
                        citation_url='',
                        observed_on='',
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
                       information_credibility, claim_type, citation_url, observed_on, updated_by, updated_at
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
                           information_credibility, claim_type, citation_url, observed_on, updated_by, updated_at
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
                'claim_type': str(row[5] or 'assessment'),
                'citation_url': str(row[6] or ''),
                'observed_on': str(row[7] or ''),
                'updated_by': str(row[8] or ''),
                'updated_at': str(row[9] or ''),
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

