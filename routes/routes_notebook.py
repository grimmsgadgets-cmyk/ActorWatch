import io
import sqlite3
import uuid
import csv
import zlib
import re
import json
import logging
from datetime import datetime, timedelta, timezone

import services.observation_service as observation_service
import services.quick_checks_view_service as quick_checks_view_service
import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

LOGGER = logging.getLogger(__name__)


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
    _generate_ioc_hunt_queries = deps['generate_ioc_hunt_queries']
    _get_ollama_status = deps['get_ollama_status']
    _store_feedback_event = deps['store_feedback_event']
    _feedback_summary_for_actor = deps['feedback_summary_for_actor']
    _normalize_environment_profile = deps['normalize_environment_profile']
    _upsert_environment_profile = deps['upsert_environment_profile']
    _load_environment_profile = deps['load_environment_profile']
    _apply_feedback_to_source_domains = deps['apply_feedback_to_source_domains']
    _get_tracking_intent = deps['get_tracking_intent']
    _upsert_tracking_intent = deps['upsert_tracking_intent']
    _confirm_actor_assessment = deps['confirm_actor_assessment']
    _dispatch_alert_deliveries = deps.get('dispatch_alert_deliveries')
    _recover_stale_running_states = deps.get('recover_stale_running_states')

    def _pdf_escape_text(value: str) -> str:
        return str(value or '').replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')

    def _render_simple_text_pdf(*, title: str, lines: list[str]) -> bytes:
        safe_title = str(title or 'Analyst Pack')
        safe_lines = [str(line or '')[:220] for line in lines if str(line or '').strip()]
        pages: list[list[str]] = []
        lines_per_page = 46
        if not safe_lines:
            safe_lines = ['(no content)']
        for index in range(0, len(safe_lines), lines_per_page):
            pages.append(safe_lines[index:index + lines_per_page])

        objects: dict[int, bytes] = {}
        objects[1] = b'<< /Type /Catalog /Pages 2 0 R >>'
        font_obj_id = 3
        objects[font_obj_id] = b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>'

        kids_refs: list[str] = []
        page_obj_id = 4
        for page_idx, page_lines in enumerate(pages, start=1):
            content_rows: list[str] = [
                'BT',
                '/F1 11 Tf',
                '72 760 Td',
                f'({_pdf_escape_text(f"{safe_title} (page {page_idx}/{len(pages)})")}) Tj',
                '0 -18 Td',
            ]
            for line in page_lines:
                content_rows.append(f'({_pdf_escape_text(line)}) Tj')
                content_rows.append('0 -14 Td')
            content_rows.append('ET')
            content_stream = '\n'.join(content_rows).encode('latin-1', 'replace')
            compressed = zlib.compress(content_stream)
            content_obj_id = page_obj_id + 1
            objects[content_obj_id] = (
                f'<< /Length {len(compressed)} /Filter /FlateDecode >>\nstream\n'.encode('ascii')
                + compressed
                + b'\nendstream'
            )
            objects[page_obj_id] = (
                f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] '
                f'/Resources << /Font << /F1 {font_obj_id} 0 R >> >> '
                f'/Contents {content_obj_id} 0 R >>'
            ).encode('ascii')
            kids_refs.append(f'{page_obj_id} 0 R')
            page_obj_id += 2

        objects[2] = f'<< /Type /Pages /Count {len(kids_refs)} /Kids [{" ".join(kids_refs)}] >>'.encode('ascii')

        output = bytearray()
        output.extend(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n')
        offsets: dict[int, int] = {}
        max_id = max(objects.keys())
        for obj_id in sorted(objects.keys()):
            offsets[obj_id] = len(output)
            output.extend(f'{obj_id} 0 obj\n'.encode('ascii'))
            output.extend(objects[obj_id])
            output.extend(b'\nendobj\n')

        xref_start = len(output)
        output.extend(f'xref\n0 {max_id + 1}\n'.encode('ascii'))
        output.extend(b'0000000000 65535 f \n')
        for obj_id in range(1, max_id + 1):
            offset = offsets.get(obj_id, 0)
            in_use = 'n' if obj_id in offsets else 'f'
            output.extend(f'{offset:010d} 00000 {in_use} \n'.encode('ascii'))
        output.extend(
            (
                'trailer\n'
                f'<< /Size {max_id + 1} /Root 1 0 R >>\n'
                f'startxref\n{xref_start}\n%%EOF\n'
            ).encode('ascii')
        )
        return bytes(output)

    def _change_matches_trigger_conditions(
        *,
        trigger_conditions: list[str],
        change_summary: str,
        change_type: str,
        tag_tokens: list[str],
    ) -> bool:
        normalized_conditions = [
            str(item or '').strip().lower()
            for item in trigger_conditions
            if str(item or '').strip()
        ]
        if not normalized_conditions:
            return True
        haystack = ' '.join(
            [
                str(change_summary or '').lower(),
                str(change_type or '').lower(),
                ' '.join([str(token or '').lower() for token in tag_tokens if str(token or '').strip()]),
            ]
        )
        return any(condition in haystack for condition in normalized_conditions)

    def _severity_from_change(*, confidence: str, change_type: str, tag_count: int) -> str:
        normalized_confidence = str(confidence or 'moderate').strip().lower()
        normalized_type = str(change_type or '').strip().lower()
        if normalized_confidence == 'high' and tag_count >= 2:
            return 'high'
        if normalized_type in {'infra', 'access_vector', 'targeting'} and normalized_confidence in {'moderate', 'high'}:
            return 'high'
        if normalized_confidence == 'low':
            return 'low'
        return 'medium'

    def _enqueue_change_alert_if_needed(
        *,
        connection: sqlite3.Connection,
        actor_id: str,
        change_item_id: str,
        change_summary: str,
        change_type: str,
        confidence: str,
        source_ref: str,
        tags: dict[str, int],
    ) -> dict[str, object]:
        plan_row = connection.execute(
            '''
            SELECT trigger_conditions_json, alert_subscriptions_json, alert_notifications_enabled
            FROM actor_collection_plans
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        if plan_row:
            trigger_conditions = _safe_json_string_list(str(plan_row[0] or '[]'))
            alert_subscriptions = _safe_json_string_list(str(plan_row[1] or '[]'))
            notifications_enabled = int(plan_row[2] or 0) == 1
        else:
            trigger_conditions = []
            alert_subscriptions = []
            notifications_enabled = False
        tag_tokens = [name for name, enabled in tags.items() if int(enabled or 0) == 1]
        if not _change_matches_trigger_conditions(
            trigger_conditions=trigger_conditions,
            change_summary=change_summary,
            change_type=change_type,
            tag_tokens=tag_tokens,
        ):
            return {'created': False, 'delivered': False, 'notifications_enabled': notifications_enabled}
        duplicate_row = connection.execute(
            '''
            SELECT id
            FROM actor_alert_events
            WHERE actor_id = ? AND change_item_id = ? AND status = 'open'
            LIMIT 1
            ''',
            (actor_id, change_item_id),
        ).fetchone()
        if duplicate_row:
            return {'created': False, 'delivered': False, 'notifications_enabled': notifications_enabled}
        now_iso = _utc_now_iso()
        tag_count = sum(int(value or 0) for value in tags.values())
        severity = _severity_from_change(confidence=confidence, change_type=change_type, tag_count=tag_count)
        readable_type = str(change_type or 'change').replace('_', ' ').title()
        title = f'{readable_type} change detected'
        detail = str(change_summary or '').strip()[:1200]
        alert_id = str(uuid.uuid4())
        connection.execute(
            '''
            INSERT INTO actor_alert_events (
                id, actor_id, alert_type, severity, title, detail, status,
                source_ref, channel_targets_json, change_item_id, created_at,
                acknowledged_at, acknowledged_by
            )
            VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, '', '')
            ''',
            (
                alert_id,
                actor_id,
                'change_detection',
                severity,
                title,
                detail,
                str(source_ref or '')[:500],
                str(json.dumps(alert_subscriptions)),
                change_item_id,
                now_iso,
            ),
        )
        return {
            'created': True,
            'notifications_enabled': notifications_enabled,
            'alert_id': alert_id,
            'title': title,
            'detail': detail,
            'severity': severity,
            'alert_subscriptions': alert_subscriptions,
        }

    def _build_analyst_pack_payload(
        actor_id: str,
        *,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 1000,
        history_limit: int = 1000,
    ) -> dict[str, object]:
        safe_observations_limit = max(1, min(5000, int(observations_limit)))
        safe_history_limit = max(1, min(5000, int(history_limit)))
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        observations = _fetch_analyst_observations(actor_id, limit=safe_observations_limit, offset=0)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            history_rows = connection.execute(
                '''
                SELECT item_type, item_key, note, source_ref, confidence,
                       source_reliability, information_credibility, claim_type, citation_url, observed_on,
                       updated_by, updated_at
                FROM analyst_observation_history
                WHERE actor_id = ?
                ORDER BY updated_at DESC
                LIMIT ?
                ''',
                (actor_id, safe_history_limit),
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
                'claim_type': str(row[7] or 'assessment'),
                'citation_url': str(row[8] or ''),
                'observed_on': str(row[9] or ''),
                'updated_by': str(row[10] or ''),
                'updated_at': str(row[11] or ''),
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
            'limits': {
                'observations': safe_observations_limit,
                'history': safe_history_limit,
            },
            'source_quality_filters': quality_filters_dict,
            'actor': notebook.get('actor', {}),
            'recent_change_summary': notebook.get('recent_change_summary', {}),
            'priority_questions': notebook.get('priority_questions', [])[:3],
            'ioc_items': notebook.get('ioc_items', []),
            'observations': observations,
            'observation_history': history_items,
        }

    def _ioc_value_is_hunt_relevant(ioc_type: str, ioc_value: str) -> bool:
        value = str(ioc_value or '').strip().lower()
        indicator_type = str(ioc_type or '').strip().lower()
        if not value or not indicator_type:
            return False
        if len(value) < 4:
            return False
        if indicator_type == 'domain':
            if re.fullmatch(r'^[a-z0-9-]+\.(js|json|css|html|xml|yaml|yml|md|txt|jsx|tsx)$', value):
                return False
        return True

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
        claim_type: str,
        citation_url: str,
        observed_on: str,
        updated_by: str,
        updated_at: str,
    ) -> None:
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility, claim_type, citation_url, observed_on,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(actor_id, item_type, item_key)
            DO UPDATE SET
                note = excluded.note,
                source_ref = excluded.source_ref,
                confidence = excluded.confidence,
                source_reliability = excluded.source_reliability,
                information_credibility = excluded.information_credibility,
                claim_type = excluded.claim_type,
                citation_url = excluded.citation_url,
                observed_on = excluded.observed_on,
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
                claim_type,
                citation_url,
                observed_on,
                updated_by,
                updated_at,
            ),
        )
        connection.execute(
            '''
            INSERT INTO analyst_observation_history (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility, claim_type, citation_url, observed_on,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                claim_type,
                citation_url,
                observed_on,
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
                       source_reliability, information_credibility, claim_type, citation_url, observed_on,
                       updated_by, updated_at
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

    @router.get(route_paths.ACTOR_TRACKING_INTENT, response_class=JSONResponse)
    def get_tracking_intent(actor_id: str) -> dict[str, object]:
        return _get_tracking_intent(actor_id)

    @router.post(route_paths.ACTOR_TRACKING_INTENT, response_class=JSONResponse)
    async def upsert_tracking_intent(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}

        key_questions_raw = payload.get('key_questions')
        if isinstance(key_questions_raw, str):
            key_questions = [line.strip() for line in key_questions_raw.splitlines() if line.strip()]
        elif isinstance(key_questions_raw, list):
            key_questions = [str(item).strip() for item in key_questions_raw if str(item).strip()]
        else:
            key_questions = []
        def _safe_int(raw_value: object, default_value: int) -> int:
            try:
                return int(str(raw_value or str(default_value)).strip() or str(default_value))
            except Exception:
                return default_value

        updated = _upsert_tracking_intent(
            actor_id,
            why_track=str(payload.get('why_track') or ''),
            mission_impact=str(payload.get('mission_impact') or ''),
            intelligence_focus=str(payload.get('intelligence_focus') or ''),
            key_questions=key_questions,
            priority=str(payload.get('priority') or 'medium'),
            impact=str(payload.get('impact') or 'medium'),
            review_cadence_days=_safe_int(payload.get('review_cadence_days'), 30),
            confirmation_min_sources=_safe_int(payload.get('confirmation_min_sources'), 2),
            confirmation_max_age_days=_safe_int(payload.get('confirmation_max_age_days'), 45),
            confirmation_criteria=str(payload.get('confirmation_criteria') or ''),
            updated_by=str(payload.get('updated_by') or ''),
        )
        if 'application/json' in content_type:
            return JSONResponse(updated)
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Tracking+intent+saved', status_code=303)

    @router.post(route_paths.ACTOR_CONFIRM_ASSESSMENT, response_class=JSONResponse)
    async def confirm_assessment(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}

        analyst = str(payload.get('analyst') or '').strip()
        note = str(payload.get('note') or '').strip()
        if not analyst:
            raise HTTPException(status_code=400, detail='analyst is required')
        result = _confirm_actor_assessment(actor_id, analyst=analyst, note=note)
        if 'application/json' in content_type:
            return JSONResponse(result)
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Assessment+confirmed', status_code=303)

    @router.post(route_paths.ACTOR_COLLECTION_PLAN, response_class=JSONResponse)
    async def upsert_collection_plan(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}

        def _as_list(raw_value: object) -> list[str]:
            if isinstance(raw_value, list):
                return [str(item).strip() for item in raw_value if str(item).strip()]
            if isinstance(raw_value, str):
                return [line.strip() for line in raw_value.splitlines() if line.strip()]
            return []

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            now_iso = _utc_now_iso()
            monitored_sources = _as_list(payload.get('monitored_sources'))[:30]
            trigger_conditions = _as_list(payload.get('trigger_conditions'))[:30]
            alert_subscriptions = _as_list(payload.get('alert_subscriptions'))[:30]
            raw_notifications_enabled = str(payload.get('alert_notifications_enabled') or '').strip().lower()
            alert_notifications_enabled = 1 if raw_notifications_enabled in {'1', 'true', 'yes', 'on'} else 0
            monitor_frequency = str(payload.get('monitor_frequency') or 'daily').strip().lower()
            if monitor_frequency not in {'hourly', 'daily', 'weekly'}:
                monitor_frequency = 'daily'
            updated_by = str(payload.get('updated_by') or '').strip()[:120]
            connection.execute(
                '''
                INSERT INTO actor_collection_plans (
                    actor_id, monitored_sources_json, monitor_frequency,
                    trigger_conditions_json, alert_subscriptions_json, alert_notifications_enabled, updated_by, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(actor_id) DO UPDATE SET
                    monitored_sources_json = excluded.monitored_sources_json,
                    monitor_frequency = excluded.monitor_frequency,
                    trigger_conditions_json = excluded.trigger_conditions_json,
                    alert_subscriptions_json = excluded.alert_subscriptions_json,
                    alert_notifications_enabled = excluded.alert_notifications_enabled,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                ''',
                (
                    actor_id,
                    str(json.dumps(monitored_sources)),
                    monitor_frequency,
                    str(json.dumps(trigger_conditions)),
                    str(json.dumps(alert_subscriptions)),
                    alert_notifications_enabled,
                    updated_by,
                    now_iso,
                ),
            )
            connection.commit()
            result = {
                'actor_id': actor_id,
                'monitored_sources': monitored_sources,
                'monitor_frequency': monitor_frequency,
                'trigger_conditions': trigger_conditions,
                'alert_subscriptions': alert_subscriptions,
                'alert_notifications_enabled': bool(alert_notifications_enabled),
                'updated_by': updated_by,
                'updated_at': now_iso,
            }
        if 'application/json' in content_type:
            return JSONResponse(result)
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Collection+plan+saved', status_code=303)

    @router.post(route_paths.ACTOR_REPORT_PREFERENCES, response_class=JSONResponse)
    async def upsert_report_preferences(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}

        period = str(payload.get('delta_brief_period') or 'weekly').strip().lower()
        if period not in {'weekly', 'monthly'}:
            period = 'weekly'
        default_window = 7 if period == 'weekly' else 30
        try:
            window_days = int(str(payload.get('delta_brief_window_days') or default_window).strip())
        except Exception:
            window_days = default_window
        window_days = max(1, min(365, window_days))
        enabled_raw = str(payload.get('delta_brief_enabled') or '').strip().lower()
        enabled = 1 if enabled_raw in {'1', 'true', 'yes', 'on'} else 0
        updated_by = str(payload.get('updated_by') or '').strip()[:120]
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_report_preferences (
                    actor_id, delta_brief_enabled, delta_brief_period, delta_brief_window_days, updated_by, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(actor_id) DO UPDATE SET
                    delta_brief_enabled = excluded.delta_brief_enabled,
                    delta_brief_period = excluded.delta_brief_period,
                    delta_brief_window_days = excluded.delta_brief_window_days,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                ''',
                (actor_id, enabled, period, window_days, updated_by, now_iso),
            )
            connection.commit()
        result = {
            'actor_id': actor_id,
            'delta_brief_enabled': bool(enabled),
            'delta_brief_period': period,
            'delta_brief_window_days': window_days,
            'updated_by': updated_by,
            'updated_at': now_iso,
        }
        if 'application/json' in content_type:
            return JSONResponse(result)
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Report+preferences+saved', status_code=303)

    @router.post(route_paths.ACTOR_RELATIONSHIPS, response_class=JSONResponse)
    async def add_relationship(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        src_entity_type = str(payload.get('src_entity_type') or '').strip().lower()[:40]
        src_entity_key = str(payload.get('src_entity_key') or '').strip()[:220]
        relationship_type = str(payload.get('relationship_type') or '').strip().lower()[:80]
        dst_entity_type = str(payload.get('dst_entity_type') or '').strip().lower()[:40]
        dst_entity_key = str(payload.get('dst_entity_key') or '').strip()[:220]
        if not all((src_entity_type, src_entity_key, relationship_type, dst_entity_type, dst_entity_key)):
            raise HTTPException(status_code=400, detail='relationship fields are required')
        source_ref = str(payload.get('source_ref') or '').strip()[:500]
        observed_on = str(payload.get('observed_on') or '').strip()[:10]
        confidence = str(payload.get('confidence') or 'moderate').strip().lower()
        if confidence not in {'low', 'moderate', 'high'}:
            confidence = 'moderate'
        analyst = str(payload.get('analyst') or '').strip()[:120]
        now_iso = _utc_now_iso()
        row_id = str(uuid.uuid4())
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_relationship_edges (
                    id, actor_id, src_entity_type, src_entity_key, relationship_type,
                    dst_entity_type, dst_entity_key, source_ref, observed_on, confidence,
                    analyst, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row_id,
                    actor_id,
                    src_entity_type,
                    src_entity_key,
                    relationship_type,
                    dst_entity_type,
                    dst_entity_key,
                    source_ref,
                    observed_on,
                    confidence,
                    analyst,
                    now_iso,
                    now_iso,
                ),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': row_id})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Relationship+added', status_code=303)

    @router.post(route_paths.ACTOR_CHANGE_ITEMS, response_class=JSONResponse)
    async def add_change_item(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}

        def _flag(key: str) -> int:
            raw = str(payload.get(key) or '').strip().lower()
            return 1 if raw in {'1', 'true', 'yes', 'on', key} else 0

        change_summary = str(payload.get('change_summary') or '').strip()[:800]
        if not change_summary:
            raise HTTPException(status_code=400, detail='change_summary is required')
        change_type = str(payload.get('change_type') or 'other').strip().lower()[:40]
        if change_type not in {'ttp', 'infra', 'tooling', 'targeting', 'timing', 'access_vector', 'other'}:
            change_type = 'other'
        confidence = str(payload.get('confidence') or 'moderate').strip().lower()
        if confidence not in {'low', 'moderate', 'high'}:
            confidence = 'moderate'
        source_ref = str(payload.get('source_ref') or '').strip()[:500]
        observed_on = str(payload.get('observed_on') or '').strip()[:10]
        created_by = str(payload.get('created_by') or '').strip()[:120]
        now_iso = _utc_now_iso()
        row_id = str(uuid.uuid4())
        tag_values = {
            'ttp': _flag('ttp_tag'),
            'infra': _flag('infra_tag'),
            'tooling': _flag('tooling_tag'),
            'targeting': _flag('targeting_tag'),
            'timing': _flag('timing_tag'),
            'access_vector': _flag('access_vector_tag'),
        }
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_change_items (
                    id, actor_id, change_summary, change_type,
                    ttp_tag, infra_tag, tooling_tag, targeting_tag, timing_tag, access_vector_tag,
                    confidence, source_ref, observed_on, created_by, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row_id,
                    actor_id,
                    change_summary,
                    change_type,
                    tag_values['ttp'],
                    tag_values['infra'],
                    tag_values['tooling'],
                    tag_values['targeting'],
                    tag_values['timing'],
                    tag_values['access_vector'],
                    confidence,
                    source_ref,
                    observed_on,
                    created_by,
                    now_iso,
                ),
            )
            alert_outcome = _enqueue_change_alert_if_needed(
                connection=connection,
                actor_id=actor_id,
                change_item_id=row_id,
                change_summary=change_summary,
                change_type=change_type,
                confidence=confidence,
                source_ref=source_ref,
                tags=tag_values,
            )
            connection.commit()
        alert_delivered = False
        if (
            alert_outcome.get('created')
            and alert_outcome.get('notifications_enabled')
            and callable(_dispatch_alert_deliveries)
        ):
            try:
                delivery_result = _dispatch_alert_deliveries(
                    actor_id=actor_id,
                    alert_id=str(alert_outcome.get('alert_id') or ''),
                    title=str(alert_outcome.get('title') or ''),
                    detail=str(alert_outcome.get('detail') or ''),
                    severity=str(alert_outcome.get('severity') or 'medium'),
                    subscriptions=list(alert_outcome.get('alert_subscriptions') or []),
                )
                alert_delivered = int((delivery_result or {}).get('delivered') or 0) > 0
            except Exception:
                alert_delivered = False
        if 'application/json' in content_type:
            return JSONResponse(
                {
                    'ok': True,
                    'id': row_id,
                    'alert_created': bool(alert_outcome.get('created')),
                    'alert_delivered': bool(alert_delivered),
                    'notifications_enabled': bool(alert_outcome.get('notifications_enabled')),
                }
            )
        notice = 'Change+item+added'
        if alert_outcome.get('created'):
            if alert_outcome.get('notifications_enabled') and alert_delivered:
                notice = 'Change+item+added+and+alert+sent'
            elif alert_outcome.get('notifications_enabled'):
                notice = 'Change+item+added+and+alert+queued'
            else:
                notice = 'Change+item+added+alert+saved+(notifications+off)'
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice={notice}', status_code=303)

    @router.post(route_paths.ACTOR_ALERT_ACK, response_class=JSONResponse)
    async def acknowledge_alert(actor_id: str, alert_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        analyst = str(payload.get('analyst') or '').strip()[:120]
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            row = connection.execute(
                '''
                SELECT id, status
                FROM actor_alert_events
                WHERE id = ? AND actor_id = ?
                ''',
                (alert_id, actor_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='alert not found')
            connection.execute(
                '''
                UPDATE actor_alert_events
                SET status = 'acknowledged',
                    acknowledged_at = ?,
                    acknowledged_by = ?
                WHERE id = ? AND actor_id = ?
                ''',
                (now_iso, analyst, alert_id, actor_id),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': alert_id, 'status': 'acknowledged'})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Alert+acknowledged', status_code=303)

    @router.post(route_paths.ACTOR_CHANGE_CONFLICTS, response_class=JSONResponse)
    async def add_change_conflict(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        conflict_topic = str(payload.get('conflict_topic') or '').strip()[:300]
        source_a_ref = str(payload.get('source_a_ref') or '').strip()[:500]
        source_b_ref = str(payload.get('source_b_ref') or '').strip()[:500]
        arbitration_outcome = str(payload.get('arbitration_outcome') or '').strip()[:1200]
        if not all((conflict_topic, source_a_ref, source_b_ref, arbitration_outcome)):
            raise HTTPException(status_code=400, detail='conflict fields are required')
        confidence = str(payload.get('confidence') or 'moderate').strip().lower()
        if confidence not in {'low', 'moderate', 'high'}:
            confidence = 'moderate'
        analyst = str(payload.get('analyst') or '').strip()[:120]
        resolved_at = _utc_now_iso()
        row_id = str(uuid.uuid4())
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_change_conflicts (
                    id, actor_id, conflict_topic, source_a_ref, source_b_ref,
                    arbitration_outcome, confidence, analyst, resolved_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row_id,
                    actor_id,
                    conflict_topic,
                    source_a_ref,
                    source_b_ref,
                    arbitration_outcome,
                    confidence,
                    analyst,
                    resolved_at,
                ),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': row_id})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Conflict+recorded', status_code=303)

    @router.post(route_paths.ACTOR_TECHNIQUE_COVERAGE, response_class=JSONResponse)
    async def upsert_technique_coverage(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        technique_id = str(payload.get('technique_id') or '').strip().upper()[:32]
        if not technique_id:
            raise HTTPException(status_code=400, detail='technique_id is required')
        technique_name = str(payload.get('technique_name') or '').strip()[:200]
        detection_name = str(payload.get('detection_name') or '').strip()[:300]
        control_name = str(payload.get('control_name') or '').strip()[:300]
        coverage_status = str(payload.get('coverage_status') or 'unknown').strip().lower()
        if coverage_status not in {'covered', 'partial', 'gap', 'unknown'}:
            coverage_status = 'unknown'
        validation_status = str(payload.get('validation_status') or 'unknown').strip().lower()
        if validation_status not in {'validated', 'not_validated', 'unknown'}:
            validation_status = 'unknown'
        validation_evidence = str(payload.get('validation_evidence') or '').strip()[:1200]
        updated_by = str(payload.get('updated_by') or '').strip()[:120]
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            existing = connection.execute(
                'SELECT id FROM actor_technique_coverage WHERE actor_id = ? AND technique_id = ?',
                (actor_id, technique_id),
            ).fetchone()
            row_id = str(existing[0]) if existing else str(uuid.uuid4())
            connection.execute(
                '''
                INSERT INTO actor_technique_coverage (
                    id, actor_id, technique_id, technique_name, detection_name, control_name,
                    coverage_status, validation_status, validation_evidence, updated_by, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(actor_id, technique_id) DO UPDATE SET
                    technique_name = excluded.technique_name,
                    detection_name = excluded.detection_name,
                    control_name = excluded.control_name,
                    coverage_status = excluded.coverage_status,
                    validation_status = excluded.validation_status,
                    validation_evidence = excluded.validation_evidence,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                ''',
                (
                    row_id,
                    actor_id,
                    technique_id,
                    technique_name,
                    detection_name,
                    control_name,
                    coverage_status,
                    validation_status,
                    validation_evidence,
                    updated_by,
                    now_iso,
                ),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'technique_id': technique_id})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Technique+coverage+saved', status_code=303)

    @router.post(route_paths.ACTOR_TASKS, response_class=JSONResponse)
    async def create_task(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        title = str(payload.get('title') or '').strip()[:240]
        if not title:
            raise HTTPException(status_code=400, detail='title is required')
        details = str(payload.get('details') or '').strip()[:1500]
        priority = str(payload.get('priority') or 'medium').strip().lower()
        if priority not in {'low', 'medium', 'high', 'critical'}:
            priority = 'medium'
        status = str(payload.get('status') or 'open').strip().lower()
        if status not in {'open', 'in_progress', 'blocked', 'done'}:
            status = 'open'
        owner = str(payload.get('owner') or '').strip()[:120]
        due_date = str(payload.get('due_date') or '').strip()[:10]
        linked_type = str(payload.get('linked_type') or '').strip()[:40]
        linked_key = str(payload.get('linked_key') or '').strip()[:160]
        row_id = str(uuid.uuid4())
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_tasks (
                    id, actor_id, title, details, priority, status, owner, due_date,
                    linked_type, linked_key, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row_id,
                    actor_id,
                    title,
                    details,
                    priority,
                    status,
                    owner,
                    due_date,
                    linked_type,
                    linked_key,
                    now_iso,
                    now_iso,
                ),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': row_id})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Task+created', status_code=303)

    @router.post(route_paths.ACTOR_TASK_UPDATE, response_class=JSONResponse)
    async def update_task(actor_id: str, task_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        next_status = str(payload.get('status') or '').strip().lower()
        if next_status not in {'open', 'in_progress', 'blocked', 'done'}:
            raise HTTPException(status_code=400, detail='status is required')
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            row = connection.execute(
                'SELECT id FROM actor_tasks WHERE id = ? AND actor_id = ?',
                (task_id, actor_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='task not found')
            connection.execute(
                'UPDATE actor_tasks SET status = ?, updated_at = ? WHERE id = ?',
                (next_status, now_iso, task_id),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': task_id, 'status': next_status})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Task+updated', status_code=303)

    @router.post(route_paths.ACTOR_OUTCOMES, response_class=JSONResponse)
    async def create_outcome(actor_id: str, request: Request) -> Response:
        await _enforce_request_size(request, _default_body_limit_bytes)
        content_type = str(request.headers.get('content-type') or '').lower()
        payload: dict[str, object] = {}
        if 'application/json' in content_type:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        else:
            form = await request.form()
            payload = {str(key): form.get(key) for key in form.keys()}
        outcome_type = str(payload.get('outcome_type') or '').strip().lower()[:40]
        if outcome_type not in {'detection_created', 'hunt_ran', 'finding', 'false_positive', 'mitigation_applied'}:
            raise HTTPException(status_code=400, detail='invalid outcome_type')
        summary = str(payload.get('summary') or '').strip()[:1200]
        if not summary:
            raise HTTPException(status_code=400, detail='summary is required')
        result = str(payload.get('result') or '').strip()[:500]
        linked_task_id = str(payload.get('linked_task_id') or '').strip()[:64]
        linked_technique_id = str(payload.get('linked_technique_id') or '').strip().upper()[:32]
        evidence_ref = str(payload.get('evidence_ref') or '').strip()[:500]
        created_by = str(payload.get('created_by') or '').strip()[:120]
        row_id = str(uuid.uuid4())
        now_iso = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            connection.execute(
                '''
                INSERT INTO actor_operational_outcomes (
                    id, actor_id, outcome_type, summary, result,
                    linked_task_id, linked_technique_id, evidence_ref, created_by, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row_id,
                    actor_id,
                    outcome_type,
                    summary,
                    result,
                    linked_task_id,
                    linked_technique_id,
                    evidence_ref,
                    created_by,
                    now_iso,
                ),
            )
            connection.commit()
        if 'application/json' in content_type:
            return JSONResponse({'ok': True, 'id': row_id})
        return RedirectResponse(url=f'/?actor_id={actor_id}&notice=Outcome+recorded', status_code=303)

    @router.get(route_paths.ACTOR_REPORT_VIEW, response_class=JSONResponse)
    def report_view(
        actor_id: str,
        audience: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        def _safe_parse_dt(raw_value: object) -> datetime | None:
            raw = str(raw_value or '').strip()
            if not raw:
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except Exception:
                return None

        def _extract_evidence_bundle(*, actor_id_value: str, notebook_payload: dict[str, object], since_days_value: int) -> dict[str, object]:
            actor_meta = notebook_payload.get('actor', {}) if isinstance(notebook_payload.get('actor', {}), dict) else {}
            actor_name_value = str(actor_meta.get('display_name') or actor_id_value)
            cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(since_days_value)))
            top_changes_raw = notebook_payload.get('top_change_signals', [])
            top_changes = top_changes_raw if isinstance(top_changes_raw, list) else []
            claims: list[dict[str, object]] = []
            for index, item in enumerate(top_changes, start=1):
                if not isinstance(item, dict):
                    continue
                claim_dt = _safe_parse_dt(item.get('observed_on') or item.get('created_at'))
                if claim_dt and claim_dt < cutoff:
                    continue
                validated_sources_raw = item.get('validated_sources')
                validated_sources = validated_sources_raw if isinstance(validated_sources_raw, list) else []
                citations = []
                for source in validated_sources:
                    if not isinstance(source, dict):
                        continue
                    citations.append(
                        {
                            'source_label': str(source.get('source_name') or source.get('source_domain') or ''),
                            'source_url': str(source.get('source_url') or ''),
                            'source_date': str(source.get('source_date') or ''),
                            'source_excerpt': str(source.get('supporting_excerpt') or source.get('source_excerpt') or '')[:500],
                        }
                    )
                claims.append(
                    {
                        'claim_id': f'change-{index}',
                        'claim_text': str(item.get('change_summary') or ''),
                        'claim_type': 'assessment',
                        'confidence': str(item.get('confidence_label') or item.get('confidence') or 'moderate').lower(),
                        'updated_at': str(item.get('observed_on') or item.get('created_at') or ''),
                        'analyst': str(item.get('created_by') or ''),
                        'citations': citations,
                    }
                )

            with sqlite3.connect(_db_path()) as connection:
                obs_rows = connection.execute(
                    '''
                    SELECT item_type, item_key, note, citation_url, observed_on,
                           updated_by, updated_at, confidence, source_ref
                    FROM analyst_observations
                    WHERE actor_id = ? AND claim_type = 'evidence'
                    ORDER BY updated_at DESC
                    LIMIT 500
                    ''',
                    (actor_id_value,),
                ).fetchall()
            for row in obs_rows:
                observed_on_value = str(row[4] or '')
                observed_dt = _safe_parse_dt(observed_on_value)
                if observed_dt and observed_dt < cutoff:
                    continue
                citation_url = str(row[3] or '').strip()
                if not citation_url:
                    continue
                claims.append(
                    {
                        'claim_id': f'obs-{str(row[0] or "")}-{str(row[1] or "")}',
                        'claim_text': str(row[2] or ''),
                        'claim_type': 'evidence',
                        'confidence': str(row[7] or 'moderate'),
                        'updated_at': str(row[6] or ''),
                        'analyst': str(row[5] or ''),
                        'citations': [
                            {
                                'source_label': str(row[8] or ''),
                                'source_url': citation_url,
                                'source_date': observed_on_value,
                                'source_excerpt': '',
                            }
                        ],
                    }
                )
            return {
                'actor_id': actor_id_value,
                'actor_name': actor_name_value,
                'window_days': max(1, int(since_days_value)),
                'generated_at': _utc_now_iso(),
                'claim_count': len(claims),
                'claims': claims,
            }

        def _build_delta_brief(*, actor_id_value: str, notebook_payload: dict[str, object], period_value: str, since_days_value: int) -> dict[str, object]:
            actor_meta = notebook_payload.get('actor', {}) if isinstance(notebook_payload.get('actor', {}), dict) else {}
            actor_name_value = str(actor_meta.get('display_name') or actor_id_value)
            cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(since_days_value)))
            summary = notebook_payload.get('recent_change_summary', {}) if isinstance(notebook_payload.get('recent_change_summary', {}), dict) else {}
            top_changes_raw = notebook_payload.get('top_change_signals', [])
            top_changes = top_changes_raw if isinstance(top_changes_raw, list) else []
            window_changes = []
            for item in top_changes:
                if not isinstance(item, dict):
                    continue
                event_dt = _safe_parse_dt(item.get('observed_on') or item.get('created_at'))
                if event_dt and event_dt < cutoff:
                    continue
                window_changes.append(item)
            taxonomy_counts = {
                'ttp': 0,
                'infra': 0,
                'tooling': 0,
                'targeting': 0,
                'timing': 0,
                'access_vector': 0,
            }
            for item in (notebook_payload.get('change_items') if isinstance(notebook_payload.get('change_items'), list) else []):
                if not isinstance(item, dict):
                    continue
                item_dt = _safe_parse_dt(item.get('observed_on') or item.get('created_at'))
                if item_dt and item_dt < cutoff:
                    continue
                for key in taxonomy_counts:
                    if bool(item.get(f'{key}_tag')):
                        taxonomy_counts[key] += 1
            alert_queue = notebook_payload.get('alert_queue') if isinstance(notebook_payload.get('alert_queue'), list) else []
            open_alerts = [a for a in alert_queue if isinstance(a, dict) and str(a.get('status') or '').lower() == 'open']
            return {
                'actor_id': actor_id_value,
                'actor_name': actor_name_value,
                'period': period_value,
                'window_days': max(1, int(since_days_value)),
                'generated_at': _utc_now_iso(),
                'summary': {
                    'new_reports': summary.get('new_reports', 0),
                    'targets': summary.get('targets', ''),
                    'damage': summary.get('damage', ''),
                    'open_alerts': len(open_alerts),
                },
                'taxonomy_counts': taxonomy_counts,
                'headline_changes': [
                    str(item.get('change_summary') or '')[:240]
                    for item in window_changes[:12]
                    if isinstance(item, dict)
                ],
                'recommended_followups': [
                    str(item.get('quick_check_title') or item.get('question_text') or '')[:240]
                    for item in (notebook_payload.get('priority_questions') if isinstance(notebook_payload.get('priority_questions'), list) else [])[:8]
                    if isinstance(item, dict)
                ],
            }

        audience_key = str(audience or '').strip().lower()
        if audience_key not in {'exec', 'soc', 'ir'}:
            raise HTTPException(status_code=400, detail='audience must be one of: exec, soc, ir')
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        actor = notebook.get('actor', {}) if isinstance(notebook.get('actor', {}), dict) else {}
        summary = notebook.get('recent_change_summary', {}) if isinstance(notebook.get('recent_change_summary', {}), dict) else {}
        top_changes = notebook.get('top_change_signals', []) if isinstance(notebook.get('top_change_signals', []), list) else []
        quick_checks = notebook.get('priority_questions', []) if isinstance(notebook.get('priority_questions', []), list) else []
        since_days = 30
        evidence_bundle = _extract_evidence_bundle(actor_id_value=actor_id, notebook_payload=notebook, since_days_value=since_days)
        delta_brief = _build_delta_brief(actor_id_value=actor_id, notebook_payload=notebook, period_value='monthly', since_days_value=since_days)
        base = {
            'audience': audience_key,
            'actor_id': actor_id,
            'actor_name': str(actor.get('display_name') or actor_id),
            'generated_at': _utc_now_iso(),
            'window_days': since_days,
            'summary': {
                'new_reports': summary.get('new_reports', 0),
                'targets': summary.get('targets', 0),
                'damage': summary.get('damage', 0),
            },
        }
        if audience_key == 'exec':
            return {
                **base,
                'what_changed': base['summary'],
                'headline_changes': [
                    str(item.get('change_summary') or '')[:220]
                    for item in top_changes[:5]
                    if isinstance(item, dict)
                ],
                'delta_brief': delta_brief,
            }
        if audience_key == 'soc':
            return {
                **base,
                'top_checks': [
                    {
                        'question': str(item.get('question_text') or ''),
                        'where_to_look': str(item.get('where_to_look') or ''),
                        'what_to_look_for': str(item.get('what_to_look_for') or ''),
                    }
                    for item in quick_checks[:8]
                    if isinstance(item, dict)
                ],
                'top_techniques': notebook.get('top_techniques', []),
                'ioc_items': notebook.get('ioc_items', [])[:40],
                'open_alerts': [item for item in (notebook.get('alert_queue') if isinstance(notebook.get('alert_queue'), list) else []) if isinstance(item, dict) and str(item.get('status') or '').lower() == 'open'][:20],
            }
        return {
            **base,
            'timeline_recent_items': notebook.get('timeline_recent_items', [])[:40],
            'top_change_signals': top_changes[:10],
            'evidence_bundle': evidence_bundle,
            'delta_brief': delta_brief,
        }

    @router.get(route_paths.ACTOR_EXPORT_EVIDENCE_BUNDLE_JSON, response_class=JSONResponse)
    def export_evidence_bundle_json(
        actor_id: str,
        since_days: int = 30,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        def _safe_parse_dt(raw_value: object) -> datetime | None:
            raw = str(raw_value or '').strip()
            if not raw:
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except Exception:
                return None

        since = max(1, min(365, int(since_days or 30)))
        cutoff = datetime.now(timezone.utc) - timedelta(days=since)
        ir_report = report_view(
            actor_id=actor_id,
            audience='ir',
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        bundle = ir_report.get('evidence_bundle', {}) if isinstance(ir_report, dict) else {}
        claims = bundle.get('claims', []) if isinstance(bundle, dict) else []
        filtered_claims = []
        for claim in claims if isinstance(claims, list) else []:
            if not isinstance(claim, dict):
                continue
            claim_dt = _safe_parse_dt(claim.get('updated_at'))
            if claim_dt and claim_dt < cutoff:
                continue
            filtered_claims.append(claim)
        return {
            'actor_id': actor_id,
            'generated_at': _utc_now_iso(),
            'window_days': since,
            'claim_count': len(filtered_claims),
            'claims': filtered_claims,
        }

    @router.get(route_paths.ACTOR_EXPORT_EVIDENCE_BUNDLE_CSV)
    def export_evidence_bundle_csv(
        actor_id: str,
        since_days: int = 30,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> Response:
        ir_report = report_view(
            actor_id=actor_id,
            audience='ir',
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        bundle = ir_report.get('evidence_bundle', {}) if isinstance(ir_report, dict) else {}
        claims = bundle.get('claims', []) if isinstance(bundle, dict) else []
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                'actor_id',
                'claim_id',
                'claim_type',
                'claim_text',
                'confidence',
                'updated_at',
                'analyst',
                'source_label',
                'source_url',
                'source_date',
                'source_excerpt',
            ]
        )
        for claim in claims if isinstance(claims, list) else []:
            if not isinstance(claim, dict):
                continue
            citations = claim.get('citations', []) if isinstance(claim.get('citations'), list) else []
            if not citations:
                writer.writerow(
                    [
                        actor_id,
                        str(claim.get('claim_id') or ''),
                        str(claim.get('claim_type') or ''),
                        str(claim.get('claim_text') or ''),
                        str(claim.get('confidence') or ''),
                        str(claim.get('updated_at') or ''),
                        str(claim.get('analyst') or ''),
                        '',
                        '',
                        '',
                        '',
                    ]
                )
                continue
            for citation in citations:
                if not isinstance(citation, dict):
                    continue
                writer.writerow(
                    [
                        actor_id,
                        str(claim.get('claim_id') or ''),
                        str(claim.get('claim_type') or ''),
                        str(claim.get('claim_text') or ''),
                        str(claim.get('confidence') or ''),
                        str(claim.get('updated_at') or ''),
                        str(claim.get('analyst') or ''),
                        str(citation.get('source_label') or ''),
                        str(citation.get('source_url') or ''),
                        str(citation.get('source_date') or ''),
                        str(citation.get('source_excerpt') or ''),
                    ]
                )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{actor_id}-evidence-bundle.csv"'},
        )

    @router.get(route_paths.ACTOR_EXPORT_DELTA_BRIEF_JSON, response_class=JSONResponse)
    def export_delta_brief_json(
        actor_id: str,
        period: str = 'weekly',
        since_days: int | None = None,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        pref_period = 'weekly'
        pref_window_days = 7
        with sqlite3.connect(_db_path()) as connection:
            pref_row = connection.execute(
                '''
                SELECT delta_brief_period, delta_brief_window_days
                FROM actor_report_preferences
                WHERE actor_id = ?
                ''',
                (actor_id,),
            ).fetchone()
            if pref_row:
                pref_period = str(pref_row[0] or 'weekly').strip().lower()
                try:
                    pref_window_days = int(pref_row[1] or 7)
                except Exception:
                    pref_window_days = 7
        period_key = str(period or pref_period).strip().lower()
        if period_key not in {'weekly', 'monthly'}:
            period_key = pref_period if pref_period in {'weekly', 'monthly'} else 'weekly'
        default_days = pref_window_days if pref_window_days > 0 else (7 if period_key == 'weekly' else 30)
        window_days = max(1, min(365, int(since_days or default_days)))
        report = report_view(
            actor_id=actor_id,
            audience='exec',
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        base_summary = report.get('summary', {}) if isinstance(report, dict) else {}
        headlines = report.get('headline_changes', []) if isinstance(report, dict) else []
        return {
            'actor_id': actor_id,
            'period': period_key,
            'window_days': window_days,
            'generated_at': _utc_now_iso(),
            'summary': base_summary if isinstance(base_summary, dict) else {},
            'headline_changes': headlines if isinstance(headlines, list) else [],
        }

    @router.get(route_paths.ACTOR_TIMELINE_DETAILS, response_class=HTMLResponse)
    def actor_timeline_details(request: Request, actor_id: str, limit: int = 300, offset: int = 0) -> HTMLResponse:
        safe_limit = max(1, min(1000, int(limit)))
        safe_offset = max(0, int(offset))
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
                LIMIT ? OFFSET ?
                ''',
                (actor_id, safe_limit, safe_offset),
            ).fetchall()

        detail_rows: list[dict[str, object]] = []
        for row in rows:
            event_title = str(row[2] or row[9] or row[3] or '').strip()
            if event_title.startswith(('http://', 'https://')):
                event_title = str(row[3] or row[2] or 'Untitled report').strip()
            if 'who/what/when/where/how' in event_title.lower():
                fallback_title = str(row[2] or '').strip()
                if fallback_title and 'who/what/when/where/how' not in fallback_title.lower():
                    event_title = fallback_title
                else:
                    event_title = 'Ransomware disclosure and targeting update'
            detail_rows.append(
                {
                    'occurred_at': row[0],
                    'category': str(row[1]).replace('_', ' '),
                    'title': event_title,
                    'summary': row[3],
                    'target_text': row[4] or '',
                    'ttp_ids': _safe_json_string_list(row[5]),
                    'source_name': row[6] or '',
                    'source_url': row[7] or '',
                    'source_published_at': row[8] or '',
                    'source_title': row[9] or row[10] or row[11] or row[12] or '',
                }
            )
        return _templates.TemplateResponse(
            request,
            'timeline_details.html',
            {
                'actor_id': actor_id,
                'actor_name': str(actor_row[1]),
                'detail_rows': detail_rows,
                'limit': safe_limit,
                'offset': safe_offset,
            },
        )

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

    @router.get(route_paths.ACTOR_IOC_HUNT_QUERIES, response_class=HTMLResponse)
    def actor_ioc_hunt_queries(
        request: Request,
        actor_id: str,
        quick_check_id: str | None = None,
        check_template_id: str | None = None,
        thread_id: str | None = None,
        window_days: int = 30,
        window_start: str | None = None,
        window_end: str | None = None,
        ioc_type: str | None = None,
        confidence: str | None = None,
        source_count: str | None = None,
        freshness: str | None = None,
        export: str | None = None,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        query_lookback_hours: str | None = None,
    ) -> Response:
        selected_check_id = str(quick_check_id or check_template_id or thread_id or '').strip()
        try:
            safe_window_days = max(1, min(365, int(window_days or 30)))
        except Exception:
            safe_window_days = 30
        now_utc = datetime.now(timezone.utc)
        parsed_start: datetime | None = None
        parsed_end: datetime | None = None
        try:
            if str(window_start or '').strip():
                parsed_start = datetime.fromisoformat(str(window_start).strip().replace('Z', '+00:00'))
                if parsed_start.tzinfo is None:
                    parsed_start = parsed_start.replace(tzinfo=timezone.utc)
                parsed_start = parsed_start.astimezone(timezone.utc)
        except Exception:
            parsed_start = None
        try:
            if str(window_end or '').strip():
                parsed_end = datetime.fromisoformat(str(window_end).strip().replace('Z', '+00:00'))
                if parsed_end.tzinfo is None:
                    parsed_end = parsed_end.replace(tzinfo=timezone.utc)
                parsed_end = parsed_end.astimezone(timezone.utc)
        except Exception:
            parsed_end = None
        if parsed_start is not None and parsed_end is not None and parsed_start <= parsed_end:
            window_start_dt = parsed_start
            window_end_dt = parsed_end
        else:
            window_start_dt, window_end_dt = quick_checks_view_service.window_bounds_core(
                now=now_utc,
                window_days=safe_window_days,
            )
        window_start_iso = window_start_dt.isoformat()
        window_end_iso = window_end_dt.isoformat()
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days or '30',
        )
        actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        actor_name = str(actor_meta.get('display_name') or actor_id)
        cards_raw = notebook.get('priority_questions', []) if isinstance(notebook, dict) else []
        cards_list = cards_raw if isinstance(cards_raw, list) else []
        if selected_check_id:
            cards_list = [card for card in cards_list if str(card.get('id') or '').strip() == selected_check_id]

        cards_by_id = {
            str(card.get('id') or '').strip(): card
            for card in cards_list
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        selected_card = cards_by_id.get(selected_check_id) if selected_check_id else None
        selected_card_text = (
            ' '.join(
                [
                    str(selected_card.get('quick_check_title') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('question_text') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('behavior_to_hunt') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('what_to_watch') or '') if isinstance(selected_card, dict) else '',
                ]
            ).strip().lower()
        )
        relevant_types = {
            str(ioc.get('ioc_type') or '').strip().lower()
            for ioc in (selected_card.get('related_iocs') if isinstance(selected_card, dict) and isinstance(selected_card.get('related_iocs'), list) else [])
            if isinstance(ioc, dict) and str(ioc.get('ioc_type') or '').strip()
        }
        relevant_values = {
            str(ioc.get('ioc_value') or '').strip().lower()
            for ioc in (selected_card.get('related_iocs') if isinstance(selected_card, dict) and isinstance(selected_card.get('related_iocs'), list) else [])
            if isinstance(ioc, dict) and str(ioc.get('ioc_value') or '').strip()
        }

        cards_for_hunts: list[dict[str, object]] = []
        card_behavior_queries: dict[str, list[dict[str, str]]] = {}
        environment_profile: dict[str, object] = {}
        with sqlite3.connect(_db_path()) as connection:
            environment_profile = _load_environment_profile(connection, actor_id=actor_id)
            for card in cards_list:
                if not isinstance(card, dict):
                    continue
                card_id = str(card.get('id') or '').strip()
                if not card_id:
                    continue
                behavior_queries_raw = card.get('behavior_queries')
                behavior_queries_list = behavior_queries_raw if isinstance(behavior_queries_raw, list) else []
                clean_behavior_queries: list[dict[str, str]] = []
                for query_item in behavior_queries_list:
                    if not isinstance(query_item, dict):
                        continue
                    platform = str(query_item.get('platform') or '').strip()
                    query = str(query_item.get('query') or '').strip()
                    why_this_query = str(query_item.get('why_this_query') or '').strip()
                    if not platform or not query:
                        continue
                    clean_behavior_queries.append(
                        {
                            'platform': platform[:80],
                            'query': query[:1200],
                            'why_this_query': why_this_query[:220],
                        }
                    )
                if clean_behavior_queries:
                    card_behavior_queries[card_id] = clean_behavior_queries[:6]
                related_iocs_raw = card.get('related_iocs')
                related_iocs = related_iocs_raw if isinstance(related_iocs_raw, list) else []
                evidence_rows = connection.execute(
                    '''
                    SELECT qu.source_id, qu.trigger_excerpt, s.url, s.title, s.headline, s.og_title, s.html_title, s.published_at
                    FROM question_updates qu
                    JOIN sources s ON s.id = qu.source_id
                    WHERE qu.thread_id = ?
                      AND s.actor_id = ?
                      AND COALESCE(s.published_at, s.ingested_at, s.retrieved_at, '') >= ?
                    ORDER BY qu.created_at DESC
                    LIMIT 8
                    ''',
                    (card_id, actor_id, window_start_iso),
                ).fetchall()
                evidence_items: list[dict[str, str]] = []
                seen_evidence_ids: set[str] = set()
                for row in evidence_rows:
                    evidence_id = str(row[0] or '').strip()
                    source_url = str(row[2] or '').strip()
                    if not evidence_id or not source_url or evidence_id in seen_evidence_ids:
                        continue
                    seen_evidence_ids.add(evidence_id)
                    evidence_items.append(
                        {
                            'id': evidence_id,
                            'source_url': source_url,
                            'source_title': str(row[3] or row[4] or row[5] or row[6] or source_url),
                            'source_date': str(row[7] or ''),
                            'excerpt': str(row[1] or '')[:320],
                        }
                    )

                if not related_iocs or not evidence_items:
                    continue

                cards_for_hunts.append(
                    {
                        'id': card_id,
                        'check_template_id': card_id,
                        'quick_check_title': str(card.get('quick_check_title') or card.get('question_text') or ''),
                        'question_text': str(card.get('question_text') or ''),
                        'related_iocs': related_iocs[:8],
                        'evidence': evidence_items[:10],
                    }
                )

        hunt_payload = _generate_ioc_hunt_queries(
            actor_name,
            cards_for_hunts,
            environment_profile=environment_profile,
        )
        hunt_by_card = hunt_payload.get('items_by_card', {}) if isinstance(hunt_payload, dict) else {}
        reason = str(hunt_payload.get('reason') or '') if isinstance(hunt_payload, dict) else ''
        ollama_status = _get_ollama_status()
        evidence_map_by_card: dict[str, list[dict[str, object]]] = {
            str(card.get('id') or ''): (card.get('evidence') if isinstance(card.get('evidence'), list) else [])
            for card in cards_for_hunts
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        ioc_map_by_card: dict[str, list[dict[str, object]]] = {
            str(card.get('id') or ''): (card.get('related_iocs') if isinstance(card.get('related_iocs'), list) else [])
            for card in cards_for_hunts
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        used_ioc_pairs = {
            (
                str(ioc.get('ioc_type') or '').strip().lower(),
                str(ioc.get('ioc_value') or '').strip().lower(),
            )
            for iocs in ioc_map_by_card.values()
            for ioc in (iocs if isinstance(iocs, list) else [])
            if isinstance(ioc, dict)
        }
        ioc_items_raw = notebook.get('ioc_items', []) if isinstance(notebook, dict) else []
        ioc_items = ioc_items_raw if isinstance(ioc_items_raw, list) else []
        filtered_iocs = quick_checks_view_service.filter_iocs_for_check_core(
            ioc_items,
            relevant_types=relevant_types,
            relevant_values=relevant_values,
        )
        iocs_in_window: list[dict[str, object]] = []
        for ioc in filtered_iocs:
            if not isinstance(ioc, dict):
                continue
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type_value or not ioc_value:
                continue
            last_seen_raw = str(ioc.get('last_seen_at') or ioc.get('created_at') or '')
            if not quick_checks_view_service.is_in_window_core(
                last_seen_raw,
                window_start=window_start_dt,
                window_end=window_end_dt,
            ):
                continue
            iocs_in_window.append(ioc)

        ioc_buckets: dict[str, list[str]] = {
            'domain': [],
            'url': [],
            'ip': [],
            'hash': [],
            'email': [],
        }
        for ioc in iocs_in_window:
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if ioc_type_value in ioc_buckets and ioc_value:
                if ioc_value not in ioc_buckets[ioc_type_value]:
                    ioc_buckets[ioc_type_value].append(ioc_value)
        for key in ioc_buckets:
            ioc_buckets[key] = ioc_buckets[key][:25]

        profile_default_lookback_hours = 24
        try:
            profile_default_lookback_hours = max(
                1,
                min(24 * 30, int(environment_profile.get('default_time_window_hours') or 24)),
            )
        except Exception:
            profile_default_lookback_hours = 24
        default_lookback_hours = profile_default_lookback_hours
        try:
            requested_lookback = int(str(query_lookback_hours or '').strip())
            if requested_lookback in {24, 24 * 7, 24 * 14, 24 * 30}:
                default_lookback_hours = requested_lookback
        except Exception:
            pass
        lookback_presets = [
            {'hours': 24, 'label': '24h', 'active': default_lookback_hours == 24},
            {'hours': 24 * 7, 'label': '7d', 'active': default_lookback_hours == 24 * 7},
            {'hours': 24 * 14, 'label': '14d', 'active': default_lookback_hours == 24 * 14},
            {'hours': 24 * 30, 'label': '30d', 'active': default_lookback_hours == 24 * 30},
        ]

        with sqlite3.connect(_db_path()) as connection:
            feedback_rows = connection.execute(
                '''
                SELECT item_id, COUNT(*), SUM(rating_score)
                FROM analyst_feedback_events
                WHERE actor_id = ? AND item_type = 'hunt_query'
                GROUP BY item_id
                ''',
                (actor_id,),
            ).fetchall()
        query_feedback_map: dict[str, dict[str, object]] = {
            str(row[0]): {'votes': int(row[1] or 0), 'score': int(row[2] or 0)}
            for row in feedback_rows
        }

        section_templates = [
            {
                'id': 'dns_proxy_web',
                'label': 'DNS/Proxy/Web',
                'required_data': 'DNS query logs, web proxy logs, HTTP gateway telemetry',
                'returns': 'Potential outbound C2/beaconing and suspicious web destination matches',
            },
            {
                'id': 'network',
                'label': 'Network',
                'required_data': 'Firewall flows, NetFlow/Zeek, IDS/IPS alerts',
                'returns': 'Network connections, destination patterns, and recurrent communication paths',
            },
            {
                'id': 'endpoint_edr',
                'label': 'Endpoint/EDR',
                'required_data': 'EDR process telemetry, command-line logs, host events',
                'returns': 'Host/process execution tied to IOC or behavior patterns',
            },
            {
                'id': 'identity',
                'label': 'Identity',
                'required_data': 'Identity provider sign-in logs, auth events, directory audit logs',
                'returns': 'Suspicious account auth activity and anomalous access behavior',
            },
        ]
        platform_tabs = [
            {'key': 'generic', 'label': 'Generic (Vendor-neutral)'},
            {'key': 'sentinel', 'label': 'Sentinel KQL'},
            {'key': 'splunk', 'label': 'Splunk SPL'},
            {'key': 'elastic', 'label': 'Elastic KQL/ES|QL'},
        ]
        platform_keys = [str(item['key']) for item in platform_tabs]
        section_views: list[dict[str, object]] = []
        for section in section_templates:
            section_views.append(
                {
                    **section,
                    'platforms': {key: [] for key in platform_keys},
                }
            )
        section_by_id = {str(section['id']): section for section in section_views}

        def _platform_key(value: str) -> str:
            lowered = str(value or '').strip().lower()
            if any(token in lowered for token in ('generic', 'vendor-neutral', 'pseudocode')):
                return 'generic'
            if 'kql' in lowered or 'sentinel' in lowered:
                return 'sentinel'
            if 'splunk' in lowered or 'spl' in lowered:
                return 'splunk'
            if 'elastic' in lowered or 'es|ql' in lowered:
                return 'elastic'
            return 'generic'

        def _section_id_for_query(*, card: dict[str, object], query_item: dict[str, object]) -> str:
            text_blob = ' '.join(
                [
                    str(card.get('quick_check_title') or card.get('question_text') or ''),
                    str(query_item.get('why_this_query') or ''),
                    str(query_item.get('query') or ''),
                    str(query_item.get('ioc_value') or ''),
                ]
            ).lower()
            if any(token in text_blob for token in ('dns', 'proxy', 'domain', 'url', 'web')):
                return 'dns_proxy_web'
            if any(token in text_blob for token in ('sign-in', 'signin', 'identity', 'account', 'auth', 'logon')):
                return 'identity'
            if any(token in text_blob for token in ('process', 'powershell', 'edr', 'endpoint', 'commandline', 'eventid')):
                return 'endpoint_edr'
            return 'network'

        def _kql_dynamic(values: list[str]) -> str:
            escaped = [str(value).replace("\\", "\\\\").replace("'", "\\'") for value in values if str(value).strip()]
            if not escaped:
                return 'dynamic([])'
            return "dynamic([" + ', '.join([f"'{value}'" for value in escaped]) + "])"

        def _splunk_or(field: str, values: list[str]) -> str:
            escaped = [str(value).replace('"', '\\"') for value in values if str(value).strip()]
            if not escaped:
                return 'false()'
            return '(' + ' OR '.join([f'{field}="{value}"' for value in escaped]) + ')'

        def _es_values(values: list[str]) -> str:
            escaped = [str(value).replace('\\', '\\\\').replace('"', '\\"') for value in values if str(value).strip()]
            if not escaped:
                return '"__no_ioc__"'
            return ', '.join([f'"{value}"' for value in escaped])

        def _section_required_data(section_id: str, platform: str) -> str:
            mapping = {
                ('dns_proxy_web', 'generic'): 'DNS + proxy + web logs with source, destination, and URL/domain fields',
                ('dns_proxy_web', 'sentinel'): 'DnsEvents, DeviceNetworkEvents, CommonSecurityLog (proxy/web gateway)',
                ('dns_proxy_web', 'splunk'): 'DNS logs (bind/infoblox/sysmon), proxy logs (zscaler/bluecoat), web gateway indexes',
                ('dns_proxy_web', 'elastic'): 'logs-dns*, logs-proxy*, logs-web* data streams',
                ('network', 'generic'): 'Firewall/flow telemetry with src/dst IP, port, protocol, and device context',
                ('network', 'sentinel'): 'CommonSecurityLog (firewall), DeviceNetworkEvents, VMConnection/Zeek if available',
                ('network', 'splunk'): 'Firewall/NetFlow/Zeek indexes, IDS logs',
                ('network', 'elastic'): 'logs-network*, logs-firewall*, logs-zeek*',
                ('endpoint_edr', 'generic'): 'EDR/endpoint process and file telemetry with command line, hash, and user/host context',
                ('endpoint_edr', 'sentinel'): 'DeviceProcessEvents, DeviceFileEvents, SecurityEvent (4688/4104)',
                ('endpoint_edr', 'splunk'): 'EDR process/file telemetry, WinEventLog:Security, PowerShell logs',
                ('endpoint_edr', 'elastic'): 'logs-endpoint*, logs-windows*, logs-powershell*',
                ('identity', 'generic'): 'Identity/authentication logs with account, source IP, result, and app/resource context',
                ('identity', 'sentinel'): 'SigninLogs, IdentityLogonEvents, AuditLogs, SecurityEvent(4624/4625)',
                ('identity', 'splunk'): 'IdP sign-in indexes (AAD/Okta), Windows auth logs, directory audit logs',
                ('identity', 'elastic'): 'logs-identity*, logs-auth*, logs-audit*',
            }
            return mapping.get((section_id, platform), '')

        def _baseline_query_item(section_id: str, platform: str) -> dict[str, str]:
            domains = ioc_buckets.get('domain', [])
            urls = ioc_buckets.get('url', [])
            ips = ioc_buckets.get('ip', [])
            hashes = ioc_buckets.get('hash', [])
            emails = ioc_buckets.get('email', [])
            lookback = f'{default_lookback_hours}h'
            if platform == 'sentinel':
                if section_id == 'dns_proxy_web':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        f"let urls = {_kql_dynamic(urls)};\n"
                        "union isfuzzy=true DnsEvents, DeviceNetworkEvents, CommonSecurityLog\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend match_domain=tostring(coalesce(Name, QueryName, DestinationHostName, RequestURL, RemoteUrl)), "
                        "match_url=tostring(coalesce(RemoteUrl, RequestURL, UrlOriginal))\n"
                        "| where (array_length(domains) > 0 and match_domain in~ (domains)) or (array_length(urls) > 0 and match_url has_any (urls))\n"
                        "| summarize hits=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by DeviceName, SourceIP, DestinationIP, match_domain, match_url\n"
                        "| sort by hits desc"
                    )
                    returns = 'Matches DNS/proxy/web telemetry to known domains/URLs and summarizes recurring destinations.'
                elif section_id == 'network':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let ips = {_kql_dynamic(ips)};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        "union isfuzzy=true CommonSecurityLog, DeviceNetworkEvents\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend dst_ip=tostring(coalesce(DestinationIP, RemoteIP, DestinationIP_s)), dst_host=tostring(coalesce(DestinationHostName, RemoteUrl, DestinationHostName_s))\n"
                        "| where (array_length(ips) > 0 and dst_ip in (ips)) or (array_length(domains) > 0 and dst_host has_any (domains))\n"
                        "| summarize conn_count=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by DeviceName, SourceIP, dst_ip, dst_host, Protocol\n"
                        "| sort by conn_count desc"
                    )
                    returns = 'Finds network flow and firewall connections to IOC destinations with recurrence and host pivots.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        f"let hashes = {_kql_dynamic(hashes)};\n"
                        "union isfuzzy=true DeviceProcessEvents, DeviceFileEvents, SecurityEvent\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend cmd=tostring(coalesce(ProcessCommandLine, CommandLine, NewProcessName)), file_hash=tostring(coalesce(SHA256, SHA1, MD5)), net_hint=tostring(coalesce(RemoteUrl, RemoteIP, DestinationHostName))\n"
                        "| where (array_length(hashes) > 0 and file_hash in (hashes)) or (array_length(domains) > 0 and net_hint has_any (domains))\n"
                        "| summarize hits=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated), commands=make_set(cmd,5) by DeviceName, InitiatingProcessAccountName, file_hash, net_hint\n"
                        "| sort by hits desc"
                    )
                    returns = 'Correlates endpoint process/file activity with IOC hashes/domains and highlights repeated host activity.'
                else:
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let ips = {_kql_dynamic(ips)};\n"
                        f"let users = {_kql_dynamic(emails)};\n"
                        "union isfuzzy=true SigninLogs, IdentityLogonEvents, SecurityEvent\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend user=tostring(coalesce(UserPrincipalName, Account, TargetUserName)), src_ip=tostring(coalesce(IPAddress, SourceIP, IpAddress))\n"
                        "| where (array_length(users) > 0 and user in~ (users)) or (array_length(ips) > 0 and src_ip in (ips))\n"
                        "| summarize attempts=count(), failures=countif(ResultType !in ('0','Success')), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by user, src_ip, AppDisplayName\n"
                        "| sort by failures desc, attempts desc"
                    )
                    returns = 'Surfaces suspicious identity sign-ins tied to IOC IPs/accounts and prioritizes failed-auth anomalies.'
            elif platform == 'splunk':
                if section_id == 'dns_proxy_web':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('query', domains) } OR { _splunk_or('dest_domain', domains) } OR { _splunk_or('url', urls) }) "
                        "| eval match=coalesce(query,dest_domain,url,uri) "
                        "| stats count as hits min(_time) as first_seen max(_time) as last_seen values(src) as src values(dest) as dest by host user match "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - hits"
                    )
                    returns = 'Finds DNS/proxy/web IOC matches and recurring source-to-destination patterns.'
                elif section_id == 'network':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('dest_ip', ips) } OR { _splunk_or('dest', domains) }) "
                        "| stats count as conn_count min(_time) as first_seen max(_time) as last_seen by src_ip dest_ip dest_port transport app "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - conn_count"
                    )
                    returns = 'Tracks firewall/flow connections to IOC endpoints and recurrent communication paths.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('process_hash', hashes) } OR { _splunk_or('CommandLine', domains) } OR { _splunk_or('Processes.process', domains) }) "
                        "| eval cmd=coalesce(CommandLine,process,Processes.process,NewProcessName) "
                        "| stats count as hits min(_time) as first_seen max(_time) as last_seen values(cmd) as commands by host user process_hash "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - hits"
                    )
                    returns = 'Maps EDR/endpoint process activity to IOC hashes/domains and returns host-user command pivots.'
                else:
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('src_ip', ips) } OR { _splunk_or('user', emails) } OR { _splunk_or('user_principal_name', emails) }) "
                        "| eval actor_user=coalesce(user,user_principal_name,Account_Name) "
                        "| stats count as attempts count(eval(action=\"failure\" OR result=\"failure\")) as failures min(_time) as first_seen max(_time) as last_seen by actor_user src_ip app result "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - failures - attempts"
                    )
                    returns = 'Highlights identity authentication anomalies associated with IOC IPs/accounts.'
            elif platform == 'elastic':
                if section_id == 'dns_proxy_web':
                    query = (
                        "FROM logs-dns*, logs-proxy*, logs-web* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE dns.question.name IN ({_es_values(domains)}) OR url.full IN ({_es_values(urls)}) OR destination.domain IN ({_es_values(domains)})\n"
                        "| STATS hits = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY host.name, user.name, source.ip, destination.ip, destination.domain, url.full\n"
                        "| SORT hits DESC"
                    )
                    returns = 'Filters DNS/proxy/web logs for IOC domains/URLs and aggregates recurring destination activity.'
                elif section_id == 'network':
                    query = (
                        "FROM logs-network*, logs-firewall*, logs-zeek* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE destination.ip IN ({_es_values(ips)}) OR destination.domain IN ({_es_values(domains)})\n"
                        "| STATS conn_count = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY source.ip, destination.ip, destination.port, network.transport, host.name\n"
                        "| SORT conn_count DESC"
                    )
                    returns = 'Finds network flow hits for IOC destinations and surfaces high-frequency connection paths.'
                elif section_id == 'endpoint_edr':
                    domain_probe = domains[0] if domains else '__no_ioc__'
                    query = (
                        "FROM logs-endpoint*, logs-windows*, logs-powershell* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE file.hash.sha256 IN ({_es_values(hashes)}) OR process.command_line LIKE \"%{domain_probe}%\"\n"
                        "| STATS hits = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp), cmds = VALUES(process.command_line) BY host.name, user.name, process.hash.sha256\n"
                        "| SORT hits DESC"
                    )
                    returns = 'Links endpoint process/file activity to IOC hashes/domains with host-user command context.'
                else:
                    query = (
                        "FROM logs-identity*, logs-auth*, logs-audit* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE source.ip IN ({_es_values(ips)}) OR user.email IN ({_es_values(emails)}) OR user.name IN ({_es_values(emails)})\n"
                        "| STATS attempts = COUNT(*), failures = COUNT_IF(event.outcome == \"failure\"), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY user.name, user.email, source.ip, event.dataset\n"
                        "| SORT failures DESC, attempts DESC"
                    )
                    returns = 'Prioritizes suspicious identity auth events tied to IOC IPs and user identities.'
            else:
                if section_id == 'dns_proxy_web':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: DNS + Proxy + Web logs\n"
                        f"Filter: domain in {domains[:10]} OR url in {urls[:10]}\n"
                        "Group by: src_host/src_user, destination_domain/url\n"
                        "Return: repeat_count, first_seen, last_seen, top destinations\n"
                        "Pivot: from repeated destinations into endpoint process and identity sign-ins"
                    )
                    returns = 'Vendor-neutral workflow for IOC matching in DNS/proxy/web telemetry.'
                elif section_id == 'network':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: firewall/netflow/network sensor logs\n"
                        f"Filter: destination_ip in {ips[:10]} OR destination_domain in {domains[:10]}\n"
                        "Group by: src_ip, dst_ip, dst_port, protocol, asset\n"
                        "Return: connection_count, first_seen, last_seen, recurrent paths\n"
                        "Pivot: correlate same src assets with endpoint/identity events"
                    )
                    returns = 'Vendor-neutral network hunt for IOC destination reachability and recurrence.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: EDR process + file + script logs\n"
                        f"Filter: process/file hash in {hashes[:10]} OR command line contains {domains[:10]}\n"
                        "Group by: host, user, process, hash\n"
                        "Return: execution_count, command_lines, first_seen, last_seen\n"
                        "Pivot: connect matching executions to network callbacks and auth anomalies"
                    )
                    returns = 'Vendor-neutral endpoint/EDR hunt for IOC-linked execution and artifacts.'
                else:
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: IdP sign-in + directory audit + auth logs\n"
                        f"Filter: source_ip in {ips[:10]} OR account/email in {emails[:10]}\n"
                        "Group by: account, source_ip, app/resource, auth result\n"
                        "Return: attempts, failures, impossible travel or unusual source patterns\n"
                        "Pivot: tie suspicious accounts back to endpoint and network indicators"
                    )
                    returns = 'Vendor-neutral identity hunt for IOC-associated authentication anomalies.'
            return {
                'required_data': _section_required_data(section_id, platform),
                'returns': returns,
                'query': query,
            }

        seen_queries: dict[tuple[str, str], set[str]] = {}

        for card_id, card in cards_by_id.items():
            evidence_items_raw = evidence_map_by_card.get(card_id, [])
            evidence_items = evidence_items_raw if isinstance(evidence_items_raw, list) else []
            evidence_lookup = {
                str(item.get('id') or ''): item
                for item in evidence_items
                if isinstance(item, dict) and str(item.get('id') or '').strip()
            }
            query_items_raw = hunt_by_card.get(card_id, []) if isinstance(hunt_by_card, dict) else []
            query_items = list(query_items_raw) if isinstance(query_items_raw, list) else []
            behavior_query_items_raw = card_behavior_queries.get(card_id, [])
            behavior_query_items = behavior_query_items_raw if isinstance(behavior_query_items_raw, list) else []
            for behavior_query in behavior_query_items:
                if not isinstance(behavior_query, dict):
                    continue
                query_items.append(
                    {
                        'platform': str(behavior_query.get('platform') or 'SIEM'),
                        'ioc_value': '',
                        'query': str(behavior_query.get('query') or ''),
                        'why_this_query': str(behavior_query.get('why_this_query') or ''),
                        'evidence_source_ids': [],
                        'evidence_sources': [],
                    }
                )
            for query_item in query_items:
                if not isinstance(query_item, dict):
                    continue
                query_value = str(query_item.get('query') or '').strip()
                if not query_value:
                    continue
                query_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{card_id}:{query_value}"))
                query_feedback = query_feedback_map.get(query_id, {'votes': 0, 'score': 0})
                refs_raw = query_item.get('evidence_source_ids')
                refs = refs_raw if isinstance(refs_raw, list) else []
                evidence_sources = [
                    evidence_lookup.get(
                        str(ref_id),
                        {'id': str(ref_id), 'source_title': str(ref_id), 'source_url': '', 'source_date': ''},
                    )
                    for ref_id in refs
                ]
                platform = _platform_key(str(query_item.get('platform') or ''))
                section_id = _section_id_for_query(card=card, query_item=query_item)
                section = section_by_id.get(section_id)
                if section is None:
                    continue
                dedupe_key = (section_id, platform)
                section_seen = seen_queries.setdefault(dedupe_key, set())
                if query_value in section_seen:
                    continue
                section_seen.add(query_value)
                section['platforms'][platform].append(
                    {
                        'card_id': card_id,
                        'card_title': str(card.get('quick_check_title') or card.get('question_text') or card_id),
                        'required_data': _section_required_data(section_id, platform) or str(section.get('required_data') or ''),
                        'returns': str(query_item.get('why_this_query') or section.get('returns') or ''),
                        'query': query_value,
                        'query_id': query_id,
                        'feedback_votes': int(query_feedback.get('votes') or 0),
                        'feedback_score': int(query_feedback.get('score') or 0),
                        'evidence_sources': evidence_sources,
                    }
                )
        has_generated_queries = any(
            isinstance(items, list) and len(items) > 0
            for items in (hunt_by_card.values() if isinstance(hunt_by_card, dict) else [])
        )

        for section in section_views:
            section_id = str(section.get('id') or '').strip()
            for platform in platform_keys:
                baseline_item = _baseline_query_item(section_id, platform)
                if not baseline_item:
                    continue
                existing_items = section['platforms'].get(platform, [])
                baseline_query = str(baseline_item.get('query') or '').strip()
                if not baseline_query:
                    continue
                if any(str(item.get('query') or '').strip() == baseline_query for item in existing_items):
                    continue
                section['platforms'][platform] = [
                    {
                        'card_id': selected_check_id or '',
                        'card_title': 'Actor-scoped baseline IOC hunt',
                        'required_data': str(baseline_item.get('required_data') or section.get('required_data') or ''),
                        'returns': str(baseline_item.get('returns') or section.get('returns') or ''),
                        'query': baseline_query,
                        'query_id': str(uuid.uuid5(uuid.NAMESPACE_URL, f"{actor_id}:{section_id}:{platform}:baseline")),
                        'feedback_votes': 0,
                        'feedback_score': 0,
                        'evidence_sources': [],
                    }
                ] + list(existing_items)
        has_primary_queries = any(
            bool(section.get('platforms', {}).get(platform))
            for section in section_views
            for platform in platform_keys
        )

        misc_ioc_rows: list[dict[str, object]] = []
        for ioc in iocs_in_window:
            if not isinstance(ioc, dict):
                continue
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type_value or not ioc_value:
                continue
            if not _ioc_value_is_hunt_relevant(ioc_type_value, ioc_value):
                continue
            last_seen_raw = str(ioc.get('last_seen_at') or ioc.get('created_at') or '')
            key = (ioc_type_value, ioc_value.lower())
            if key in used_ioc_pairs:
                continue
            row = {
                'ioc_type': ioc_type_value,
                'ioc_value': ioc_value,
                'first_seen': str(ioc.get('created_at') or ''),
                'last_seen': last_seen_raw,
                'sources_count': int(ioc.get('seen_count') or 1),
                'confidence': int(ioc.get('confidence_score') or 0),
                'source_ref': str(ioc.get('source_ref') or ''),
            }
            misc_ioc_rows.append(row)

        if selected_check_id:
            actor_name_lc = actor_name.lower()

            def _misc_matches_selected_check(row: dict[str, object]) -> bool:
                ioc_value_lc = str(row.get('ioc_value') or '').strip().lower()
                source_ref_lc = str(row.get('source_ref') or '').strip().lower()
                if relevant_values and ioc_value_lc in relevant_values:
                    return True
                if relevant_types and str(row.get('ioc_type') or '').strip().lower() in relevant_types:
                    if not relevant_values:
                        return True
                if selected_card_text and (ioc_value_lc in selected_card_text or (source_ref_lc and source_ref_lc in selected_card_text)):
                    return True
                if actor_name_lc and source_ref_lc and actor_name_lc in source_ref_lc:
                    return True
                return False

            misc_ioc_rows = [row for row in misc_ioc_rows if _misc_matches_selected_check(row)]

        normalized_type_filter = str(ioc_type or '').strip().lower()
        if normalized_type_filter:
            misc_ioc_rows = [row for row in misc_ioc_rows if str(row.get('ioc_type') or '').strip().lower() == normalized_type_filter]
        try:
            min_confidence = int(confidence) if confidence is not None and str(confidence).strip() != '' else None
        except Exception:
            min_confidence = None
        if min_confidence is not None:
            misc_ioc_rows = [row for row in misc_ioc_rows if int(row.get('confidence') or 0) >= min_confidence]
        try:
            min_source_count = int(source_count) if source_count is not None and str(source_count).strip() != '' else None
        except Exception:
            min_source_count = None
        if min_source_count is not None:
            misc_ioc_rows = [row for row in misc_ioc_rows if int(row.get('sources_count') or 0) >= min_source_count]
        freshness_value = str(freshness or '').strip().lower()
        if freshness_value in {'24h', '7d', '30d'}:
            cutoff_hours = 24 if freshness_value == '24h' else (24 * 7 if freshness_value == '7d' else 24 * 30)
            freshness_cutoff = now_utc - timedelta(hours=cutoff_hours)
            misc_ioc_rows = [
                row
                for row in misc_ioc_rows
                if quick_checks_view_service.is_in_window_core(
                    str(row.get('last_seen') or ''),
                    window_start=freshness_cutoff,
                    window_end=now_utc,
                )
            ]
        misc_ioc_rows.sort(
            key=lambda row: (str(row.get('last_seen') or ''), int(row.get('confidence') or 0)),
            reverse=True,
        )

        if str(export or '').strip().lower() == 'json':
            return JSONResponse(
                {
                    'actor_id': actor_id,
                    'window_start': window_start_iso,
                    'window_end': window_end_iso,
                    'quick_check_id': selected_check_id,
                    'count': len(misc_ioc_rows),
                    'items': misc_ioc_rows,
                }
            )
        if str(export or '').strip().lower() == 'csv':
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(['type', 'value', 'first_seen', 'last_seen', 'sources_count', 'confidence'])
            for row in misc_ioc_rows:
                writer.writerow(
                    [
                        row.get('ioc_type', ''),
                        row.get('ioc_value', ''),
                        row.get('first_seen', ''),
                        row.get('last_seen', ''),
                        row.get('sources_count', ''),
                        row.get('confidence', ''),
                    ]
                )
            return Response(
                content=buffer.getvalue(),
                media_type='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{actor_id}-misc-iocs.csv"'},
            )

        return _templates.TemplateResponse(
            request,
            'ioc_hunts.html',
            {
                'actor_id': actor_id,
                'actor_name': actor_name,
                'check_template_id': selected_check_id,
                'quick_check_id': selected_check_id,
                'reason': reason,
                'has_generated_queries': has_generated_queries,
                'has_primary_queries': has_primary_queries,
                'ollama_status': ollama_status,
                'environment_profile': environment_profile,
                'query_lookback_hours': default_lookback_hours,
                'lookback_presets': lookback_presets,
                'window_start': window_start_iso,
                'window_end': window_end_iso,
                'platform_tabs': platform_tabs,
                'sections': section_views,
                'misc_iocs': misc_ioc_rows,
                'filters': {
                    'ioc_type': ioc_type or '',
                    'confidence': confidence or '',
                    'source_count': source_count or '',
                    'freshness': freshness or '',
                },
            },
        )

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

    @router.get(route_paths.ACTOR_UI_LIVE, response_class=JSONResponse)
    def actor_live_state(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        if _recover_stale_running_states is not None:
            try:
                _recover_stale_running_states()
            except Exception:
                pass
        with sqlite3.connect(_db_path(), timeout=5.0) as connection:
            actor_row = connection.execute(
                '''
                SELECT notebook_status, notebook_message
                FROM actor_profiles
                WHERE id = ?
                ''',
                (actor_id,),
            ).fetchone()
        if actor_row is None:
            raise HTTPException(status_code=404, detail='actor not found')
        actor_status = str(actor_row[0] or 'idle')
        actor_message = str(actor_row[1] or '')
        if actor_status.lower() == 'running':
            return {
                'actor_id': actor_id,
                'actor': {
                    'id': actor_id,
                    'notebook_status': actor_status,
                    'notebook_message': actor_message or 'Refreshing notebook...',
                },
                'notebook_status': actor_status,
                'notebook_message': actor_message or 'Refreshing notebook...',
                'counts': {},
                'kpis': {},
                'recent_change_summary': {},
                'priority_questions': [],
                'top_change_signals': [],
                'recent_activity_synthesis': [],
                'top_techniques': [],
                'timeline_graph': [],
                'actor_profile_summary': '',
                'timeline_compact_rows': [],
                'timeline_window_label': '',
            }

        try:
            notebook = _fetch_actor_notebook(
                actor_id,
                source_tier=source_tier,
                min_confidence_weight=min_confidence_weight,
                source_days=source_days,
            )
        except sqlite3.OperationalError as exc:
            if 'database is locked' not in str(exc).lower():
                raise
            LOGGER.warning('live_state_locked actor_id=%s', actor_id)
            return {
                'actor_id': actor_id,
                'actor': {
                    'id': actor_id,
                    'notebook_status': actor_status,
                    'notebook_message': actor_message or 'Notebook refresh in progress.',
                },
                'notebook_status': actor_status,
                'notebook_message': actor_message or 'Notebook refresh in progress.',
                'counts': {},
                'kpis': {},
                'recent_change_summary': {},
                'priority_questions': [],
                'top_change_signals': [],
                'recent_activity_synthesis': [],
                'top_techniques': [],
                'timeline_graph': [],
                'actor_profile_summary': '',
                'timeline_compact_rows': [],
                'timeline_window_label': '',
            }

        notebook_actor = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        return {
            'actor_id': actor_id,
            'actor': notebook_actor,
            'notebook_status': str(notebook_actor.get('notebook_status') or actor_status),
            'notebook_message': str(notebook_actor.get('notebook_message') or actor_message),
            'counts': notebook.get('counts', {}),
            'kpis': notebook.get('kpis', {}),
            'recent_change_summary': notebook.get('recent_change_summary', {}),
            'priority_questions': notebook.get('priority_questions', []),
            'top_change_signals': notebook.get('top_change_signals', []),
            'recent_activity_synthesis': notebook.get('recent_activity_synthesis', []),
            'top_techniques': notebook.get('top_techniques', []),
            'timeline_graph': notebook.get('timeline_graph', []),
            'actor_profile_summary': notebook.get('actor_profile_summary', ''),
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

    @router.get(route_paths.ACTOR_EXPORT_ANALYST_PACK, response_class=JSONResponse)
    def export_analyst_pack(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 1000,
        history_limit: int = 1000,
        ) -> dict[str, object]:
        return _build_analyst_pack_payload(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            observations_limit=observations_limit,
            history_limit=history_limit,
        )

    @router.get(route_paths.ACTOR_EXPORT_TASKS_JSON, response_class=JSONResponse)
    def export_tasks_json(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            rows = connection.execute(
                '''
                SELECT id, title, details, priority, status, owner, due_date,
                       linked_type, linked_key, created_at, updated_at
                FROM actor_tasks
                WHERE actor_id = ?
                ORDER BY updated_at DESC
                ''',
                (actor_id,),
            ).fetchall()
        items = [
            {
                'id': str(row[0] or ''),
                'title': str(row[1] or ''),
                'details': str(row[2] or ''),
                'priority': str(row[3] or ''),
                'status': str(row[4] or ''),
                'owner': str(row[5] or ''),
                'due_date': str(row[6] or ''),
                'linked_type': str(row[7] or ''),
                'linked_key': str(row[8] or ''),
                'created_at': str(row[9] or ''),
                'updated_at': str(row[10] or ''),
            }
            for row in rows
        ]
        return {'actor_id': actor_id, 'count': len(items), 'items': items}

    @router.get(route_paths.ACTOR_EXPORT_TASKS_CSV)
    def export_tasks_csv(actor_id: str) -> Response:
        payload = export_tasks_json(actor_id)
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(['actor_id', 'id', 'title', 'details', 'priority', 'status', 'owner', 'due_date', 'linked_type', 'linked_key', 'created_at', 'updated_at'])
        for item in payload.get('items', []):
            if not isinstance(item, dict):
                continue
            writer.writerow(
                [
                    actor_id,
                    item.get('id', ''),
                    item.get('title', ''),
                    item.get('details', ''),
                    item.get('priority', ''),
                    item.get('status', ''),
                    item.get('owner', ''),
                    item.get('due_date', ''),
                    item.get('linked_type', ''),
                    item.get('linked_key', ''),
                    item.get('created_at', ''),
                    item.get('updated_at', ''),
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{actor_id}-tasks.csv"'},
        )

    @router.get(route_paths.ACTOR_EXPORT_OUTCOMES_JSON, response_class=JSONResponse)
    def export_outcomes_json(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            rows = connection.execute(
                '''
                SELECT id, outcome_type, summary, result, linked_task_id,
                       linked_technique_id, evidence_ref, created_by, created_at
                FROM actor_operational_outcomes
                WHERE actor_id = ?
                ORDER BY created_at DESC
                ''',
                (actor_id,),
            ).fetchall()
        items = [
            {
                'id': str(row[0] or ''),
                'outcome_type': str(row[1] or ''),
                'summary': str(row[2] or ''),
                'result': str(row[3] or ''),
                'linked_task_id': str(row[4] or ''),
                'linked_technique_id': str(row[5] or ''),
                'evidence_ref': str(row[6] or ''),
                'created_by': str(row[7] or ''),
                'created_at': str(row[8] or ''),
            }
            for row in rows
        ]
        return {'actor_id': actor_id, 'count': len(items), 'items': items}

    @router.get(route_paths.ACTOR_EXPORT_OUTCOMES_CSV)
    def export_outcomes_csv(actor_id: str) -> Response:
        payload = export_outcomes_json(actor_id)
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(['actor_id', 'id', 'outcome_type', 'summary', 'result', 'linked_task_id', 'linked_technique_id', 'evidence_ref', 'created_by', 'created_at'])
        for item in payload.get('items', []):
            if not isinstance(item, dict):
                continue
            writer.writerow(
                [
                    actor_id,
                    item.get('id', ''),
                    item.get('outcome_type', ''),
                    item.get('summary', ''),
                    item.get('result', ''),
                    item.get('linked_task_id', ''),
                    item.get('linked_technique_id', ''),
                    item.get('evidence_ref', ''),
                    item.get('created_by', ''),
                    item.get('created_at', ''),
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{actor_id}-outcomes.csv"'},
        )

    @router.get(route_paths.ACTOR_EXPORT_COVERAGE_JSON, response_class=JSONResponse)
    def export_coverage_json(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            rows = connection.execute(
                '''
                SELECT technique_id, technique_name, detection_name, control_name,
                       coverage_status, validation_status, validation_evidence,
                       updated_by, updated_at
                FROM actor_technique_coverage
                WHERE actor_id = ?
                ORDER BY updated_at DESC
                ''',
                (actor_id,),
            ).fetchall()
        items = [
            {
                'technique_id': str(row[0] or ''),
                'technique_name': str(row[1] or ''),
                'detection_name': str(row[2] or ''),
                'control_name': str(row[3] or ''),
                'coverage_status': str(row[4] or ''),
                'validation_status': str(row[5] or ''),
                'validation_evidence': str(row[6] or ''),
                'updated_by': str(row[7] or ''),
                'updated_at': str(row[8] or ''),
            }
            for row in rows
        ]
        return {'actor_id': actor_id, 'count': len(items), 'items': items}

    @router.get(route_paths.ACTOR_EXPORT_COVERAGE_CSV)
    def export_coverage_csv(actor_id: str) -> Response:
        payload = export_coverage_json(actor_id)
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(['actor_id', 'technique_id', 'technique_name', 'detection_name', 'control_name', 'coverage_status', 'validation_status', 'validation_evidence', 'updated_by', 'updated_at'])
        for item in payload.get('items', []):
            if not isinstance(item, dict):
                continue
            writer.writerow(
                [
                    actor_id,
                    item.get('technique_id', ''),
                    item.get('technique_name', ''),
                    item.get('detection_name', ''),
                    item.get('control_name', ''),
                    item.get('coverage_status', ''),
                    item.get('validation_status', ''),
                    item.get('validation_evidence', ''),
                    item.get('updated_by', ''),
                    item.get('updated_at', ''),
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{actor_id}-coverage.csv"'},
        )

    @router.get(route_paths.ACTOR_EXPORT_ANALYST_PACK_PDF)
    def export_analyst_pack_pdf(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 500,
        history_limit: int = 500,
    ) -> Response:
        pack = _build_analyst_pack_payload(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            observations_limit=observations_limit,
            history_limit=history_limit,
        )
        actor_meta = pack.get('actor', {}) if isinstance(pack.get('actor', {}), dict) else {}
        actor_name = str(actor_meta.get('display_name') or actor_id).strip() or actor_id
        summary = pack.get('recent_change_summary', {})
        summary_dict = summary if isinstance(summary, dict) else {}
        lines: list[str] = [
            f'Actor ID: {actor_id}',
            f'Actor: {actor_name}',
            f'Exported At (UTC): {str(pack.get("exported_at") or "")}',
            '',
            'Recent Change Summary',
        ]
        for key in ('new_reports', 'new_items', 'targets', 'damage', 'ransomware'):
            lines.append(f'- {key}: {summary_dict.get(key, 0)}')

        lines.append('')
        lines.append('Priority Questions')
        for card in (pack.get('priority_questions', []) if isinstance(pack.get('priority_questions', []), list) else [])[:10]:
            if not isinstance(card, dict):
                continue
            lines.append(f"- {str(card.get('question_text') or '').strip()[:180]}")
            lines.append(f"  First Step: {str(card.get('first_step') or '').strip()[:180]}")
            lines.append(f"  Watch: {str(card.get('what_to_look_for') or '').strip()[:180]}")

        lines.append('')
        lines.append('Recent IOCs')
        for ioc in (pack.get('ioc_items', []) if isinstance(pack.get('ioc_items', []), list) else [])[:25]:
            if not isinstance(ioc, dict):
                continue
            ioc_type = str(ioc.get('ioc_type') or '').strip()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            source_ref = str(ioc.get('source_ref') or '').strip()
            lines.append(f'- [{ioc_type}] {ioc_value} {f"({source_ref})" if source_ref else ""}'.strip())

        lines.append('')
        lines.append('Observations')
        for obs in (pack.get('observations', []) if isinstance(pack.get('observations', []), list) else [])[:40]:
            if not isinstance(obs, dict):
                continue
            updated_at = str(obs.get('updated_at') or '').strip()[:19]
            updated_by = str(obs.get('updated_by') or '').strip()
            note = str(obs.get('note') or '').strip().replace('\n', ' ')[:180]
            lines.append(f'- {updated_at} {updated_by}: {note}')

        pdf_bytes = _render_simple_text_pdf(title=f'Analyst Pack - {actor_name}', lines=lines)
        safe_actor = ''.join(ch if ch.isalnum() or ch in {'-', '_'} else '-' for ch in actor_id).strip('-') or 'actor'
        return Response(
            content=pdf_bytes,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{safe_actor}-analyst-pack.pdf"'},
        )

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

    return router
