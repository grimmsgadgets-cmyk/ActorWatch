import json
import sqlite3
import uuid

import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response


def register_notebook_operation_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _generate_actor_requirements = deps['generate_actor_requirements']
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _safe_json_string_list = deps['safe_json_string_list']
    _actor_exists = deps['actor_exists']
    _get_tracking_intent = deps['get_tracking_intent']
    _upsert_tracking_intent = deps['upsert_tracking_intent']
    _confirm_actor_assessment = deps['confirm_actor_assessment']
    _dispatch_alert_deliveries = deps.get('dispatch_alert_deliveries')

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





