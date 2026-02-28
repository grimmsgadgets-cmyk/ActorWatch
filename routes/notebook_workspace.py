import csv
import io
import sqlite3

import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response


def register_notebook_workspace_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _safe_json_string_list = deps['safe_json_string_list']
    _templates = deps['templates']
    _fetch_actor_notebook = deps['fetch_actor_notebook']

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

