import csv
import io
import sqlite3

import route_paths
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response


def register_notebook_export_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _actor_exists = deps['actor_exists']
    _build_analyst_pack_payload = deps['build_analyst_pack_payload']
    _render_simple_text_pdf = deps['render_simple_text_pdf']

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


