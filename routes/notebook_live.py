import logging
import sqlite3

import route_paths
import services.notebook_service as notebook_service
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

LOGGER = logging.getLogger(__name__)


def register_notebook_live_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _db_path = deps['db_path']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _recover_stale_running_states = deps.get('recover_stale_running_states')

    @router.get(route_paths.ACTOR_UI_LIVE, response_class=JSONResponse)
    def actor_live_state(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        def _running_or_locked_payload(*, status: str, message: str) -> dict[str, object]:
            fallback = notebook_service.finalize_notebook_contract_core(
                {
                    'actor_id': actor_id,
                    'actor': {
                        'id': actor_id,
                        'notebook_status': status,
                        'notebook_message': message,
                    },
                    'notebook_status': status,
                    'notebook_message': message,
                    'counts': {},
                    'kpis': {},
                    'recent_change_summary': {},
                    'top_techniques': [],
                    'timeline_graph': [],
                    'actor_profile_summary': '',
                    'timeline_compact_rows': [],
                    'timeline_window_label': '',
                }
            )
            payload = _ensure_bastion_minimum_payload(fallback)
            actor_meta = payload.get('actor', {}) if isinstance(payload.get('actor'), dict) else {}
            return {
                'actor_id': actor_id,
                'actor': actor_meta,
                'notebook_status': str(actor_meta.get('notebook_status') or status),
                'notebook_message': str(actor_meta.get('notebook_message') or message),
                'counts': payload.get('counts', {}),
                'kpis': payload.get('kpis', {}),
                'recent_change_summary': payload.get('recent_change_summary', {}),
                'priority_questions': payload.get('priority_questions', []),
                'top_change_signals': payload.get('top_change_signals', []),
                'recent_activity_synthesis': payload.get('recent_activity_synthesis', []),
                'top_techniques': payload.get('top_techniques', []),
                'timeline_graph': payload.get('timeline_graph', []),
                'actor_profile_summary': payload.get('actor_profile_summary', ''),
                'timeline_compact_rows': payload.get('timeline_compact_rows', []),
                'timeline_window_label': payload.get('timeline_window_label', ''),
            }

        def _ensure_bastion_minimum_payload(payload: dict[str, object]) -> dict[str, object]:
            data = dict(payload or {})
            top_change_signals = data.get('top_change_signals')
            highlights = data.get('recent_activity_highlights')
            timeline_recent = data.get('timeline_recent_items')
            synthesis = data.get('recent_activity_synthesis')
            summary = data.get('recent_change_summary')

            top_items = top_change_signals if isinstance(top_change_signals, list) else []
            highlight_items = highlights if isinstance(highlights, list) else []
            timeline_items = timeline_recent if isinstance(timeline_recent, list) else []
            synthesis_items = synthesis if isinstance(synthesis, list) else []
            summary_obj = summary if isinstance(summary, dict) else {}

            if not top_items:
                derived_changes: list[dict[str, object]] = []
                for item in highlight_items[:5]:
                    if not isinstance(item, dict):
                        continue
                    text = (
                        str(item.get('text') or '').strip()
                        or str(item.get('summary') or '').strip()
                        or str(item.get('evidence_title') or '').strip()
                    )
                    if not text:
                        continue
                    derived_changes.append(
                        {
                            'change_summary': text[:220],
                            'change_why_new': text[:320],
                        }
                    )
                if not derived_changes:
                    for item in timeline_items[:5]:
                        if not isinstance(item, dict):
                            continue
                        text = (
                            str(item.get('summary') or '').strip()
                            or str(item.get('title') or '').strip()
                        )
                        if not text:
                            continue
                        derived_changes.append(
                            {
                                'change_summary': text[:220],
                                'change_why_new': text[:320],
                            }
                        )
                data['top_change_signals'] = derived_changes
                top_items = derived_changes

            if not synthesis_items:
                derived_synthesis: list[dict[str, str]] = []
                for item in top_items[:5]:
                    if not isinstance(item, dict):
                        continue
                    text = (
                        str(item.get('change_why_new') or '').strip()
                        or str(item.get('change_summary') or '').strip()
                    )
                    if not text:
                        continue
                    derived_synthesis.append(
                        {
                            'label': 'What changed',
                            'text': text[:320],
                        }
                    )
                data['recent_activity_synthesis'] = derived_synthesis

            if not summary_obj:
                change_count = max(len(highlight_items), len(timeline_items))
                data['recent_change_summary'] = {
                    'new_reports': str(change_count),
                    'targets': 'See latest timeline evidence',
                    'damage': 'See latest actor activity details',
                }
            else:
                data['recent_change_summary'] = {
                    'new_reports': str(summary_obj.get('new_reports') or max(len(highlight_items), len(timeline_items))),
                    'targets': str(summary_obj.get('targets') or 'See latest timeline evidence'),
                    'damage': str(summary_obj.get('damage') or 'See latest actor activity details'),
                }
            return data

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
            try:
                running_notebook = _fetch_actor_notebook(
                    actor_id,
                    source_tier=source_tier,
                    min_confidence_weight=min_confidence_weight,
                    source_days=source_days,
                    build_on_cache_miss=False,
                    allow_stale_cache=True,
                )
                if isinstance(running_notebook, dict):
                    running_actor = dict(running_notebook.get('actor', {})) if isinstance(running_notebook.get('actor'), dict) else {}
                    running_actor['notebook_status'] = actor_status
                    running_actor['notebook_message'] = actor_message or 'Refreshing notebook...'
                    running_payload = _ensure_bastion_minimum_payload(running_notebook)
                    return {
                        'actor_id': actor_id,
                        'actor': running_actor,
                        'notebook_status': str(running_actor.get('notebook_status') or actor_status),
                        'notebook_message': str(running_actor.get('notebook_message') or actor_message or 'Refreshing notebook...'),
                        'counts': running_payload.get('counts', {}),
                        'kpis': running_payload.get('kpis', {}),
                        'recent_change_summary': running_payload.get('recent_change_summary', {}),
                        'priority_questions': running_payload.get('priority_questions', []),
                        'top_change_signals': running_payload.get('top_change_signals', []),
                        'recent_activity_synthesis': running_payload.get('recent_activity_synthesis', []),
                        'top_techniques': running_payload.get('top_techniques', []),
                        'timeline_graph': running_payload.get('timeline_graph', []),
                        'actor_profile_summary': running_payload.get('actor_profile_summary', ''),
                        'timeline_compact_rows': running_payload.get('timeline_compact_rows', []),
                        'timeline_window_label': running_payload.get('timeline_window_label', ''),
                    }
            except Exception:
                pass
            return _running_or_locked_payload(
                status=actor_status,
                message=actor_message or 'Refreshing notebook...',
            )

        try:
            notebook = _fetch_actor_notebook(
                actor_id,
                source_tier=source_tier,
                min_confidence_weight=min_confidence_weight,
                source_days=source_days,
                build_on_cache_miss=False,
                allow_stale_cache=True,
            )
        except sqlite3.OperationalError as exc:
            if 'database is locked' not in str(exc).lower():
                raise
            LOGGER.warning('live_state_locked actor_id=%s', actor_id)
            return _running_or_locked_payload(
                status=actor_status,
                message=actor_message or 'Notebook refresh in progress.',
            )

        notebook_actor = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        notebook_payload = (
            _ensure_bastion_minimum_payload(notebook)
            if isinstance(notebook, dict)
            else {}
        )
        resolved_status = str(notebook_actor.get('notebook_status') or actor_status)
        resolved_message = str(notebook_actor.get('notebook_message') or actor_message)
        if actor_status.lower() in {'running', 'error'}:
            resolved_status = actor_status
            if actor_message.strip():
                resolved_message = actor_message
        resolved_actor = dict(notebook_actor)
        resolved_actor['notebook_status'] = resolved_status
        resolved_actor['notebook_message'] = resolved_message
        return {
            'actor_id': actor_id,
            'actor': resolved_actor,
            'notebook_status': resolved_status,
            'notebook_message': resolved_message,
            'counts': notebook_payload.get('counts', {}),
            'kpis': notebook_payload.get('kpis', {}),
            'recent_change_summary': notebook_payload.get('recent_change_summary', {}),
            'priority_questions': notebook_payload.get('priority_questions', []),
            'top_change_signals': notebook_payload.get('top_change_signals', []),
            'recent_activity_synthesis': notebook_payload.get('recent_activity_synthesis', []),
            'top_techniques': notebook_payload.get('top_techniques', []),
            'timeline_graph': notebook_payload.get('timeline_graph', []),
            'actor_profile_summary': notebook_payload.get('actor_profile_summary', ''),
            'timeline_compact_rows': notebook_payload.get('timeline_compact_rows', []),
            'timeline_window_label': notebook_payload.get('timeline_window_label', ''),
        }
