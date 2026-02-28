import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
import services.ioc_store_service as ioc_store_service
import services.ioc_validation_service as ioc_validation_service
import services.quick_checks_view_service as quick_checks_view_service
from pipelines.notebook_ioc_helpers import _derived_ioc_items_from_sources
from pipelines.notebook_ioc_helpers import _extract_ioc_candidates_from_text
from pipelines.notebook_ioc_helpers import _relevant_iocs_for_quick_check
from pipelines.notebook_ioc_helpers import _ioc_seen_within_days
from pipelines.notebook_ioc_helpers import latest_reporting_recency_label
from pipelines.notebook_behavior_helpers import build_environment_checks
from pipelines.notebook_behavior_helpers import recent_change_summary
from pipelines.notebook_behavior_helpers import build_top_change_signals
from pipelines.notebook_behavior_helpers import build_recent_activity_highlights
from pipelines.notebook_quickcheck_helpers import _behavior_id_from_context
from pipelines.notebook_quickcheck_helpers import _behavior_query_pack
from pipelines.notebook_quickcheck_helpers import _extract_behavior_observables
from pipelines.notebook_quickcheck_helpers import _format_evidence_ref_core
from pipelines.notebook_quickcheck_helpers import _quick_check_is_evidence_backed_core
from pipelines.notebook_quickcheck_helpers import _quick_check_update_effective_dt
from pipelines.notebook_quickcheck_helpers import _select_event_ids_for_where_to_start_core
from pipelines.notebook_quickcheck_helpers import QUICK_CHECK_TEMPLATE_HINTS


def fetch_actor_notebook_core(
    actor_id: str,
    *,
    db_path: str,
    source_tier: str | None = None,
    min_confidence_weight: int | None = None,
    source_days: int | None = None,
    deps: dict[str, object],
) -> dict[str, object]:
    _parse_published_datetime = deps['parse_published_datetime']
    _safe_json_string_list = deps['safe_json_string_list']
    _actor_signal_categories = deps['actor_signal_categories']
    _question_actor_relevance = deps['question_actor_relevance']
    _question_org_alignment = deps['question_org_alignment']
    _priority_rank_score = deps['priority_rank_score']
    _phase_label_for_question = deps['phase_label_for_question']
    _priority_where_to_check = deps['priority_where_to_check']
    _priority_confidence_label = deps['priority_confidence_label']
    _quick_check_title = deps['quick_check_title']
    _short_decision_trigger = deps['short_decision_trigger']
    _telemetry_anchor_line = deps['telemetry_anchor_line']
    _priority_next_best_action = deps['priority_next_best_action']
    _guidance_line = deps['guidance_line']
    _guidance_query_hint = deps['guidance_query_hint']
    _priority_disconfirming_signal = deps['priority_disconfirming_signal']
    _confidence_change_threshold_line = deps.get('confidence_change_threshold_line', deps['escalation_threshold_line'])
    _expected_output_line = deps.get('expected_output_line', _short_decision_trigger)
    _priority_update_recency_label = deps['priority_update_recency_label']
    _org_alignment_label = deps['org_alignment_label']
    _fallback_priority_questions = deps['fallback_priority_questions']
    _token_overlap = deps['token_overlap']
    _build_actor_profile_from_mitre = deps['build_actor_profile_from_mitre']
    _group_top_techniques = deps['group_top_techniques']
    _favorite_attack_vectors = deps['favorite_attack_vectors']
    _known_technique_ids_for_entity = deps['known_technique_ids_for_entity']
    _emerging_techniques_from_timeline = deps['emerging_techniques_from_timeline']
    _build_timeline_graph = deps['build_timeline_graph']
    _compact_timeline_rows = deps['compact_timeline_rows']
    _actor_terms = deps['actor_terms']
    _build_recent_activity_highlights = deps['build_recent_activity_highlights']
    _build_top_change_signals = deps.get('build_top_change_signals', build_top_change_signals)
    _ollama_review_change_signals = deps.get('ollama_review_change_signals', lambda *_args, **_kwargs: [])
    _ollama_synthesize_recent_activity = deps.get('ollama_synthesize_recent_activity', lambda *_args, **_kwargs: [])
    _enforce_ollama_synthesis = bool(deps.get('enforce_ollama_synthesis', False))
    _build_recent_activity_synthesis = deps['build_recent_activity_synthesis']
    _recent_change_summary = deps['recent_change_summary']
    _build_environment_checks = deps['build_environment_checks']
    _build_notebook_kpis = deps['build_notebook_kpis']
    _format_date_or_unknown = deps['format_date_or_unknown']
    _recent_change_max_days = int(deps.get('recent_change_max_days', 45))
    _load_quick_check_overrides = deps.get('load_quick_check_overrides')
    _load_source_reliability_map = deps.get(
        'load_source_reliability_map',
        lambda _connection, *, actor_id: {},
    )
    _domain_from_url = deps.get('domain_from_url', lambda _url: '')
    _confidence_weight_adjustment = deps.get('confidence_weight_adjustment', lambda _score: 0)
    _run_cold_actor_backfill = deps.get('run_cold_actor_backfill')
    _rebuild_notebook = deps.get('rebuild_notebook')
    _backfill_debug_ui_enabled = bool(deps.get('backfill_debug_ui_enabled', False))

    quick_check_overrides: dict[str, dict[str, str]] = {}
    question_feedback: dict[str, dict[str, int]] = {}
    source_reliability_map: dict[str, dict[str, object]] = {}
    backfill_notice = ''
    backfill_debug = ''
    with sqlite3.connect(db_path) as precheck_connection:
        actor_row_pre = precheck_connection.execute(
            'SELECT display_name FROM actor_profiles WHERE id = ?',
            (actor_id,),
        ).fetchone()
        if actor_row_pre is None:
            raise HTTPException(status_code=404, detail='actor not found')
        max_source_row = precheck_connection.execute(
            '''
            SELECT MAX(COALESCE(published_at, ingested_at, retrieved_at))
            FROM sources
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        max_source_dt = _parse_published_datetime(str(max_source_row[0] or '')) if max_source_row is not None else None
        cold_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        is_cold_actor = max_source_dt is None or max_source_dt < cold_cutoff
    if is_cold_actor and callable(_run_cold_actor_backfill):
        backfill_aliases: list[str] = []
        try:
            profile_for_alias = _build_actor_profile_from_mitre(str(actor_row_pre[0] or ''))
            aliases_csv = str(profile_for_alias.get('aliases_csv') or '')
            backfill_aliases = [item.strip() for item in aliases_csv.split(',') if item.strip()][:8]
        except Exception:
            backfill_aliases = []
        try:
            backfill_result = _run_cold_actor_backfill(
                actor_id,
                str(actor_row_pre[0] or actor_id),
                backfill_aliases,
            )
        except Exception:
            backfill_result = {'ran': False, 'inserted': 0}
        telemetry = backfill_result.get('telemetry') if isinstance(backfill_result, dict) else {}
        telemetry_dict = telemetry if isinstance(telemetry, dict) else {}
        top_reason = str(backfill_result.get('top_error_reason') or '')
        if _backfill_debug_ui_enabled and (bool(backfill_result.get('ran')) or int(backfill_result.get('inserted') or 0) > 0):
            dropped_domains = backfill_result.get('dropped_domains') if isinstance(backfill_result, dict) else []
            dropped_summary = 'none'
            if isinstance(dropped_domains, list) and dropped_domains:
                parts: list[str] = []
                for item in dropped_domains[:3]:
                    if not isinstance(item, list) or len(item) < 3:
                        continue
                    parts.append(f"{str(item[0])}:{str(item[1])}:{int(item[2])}")
                if parts:
                    dropped_summary = ','.join(parts)
            backfill_debug = (
                f"backfill: candidates={int(telemetry_dict.get('candidates_found') or 0)} "
                f"prefetch_kept={int(telemetry_dict.get('prefetch_kept') or 0)} "
                f"prefetch_dropped={int(telemetry_dict.get('prefetch_dropped') or 0)} "
                f"fetched={int(telemetry_dict.get('pages_fetched') or 0)} "
                f"inserted={int(backfill_result.get('inserted') or 0)} "
                f"top_error={top_reason or 'none'} "
                f"dropped_domains={dropped_summary}"
            )
        if int(backfill_result.get('inserted') or 0) > 0:
            backfill_notice = 'Backfilled sources (cold actor)'
        elif bool(backfill_result.get('ran')):
            backfill_notice = 'Cold actor backfill ran (no new sources found)'
        if int(backfill_result.get('inserted') or 0) > 0 and callable(_rebuild_notebook):
            try:
                _rebuild_notebook(actor_id, generate_questions=False, rebuild_timeline=True)
                _rebuild_notebook(actor_id, generate_questions=True, rebuild_timeline=False)
            except Exception:
                pass

    with sqlite3.connect(db_path) as connection:
        actor_row = connection.execute(
            '''
            SELECT
                id, display_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, notebook_updated_at,
                last_refresh_duration_ms, last_refresh_sources_processed,
                last_confirmed_at, last_confirmed_by, last_confirmed_note
            FROM actor_profiles
            WHERE id = ?
            ''',
            (actor_id,),
        ).fetchone()
        if actor_row is None:
            raise HTTPException(status_code=404, detail='actor not found')

        sources = connection.execute(
            '''
            SELECT
                id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at, pasted_text,
                title, headline, og_title, html_title, publisher, site_name,
                source_type, source_tier, confidence_weight
            FROM sources
            WHERE actor_id = ?
            ORDER BY COALESCE(published_at, ingested_at, retrieved_at) DESC
            ''',
            (actor_id,),
        ).fetchall()
        source_items_for_ioc = [
            {
                'id': row[0],
                'source_name': row[1],
                'url': row[2],
                'published_at': row[3],
                'ingested_at': row[4],
                'source_date_type': row[5],
                'retrieved_at': row[6],
                'pasted_text': row[7],
                'title': row[8],
                'headline': row[9],
                'og_title': row[10],
                'html_title': row[11],
                'publisher': row[12],
                'site_name': row[13],
                'source_type': row[14],
                'source_tier': row[15],
                'confidence_weight': row[16],
            }
            for row in sources
        ]
        derived_ioc_candidates = _derived_ioc_items_from_sources(source_items_for_ioc, max_items=80)
        if derived_ioc_candidates:
            now_iso = datetime.now(timezone.utc).isoformat()
            for candidate in derived_ioc_candidates:
                ioc_store_service.upsert_ioc_item_core(
                    connection,
                    actor_id=actor_id,
                    raw_ioc_type=str(candidate.get('ioc_type') or 'indicator'),
                    raw_ioc_value=str(candidate.get('ioc_value') or ''),
                    source_ref=str(candidate.get('source_ref') or '') or None,
                    source_id=str(candidate.get('source_id') or '') or None,
                    source_tier=str(candidate.get('source_tier') or '') or None,
                    extraction_method='auto_source_regex',
                    now_iso=now_iso,
                    observed_at=str(candidate.get('observed_at') or '') or None,
                    deps={
                        'validate_ioc_candidate': ioc_validation_service.validate_ioc_candidate_core,
                    },
                )

        timeline_rows = connection.execute(
            '''
            SELECT id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            FROM timeline_events
            WHERE actor_id = ?
            ORDER BY occurred_at ASC
            ''',
            (actor_id,),
        ).fetchall()

        thread_rows = connection.execute(
            '''
            SELECT id, question_text, status, created_at, updated_at
            FROM question_threads
            WHERE actor_id = ?
            ORDER BY updated_at DESC
            ''',
            (actor_id,),
        ).fetchall()

        updates_by_thread: dict[str, list[dict[str, object]]] = {}
        for thread_row in thread_rows:
            thread_id = thread_row[0]
            update_rows = connection.execute(
                '''
                SELECT
                    qu.id,
                    qu.trigger_excerpt,
                    qu.update_note,
                    qu.created_at,
                    s.source_name,
                    s.url,
                    s.published_at,
                    s.ingested_at,
                    s.source_date_type,
                    s.retrieved_at
                FROM question_updates qu
                JOIN sources s ON s.id = qu.source_id
                WHERE qu.thread_id = ?
                ORDER BY COALESCE(s.published_at, s.ingested_at, s.retrieved_at, qu.created_at) DESC, qu.created_at DESC
                ''',
                (thread_id,),
            ).fetchall()
            updates_by_thread[thread_id] = [
                {
                    'id': update_row[0],
                    'trigger_excerpt': update_row[1],
                    'update_note': update_row[2],
                    'created_at': update_row[3],
                    'source_name': update_row[4],
                    'source_url': update_row[5],
                    'source_published_at': update_row[6],
                    'source_ingested_at': update_row[7],
                    'source_date_type': update_row[8],
                    'source_retrieved_at': update_row[9],
                }
                for update_row in update_rows
            ]

        guidance_rows = connection.execute(
            '''
            SELECT id, thread_id, platform, what_to_look_for, where_to_look, query_hint, created_at
            FROM environment_guidance
            WHERE actor_id = ?
            ORDER BY created_at ASC
            ''',
            (actor_id,),
        ).fetchall()
        ioc_now_iso = datetime.now(timezone.utc).isoformat()
        ioc_rows = connection.execute(
            '''
            SELECT
                id, ioc_type, ioc_value, source_ref, created_at,
                lifecycle_status, handling_tlp, seen_count, last_seen_at, confidence_score
            FROM ioc_items
            WHERE actor_id = ?
              AND is_active = 1
              AND validation_status = 'valid'
              AND COALESCE(revoked, 0) = 0
              AND lifecycle_status IN ('active', 'monitor')
              AND (valid_until IS NULL OR TRIM(valid_until) = '' OR valid_until >= ?)
            ORDER BY last_seen_at DESC, created_at DESC
            ''',
            (actor_id, ioc_now_iso),
        ).fetchall()
        ioc_items = [
            {
                'id': row[0],
                'ioc_type': row[1],
                'ioc_value': row[2],
                'source_ref': row[3],
                'created_at': row[4],
                'lifecycle_status': row[5],
                'handling_tlp': row[6],
                'seen_count': row[7],
                'last_seen_at': row[8],
                'confidence_score': row[9],
            }
            for row in ioc_rows
        ]
        context_row = connection.execute(
            '''
            SELECT org_context, priority_mode, updated_at
            FROM requirement_context
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        requirement_rows = connection.execute(
            '''
            SELECT id, req_type, requirement_text, rationale_text,
                   source_name, source_url, source_published_at,
                   validation_score, validation_notes,
                   status, created_at
            FROM requirement_items
            WHERE actor_id = ?
            ORDER BY created_at DESC
            ''',
            (actor_id,),
        ).fetchall()
        collection_plan_row = connection.execute(
            '''
            SELECT monitored_sources_json, monitor_frequency, trigger_conditions_json,
                   alert_subscriptions_json, alert_notifications_enabled, updated_by, updated_at
            FROM actor_collection_plans
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        relationship_rows = connection.execute(
            '''
            SELECT src_entity_type, src_entity_key, relationship_type,
                   dst_entity_type, dst_entity_key, source_ref, observed_on, confidence, analyst, updated_at
            FROM actor_relationship_edges
            WHERE actor_id = ?
            ORDER BY updated_at DESC
            LIMIT 50
            ''',
            (actor_id,),
        ).fetchall()
        change_item_rows = connection.execute(
            '''
            SELECT id, change_summary, change_type, ttp_tag, infra_tag, tooling_tag,
                   targeting_tag, timing_tag, access_vector_tag, confidence,
                   source_ref, observed_on, created_by, created_at
            FROM actor_change_items
            WHERE actor_id = ?
            ORDER BY created_at DESC
            LIMIT 50
            ''',
            (actor_id,),
        ).fetchall()
        change_conflict_rows = connection.execute(
            '''
            SELECT conflict_topic, source_a_ref, source_b_ref, arbitration_outcome,
                   confidence, analyst, resolved_at
            FROM actor_change_conflicts
            WHERE actor_id = ?
            ORDER BY resolved_at DESC
            LIMIT 25
            ''',
            (actor_id,),
        ).fetchall()
        coverage_rows = connection.execute(
            '''
            SELECT technique_id, technique_name, detection_name, control_name,
                   coverage_status, validation_status, validation_evidence,
                   updated_by, updated_at
            FROM actor_technique_coverage
            WHERE actor_id = ?
            ORDER BY updated_at DESC
            LIMIT 200
            ''',
            (actor_id,),
        ).fetchall()
        task_rows = connection.execute(
            '''
            SELECT id, title, details, priority, status, owner, due_date,
                   linked_type, linked_key, created_at, updated_at
            FROM actor_tasks
            WHERE actor_id = ?
            ORDER BY
                CASE LOWER(status)
                    WHEN 'open' THEN 0
                    WHEN 'in_progress' THEN 1
                    WHEN 'blocked' THEN 2
                    WHEN 'done' THEN 3
                    ELSE 4
                END,
                COALESCE(NULLIF(due_date, ''), '9999-12-31') ASC,
                updated_at DESC
            LIMIT 200
            ''',
            (actor_id,),
        ).fetchall()
        outcome_rows = connection.execute(
            '''
            SELECT id, outcome_type, summary, result, linked_task_id,
                   linked_technique_id, evidence_ref, created_by, created_at
            FROM actor_operational_outcomes
            WHERE actor_id = ?
            ORDER BY created_at DESC
            LIMIT 200
            ''',
            (actor_id,),
        ).fetchall()
        report_pref_row = connection.execute(
            '''
            SELECT delta_brief_enabled, delta_brief_period, delta_brief_window_days, updated_by, updated_at
            FROM actor_report_preferences
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
        alert_rows = connection.execute(
            '''
            SELECT id, alert_type, severity, title, detail, status, source_ref,
                   channel_targets_json, change_item_id, created_at, acknowledged_at, acknowledged_by
            FROM actor_alert_events
            WHERE actor_id = ?
            ORDER BY
                CASE LOWER(status)
                    WHEN 'open' THEN 0
                    WHEN 'acknowledged' THEN 1
                    ELSE 2
                END,
                created_at DESC
            LIMIT 60
            ''',
            (actor_id,),
        ).fetchall()

        guidance_by_thread: dict[str, list[dict[str, object]]] = {}
        for row in guidance_rows:
            guidance_by_thread.setdefault(row[1], []).append(
                {
                    'id': row[0],
                    'platform': row[2],
                    'what_to_look_for': row[3],
                    'where_to_look': row[4],
                    'query_hint': row[5],
                    'created_at': row[6],
                }
            )
        if callable(_load_quick_check_overrides):
            try:
                loaded = _load_quick_check_overrides(connection, actor_id)
                if isinstance(loaded, dict):
                    quick_check_overrides = {
                        str(key): value
                        for key, value in loaded.items()
                        if isinstance(value, dict)
                    }
            except Exception:
                quick_check_overrides = {}
        feedback_rows = connection.execute(
            '''
            SELECT item_id, COUNT(*), SUM(rating_score)
            FROM analyst_feedback_events
            WHERE actor_id = ? AND item_type = 'priority_question'
            GROUP BY item_id
            ''',
            (actor_id,),
        ).fetchall()
        question_feedback = {
            str(row[0]): {
                'votes': int(row[1] or 0),
                'score': int(row[2] or 0),
            }
            for row in feedback_rows
            if str(row[0] or '').strip()
        }
        try:
            loaded_reliability = _load_source_reliability_map(connection, actor_id=actor_id)
            if isinstance(loaded_reliability, dict):
                source_reliability_map = loaded_reliability
        except Exception:
            source_reliability_map = {}

    actor = {
        'id': actor_row[0],
        'display_name': actor_row[1],
        'scope_statement': actor_row[2],
        'created_at': actor_row[3],
        'is_tracked': bool(actor_row[4]),
        'notebook_status': actor_row[5],
        'notebook_message': actor_row[6],
        'notebook_updated_at': actor_row[7],
        'last_refresh_duration_ms': actor_row[8],
        'last_refresh_sources_processed': actor_row[9],
        'last_confirmed_at': actor_row[10],
        'last_confirmed_by': actor_row[11],
        'last_confirmed_note': actor_row[12],
    }
    with sqlite3.connect(db_path) as connection:
        tracking_intent_row = connection.execute(
            '''
            SELECT
                why_track, mission_impact, intelligence_focus, key_questions_json,
                priority, impact, review_cadence_days,
                confirmation_min_sources, confirmation_max_age_days, confirmation_criteria,
                updated_by, updated_at
            FROM tracking_intent_register
            WHERE actor_id = ?
            ''',
            (actor_id,),
        ).fetchone()
    if tracking_intent_row is None:
        tracking_intent = {
            'actor_id': actor_id,
            'why_track': '',
            'mission_impact': '',
            'intelligence_focus': '',
            'key_questions': [],
            'priority': 'medium',
            'impact': 'medium',
            'review_cadence_days': 30,
            'confirmation_min_sources': 2,
            'confirmation_max_age_days': 45,
            'confirmation_criteria': '',
            'updated_by': '',
            'updated_at': '',
        }
    else:
        try:
            key_questions_value = _safe_json_string_list(str(tracking_intent_row[3] or '[]'))
        except Exception:
            key_questions_value = []
        tracking_intent = {
            'actor_id': actor_id,
            'why_track': str(tracking_intent_row[0] or ''),
            'mission_impact': str(tracking_intent_row[1] or ''),
            'intelligence_focus': str(tracking_intent_row[2] or ''),
            'key_questions': key_questions_value,
            'priority': str(tracking_intent_row[4] or 'medium'),
            'impact': str(tracking_intent_row[5] or 'medium'),
            'review_cadence_days': int(tracking_intent_row[6] or 30),
            'confirmation_min_sources': int(tracking_intent_row[7] or 2),
            'confirmation_max_age_days': int(tracking_intent_row[8] or 45),
            'confirmation_criteria': str(tracking_intent_row[9] or ''),
            'updated_by': str(tracking_intent_row[10] or ''),
            'updated_at': str(tracking_intent_row[11] or ''),
        }
    if collection_plan_row is None:
        collection_plan = {
            'actor_id': actor_id,
            'monitored_sources': [],
            'monitor_frequency': 'daily',
            'trigger_conditions': [],
            'alert_subscriptions': [],
            'alert_notifications_enabled': True,
            'updated_by': '',
            'updated_at': '',
        }
    else:
        collection_plan = {
            'actor_id': actor_id,
            'monitored_sources': _safe_json_string_list(str(collection_plan_row[0] or '[]')),
            'monitor_frequency': str(collection_plan_row[1] or 'daily'),
            'trigger_conditions': _safe_json_string_list(str(collection_plan_row[2] or '[]')),
            'alert_subscriptions': _safe_json_string_list(str(collection_plan_row[3] or '[]')),
            'alert_notifications_enabled': int(collection_plan_row[4] or 0) == 1,
            'updated_by': str(collection_plan_row[5] or ''),
            'updated_at': str(collection_plan_row[6] or ''),
        }
    relationship_items = [
        {
            'src_entity_type': str(row[0] or ''),
            'src_entity_key': str(row[1] or ''),
            'relationship_type': str(row[2] or ''),
            'dst_entity_type': str(row[3] or ''),
            'dst_entity_key': str(row[4] or ''),
            'source_ref': str(row[5] or ''),
            'observed_on': str(row[6] or ''),
            'confidence': str(row[7] or 'moderate'),
            'analyst': str(row[8] or ''),
            'updated_at': str(row[9] or ''),
        }
        for row in relationship_rows
    ]
    change_items = [
        {
            'id': str(row[0] or ''),
            'change_summary': str(row[1] or ''),
            'change_type': str(row[2] or 'other'),
            'ttp_tag': bool(row[3]),
            'infra_tag': bool(row[4]),
            'tooling_tag': bool(row[5]),
            'targeting_tag': bool(row[6]),
            'timing_tag': bool(row[7]),
            'access_vector_tag': bool(row[8]),
            'confidence': str(row[9] or 'moderate'),
            'source_ref': str(row[10] or ''),
            'observed_on': str(row[11] or ''),
            'created_by': str(row[12] or ''),
            'created_at': str(row[13] or ''),
        }
        for row in change_item_rows
    ]
    change_conflicts = [
        {
            'conflict_topic': str(row[0] or ''),
            'source_a_ref': str(row[1] or ''),
            'source_b_ref': str(row[2] or ''),
            'arbitration_outcome': str(row[3] or ''),
            'confidence': str(row[4] or 'moderate'),
            'analyst': str(row[5] or ''),
            'resolved_at': str(row[6] or ''),
        }
        for row in change_conflict_rows
    ]
    technique_coverage = [
        {
            'technique_id': str(row[0] or ''),
            'technique_name': str(row[1] or ''),
            'detection_name': str(row[2] or ''),
            'control_name': str(row[3] or ''),
            'coverage_status': str(row[4] or 'unknown'),
            'validation_status': str(row[5] or 'unknown'),
            'validation_evidence': str(row[6] or ''),
            'updated_by': str(row[7] or ''),
            'updated_at': str(row[8] or ''),
        }
        for row in coverage_rows
    ]
    ops_tasks = [
        {
            'id': str(row[0] or ''),
            'title': str(row[1] or ''),
            'details': str(row[2] or ''),
            'priority': str(row[3] or 'medium'),
            'status': str(row[4] or 'open'),
            'owner': str(row[5] or ''),
            'due_date': str(row[6] or ''),
            'linked_type': str(row[7] or ''),
            'linked_key': str(row[8] or ''),
            'created_at': str(row[9] or ''),
            'updated_at': str(row[10] or ''),
        }
        for row in task_rows
    ]
    operational_outcomes = [
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
        for row in outcome_rows
    ]
    alert_queue = [
        {
            'id': str(row[0] or ''),
            'alert_type': str(row[1] or 'change_detection'),
            'severity': str(row[2] or 'medium'),
            'title': str(row[3] or ''),
            'detail': str(row[4] or ''),
            'status': str(row[5] or 'open'),
            'source_ref': str(row[6] or ''),
            'channel_targets': _safe_json_string_list(str(row[7] or '[]')),
            'change_item_id': str(row[8] or ''),
            'created_at': str(row[9] or ''),
            'acknowledged_at': str(row[10] or ''),
            'acknowledged_by': str(row[11] or ''),
        }
        for row in alert_rows
    ]
    if report_pref_row is None:
        report_preferences = {
            'delta_brief_enabled': True,
            'delta_brief_period': 'weekly',
            'delta_brief_window_days': 7,
            'updated_by': '',
            'updated_at': '',
        }
    else:
        report_preferences = {
            'delta_brief_enabled': int(report_pref_row[0] or 0) == 1,
            'delta_brief_period': str(report_pref_row[1] or 'weekly'),
            'delta_brief_window_days': int(report_pref_row[2] or 7),
            'updated_by': str(report_pref_row[3] or ''),
            'updated_at': str(report_pref_row[4] or ''),
        }
    timeline_items: list[dict[str, object]] = [
        {
            'id': row[0],
            'occurred_at': row[1],
            'category': row[2],
            'title': row[3],
            'summary': row[4],
            'source_id': row[5],
            'target_text': row[6],
            'ttp_ids': _safe_json_string_list(row[7]),
        }
        for row in timeline_rows
    ]
    cutoff_90 = datetime.now(timezone.utc) - timedelta(days=90)
    timeline_recent_items = [
        item
        for item in timeline_items
        if (
            (dt := _parse_published_datetime(str(item.get('occurred_at') or ''))) is not None
            and dt >= cutoff_90
        )
    ]

    thread_items: list[dict[str, object]] = []
    for row in thread_rows:
        thread_items.append(
            {
                'id': row[0],
                'question_text': row[1],
                'status': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'updates': updates_by_thread.get(row[0], []),
            }
        )

    open_thread_ids = [thread['id'] for thread in thread_items if thread['status'] == 'open']
    guidance_for_open = [
        {
            'thread_id': thread_id,
            'question_text': next(item['question_text'] for item in thread_items if item['id'] == thread_id),
            'guidance_items': guidance_by_thread.get(thread_id, []),
        }
        for thread_id in open_thread_ids
    ]
    priority_questions: list[dict[str, object]] = []
    open_threads = [thread for thread in thread_items if thread['status'] == 'open']
    actor_categories = _actor_signal_categories(timeline_recent_items)
    signal_text = ' '.join([str(item.get('summary') or '') for item in timeline_recent_items]).lower()
    org_context_text = str(context_row[0]) if context_row and context_row[0] else ''
    source_relevance_cutoff_30 = datetime.now(timezone.utc) - timedelta(days=30)
    recent_source_blobs_30: list[str] = []
    for source_row in sources:
        effective_dt = _parse_published_datetime(str(source_row[3] or source_row[4] or source_row[6] or ''))
        if effective_dt is None or effective_dt < source_relevance_cutoff_30:
            continue
        recent_source_blobs_30.append(
            ' '.join(
                [
                    str(source_row[1] or ''),
                    str(source_row[8] or ''),
                    str(source_row[9] or ''),
                    str(source_row[10] or ''),
                    str(source_row[11] or ''),
                    str(source_row[7] or ''),
                ]
            )
        )
    scored_threads: list[dict[str, object]] = []
    for thread in open_threads:
        question_text = str(thread.get('question_text') or '')
        relevance = _question_actor_relevance(question_text, actor_categories, signal_text)
        if relevance <= 0 and recent_source_blobs_30:
            max_overlap = 0.0
            for source_blob in recent_source_blobs_30:
                max_overlap = max(max_overlap, float(_token_overlap(question_text, source_blob)))
            if max_overlap >= 0.08:
                relevance = 1
        if relevance <= 0:
            continue
        updates = thread.get('updates', [])
        updates_list = updates if isinstance(updates, list) else []
        evidence_dts = [
            dt
            for dt in (
                _quick_check_update_effective_dt(
                    update,
                    parse_published_datetime=_parse_published_datetime,
                )
                for update in updates_list
                if isinstance(update, dict)
            )
            if dt is not None
        ]
        latest_evidence_dt = max(evidence_dts) if evidence_dts else None
        corroborating_sources = len(
            {
                str(update.get('source_url') or update.get('source_name') or '').strip().lower()
                for update in updates_list
                if isinstance(update, dict)
                and str(update.get('source_url') or update.get('source_name') or '').strip()
            }
        )
        org_alignment = _question_org_alignment(question_text, org_context_text)
        rank_score = _priority_rank_score(
            thread,
            relevance,
            latest_evidence_dt,
            corroborating_sources,
            org_alignment,
        )
        feedback = question_feedback.get(str(thread.get('id') or ''), {'score': 0, 'votes': 0})
        feedback_score = int(feedback.get('score') or 0)
        rank_score += max(-3, min(3, feedback_score))
        scored_threads.append(
            {
                'thread': thread,
                'relevance': relevance,
                'rank_score': rank_score,
                'latest_evidence_dt': latest_evidence_dt,
                'corroborating_sources': corroborating_sources,
                'org_alignment': org_alignment,
                'feedback_score': feedback_score,
                'feedback_votes': int(feedback.get('votes') or 0),
            }
        )

    sorted_scored_threads = sorted(
        scored_threads,
        key=lambda item: (
            int(item['rank_score']),
            item['latest_evidence_dt'] or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    for scored in sorted_scored_threads:
        thread = scored['thread']
        question_text = str(thread.get('question_text') or '')
        relevance = int(scored['relevance'])
        rank_score = int(scored['rank_score'])
        updates = thread.get('updates', [])
        updates_list = updates if isinstance(updates, list) else []
        latest_update = updates_list[0] if updates_list and isinstance(updates_list[0], dict) else None
        latest_excerpt = str(latest_update.get('trigger_excerpt') or '') if isinstance(latest_update, dict) else ''
        latest_excerpt = ' '.join(latest_excerpt.split())
        if len(latest_excerpt) > 180:
            latest_excerpt = latest_excerpt[:180].rsplit(' ', 1)[0] + '...'
        if rank_score >= 10:
            priority = 'High'
        elif rank_score >= 7:
            priority = 'Medium'
        else:
            priority = 'Low'
        guidance_items = guidance_by_thread.get(str(thread['id']), [])
        updates_count = len(updates_list)
        phase_label = _phase_label_for_question(question_text)
        where_to_check = _priority_where_to_check(guidance_items, question_text)
        confidence = _priority_confidence_label(updates_count, relevance, latest_excerpt)
        priority_questions.append(
            {
                'id': thread['id'],
                'question_text': question_text,
                'phase_label': phase_label,
                'quick_check_title': _quick_check_title(question_text, phase_label),
                'decision_trigger': _short_decision_trigger(question_text),
                'telemetry_anchor': _telemetry_anchor_line(guidance_items, question_text),
                'first_step': _priority_next_best_action(question_text, where_to_check),
                'what_to_look_for': _guidance_line(guidance_items, 'what_to_look_for'),
                'query_hint': _guidance_query_hint(guidance_items, question_text),
                'success_condition': _priority_disconfirming_signal(question_text),
                'confidence_change_threshold': _confidence_change_threshold_line(question_text),
                'escalation_threshold': _confidence_change_threshold_line(question_text),
                'expected_output': _expected_output_line(question_text),
                'priority': priority,
                'confidence': confidence,
                'evidence_recency': _priority_update_recency_label(
                    scored['latest_evidence_dt'] if isinstance(scored['latest_evidence_dt'], datetime) else None
                ),
                'corroborating_sources': int(scored['corroborating_sources']),
                'org_alignment': _org_alignment_label(int(scored.get('org_alignment') or 0)),
                'analyst_feedback_score': int(scored.get('feedback_score') or 0),
                'analyst_feedback_votes': int(scored.get('feedback_votes') or 0),
                'updates_count': updates_count,
                'updated_at': thread['updated_at'],
            }
        )
        if len(priority_questions) >= 5:
            break

    if len(priority_questions) < 3:
        fallback_items = _fallback_priority_questions(str(actor['display_name']), actor_categories)
        for idx, item in enumerate(fallback_items, start=1):
            fallback_question_text = str(item['question_text'])
            if any(
                _token_overlap(str(existing.get('question_text') or ''), fallback_question_text) >= 0.7
                for existing in priority_questions
            ):
                continue
            priority_questions.append(
                {
                    'id': f'fallback-{idx}',
                    'question_text': fallback_question_text,
                    'phase_label': _phase_label_for_question(fallback_question_text),
                    'quick_check_title': _quick_check_title(
                        fallback_question_text,
                        _phase_label_for_question(fallback_question_text),
                    ),
                    'decision_trigger': _short_decision_trigger(fallback_question_text),
                    'telemetry_anchor': f'Anchor: {str(item["where_to_check"])}.',
                    'first_step': _priority_next_best_action(fallback_question_text, str(item['where_to_check'])),
                    'what_to_look_for': str(item.get('hunt_focus') or ''),
                    'query_hint': f'Start in: {str(item["where_to_check"])}.',
                    'success_condition': str(item.get('disconfirming_signal') or ''),
                    'confidence_change_threshold': _confidence_change_threshold_line(fallback_question_text),
                    'escalation_threshold': _confidence_change_threshold_line(fallback_question_text),
                    'expected_output': _expected_output_line(fallback_question_text),
                    'priority': str(item['priority']),
                    'confidence': str(item.get('confidence') or 'Low'),
                    'evidence_recency': 'Evidence recency unknown',
                    'corroborating_sources': 0,
                    'org_alignment': 'Unknown',
                    'updates_count': 0,
                    'updated_at': '',
                }
            )
            if len(priority_questions) >= 5:
                break

    if quick_check_overrides:
        for card in priority_questions:
            card_id = str(card.get('id') or '').strip()
            if not card_id:
                continue
            override = quick_check_overrides.get(card_id)
            if not override:
                continue
            first_step = str(override.get('first_step') or '').strip()
            what_to_look_for = str(override.get('what_to_look_for') or '').strip()
            expected_output = str(override.get('expected_output') or '').strip()
            if first_step:
                card['first_step'] = first_step
            if what_to_look_for:
                card['what_to_look_for'] = what_to_look_for
            if expected_output:
                card['expected_output'] = expected_output
    priority_phase_groups: list[dict[str, object]] = []

    source_items = [
        {
            'id': row[0],
            'source_name': row[1],
            'url': row[2],
            'published_at': row[3],
            'ingested_at': row[4],
            'source_date_type': row[5],
            'retrieved_at': row[6],
            'pasted_text': row[7],
            'title': row[8],
            'headline': row[9],
            'og_title': row[10],
            'html_title': row[11],
            'publisher': row[12],
            'site_name': row[13],
            'source_type': row[14],
            'source_tier': row[15],
            'confidence_weight': row[16],
        }
        for row in sources
    ]
    for source in source_items:
        source_url = str(source.get('url') or '').strip()
        domain = _domain_from_url(source_url)
        reliability = source_reliability_map.get(domain, {}) if domain else {}
        reliability_score = float(reliability.get('reliability_score') or 0.5)
        weight_adjust = int(_confidence_weight_adjustment(reliability_score))
        try:
            base_weight = int(source.get('confidence_weight') or 0)
        except Exception:
            base_weight = 0
        source['confidence_weight'] = max(0, min(4, base_weight + weight_adjust))
        source['source_reliability_score'] = reliability_score
        source['source_reliability_votes'] = int(reliability.get('helpful_count') or 0) + int(
            reliability.get('unhelpful_count') or 0
        )
    allowed_tiers = {'high', 'medium', 'trusted', 'context', 'unrated'}
    normalized_source_tier = str(source_tier or '').strip().lower() or None
    if normalized_source_tier not in allowed_tiers:
        normalized_source_tier = None

    normalized_min_confidence: int | None = None
    if min_confidence_weight is not None:
        try:
            normalized_min_confidence = max(0, min(4, int(min_confidence_weight)))
        except Exception:
            normalized_min_confidence = None

    normalized_source_days: int | None = None
    if source_days is not None:
        try:
            parsed_days = int(source_days)
            normalized_source_days = parsed_days if parsed_days > 0 else None
        except Exception:
            normalized_source_days = None

    ioc_recency_days = 30
    ioc_items = [
        item
        for item in ioc_items
        if _ioc_seen_within_days(
            item,
            days=ioc_recency_days,
            parse_published_datetime=_parse_published_datetime,
        )
    ]

    quick_check_ioc_pool: list[dict[str, str]] = []
    seen_ioc_pairs: set[tuple[str, str]] = set()
    for item in ioc_items:
        ioc_type = str(item.get('ioc_type') or '').strip().lower()
        ioc_value = str(item.get('ioc_value') or '').strip()
        if not ioc_type or not ioc_value:
            continue
        key = (ioc_type, ioc_value.lower())
        if key in seen_ioc_pairs:
            continue
        seen_ioc_pairs.add(key)
        quick_check_ioc_pool.append(
            {
                'ioc_type': ioc_type,
                'ioc_value': ioc_value,
                'source_ref': str(item.get('source_ref') or ''),
                'confidence_score': int(item.get('confidence_score') or 0),
                'last_seen_at': str(item.get('last_seen_at') or item.get('created_at') or ''),
            }
        )
    now_utc = datetime.now(timezone.utc)
    cutoff_30 = now_utc - timedelta(days=30)
    recent_30_timeline = [
        item
        for item in timeline_items
        if (
            (dt := _parse_published_datetime(str(item.get('occurred_at') or ''))) is not None
            and dt >= cutoff_30
        )
    ]
    category_counts_30: dict[str, int] = {}
    for item in recent_30_timeline:
        category = str(item.get('category') or '').strip().replace('_', ' ')
        if not category:
            continue
        category_counts_30[category] = category_counts_30.get(category, 0) + 1
    top_categories_30 = sorted(category_counts_30.items(), key=lambda row: row[1], reverse=True)[:3]
    top_categories_label = ', '.join([f'{name} ({count})' for name, count in top_categories_30]) or 'general activity'
    source_by_id = {
        str(source.get('id') or '').strip(): source
        for source in source_items
        if isinstance(source, dict) and str(source.get('id') or '').strip()
    }
    thread_by_id = {
        str(thread.get('id') or ''): thread
        for thread in thread_items
        if isinstance(thread, dict) and str(thread.get('id') or '').strip()
    }
    window_start_30_iso = cutoff_30.astimezone(timezone.utc).isoformat()
    window_end_30_iso = now_utc.astimezone(timezone.utc).isoformat()
    recent_source_pool: list[dict[str, object]] = []
    for source in source_items:
        if not isinstance(source, dict):
            continue
        effective_dt = _parse_published_datetime(
            str(
                source.get('published_at')
                or source.get('ingested_at')
                or source.get('retrieved_at')
                or ''
            )
        )
        if effective_dt is None or effective_dt < cutoff_30:
            continue
        source_text_blob = ' '.join(
            [
                str(source.get('title') or ''),
                str(source.get('headline') or ''),
                str(source.get('og_title') or ''),
                str(source.get('html_title') or ''),
                str(source.get('pasted_text') or ''),
            ]
        ).strip()
        recent_source_pool.append(
            {
                'source': source,
                'effective_dt': effective_dt,
                'text_blob': source_text_blob,
            }
        )
    recent_source_pool.sort(
        key=lambda item: item.get('effective_dt') if isinstance(item.get('effective_dt'), datetime) else datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    for card in priority_questions:
        related = _relevant_iocs_for_quick_check(card, quick_check_ioc_pool, limit=4)
        card['related_iocs'] = related
        card_id = str(card.get('id') or '').strip()
        override = quick_check_overrides.get(card_id, {}) if isinstance(quick_check_overrides, dict) else {}
        override_first_step = str(override.get('first_step') or '').strip() if isinstance(override, dict) else ''
        override_what_to_look_for = str(override.get('what_to_look_for') or '').strip() if isinstance(override, dict) else ''
        override_query_hint = str(override.get('query_hint') or '').strip() if isinstance(override, dict) else ''
        actor_name = str(actor.get('display_name') or actor.get('id') or '').strip()
        thread = thread_by_id.get(card_id, {})
        updates_raw = thread.get('updates') if isinstance(thread, dict) else []
        updates = updates_raw if isinstance(updates_raw, list) else []
        recent_updates = []
        for update in updates:
            if not isinstance(update, dict):
                continue
            evidence_dt = _quick_check_update_effective_dt(
                update,
                parse_published_datetime=_parse_published_datetime,
            )
            if evidence_dt is not None and evidence_dt >= cutoff_30:
                recent_updates.append(update)
        if not recent_updates and recent_source_pool:
            card_context = ' '.join(
                [
                    str(actor_name or ''),
                    str(card.get('question_text') or ''),
                    str(card.get('quick_check_title') or ''),
                    str(card.get('phase_label') or ''),
                ]
            ).strip()
            fallback_candidates: list[tuple[float, datetime, dict[str, object]]] = []
            seen_urls: set[str] = set()
            for pool_item in recent_source_pool:
                source_obj = pool_item.get('source')
                if not isinstance(source_obj, dict):
                    continue
                source_url = str(source_obj.get('url') or '').strip()
                if not source_url or source_url in seen_urls:
                    continue
                seen_urls.add(source_url)
                source_text_blob = str(pool_item.get('text_blob') or '')
                relevance = float(_token_overlap(card_context, source_text_blob)) if source_text_blob else 0.0
                actor_hit = actor_name.lower() in source_text_blob.lower() if actor_name and source_text_blob else False
                if relevance < 0.03 and not actor_hit:
                    continue
                effective_dt_obj = pool_item.get('effective_dt')
                if not isinstance(effective_dt_obj, datetime):
                    continue
                fallback_candidates.append(
                    (
                        relevance,
                        effective_dt_obj,
                        {
                            'id': f"src-fallback-{str(source_obj.get('id') or '')}",
                            'trigger_excerpt': str(source_obj.get('title') or source_obj.get('headline') or card.get('question_text') or '').strip(),
                            'update_note': '',
                            'created_at': effective_dt_obj.astimezone(timezone.utc).isoformat(),
                            'source_name': str(source_obj.get('source_name') or '').strip(),
                            'source_url': source_url,
                            'source_published_at': str(source_obj.get('published_at') or '').strip(),
                            'source_ingested_at': str(source_obj.get('ingested_at') or '').strip(),
                            'source_date_type': str(source_obj.get('source_date_type') or '').strip() or (
                                'published' if str(source_obj.get('published_at') or '').strip() else 'ingested'
                            ),
                            'source_retrieved_at': str(source_obj.get('retrieved_at') or '').strip(),
                        },
                    )
                )
            fallback_candidates.sort(key=lambda item: (item[1], item[0]), reverse=True)
            recent_updates = [item[2] for item in fallback_candidates[:8]]
        recent_source_count = len(
            {
                str(update.get('source_url') or update.get('source_name') or '').strip().lower()
                for update in recent_updates
                if isinstance(update, dict) and str(update.get('source_url') or update.get('source_name') or '').strip()
            }
        )
        recent_source_labels = []
        for update in recent_updates:
            if not isinstance(update, dict):
                continue
            label = str(update.get('source_name') or update.get('source_url') or '').strip()
            if label and label not in recent_source_labels:
                recent_source_labels.append(label)
        phase_hint = str(card.get('phase_label') or '').strip().lower()
        phase_behavior_map = {
            'initial access': 'phishing',
            'execution': 'execution',
            'persistence': 'execution',
            'lateral movement': 'lateral_movement',
            'command and control': 'command_and_control',
            'exfiltration': 'exfiltration',
            'impact': 'impact',
        }
        evidence_text_parts: list[str] = []
        for update in recent_updates:
            if not isinstance(update, dict):
                continue
            source_url = str(update.get('source_url') or '').strip()
            trigger_excerpt = str(update.get('trigger_excerpt') or '').strip()
            if trigger_excerpt:
                evidence_text_parts.append(trigger_excerpt)
            source_match = None
            for source_item in source_items:
                if not isinstance(source_item, dict):
                    continue
                if str(source_item.get('url') or '').strip() == source_url:
                    source_match = source_item
                    break
            if source_match is not None:
                source_text = str(source_match.get('pasted_text') or '').strip()
                if source_text:
                    evidence_text_parts.append(source_text[:4000])
        evidence_context_blob = ' '.join(evidence_text_parts).strip()
        behavior_id = phase_behavior_map.get(phase_hint) or _behavior_id_from_context(evidence_context_blob, phase_hint)
        behavior_category_map = {
            'impact': {'impact'},
            'exfiltration': {'exfiltration'},
            'lateral_movement': {'lateral_movement'},
            'execution': {'execution', 'persistence', 'defense_evasion'},
            'command_and_control': {'command_and_control'},
            'phishing': {'initial_access'},
            'general_activity': {'initial_access', 'execution', 'persistence', 'lateral_movement', 'command_and_control', 'exfiltration', 'impact'},
        }
        matched_categories = behavior_category_map.get(behavior_id, behavior_category_map['general_activity'])
        timeline_matches = [
            item
            for item in recent_30_timeline
            if str(item.get('category') or '').strip().lower() in matched_categories
        ]
        timeline_dates = [
            dt
            for dt in (
                _parse_published_datetime(str(item.get('occurred_at') or ''))
                for item in timeline_matches
                if isinstance(item, dict)
            )
            if dt is not None
        ]
        ioc_dates = [
            dt
            for dt in (
                _parse_published_datetime(str(item.get('last_seen_at') or ''))
                for item in related
                if isinstance(item, dict)
            )
            if dt is not None
        ]
        update_dates = [
            dt
            for dt in (
                _quick_check_update_effective_dt(
                    update,
                    parse_published_datetime=_parse_published_datetime,
                )
                for update in recent_updates
                if isinstance(update, dict)
            )
            if dt is not None
        ]
        all_evidence_dates = update_dates + timeline_dates + ioc_dates
        last_seen_evidence_at = max(all_evidence_dates).astimezone(timezone.utc).isoformat() if all_evidence_dates else ''
        source_refs: list[dict[str, str]] = []
        seen_ref_keys: set[tuple[str, str]] = set()
        recent_updates_sorted = sorted(
            [item for item in recent_updates if isinstance(item, dict)],
            key=lambda item: _quick_check_update_effective_dt(
                item,
                parse_published_datetime=_parse_published_datetime,
            ) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        for update in recent_updates_sorted:
            if not isinstance(update, dict):
                continue
            ref_title = str(update.get('source_name') or '').strip()
            ref_url = str(update.get('source_url') or '').strip()
            ref_date_type = 'published'
            ref_date_raw = str(update.get('source_published_at') or '').strip()
            if not ref_date_raw:
                ref_date_raw = str(update.get('source_ingested_at') or '').strip()
                ref_date_type = str(update.get('source_date_type') or 'ingested').strip().lower() or 'ingested'
            if not ref_date_raw:
                ref_date_raw = str(update.get('source_retrieved_at') or '').strip()
                ref_date_type = 'retrieved'
            ref_date = ref_date_raw.split('T', 1)[0] if 'T' in ref_date_raw else ref_date_raw
            if ref_date and ref_date_type == 'ingested':
                ref_date = f'{ref_date} (ingested)'
            dedupe_key = (ref_url, ref_date)
            if (not ref_url) or dedupe_key in seen_ref_keys:
                continue
            seen_ref_keys.add(dedupe_key)
            source_refs.append(
                {
                    'title': ref_title or ref_url,
                    'date': ref_date,
                    'url': ref_url,
                }
            )
            if len(source_refs) >= 8:
                break
        observables = _extract_behavior_observables(evidence_context_blob)
        evidence_backed = _quick_check_is_evidence_backed_core(
            evidence_refs=source_refs,
            observables=observables,
        )
        card['evidence_backed'] = evidence_backed
        card['evidence_tier'] = 'A' if evidence_backed else 'D'
        card['has_evidence'] = evidence_backed
        card['last_seen_evidence_at'] = last_seen_evidence_at
        source_label_text = ', '.join(recent_source_labels[:3]) or 'recent actor reporting'
        event_ids = [str(token).strip() for token in observables.get('event_ids', []) if str(token).strip()]
        if related:
            top_iocs = [
                str(item.get('ioc_value') or '').strip()
                for item in related[:2]
                if isinstance(item, dict) and str(item.get('ioc_value') or '').strip()
            ]
        else:
            top_iocs = []
        ioc_hint = ', '.join(top_iocs)
        template_hints = QUICK_CHECK_TEMPLATE_HINTS.get(
            behavior_id,
            QUICK_CHECK_TEMPLATE_HINTS['general_activity'],
        )
        selected_event_scope = _select_event_ids_for_where_to_start_core(
            evidence_event_ids=event_ids,
            template_hint_event_ids=template_hints.get('event_ids', []),
        )
        selected_event_ids = [
            str(item).strip()
            for item in (selected_event_scope.get('event_ids') if isinstance(selected_event_scope, dict) else [])
            if str(item).strip()
        ]

        behavior_watch_map = {
            'phishing': 'Repeated suspicious sender domains, lookalike addresses, and clustered subjects targeting same teams.',
            'impact': 'Recovery-inhibit commands, service-stop bursts, and rapid host-level disruption behavior.',
            'exfiltration': 'Archive/staging commands plus unusual outbound transfer indicators by repeated host/user pairs.',
            'lateral_movement': 'Repeated remote logon pivots (type 3/10), privileged auth chains, and new host-to-host admin paths.',
            'execution': 'Encoded/scripted command execution (-enc, frombase64string, iex) and suspicious task creation.',
            'command_and_control': 'Repeated beacon-like outbound patterns, rare destination recurrence, and host-user clustering.',
            'general_activity': 'Repeated suspicious host/user entities across core Windows telemetry.',
        }
        behavior_to_hunt_map = {
            'phishing': 'Repeated sender-driven delivery activity consistent with phishing operations.',
            'impact': 'Recovery-inhibit and disruptive host actions preceding ransomware impact.',
            'exfiltration': 'Archive-and-transfer behavior consistent with staged data exfiltration.',
            'lateral_movement': 'Repeated remote-auth pivots suggesting internal host-to-host spread.',
            'execution': 'Scripted/encoded execution behavior tied to suspicious task/process launch chains.',
            'command_and_control': 'Recurring beacon/callback patterns indicating command-and-control activity.',
            'general_activity': 'Repeated suspicious host/user activity requiring triage and scoping.',
        }

        observable_lines: list[str] = []
        for cmd in observables.get('commands', [])[:5]:
            observable_lines.append(f'command token: `{cmd}`')
        for event_id in event_ids[:4]:
            observable_lines.append(f'event id: `{event_id}`')
        for marker in observables.get('markers', [])[:3]:
            observable_lines.append(f'behavior marker: `{marker}`')
        for ioc in top_iocs[:3]:
            observable_lines.append(f'ioc pivot: `{ioc}`')
        data_gap = not evidence_backed

        if evidence_backed:
            card['decision_trigger'] = (
                f'Last-30d evidence for this check: {len(recent_updates)} linked updates from '
                f'{max(1, recent_source_count)} corroborating sources.'
            )
        else:
            card['decision_trigger'] = 'Data gap: no actor-linked evidence in last 30 days.'
        card['telemetry_anchor'] = str(card.get('telemetry_anchor') or 'Windows Event Logs').strip()
        if not override_first_step:
            observable_cmds = observables.get('commands', []) if isinstance(observables, dict) else []
            if data_gap:
                baseline_line = str(selected_event_scope.get('line') or '').strip()
                if str(selected_event_scope.get('mode') or '') == 'baseline':
                    first_step = (
                        'Data gap: no actor-linked evidence-derived observables in last 30 days. '
                        f'{baseline_line}.'
                    )
                else:
                    first_step = str(selected_event_scope.get("line") or "").strip()
            else:
                if selected_event_ids:
                    first_step = (
                        f'Start with {str(selected_event_scope.get("line") or "").strip()} for the last 24h; '
                        'cluster repeated host/user pairs, then pivot ±30 minutes around repeats.'
                    )
                else:
                    first_step = (
                        'Start with last-24h telemetry for this behavior; '
                        'cluster repeated host/user pairs, then pivot ±30 minutes around repeats.'
                    )
                if observable_cmds:
                    first_step += ' Prioritize command tokens: ' + ', '.join([f'`{cmd}`' for cmd in observable_cmds[:4]]) + '.'
                if ioc_hint:
                    first_step += f' IOC pivots: {ioc_hint}.'
            card['first_step'] = first_step

        card['behavior_to_hunt'] = behavior_to_hunt_map.get(behavior_id, behavior_to_hunt_map['general_activity'])

        if not override_what_to_look_for:
            watch_line = behavior_watch_map.get(behavior_id, behavior_watch_map['general_activity'])
            if evidence_backed and observable_lines:
                watch_line = f'{watch_line} Observables: ' + '; '.join(observable_lines[:8]) + '.'
            card['what_to_look_for'] = watch_line

        behavior_queries = _behavior_query_pack(
            behavior_id=behavior_id,
            ioc_values=top_iocs,
            event_ids=selected_event_ids,
        )
        card['check_template_id'] = card_id
        card['quick_check_id'] = card_id
        card['query_bundle_key'] = behavior_id
        card['mitre_ids'] = []
        card['behavior_queries'] = behavior_queries

        if not override_query_hint:
            card['query_hint'] = (
                'Run the behavior queries, cluster repeat_count_24h by host/user, and pivot into adjacent events within ±30 minutes.'
            )

        required_data = [
            'Windows Security event logs (process + authentication + task events).',
            'PowerShell script logging if enabled.',
            'Host and user identifiers present in event records.',
        ]
        if top_iocs:
            required_data.append('Actor-linked IOC values for pivoting in same 24h window.')
        card['required_data'] = required_data
        card['required_telemetry'] = list(required_data)

        card['data_gap'] = data_gap
        if data_gap:
            card['success_condition'] = (
                'Data gap: insufficient actor-linked 30-day evidence to assert actor-specific behavior. Run baseline hunt and collect evidence.'
            )
        else:
            card['success_condition'] = (
                'Escalate when at least two independent behavior-aligned repeats are confirmed on the same host/user pair in 24h.'
            )
        card['confidence_change_threshold'] = (
            'Increase confidence when repeated behavior patterns match actor-linked 30-day context; decrease when no repeats are found.'
        )
        card['escalation_threshold'] = card['confidence_change_threshold']
        card['decision_rule'] = card['success_condition']
        card['analyst_output'] = (
            'Output table: host_or_system, user_or_sid, event_id, timestamp, behavior_tag, evidence_note, source_reference.'
        )
        card['expected_output'] = card['analyst_output']
        evidence_used = [
            _format_evidence_ref_core(
                title=str(item.get('title') or ''),
                date_value=str(item.get('date') or ''),
                url=str(item.get('url') or ''),
            )
            for item in source_refs[:2]
        ]
        if not evidence_used:
            evidence_used = ['No actor-linked evidence in last 30 days.']
        card['evidence_used'] = evidence_used
        card['window_start'] = window_start_30_iso
        card['window_end'] = window_end_30_iso
        card['severity'] = str(card.get('priority') or 'Low')
        card['title'] = str(card.get('quick_check_title') or card.get('question_text') or '')
        card['behavior_to_hunt'] = str(card.get('behavior_to_hunt') or '')
        card['where_to_start'] = str(card.get('first_step') or '')
        card['what_to_watch'] = str(card.get('what_to_look_for') or '')
        card['required_data'] = ' | '.join(str(item) for item in required_data)
        card['decision_rule'] = str(card.get('decision_rule') or '')
        card['analyst_output'] = str(card.get('analyst_output') or '')
        card['populated_text_fields'] = {
            'behavior_to_hunt': str(card.get('behavior_to_hunt') or ''),
            'where_to_start': str(card.get('where_to_start') or card.get('first_step') or ''),
            'what_to_watch': str(card.get('what_to_watch') or card.get('what_to_look_for') or ''),
            'required_data': str(card.get('required_data') or ''),
            'decision_rule': str(card.get('decision_rule') or ''),
            'analyst_output': str(card.get('analyst_output') or ''),
            'evidence_used': ' | '.join(str(item) for item in card.get('evidence_used', []) if str(item).strip()),
        }
        quick_checks_view_service.apply_no_evidence_rule_core(card)
        if not evidence_backed:
            card['first_step'] = str(card.get('first_step') or '').strip()
            card['where_to_start'] = str(card.get('first_step') or '')
            card['evidence_used'] = ['No actor-linked evidence in last 30 days.']
        else:
            card['where_to_start'] = str(card.get('first_step') or '')
        card['what_to_watch'] = str(card.get('what_to_look_for') or '')
        card['decision_rule'] = str(card.get('success_condition') or card.get('decision_rule') or '')
        card['analyst_output'] = str(card.get('analyst_output') or card.get('expected_output') or '')
    priority_questions = quick_checks_view_service.rank_quick_checks_core(priority_questions)
    for rank_index, card in enumerate(priority_questions):
        card['quick_check_rank'] = rank_index

    phase_group_order: list[str] = []
    phase_groups_map: dict[str, list[dict[str, object]]] = {}
    for card in priority_questions:
        phase = str(card.get('phase_label') or 'Operational Signal')
        if phase not in phase_groups_map:
            phase_groups_map[phase] = []
            phase_group_order.append(phase)
        phase_groups_map[phase].append(card)
    priority_phase_groups = [{'phase': phase, 'cards': phase_groups_map[phase]} for phase in phase_group_order]

    strict_default_mode = (
        normalized_source_tier is None
        and normalized_min_confidence is None
        and normalized_source_days is None
    )
    if strict_default_mode:
        # Rich-by-default analyst view: retain broader source context by default.
        normalized_min_confidence = 1
        normalized_source_days = 365

    source_cutoff_dt = (
        datetime.now(timezone.utc) - timedelta(days=normalized_source_days)
        if normalized_source_days is not None
        else None
    )

    source_items_for_changes = source_items
    if (
        normalized_source_tier is not None
        or normalized_min_confidence is not None
        or source_cutoff_dt is not None
    ):
        filtered_sources: list[dict[str, object]] = []
        for source in source_items:
            source_tier_value = str(source.get('source_tier') or '').strip().lower() or 'unrated'
            if normalized_source_tier is not None and source_tier_value != normalized_source_tier:
                continue
            try:
                source_weight = int(source.get('confidence_weight') or 0)
            except Exception:
                source_weight = 0
            if normalized_min_confidence is not None and source_weight < normalized_min_confidence:
                continue
            raw_date = str(source.get('published_at') or source.get('ingested_at') or source.get('retrieved_at') or '')
            source_dt = _parse_published_datetime(raw_date)
            if source_cutoff_dt is not None and source_dt is not None:
                if source_dt < source_cutoff_dt:
                    continue
            filtered_sources.append(source)
        source_items_for_changes = filtered_sources

    allowed_source_ids_for_changes = {
        str(source.get('id') or '').strip()
        for source in source_items_for_changes
        if str(source.get('id') or '').strip()
    }
    timeline_items_for_changes = (
        [
            item
            for item in timeline_items
            if str(item.get('source_id') or '').strip() in allowed_source_ids_for_changes
        ]
        if (
            normalized_source_tier is not None
            or normalized_min_confidence is not None
            or source_cutoff_dt is not None
        )
        else timeline_items
    )
    timeline_recent_items_for_changes = [
        item
        for item in timeline_items_for_changes
        if (
            (dt := _parse_published_datetime(str(item.get('occurred_at') or ''))) is not None
            and dt >= cutoff_90
        )
    ]
    mitre_profile = _build_actor_profile_from_mitre(str(actor['display_name']))
    actor_profile_summary = str(mitre_profile['summary'])
    top_techniques = _group_top_techniques(str(mitre_profile.get('stix_id') or ''))
    favorite_vectors = _favorite_attack_vectors(top_techniques)
    known_technique_ids = _known_technique_ids_for_entity(str(mitre_profile.get('stix_id') or ''))
    if not known_technique_ids:
        known_technique_ids = {
            str(item.get('technique_id') or '').upper()
            for item in top_techniques
            if item.get('technique_id')
        }
    emerging_techniques = _emerging_techniques_from_timeline(
        timeline_recent_items_for_changes,
        known_technique_ids,
    )
    emerging_technique_ids = [str(item.get('technique_id') or '') for item in emerging_techniques]
    emerging_techniques_with_dates = [
        {
            'technique_id': str(item.get('technique_id') or ''),
            'first_seen': str(item.get('first_seen') or ''),
        }
        for item in emerging_techniques
    ]
    timeline_graph = _build_timeline_graph(timeline_recent_items)
    timeline_compact_rows = _compact_timeline_rows(timeline_items, known_technique_ids)
    actor_terms = _actor_terms(
        str(actor['display_name']),
        str(mitre_profile.get('group_name') or ''),
        str(mitre_profile.get('aliases_csv') or ''),
    )
    recent_activity_highlights = _build_recent_activity_highlights(
        timeline_items_for_changes,
        source_items_for_changes,
        actor_terms,
    )
    llm_change_signals_raw = _ollama_review_change_signals(
        str(actor.get('display_name') or ''),
        source_items_for_changes,
        recent_activity_highlights,
    )
    llm_change_signals = (
        [item for item in llm_change_signals_raw if isinstance(item, dict)]
        if isinstance(llm_change_signals_raw, list)
        else []
    )

    # Carry over freshness metadata from known sources referenced in validated evidence.
    known_source_urls = {
        str(source.get('url') or '').strip()
        for source in source_items_for_changes
        if str(source.get('url') or '').strip()
    }
    highlight_by_url = {
        str(item.get('source_url') or '').strip(): item
        for item in recent_activity_highlights
        if str(item.get('source_url') or '').strip()
    }
    now_utc = datetime.now(timezone.utc)
    min_recent_dt = now_utc - timedelta(days=max(1, _recent_change_max_days))
    for item in llm_change_signals:
        evidence_values = item.get('validated_sources')
        if not isinstance(evidence_values, list):
            item['validated_sources'] = []
            continue
        validated_recent: list[dict[str, object]] = []
        for evidence in evidence_values:
            if not isinstance(evidence, dict):
                continue
            source_url = str(evidence.get('source_url') or '').strip()
            if not source_url:
                continue
            if source_url not in known_source_urls and source_url not in highlight_by_url:
                continue
            original = highlight_by_url.get(source_url)
            evidence_date_raw = str(evidence.get('source_date') or (original.get('date') if original else '') or '').strip()
            evidence_dt = _parse_published_datetime(evidence_date_raw)
            if evidence_dt is None and original is not None:
                evidence_dt = _parse_published_datetime(str(original.get('date') or ''))
            if evidence_dt is None or evidence_dt < min_recent_dt:
                continue
            if original is not None:
                evidence.setdefault('freshness_label', str(original.get('freshness_label') or ''))
                evidence.setdefault('freshness_class', str(original.get('freshness_class') or 'badge'))
                evidence.setdefault('source_date', str(evidence.get('source_date') or original.get('date') or ''))
            validated_recent.append(evidence)
        item['validated_sources'] = validated_recent

    top_change_signals = [
        item
        for item in llm_change_signals[:8]
        if str(item.get('change_summary') or '').strip()
        and isinstance(item.get('validated_sources'), list)
        and len(item.get('validated_sources') or []) > 0
    ]
    llm_change_signals_degraded = False
    if not top_change_signals:
        llm_change_signals_degraded = True
        deterministic_signals = _build_top_change_signals(
            recent_activity_highlights,
            actor_terms=actor_terms,
            limit=8,
        )
        for item in deterministic_signals:
            evidence_url = str(item.get('source_url') or '').strip()
            evidence_date = str(item.get('source_published_at') or item.get('date') or '').strip()
            evidence_dt = _parse_published_datetime(evidence_date)
            if evidence_dt is None or evidence_dt < min_recent_dt:
                continue
            evidence_label = str(item.get('evidence_source_label') or item.get('source_name') or evidence_url).strip()
            proof = ' '.join(str(item.get('text') or '').split()).strip()[:220]
            corroboration = int(str(item.get('corroboration_sources') or '0') or '0')
            confidence = 'high' if corroboration >= 3 else 'medium' if corroboration >= 2 else 'low'

            window_days = '90'
            observed_dt = _parse_published_datetime(evidence_date)
            if observed_dt is not None:
                age_days = max(0, (datetime.now(timezone.utc) - observed_dt).days)
                if age_days <= 30:
                    window_days = '30'
                elif age_days <= 60:
                    window_days = '60'

            top_change_signals.append(
                {
                    'change_summary': str(item.get('evidence_title') or item.get('text') or '').strip()[:180],
                    'change_why_new': str(item.get('text') or '').strip()[:300],
                    'category': str(item.get('category') or ''),
                    'ttp_ids': str(item.get('ttp_ids') or ''),
                    'target_text': str(item.get('target_text') or ''),
                    'change_window_days': window_days,
                    'change_confidence': confidence,
                    'validated_source_count': str(max(1, corroboration)),
                    'validated_sources': [
                        {
                            'source_url': evidence_url,
                            'source_label': evidence_label,
                            'source_date': evidence_date,
                            'proof': proof,
                            'freshness_label': str(item.get('freshness_label') or ''),
                            'freshness_class': str(item.get('freshness_class') or 'badge'),
                        }
                    ]
                    if evidence_url
                    else [],
                }
            )
    recent_activity_synthesis = _ollama_synthesize_recent_activity(
        str(actor.get('display_name') or ''),
        recent_activity_highlights,
    )
    if not isinstance(recent_activity_synthesis, list):
        recent_activity_synthesis = []
    llm_recent_synthesis_degraded = False
    if not recent_activity_synthesis:
        llm_recent_synthesis_degraded = True
        recent_activity_synthesis = _build_recent_activity_synthesis(recent_activity_highlights)
    recent_change_summary = _recent_change_summary(
        timeline_recent_items_for_changes,
        recent_activity_highlights,
        source_items_for_changes,
    )
    environment_checks = _build_environment_checks(
        timeline_recent_items_for_changes,
        recent_activity_highlights,
        top_techniques,
    )
    notebook_kpis = _build_notebook_kpis(
        timeline_items,
        known_technique_ids,
        len(open_thread_ids),
        source_items,
    )

    return {
        'actor': actor,
        'sources': source_items,
        'timeline_items': timeline_items,
        'timeline_recent_items': timeline_recent_items,
        'timeline_window_label': 'Last 90 days',
        'threads': thread_items,
        'guidance_for_open': guidance_for_open,
        'actor_profile_summary': actor_profile_summary,
        'actor_profile_source_label': str(mitre_profile['source_label']),
        'actor_profile_source_url': str(mitre_profile['source_url']),
        'actor_profile_group_name': str(mitre_profile['group_name']),
        'actor_created_date': _format_date_or_unknown(str(actor.get('created_at') or '')),
        'favorite_vectors': favorite_vectors,
        'top_techniques': top_techniques,
        'emerging_techniques': emerging_techniques,
        'emerging_technique_ids': emerging_technique_ids,
        'emerging_techniques_with_dates': emerging_techniques_with_dates,
        'timeline_graph': timeline_graph,
        'timeline_compact_rows': timeline_compact_rows,
        'recent_activity_highlights': recent_activity_highlights,
        'top_change_signals': top_change_signals,
        'recent_activity_synthesis': recent_activity_synthesis,
        'llm_change_signals_degraded': llm_change_signals_degraded,
        'llm_recent_synthesis_degraded': llm_recent_synthesis_degraded,
        'recent_change_summary': recent_change_summary,
        'source_quality_filters': {
            'source_tier': normalized_source_tier or '',
            'min_confidence_weight': str(normalized_min_confidence) if normalized_min_confidence is not None else '',
            'source_days': str(normalized_source_days) if normalized_source_days is not None else '',
            'total_sources': str(len(source_items)),
            'applied_sources': len(source_items_for_changes),
            'filtered_out_sources': max(0, len(source_items) - len(source_items_for_changes)),
            'undated_excluded': '1',
            'strict_default_mode': '1' if strict_default_mode else '0',
        },
        'tracking_intent': tracking_intent,
        'collection_plan': collection_plan,
        'relationship_items': relationship_items,
        'change_items': change_items,
        'change_conflicts': change_conflicts,
        'technique_coverage': technique_coverage,
        'ops_tasks': ops_tasks,
        'operational_outcomes': operational_outcomes,
        'alert_queue': alert_queue,
        'report_preferences': report_preferences,
        'environment_checks': environment_checks,
        'kpis': notebook_kpis,
        'ioc_items': ioc_items,
        'requirements_context': {
            'org_context': str(context_row[0]) if context_row else '',
            'priority_mode': str(context_row[1]) if context_row else 'Operational',
            'updated_at': str(context_row[2]) if context_row and context_row[2] else '',
        },
        'requirements': [
            {
                'id': row[0],
                'req_type': row[1],
                'requirement_text': row[2],
                'rationale_text': row[3],
                'source_name': row[4],
                'source_url': row[5],
                'source_published_at': row[6],
                'validation_score': row[7],
                'validation_notes': row[8],
                'status': row[9],
                'created_at': row[10],
            }
            for row in requirement_rows
        ],
        'priority_questions': priority_questions,
        'priority_phase_groups': priority_phase_groups,
        'backfill_notice': backfill_notice,
        'backfill_debug': backfill_debug,
        'counts': {
            'sources': len(sources),
            'timeline_events': len(timeline_rows),
            'open_questions': len(open_thread_ids),
        },
    }
    normalized_actor_terms = [
        _norm_text(term)
        for term in (actor_terms or [])
        if _norm_text(term)
    ]

    boilerplate_patterns = (
        'provides protection against this threat',
        'provides protection against',
        'threat emulation',
        'threat prevention',
        'ips provides protection',
        'signature update',
        'security vendor advisory',
    )

    def _contains_actor_term(item: dict[str, object]) -> bool:
        if not normalized_actor_terms:
            return False
        combined = ' '.join(
            [
                _norm_text(item.get('evidence_title')),
                _norm_text(item.get('text')),
                _norm_text(item.get('change_summary')),
                _norm_text(item.get('target_text')),
            ]
        )
        return any(term and term in combined for term in normalized_actor_terms)

    def _evidence_density(item: dict[str, object]) -> int:
        corroboration = _int_value(item.get('corroboration_sources'))
        validated_count = _int_value(item.get('validated_source_count'))
        validated_sources = item.get('validated_sources')
        validated_len = len(validated_sources) if isinstance(validated_sources, list) else 0
        return max(corroboration, validated_count, validated_len)

    def _looks_like_vendor_boilerplate(item: dict[str, object]) -> bool:
        combined = ' '.join(
            [
                _norm_text(item.get('evidence_title')),
                _norm_text(item.get('text')),
                _norm_text(item.get('change_summary')),
            ]
        )
        return any(pattern in combined for pattern in boilerplate_patterns)
