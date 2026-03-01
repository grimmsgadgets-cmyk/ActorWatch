import sqlite3
from datetime import datetime, timedelta, timezone

from services import notebook_cache_service
from services.notebook_contract_service import finalize_notebook_contract_core


def build_notebook_wrapper_core(
    *,
    actor_id: str,
    generate_questions: bool,
    rebuild_timeline: bool,
    deps: dict[str, object],
) -> None:
    _build_notebook_core = deps['build_notebook_core']
    _db_path = deps['db_path']

    _build_notebook_core(
        actor_id,
        db_path=_db_path(),
        generate_questions=generate_questions,
        rebuild_timeline=rebuild_timeline,
        now_iso=deps['now_iso'],
        actor_exists=deps['actor_exists'],
        build_actor_profile_from_mitre=deps['build_actor_profile_from_mitre'],
        actor_terms_fn=deps['actor_terms_fn'],
        extract_major_move_events=deps['extract_major_move_events'],
        normalize_text=deps['normalize_text'],
        token_overlap=deps['token_overlap'],
        extract_question_sentences=deps['extract_question_sentences'],
        sentence_mentions_actor_terms=deps['sentence_mentions_actor_terms'],
        sanitize_question_text=deps['sanitize_question_text'],
        question_from_sentence=deps['question_from_sentence'],
        ollama_generate_questions=deps['ollama_generate_questions'],
        platforms_for_question=deps['platforms_for_question'],
        guidance_for_platform=deps['guidance_for_platform'],
        ollama_enrich_quick_checks=deps.get('ollama_enrich_quick_checks'),
        store_quick_check_overrides=deps.get('store_quick_check_overrides'),
    )


def fetch_actor_notebook_wrapper_core(*, actor_id: str, deps: dict[str, object]) -> dict[str, object]:
    _pipeline_fetch_actor_notebook_core = deps['pipeline_fetch_actor_notebook_core']
    _db_path = deps['db_path']
    pipeline_deps = {
        'parse_published_datetime': deps['parse_published_datetime'],
        'safe_json_string_list': deps['safe_json_string_list'],
        'actor_signal_categories': deps['actor_signal_categories'],
        'question_actor_relevance': deps['question_actor_relevance'],
        'priority_update_evidence_dt': deps['priority_update_evidence_dt'],
        'question_org_alignment': deps['question_org_alignment'],
        'priority_rank_score': deps['priority_rank_score'],
        'phase_label_for_question': deps['phase_label_for_question'],
        'priority_where_to_check': deps['priority_where_to_check'],
        'priority_confidence_label': deps['priority_confidence_label'],
        'quick_check_title': deps['quick_check_title'],
        'short_decision_trigger': deps['short_decision_trigger'],
        'telemetry_anchor_line': deps['telemetry_anchor_line'],
        'priority_next_best_action': deps['priority_next_best_action'],
        'guidance_line': deps['guidance_line'],
        'guidance_query_hint': deps['guidance_query_hint'],
        'priority_disconfirming_signal': deps['priority_disconfirming_signal'],
        'confidence_change_threshold_line': deps['confidence_change_threshold_line'],
        'escalation_threshold_line': deps['escalation_threshold_line'],
        'expected_output_line': deps['expected_output_line'],
        'priority_update_recency_label': deps['priority_update_recency_label'],
        'org_alignment_label': deps['org_alignment_label'],
        'fallback_priority_questions': deps['fallback_priority_questions'],
        'token_overlap': deps['token_overlap'],
        'build_actor_profile_from_mitre': deps['build_actor_profile_from_mitre'],
        'group_top_techniques': deps['group_top_techniques'],
        'favorite_attack_vectors': deps['favorite_attack_vectors'],
        'known_technique_ids_for_entity': deps['known_technique_ids_for_entity'],
        'emerging_techniques_from_timeline': deps['emerging_techniques_from_timeline'],
        'build_timeline_graph': deps['build_timeline_graph'],
        'compact_timeline_rows': deps['compact_timeline_rows'],
        'actor_terms': deps['actor_terms'],
        'build_recent_activity_highlights': deps['build_recent_activity_highlights'],
        'build_top_change_signals': deps['build_top_change_signals'],
        'ollama_review_change_signals': deps.get('ollama_review_change_signals'),
        'ollama_synthesize_recent_activity': deps.get('ollama_synthesize_recent_activity'),
        'enforce_ollama_synthesis': deps.get('enforce_ollama_synthesis'),
        'build_recent_activity_synthesis': deps['build_recent_activity_synthesis'],
        'recent_change_summary': deps['recent_change_summary'],
        'build_environment_checks': deps['build_environment_checks'],
        'build_notebook_kpis': deps['build_notebook_kpis'],
        'format_date_or_unknown': deps['format_date_or_unknown'],
        'load_source_reliability_map': deps.get('load_source_reliability_map'),
        'domain_from_url': deps.get('domain_from_url'),
        'confidence_weight_adjustment': deps.get('confidence_weight_adjustment'),
        'load_quick_check_overrides': deps.get('load_quick_check_overrides'),
        'run_cold_actor_backfill': deps.get('run_cold_actor_backfill'),
        'rebuild_notebook': deps.get('rebuild_notebook'),
        'backfill_debug_ui_enabled': deps.get('backfill_debug_ui_enabled'),
    }
    source_tier = deps.get('source_tier')
    min_confidence_weight = deps.get('min_confidence_weight')
    source_days = deps.get('source_days')
    prefer_cached = bool(deps.get('prefer_cached', True))
    build_on_cache_miss = bool(deps.get('build_on_cache_miss', True))
    allow_stale_cache = bool(deps.get('allow_stale_cache', False))
    cache_key = notebook_cache_service.cache_key_core(
        source_tier=source_tier,
        min_confidence_weight=min_confidence_weight,
        source_days=source_days,
        enforce_ollama_synthesis=pipeline_deps.get('enforce_ollama_synthesis'),
        backfill_debug_ui_enabled=pipeline_deps.get('backfill_debug_ui_enabled'),
    )

    if prefer_cached:
        with sqlite3.connect(_db_path(), timeout=30.0) as connection:
            connection.execute('PRAGMA busy_timeout = 30000')
            data_fingerprint = notebook_cache_service.actor_data_fingerprint_core(connection, actor_id)
            cached = notebook_cache_service.load_cached_notebook_core(
                connection,
                actor_id=actor_id,
                cache_key=cache_key,
                data_fingerprint=data_fingerprint,
            )
            if isinstance(cached, dict):
                cached['snapshot_stale'] = False
                return finalize_notebook_contract_core(cached)
            stale_cached = notebook_cache_service.load_latest_cached_notebook_for_key_core(
                connection,
                actor_id=actor_id,
                cache_key=cache_key,
            )
            if allow_stale_cache and isinstance(stale_cached, dict):
                stale_cached['snapshot_stale'] = True
                return finalize_notebook_contract_core(stale_cached)
            if not build_on_cache_miss:
                return finalize_notebook_contract_core(
                    {
                    'cache_miss': True,
                    'actor': {
                        'id': actor_id,
                        'notebook_status': 'idle',
                        'notebook_message': 'Notebook cache is not ready yet.',
                    },
                    'counts': {'sources': 0},
                    }
                )

    notebook = _pipeline_fetch_actor_notebook_core(
        actor_id,
        db_path=_db_path(),
        source_tier=source_tier,
        min_confidence_weight=min_confidence_weight,
        source_days=source_days,
        deps=pipeline_deps,
    )

    if isinstance(notebook, dict):
        notebook = finalize_notebook_contract_core(notebook)
        with sqlite3.connect(_db_path(), timeout=30.0) as connection:
            connection.execute('PRAGMA busy_timeout = 30000')
            latest_fingerprint = notebook_cache_service.actor_data_fingerprint_core(connection, actor_id)
            notebook_cache_service.save_cached_notebook_core(
                connection,
                actor_id=actor_id,
                cache_key=cache_key,
                data_fingerprint=latest_fingerprint,
                payload=notebook,
            )
            connection.commit()
    return finalize_notebook_contract_core(notebook if isinstance(notebook, dict) else {})


def compute_bastion_nudges_core(notebook: dict | None) -> list[str]:
    """Data-driven analyst nudges for the Bastion HUD — no AI, no drama, just record gaps."""
    if not isinstance(notebook, dict):
        return []

    nudges: list[str] = []
    counts = notebook.get('counts') or {}
    ioc_items = notebook.get('ioc_items') or []
    timeline_graph = notebook.get('timeline_graph') or []
    top_techniques = notebook.get('top_techniques') or []
    source_count = int(counts.get('sources') or 0)
    event_count = int(counts.get('timeline_events') or 0)

    if source_count == 0:
        nudges.append('No sources ingested — run a refresh to start building this record.')

    if event_count == 0:
        nudges.append('No timeline events — check source quality or add observations manually.')
    elif timeline_graph:
        recent = timeline_graph[-2:]
        if all(int(b.get('total') or 0) == 0 for b in recent):
            nudges.append('No activity in the last 2 reporting periods — actor may be dormant or sources have dried up.')

    sourceless = [i for i in ioc_items if not str(i.get('source_ref') or '').strip()]
    if sourceless:
        n = len(sourceless)
        nudges.append(f'{n} IOC{"s" if n > 1 else ""} {"have" if n > 1 else "has"} no source reference — provenance unclear.')

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
        stale = [
            i for i in ioc_items
            if str(i.get('last_seen_at') or '')[:10] and str(i.get('last_seen_at') or '')[:10] < cutoff
        ]
        if stale:
            n = len(stale)
            nudges.append(f'{n} IOC{"s" if n > 1 else ""} last seen over 90 days ago — review for expiry or retirement.')
    except Exception:
        pass

    if event_count > 5 and not top_techniques:
        nudges.append('No MITRE techniques mapped — check if this actor has a MITRE ATT&CK group record.')

    return nudges
