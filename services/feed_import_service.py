def import_default_feeds_for_actor_core(*, actor_id: str, deps: dict[str, object]) -> int:
    _pipeline_import_default_feeds_for_actor_core = deps['pipeline_import_default_feeds_for_actor_core']
    _db_path = deps['db_path']
    _default_cti_feeds = deps['default_cti_feeds']
    _primary_cti_feeds = deps.get('primary_cti_feeds')
    _secondary_context_feeds = deps.get('secondary_context_feeds')
    _actor_feed_lookback_days = deps['actor_feed_lookback_days']
    _feed_import_max_seconds = deps['feed_import_max_seconds']
    _feed_fetch_timeout_seconds = deps['feed_fetch_timeout_seconds']
    _feed_entry_scan_limit = deps['feed_entry_scan_limit']
    _feed_imported_limit = deps['feed_imported_limit']
    _feed_soft_match_limit = int(deps.get('feed_soft_match_limit', 0))
    _feed_import_mode = str(deps.get('feed_import_mode', 'background') or 'background')
    _feed_high_signal_target = max(1, int(deps.get('feed_high_signal_target', 2)))
    _retain_soft_candidates = bool(deps.get('retain_soft_candidates', False))
    _actor_search_link_limit = deps['actor_search_link_limit']
    _feed_require_published_at = deps['feed_require_published_at']
    _evidence_pipeline_v2 = bool(deps.get('evidence_pipeline_v2', False))

    return _pipeline_import_default_feeds_for_actor_core(
        actor_id,
        db_path=_db_path(),
        default_cti_feeds=_default_cti_feeds,
        primary_cti_feeds=_primary_cti_feeds,
        secondary_context_feeds=_secondary_context_feeds,
        actor_feed_lookback_days=_actor_feed_lookback_days,
        feed_import_max_seconds=_feed_import_max_seconds,
        feed_fetch_timeout_seconds=_feed_fetch_timeout_seconds,
        feed_entry_scan_limit=_feed_entry_scan_limit,
        feed_imported_limit=_feed_imported_limit,
        feed_soft_match_limit=_feed_soft_match_limit,
        import_mode=_feed_import_mode,
        high_signal_target=_feed_high_signal_target,
        retain_soft_candidates=_retain_soft_candidates,
        actor_search_link_limit=_actor_search_link_limit,
        feed_require_published_at=_feed_require_published_at,
        evidence_pipeline_v2=_evidence_pipeline_v2,
        deps={
            'actor_exists': deps['actor_exists'],
            'build_actor_profile_from_mitre': deps['build_actor_profile_from_mitre'],
            'actor_terms': deps['actor_terms'],
            'actor_query_feeds': deps['actor_query_feeds'],
            'import_ransomware_live_actor_activity': deps['import_ransomware_live_actor_activity'],
            'safe_http_get': deps['safe_http_get'],
            'parse_feed_entries': deps['parse_feed_entries'],
            'text_contains_actor_term': deps['text_contains_actor_term'],
            'within_lookback': deps['within_lookback'],
            'parse_published_datetime': deps['parse_published_datetime'],
            'derive_source_from_url': deps['derive_source_from_url'],
            'upsert_source_for_actor': deps['upsert_source_for_actor'],
            'duckduckgo_actor_search_urls': deps['duckduckgo_actor_search_urls'],
            'utc_now_iso': deps['utc_now_iso'],
            'source_trust_score': deps.get('source_trust_score'),
        },
    )
