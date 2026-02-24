def register_routers(app, *, deps: dict[str, object]) -> None:
    routes_dashboard = deps['routes_dashboard']
    routes_api = deps['routes_api']
    routes_ui = deps['routes_ui']
    routes_actor_ops = deps['routes_actor_ops']
    routes_notebook = deps['routes_notebook']
    routes_evolution = deps['routes_evolution']

    app.include_router(
        routes_dashboard.create_dashboard_router(
            deps={
                'list_actor_profiles': deps['list_actor_profiles'],
                'fetch_actor_notebook': deps['fetch_actor_notebook'],
                'set_actor_notebook_status': deps['set_actor_notebook_status'],
                'run_actor_generation': deps['run_actor_generation'],
                'enqueue_actor_generation': deps['enqueue_actor_generation'],
                'get_ollama_status': deps['get_ollama_status'],
                'get_actor_refresh_stats': deps['get_actor_refresh_stats'],
                'page_refresh_auto_trigger_minutes': deps['page_refresh_auto_trigger_minutes'],
                'running_stale_recovery_minutes': deps['running_stale_recovery_minutes'],
                'recover_stale_running_states': deps['recover_stale_running_states'],
                'format_duration_ms': deps['format_duration_ms'],
                'templates': deps['templates'],
            }
        )
    )

    app.include_router(
        routes_api.create_api_router(
            deps={
                'list_actor_profiles': deps['list_actor_profiles'],
                'enforce_request_size': deps['enforce_request_size'],
                'default_body_limit_bytes': deps['default_body_limit_bytes'],
                'create_actor_profile': deps['create_actor_profile'],
                'merge_actor_profiles': deps['merge_actor_profiles'],
                'db_path': deps['db_path'],
                'actor_exists': deps['actor_exists'],
                'set_actor_notebook_status': deps['set_actor_notebook_status'],
                'run_actor_generation': deps['run_actor_generation'],
                'enqueue_actor_generation': deps['enqueue_actor_generation'],
                'metrics_snapshot': deps.get('metrics_snapshot'),
            }
        )
    )

    app.include_router(
        routes_ui.create_ui_router(
            deps={
                'enforce_request_size': deps['enforce_request_size'],
                'default_body_limit_bytes': deps['default_body_limit_bytes'],
                'create_actor_profile': deps['create_actor_profile'],
                'set_actor_notebook_status': deps['set_actor_notebook_status'],
                'run_actor_generation': deps['run_actor_generation'],
                'enqueue_actor_generation': deps['enqueue_actor_generation'],
                'list_actor_profiles': deps['list_actor_profiles'],
            }
        )
    )

    app.include_router(
        routes_actor_ops.create_actor_ops_router(
            deps={
                'enforce_request_size': deps['enforce_request_size'],
                'source_upload_body_limit_bytes': deps['source_upload_body_limit_bytes'],
                'default_body_limit_bytes': deps['default_body_limit_bytes'],
                'db_path': deps['db_path'],
                'actor_exists': deps['actor_exists'],
                'derive_source_from_url': deps['derive_source_from_url'],
                'upsert_source_for_actor': deps['upsert_source_for_actor'],
                'import_default_feeds_for_actor': deps['import_default_feeds_for_actor'],
                'parse_ioc_values': deps['parse_ioc_values'],
                'validate_ioc_candidate': deps['validate_ioc_candidate'],
                'upsert_ioc_item': deps['upsert_ioc_item'],
                'export_actor_stix_bundle': deps['export_actor_stix_bundle'],
                'import_actor_stix_bundle': deps['import_actor_stix_bundle'],
                'utc_now_iso': deps['utc_now_iso'],
                'set_actor_notebook_status': deps['set_actor_notebook_status'],
                'get_actor_refresh_stats': deps['get_actor_refresh_stats'],
                'run_actor_generation': deps['run_actor_generation'],
                'enqueue_actor_generation': deps['enqueue_actor_generation'],
            }
        )
    )

    app.include_router(
        routes_notebook.create_notebook_router(
            deps={
                'enforce_request_size': deps['enforce_request_size'],
                'default_body_limit_bytes': deps['default_body_limit_bytes'],
                'generate_actor_requirements': deps['generate_actor_requirements'],
                'db_path': deps['db_path'],
                'utc_now_iso': deps['utc_now_iso'],
                'safe_json_string_list': deps['safe_json_string_list'],
                'fetch_actor_notebook': deps['fetch_actor_notebook'],
                'templates': deps['templates'],
                'actor_exists': deps['actor_exists'],
                'generate_ioc_hunt_queries': deps['generate_ioc_hunt_queries'],
                'get_ollama_status': deps['get_ollama_status'],
                'store_feedback_event': deps['store_feedback_event'],
                'feedback_summary_for_actor': deps['feedback_summary_for_actor'],
                'normalize_environment_profile': deps['normalize_environment_profile'],
                'upsert_environment_profile': deps['upsert_environment_profile'],
                'load_environment_profile': deps['load_environment_profile'],
                'apply_feedback_to_source_domains': deps['apply_feedback_to_source_domains'],
            }
        )
    )

    app.include_router(
        routes_evolution.create_evolution_router(
            deps={
                'enforce_request_size': deps['enforce_request_size'],
                'observation_body_limit_bytes': deps['observation_body_limit_bytes'],
                'default_body_limit_bytes': deps['default_body_limit_bytes'],
                'db_path': deps['db_path'],
                'actor_exists': deps['actor_exists'],
                'normalize_technique_id': deps['normalize_technique_id'],
                'normalize_string_list': deps['normalize_string_list'],
                'utc_now_iso': deps['utc_now_iso'],
                'capability_category_from_technique_id': deps['capability_category_from_technique_id'],
                'generate_validation_template': deps['generate_validation_template'],
                'baseline_entry': deps['baseline_entry'],
                'resolve_delta_action': deps['resolve_delta_action'],
            }
        )
    )
