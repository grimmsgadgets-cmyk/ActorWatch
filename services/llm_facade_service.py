import time


def ollama_available_core(*, status_service, get_env, http_get) -> bool:
    return status_service.ollama_available_core(
        deps={
            'get_env': get_env,
            'http_get': http_get,
        }
    )


def ollama_generate_questions_core(
    actor_name: str,
    scope_statement: str | None,
    excerpts: list[str],
    *,
    analyst_text_service,
    ollama_available,
    get_env,
    http_post,
    sanitize_question_text,
) -> list[str]:
    return analyst_text_service.ollama_generate_questions_core(
        actor_name,
        scope_statement,
        excerpts,
        deps={
            'ollama_available': ollama_available,
            'get_env': get_env,
            'http_post': http_post,
            'sanitize_question_text': sanitize_question_text,
        },
    )


def ollama_review_change_signals_with_cache_core(
    actor_name: str,
    source_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    *,
    llm_cache_service,
    hashlib_sha256,
    get_env,
    analyst_text_service,
    ollama_available,
    http_post,
    parse_published_datetime,
    db_path,
    utc_now_iso,
) -> list[dict[str, object]]:
    actor_key = llm_cache_service.actor_key_core(actor_name)
    cache_payload = {
        'actor_key': actor_key,
        'source_items': [
            {
                'id': str(item.get('id') or ''),
                'url': str(item.get('url') or ''),
                'published_at': str(item.get('published_at') or ''),
                'retrieved_at': str(item.get('retrieved_at') or ''),
                'fingerprint': str(item.get('source_fingerprint') or ''),
            }
            for item in source_items[:120]
            if isinstance(item, dict)
        ],
        'recent_activity_highlights': [
            {
                'source_url': str(item.get('source_url') or ''),
                'date': str(item.get('date') or ''),
                'text': str(item.get('text') or '')[:260],
            }
            for item in recent_activity_highlights[:60]
            if isinstance(item, dict)
        ],
    }
    cache_fp = llm_cache_service.input_fingerprint_core(
        cache_payload,
        deps={'sha256': hashlib_sha256},
    )
    if get_env('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}:
        cached = llm_cache_service.load_cached_payload_core(
            actor_key=actor_key,
            cache_kind='review_change_signals',
            input_fingerprint=cache_fp,
            deps={
                'db_path': db_path,
                'utc_now_iso': utc_now_iso,
            },
        )
        if isinstance(cached, list):
            return [item for item in cached if isinstance(item, dict)]
    started = time.perf_counter()
    result = analyst_text_service.ollama_review_change_signals_core(
        actor_name,
        source_items,
        recent_activity_highlights,
        deps={
            'ollama_available': ollama_available,
            'get_env': get_env,
            'http_post': http_post,
            'parse_published_datetime': parse_published_datetime,
        },
    )
    if (
        get_env('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}
        and isinstance(result, list)
        and len(result) > 0
    ):
        llm_cache_service.save_cached_payload_core(
            actor_key=actor_key,
            cache_kind='review_change_signals',
            input_fingerprint=cache_fp,
            payload=[item for item in result if isinstance(item, dict)],
            estimated_cost_ms=int((time.perf_counter() - started) * 1000),
            deps={
                'db_path': db_path,
                'utc_now_iso': utc_now_iso,
            },
        )
    return result


def ollama_synthesize_recent_activity_with_cache_core(
    actor_name: str,
    highlights: list[dict[str, object]],
    *,
    llm_cache_service,
    hashlib_sha256,
    get_env,
    analyst_text_service,
    ollama_available,
    http_post,
    db_path,
    utc_now_iso,
) -> list[dict[str, str]]:
    actor_key = llm_cache_service.actor_key_core(actor_name)
    cache_payload = {
        'actor_key': actor_key,
        'highlights': [
            {
                'source_url': str(item.get('source_url') or ''),
                'date': str(item.get('date') or ''),
                'text': str(item.get('text') or '')[:320],
                'category': str(item.get('category') or ''),
            }
            for item in highlights[:80]
            if isinstance(item, dict)
        ],
    }
    cache_fp = llm_cache_service.input_fingerprint_core(
        cache_payload,
        deps={'sha256': hashlib_sha256},
    )
    if get_env('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}:
        cached = llm_cache_service.load_cached_payload_core(
            actor_key=actor_key,
            cache_kind='recent_activity_synthesis',
            input_fingerprint=cache_fp,
            deps={
                'db_path': db_path,
                'utc_now_iso': utc_now_iso,
            },
        )
        if isinstance(cached, list):
            return [item for item in cached if isinstance(item, dict)]
    started = time.perf_counter()
    result = analyst_text_service.ollama_synthesize_recent_activity_core(
        actor_name,
        highlights,
        deps={
            'ollama_available': ollama_available,
            'get_env': get_env,
            'http_post': http_post,
        },
    )
    if (
        get_env('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}
        and isinstance(result, list)
        and len(result) > 0
    ):
        llm_cache_service.save_cached_payload_core(
            actor_key=actor_key,
            cache_kind='recent_activity_synthesis',
            input_fingerprint=cache_fp,
            payload=[item for item in result if isinstance(item, dict)],
            estimated_cost_ms=int((time.perf_counter() - started) * 1000),
            deps={
                'db_path': db_path,
                'utc_now_iso': utc_now_iso,
            },
        )
    return result


def ollama_enrich_quick_checks_core(
    actor_name: str,
    cards: list[dict[str, object]],
    *,
    quick_check_service,
    ollama_available,
    get_env,
    http_post,
) -> dict[str, dict[str, str]]:
    return quick_check_service.generate_quick_check_overrides_core(
        actor_name,
        cards,
        deps={
            'ollama_available': ollama_available,
            'get_env': get_env,
            'http_post': http_post,
        },
    )


def ollama_generate_ioc_hunt_queries_core(
    actor_name: str,
    cards: list[dict[str, object]],
    *,
    environment_profile: dict[str, object] | None,
    ioc_hunt_service,
    ollama_available,
    get_env,
    http_post,
    personalize_query,
) -> dict[str, object]:
    return ioc_hunt_service.generate_ioc_hunt_queries_core(
        actor_name,
        cards,
        environment_profile=environment_profile,
        deps={
            'ollama_available': ollama_available,
            'get_env': get_env,
            'http_post': http_post,
            'personalize_query': personalize_query,
        },
    )
