def generate_actor_requirements_core(
    *,
    actor_id: str,
    org_context: str,
    priority_mode: str,
    deps: dict[str, object],
) -> int:
    _pipeline_generate_actor_requirements_core = deps['pipeline_generate_actor_requirements_core']
    _db_path = deps['db_path']

    return _pipeline_generate_actor_requirements_core(
        actor_id,
        org_context,
        priority_mode,
        db_path=_db_path(),
        deps={
            'now_iso': deps['now_iso'],
            'actor_exists': deps['actor_exists'],
            'build_actor_profile_from_mitre': deps['build_actor_profile_from_mitre'],
            'actor_terms': deps['actor_terms'],
            'split_sentences': deps['split_sentences'],
            'sentence_mentions_actor_terms': deps['sentence_mentions_actor_terms'],
            'looks_like_activity_sentence': deps['looks_like_activity_sentence'],
            'ollama_available': deps['ollama_available'],
            'sanitize_question_text': deps['sanitize_question_text'],
            'question_from_sentence': deps['question_from_sentence'],
            'token_overlap': deps['token_overlap'],
            'normalize_text': deps['normalize_text'],
            'new_id': deps['new_id'],
        },
    )
