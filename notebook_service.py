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
    )
