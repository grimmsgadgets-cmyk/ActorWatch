def priority_where_to_check_core(
    guidance_items: list[dict[str, object]],
    question_text: str,
    *,
    priority_service,
    priority_questions,
    platforms_for_question,
) -> str:
    return priority_service.priority_where_to_check_core(
        guidance_items,
        question_text,
        deps={
            'priority_where_to_check': priority_questions.priority_where_to_check,
            'platforms_for_question': platforms_for_question,
        },
    )


def telemetry_anchor_line_core(
    guidance_items: list[dict[str, object]],
    question_text: str,
    *,
    priority_service,
    priority_questions,
    platforms_for_question,
) -> str:
    return priority_service.telemetry_anchor_line_core(
        guidance_items,
        question_text,
        deps={
            'telemetry_anchor_line': priority_questions.telemetry_anchor_line,
            'platforms_for_question': platforms_for_question,
        },
    )


def guidance_query_hint_core(
    guidance_items: list[dict[str, object]],
    question_text: str,
    *,
    priority_service,
    priority_questions,
    platforms_for_question,
    guidance_for_platform,
) -> str:
    return priority_service.guidance_query_hint_core(
        guidance_items,
        question_text,
        deps={
            'guidance_query_hint': priority_questions.guidance_query_hint,
            'platforms_for_question': platforms_for_question,
            'guidance_for_platform': guidance_for_platform,
        },
    )


def priority_update_evidence_dt_core(
    update: dict[str, object],
    *,
    priority_service,
    priority_questions,
    parse_published_datetime,
):
    return priority_service.priority_update_evidence_dt_core(
        update,
        deps={
            'priority_update_evidence_dt': priority_questions.priority_update_evidence_dt,
            'parse_published_datetime': parse_published_datetime,
        },
    )


def question_org_alignment_core(
    question_text: str,
    org_context: str,
    *,
    priority_service,
    priority_questions,
    token_set,
) -> int:
    return priority_service.question_org_alignment_core(
        question_text,
        org_context,
        deps={
            'question_org_alignment': priority_questions.question_org_alignment,
            'token_set': token_set,
        },
    )


def latest_reporting_recency_label_core(
    timeline_recent_items: list[dict[str, object]],
    *,
    pipeline_latest_reporting_recency_label,
    parse_published_datetime,
) -> str:
    return pipeline_latest_reporting_recency_label(
        timeline_recent_items,
        parse_published_datetime=parse_published_datetime,
    )
