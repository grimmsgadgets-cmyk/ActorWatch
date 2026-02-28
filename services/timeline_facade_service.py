def short_date_core(value: str, *, parse_published_datetime):
    import services.timeline_view_service as timeline_view_service

    return timeline_view_service.short_date_core(
        value,
        deps={
            'parse_published_datetime': parse_published_datetime,
        },
    )


def format_date_or_unknown_core(value: str, *, parse_published_datetime):
    import services.timeline_view_service as timeline_view_service

    return timeline_view_service.format_date_or_unknown_core(
        value,
        deps={
            'parse_published_datetime': parse_published_datetime,
        },
    )


def freshness_badge_core(value: str | None, *, parse_published_datetime):
    import services.timeline_view_service as timeline_view_service

    return timeline_view_service.freshness_badge_core(
        value,
        deps={
            'parse_published_datetime': parse_published_datetime,
        },
    )


def bucket_label_core(value: str, *, parse_iso_for_sort):
    import services.timeline_view_service as timeline_view_service

    return timeline_view_service.bucket_label_core(
        value,
        deps={
            'parse_iso_for_sort': parse_iso_for_sort,
        },
    )


def timeline_category_color_core(category: str):
    import services.timeline_view_service as timeline_view_service

    return timeline_view_service.timeline_category_color_core(category)


def build_notebook_kpis_core(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
    open_questions_count: int,
    sources: list[dict[str, object]],
    *,
    parse_published_datetime,
    mitre_valid_technique_ids,
) -> dict[str, str]:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.build_notebook_kpis_core(
        timeline_items,
        known_technique_ids,
        open_questions_count,
        sources,
        deps={
            'parse_published_datetime': parse_published_datetime,
            'mitre_valid_technique_ids': mitre_valid_technique_ids,
        },
    )


def build_timeline_graph_core(
    timeline_items: list[dict[str, object]],
    *,
    bucket_label,
    timeline_category_color,
) -> list[dict[str, object]]:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.build_timeline_graph_core(
        timeline_items,
        deps={
            'bucket_label': bucket_label,
            'timeline_category_color': timeline_category_color,
        },
    )


def first_seen_for_techniques_core(
    timeline_items: list[dict[str, object]],
    technique_ids: list[str],
    *,
    parse_published_datetime,
    short_date,
) -> list[dict[str, str]]:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.first_seen_for_techniques_core(
        timeline_items,
        technique_ids,
        deps={
            'parse_published_datetime': parse_published_datetime,
            'short_date': short_date,
        },
    )


def severity_label_core(category: str, target_text: str, novelty: bool) -> str:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.severity_label_core(category, target_text, novelty)


def action_text_core(category: str) -> str:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.action_text_core(category)


def compact_timeline_rows_core(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
    *,
    parse_iso_for_sort,
    short_date,
    action_text,
    severity_label,
) -> list[dict[str, object]]:
    import services.timeline_analytics_service as timeline_analytics_service

    return timeline_analytics_service.compact_timeline_rows_core(
        timeline_items,
        known_technique_ids,
        parse_iso_for_sort=parse_iso_for_sort,
        short_date=short_date,
        action_text=action_text,
        severity_label=severity_label,
    )
