def build_recent_activity_highlights_core(
    timeline_items: list[dict[str, object]],
    sources: list[dict[str, object]],
    actor_terms: list[str],
    *,
    deps: dict[str, object],
) -> list[dict[str, str | None]]:
    _pipeline_build_recent_activity_highlights = deps['pipeline_build_recent_activity_highlights']
    _trusted_activity_domains = deps['trusted_activity_domains']
    _source_domain = deps['source_domain']
    _canonical_group_domain = deps['canonical_group_domain']
    _looks_like_activity_sentence = deps['looks_like_activity_sentence']
    _sentence_mentions_actor_terms = deps['sentence_mentions_actor_terms']
    _text_contains_actor_term = deps['text_contains_actor_term']
    _normalize_text = deps['normalize_text']
    _parse_published_datetime = deps['parse_published_datetime']
    _freshness_badge = deps['freshness_badge']
    _evidence_title_from_source = deps['evidence_title_from_source']
    _fallback_title_from_url = deps['fallback_title_from_url']
    _evidence_source_label_from_source = deps['evidence_source_label_from_source']
    _extract_ttp_ids = deps['extract_ttp_ids']
    _split_sentences = deps['split_sentences']
    _looks_like_navigation_noise = deps['looks_like_navigation_noise']
    _format_date_or_unknown = deps['format_date_or_unknown']
    _source_trust_score = deps.get('source_trust_score', lambda _url: 0)

    pipeline_items = _pipeline_build_recent_activity_highlights(
        timeline_items,
        sources,
        actor_terms,
        trusted_activity_domains=_trusted_activity_domains,
        source_domain=_source_domain,
        canonical_group_domain=_canonical_group_domain,
        looks_like_activity_sentence=_looks_like_activity_sentence,
        sentence_mentions_actor_terms=_sentence_mentions_actor_terms,
        text_contains_actor_term=_text_contains_actor_term,
        normalize_text=_normalize_text,
        parse_published_datetime=lambda value: _parse_published_datetime(value),
        freshness_badge=lambda value: _freshness_badge(value),
        evidence_title_from_source=_evidence_title_from_source,
        fallback_title_from_url=_fallback_title_from_url,
        evidence_source_label_from_source=_evidence_source_label_from_source,
        extract_ttp_ids=_extract_ttp_ids,
        split_sentences=lambda text: _split_sentences(text),
        looks_like_navigation_noise=_looks_like_navigation_noise,
        source_trust_score=_source_trust_score,
    )

    highlights: list[dict[str, str | None]] = []
    for item in pipeline_items:
        copy_item = dict(item)
        copy_item['date'] = _format_date_or_unknown(str(item.get('date') or ''))
        highlights.append(copy_item)
    return highlights
