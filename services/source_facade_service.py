from urllib.parse import urlparse


def build_recent_activity_highlights_core(
    timeline_items: list[dict[str, object]],
    sources: list[dict[str, object]],
    actor_terms: list[str],
    *,
    activity_highlight_service,
    pipeline_build_recent_activity_highlights,
    trusted_activity_domains: set[str],
    canonical_group_domain,
    looks_like_activity_sentence,
    sentence_mentions_actor_terms,
    text_contains_actor_term,
    normalize_text,
    parse_published_datetime,
    freshness_badge,
    evidence_title_from_source,
    fallback_title_from_url,
    evidence_source_label_from_source,
    extract_ttp_ids,
    split_sentences,
    looks_like_navigation_noise,
    format_date_or_unknown,
    source_trust_score,
) -> list[dict[str, str | None]]:
    def _source_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return ''

    return activity_highlight_service.build_recent_activity_highlights_core(
        timeline_items,
        sources,
        actor_terms,
        deps={
            'pipeline_build_recent_activity_highlights': pipeline_build_recent_activity_highlights,
            'trusted_activity_domains': trusted_activity_domains,
            'source_domain': _source_domain,
            'canonical_group_domain': canonical_group_domain,
            'looks_like_activity_sentence': looks_like_activity_sentence,
            'sentence_mentions_actor_terms': sentence_mentions_actor_terms,
            'text_contains_actor_term': text_contains_actor_term,
            'normalize_text': normalize_text,
            'parse_published_datetime': parse_published_datetime,
            'freshness_badge': freshness_badge,
            'evidence_title_from_source': evidence_title_from_source,
            'fallback_title_from_url': fallback_title_from_url,
            'evidence_source_label_from_source': evidence_source_label_from_source,
            'extract_ttp_ids': extract_ttp_ids,
            'split_sentences': split_sentences,
            'looks_like_navigation_noise': looks_like_navigation_noise,
            'format_date_or_unknown': format_date_or_unknown,
            'source_trust_score': source_trust_score,
        },
    )


def source_trust_score_core(
    url: str,
    *,
    source_reliability_service,
    high_confidence_domains: set[str],
    medium_confidence_domains: set[str],
    secondary_context_domains: set[str],
    trusted_activity_domains: set[str],
) -> int:
    return source_reliability_service.source_trust_score_core(
        url,
        high_confidence_domains=high_confidence_domains,
        medium_confidence_domains=medium_confidence_domains,
        secondary_context_domains=secondary_context_domains,
        trusted_activity_domains=trusted_activity_domains,
    )


def source_tier_label_core(url: str, *, source_trust_score, source_reliability_service) -> str:
    return source_reliability_service.source_tier_label_core(source_trust_score(url))


def build_recent_activity_synthesis_core(
    highlights: list[dict[str, str | None]],
    *,
    recent_activity_service,
    extract_target_from_activity_text,
    parse_published_datetime,
) -> list[dict[str, str]]:
    return recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            'extract_target_from_activity_text': extract_target_from_activity_text,
            'parse_published_datetime': parse_published_datetime,
        },
    )


def validate_outbound_url_core(
    source_url: str,
    *,
    allowed_domains: set[str] | None,
    network_service,
    outbound_allowed_domains: set[str],
    resolve_host,
    ipproto_tcp,
    allow_http: bool,
) -> str:
    return network_service.validate_outbound_url_core(
        source_url,
        allowed_domains=allowed_domains,
        deps={
            'outbound_allowed_domains': outbound_allowed_domains,
            'resolve_host': resolve_host,
            'ipproto_tcp': ipproto_tcp,
            'allow_http': allow_http,
        },
    )


def safe_http_get_core(
    source_url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None,
    allowed_domains: set[str] | None,
    max_redirects: int,
    network_service,
    validate_outbound_url,
):
    return network_service.safe_http_get_core(
        source_url,
        timeout=timeout,
        headers=headers,
        allowed_domains=allowed_domains,
        max_redirects=max_redirects,
        deps={
            'validate_url': lambda url, domains: validate_outbound_url(url, allowed_domains=domains),
        },
    )
