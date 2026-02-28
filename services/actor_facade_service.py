def actor_terms_core(
    actor_name: str,
    mitre_group_name: str,
    aliases_csv: str,
    *,
    actor_search_service,
    dedupe_actor_terms,
) -> list[str]:
    return actor_search_service.actor_terms_core(
        actor_name,
        mitre_group_name,
        aliases_csv,
        deps={
            'dedupe_actor_terms': dedupe_actor_terms,
        },
    )


def text_contains_actor_term_core(
    text: str,
    actor_terms: list[str],
    *,
    actor_search_service,
    sentence_mentions_actor_terms,
) -> bool:
    return actor_search_service.text_contains_actor_term_core(
        text,
        actor_terms,
        deps={
            'sentence_mentions_actor_terms': sentence_mentions_actor_terms,
        },
    )


def actor_query_feeds_core(actor_terms: list[str], *, actor_search_service) -> list[tuple[str, str]]:
    return actor_search_service.actor_query_feeds_core(actor_terms)


def actor_search_queries_core(actor_terms: list[str], *, actor_search_service) -> list[str]:
    return actor_search_service.actor_search_queries_core(actor_terms)


def domain_allowed_for_actor_search_core(
    url: str,
    *,
    actor_search_service,
    actor_search_domains: set[str],
) -> bool:
    return actor_search_service.domain_allowed_for_actor_search_core(
        url,
        domains=actor_search_domains,
    )


def duckduckgo_actor_search_urls_core(
    actor_terms: list[str],
    *,
    limit: int,
    actor_search_service,
    actor_search_queries,
    http_get,
    domain_allowed_for_actor_search,
    re_finditer,
) -> list[str]:
    return actor_search_service.duckduckgo_actor_search_urls_core(
        actor_terms,
        limit=limit,
        deps={
            'actor_search_queries': actor_search_queries,
            'http_get': http_get,
            'domain_allowed_for_actor_search': domain_allowed_for_actor_search,
            're_finditer': re_finditer,
        },
    )


def sentence_mentions_actor_core(
    sentence: str,
    actor_name: str,
    *,
    analyst_text_service,
    re_findall,
) -> bool:
    return analyst_text_service.sentence_mentions_actor_core(
        sentence,
        actor_name,
        deps={
            're_findall': re_findall,
        },
    )


def looks_like_navigation_noise_core(sentence: str, *, analyst_text_service) -> bool:
    return analyst_text_service.looks_like_navigation_noise_core(sentence)


def build_actor_profile_summary_core(
    actor_name: str,
    source_texts: list[str],
    *,
    analyst_text_service,
    split_sentences,
    looks_like_navigation_noise,
    sentence_mentions_actor,
    normalize_text,
    token_overlap,
) -> str:
    return analyst_text_service.build_actor_profile_summary_core(
        actor_name,
        source_texts,
        deps={
            'split_sentences': split_sentences,
            'looks_like_navigation_noise': looks_like_navigation_noise,
            'sentence_mentions_actor': sentence_mentions_actor,
            'normalize_text': normalize_text,
            'token_overlap': token_overlap,
        },
    )
