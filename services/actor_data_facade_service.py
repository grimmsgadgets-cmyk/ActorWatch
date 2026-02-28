def upsert_source_for_actor_wrapper_core(
    *,
    connection,
    actor_id: str,
    source_name: str,
    source_url: str,
    published_at: str | None,
    pasted_text: str,
    trigger_excerpt: str | None = None,
    title: str | None = None,
    headline: str | None = None,
    og_title: str | None = None,
    html_title: str | None = None,
    publisher: str | None = None,
    site_name: str | None = None,
    source_tier: str | None = None,
    confidence_weight: int | None = None,
    source_type: str | None = None,
    refresh_existing_content: bool = False,
    deps: dict[str, object],
) -> str:
    _source_tier_label = deps['source_tier_label']
    _source_trust_score = deps['source_trust_score']
    _source_store_service = deps['source_store_service']
    _source_quality_overwrite_on_upsert = bool(deps.get('source_quality_overwrite_on_upsert', False))
    resolved_source_tier = str(source_tier or '').strip() or _source_tier_label(source_url)
    resolved_confidence_weight = (
        int(confidence_weight)
        if confidence_weight is not None
        else int(_source_trust_score(source_url))
    )
    return _source_store_service.upsert_source_for_actor_core(
        connection=connection,
        actor_id=actor_id,
        source_name=source_name,
        source_url=source_url,
        published_at=published_at,
        pasted_text=pasted_text,
        trigger_excerpt=trigger_excerpt,
        title=title,
        headline=headline,
        og_title=og_title,
        html_title=html_title,
        publisher=publisher,
        site_name=site_name,
        source_type=source_type,
        source_tier=resolved_source_tier,
        confidence_weight=resolved_confidence_weight,
        overwrite_source_quality=_source_quality_overwrite_on_upsert,
        refresh_existing_content=refresh_existing_content,
        deps={
            'source_fingerprint': deps['source_fingerprint'],
            'new_id': deps['new_id'],
            'now_iso': deps['now_iso'],
        },
    )


def upsert_ioc_item_wrapper_core(
    connection,
    *,
    actor_id: str,
    raw_ioc_type: str,
    raw_ioc_value: str,
    source_ref: str | None,
    source_id: str | None,
    source_tier: str | None,
    extraction_method: str,
    now_iso: str,
    lifecycle_status: str = 'active',
    handling_tlp: str = 'TLP:CLEAR',
    confidence_score_override: int | None = None,
    observed_at: str | None = None,
    valid_from: str | None = None,
    valid_until: str | None = None,
    revoked: bool = False,
    deps: dict[str, object] | None = None,
) -> dict[str, object]:
    _ioc_store_service = deps['ioc_store_service']
    return _ioc_store_service.upsert_ioc_item_core(
        connection,
        actor_id=actor_id,
        raw_ioc_type=raw_ioc_type,
        raw_ioc_value=raw_ioc_value,
        source_ref=source_ref,
        source_id=source_id,
        source_tier=source_tier,
        extraction_method=extraction_method,
        now_iso=now_iso,
        lifecycle_status=lifecycle_status,
        handling_tlp=handling_tlp,
        confidence_score_override=confidence_score_override,
        observed_at=observed_at,
        valid_from=valid_from,
        valid_until=valid_until,
        revoked=revoked,
        deps={
            'validate_ioc_candidate': deps['validate_ioc_candidate'],
        },
    )


def export_actor_stix_bundle_wrapper_core(connection, *, actor_id: str, actor_name: str, deps: dict[str, object]) -> dict[str, object]:
    _stix_service = deps['stix_service']
    return _stix_service.export_actor_bundle_core(
        connection,
        actor_id=actor_id,
        actor_name=actor_name,
    )


def import_actor_stix_bundle_wrapper_core(
    connection,
    *,
    actor_id: str,
    bundle: dict[str, object],
    deps: dict[str, object],
) -> dict[str, int]:
    _stix_service = deps['stix_service']
    return _stix_service.import_actor_bundle_core(
        connection,
        actor_id=actor_id,
        bundle=bundle,
        now_iso=deps['now_iso'],
        upsert_ioc_item=deps['upsert_ioc_item'],
    )


def list_ranked_evidence_wrapper_core(
    connection,
    *,
    actor_id: str,
    limit: int = 25,
    entity_type: str = '',
    min_final_score: float = 0.0,
    source_tier: str = '',
    match_type: str = '',
    require_corroboration: bool = False,
    deps: dict[str, object],
) -> list[dict[str, object]]:
    _source_evidence_view_service = deps['source_evidence_view_service']
    return _source_evidence_view_service.list_ranked_evidence_core(
        connection,
        actor_id=actor_id,
        limit=limit,
        entity_type=entity_type,
        min_final_score=min_final_score,
        source_tier=source_tier,
        match_type=match_type,
        require_corroboration=require_corroboration,
    )


def sync_taxii_collection_wrapper_core(
    connection,
    *,
    actor_id: str,
    collection_url: str,
    auth_token: str | None,
    now_iso: str,
    lookback_hours: int,
    deps: dict[str, object],
) -> dict[str, object]:
    _taxii_ingest_service = deps['taxii_ingest_service']
    return _taxii_ingest_service.sync_taxii_collection_core(
        connection,
        actor_id=actor_id,
        collection_url=collection_url,
        auth_token=auth_token,
        now_iso=now_iso,
        lookback_hours=lookback_hours,
        deps={
            'http_get': deps['http_get'],
            'import_actor_stix_bundle': deps['import_actor_stix_bundle'],
            'upsert_ioc_item': deps['upsert_ioc_item'],
        },
    )


def list_taxii_sync_runs_wrapper_core(connection, *, actor_id: str, limit: int = 20, deps: dict[str, object]) -> list[dict[str, object]]:
    _taxii_ingest_service = deps['taxii_ingest_service']
    return _taxii_ingest_service.list_taxii_sync_runs_core(
        connection,
        actor_id=actor_id,
        limit=limit,
    )
