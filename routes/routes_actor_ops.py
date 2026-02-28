from fastapi import APIRouter

from routes.actor_ops_refresh_diagnostics import register_actor_refresh_and_diagnostic_routes
from routes.actor_ops_sources_iocs import register_actor_source_and_ioc_routes
from routes.actor_ops_stix_taxii import register_actor_stix_and_taxii_routes


def create_actor_ops_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    register_actor_source_and_ioc_routes(
        router=router,
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
            'upsert_ioc_item': deps['upsert_ioc_item'],
            'utc_now_iso': deps['utc_now_iso'],
        },
    )

    register_actor_refresh_and_diagnostic_routes(
        router=router,
        deps={
            'db_path': deps['db_path'],
            'actor_exists': deps['actor_exists'],
            'set_actor_notebook_status': deps['set_actor_notebook_status'],
            'get_actor_refresh_stats': deps['get_actor_refresh_stats'],
            'get_actor_refresh_timeline': deps.get('get_actor_refresh_timeline'),
            'submit_actor_refresh_job': deps.get('submit_actor_refresh_job'),
            'get_actor_refresh_job': deps.get('get_actor_refresh_job'),
            'enqueue_actor_generation': deps.get('enqueue_actor_generation', deps['run_actor_generation']),
        },
    )

    register_actor_stix_and_taxii_routes(
        router=router,
        deps={
            'enforce_request_size': deps['enforce_request_size'],
            'default_body_limit_bytes': deps['default_body_limit_bytes'],
            'db_path': deps['db_path'],
            'actor_exists': deps['actor_exists'],
            'export_actor_stix_bundle': deps['export_actor_stix_bundle'],
            'import_actor_stix_bundle': deps['import_actor_stix_bundle'],
            'list_ranked_evidence': deps.get('list_ranked_evidence'),
            'sync_taxii_collection': deps.get('sync_taxii_collection'),
            'list_taxii_sync_runs': deps.get('list_taxii_sync_runs'),
            'taxii_collection_url': deps.get('taxii_collection_url', lambda: ''),
            'taxii_auth_token': deps.get('taxii_auth_token', lambda: ''),
            'taxii_lookback_hours': int(deps.get('taxii_lookback_hours', 72)),
            'utc_now_iso': deps['utc_now_iso'],
        },
    )

    return router
