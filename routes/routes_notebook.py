import sqlite3

from routes.notebook_feedback import register_notebook_feedback_routes
from routes.notebook_exports import register_notebook_export_routes
from routes.notebook_hunts import register_notebook_hunts_routes
from routes.notebook_live import register_notebook_live_routes
from routes.notebook_observations import register_notebook_observation_routes
from routes.notebook_operations import register_notebook_operation_routes
from routes.notebook_workspace import register_notebook_workspace_routes
from routes.notebook_router_helpers import build_analyst_pack_payload
from routes.notebook_router_helpers import enqueue_change_alert_if_needed
from routes.notebook_router_helpers import fetch_analyst_observations
from routes.notebook_router_helpers import ioc_value_is_hunt_relevant
from routes.notebook_router_helpers import render_simple_text_pdf
from routes.notebook_router_helpers import upsert_observation_with_history
from fastapi import APIRouter


def create_notebook_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _safe_json_string_list = deps['safe_json_string_list']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _templates = deps['templates']
    _actor_exists = deps['actor_exists']
    _store_feedback_event = deps['store_feedback_event']
    _feedback_summary_for_actor = deps['feedback_summary_for_actor']
    _normalize_environment_profile = deps['normalize_environment_profile']
    _upsert_environment_profile = deps['upsert_environment_profile']
    _load_environment_profile = deps['load_environment_profile']
    _apply_feedback_to_source_domains = deps['apply_feedback_to_source_domains']
    _recover_stale_running_states = deps.get('recover_stale_running_states')

    register_notebook_live_routes(
        router=router,
        deps={
            'db_path': _db_path,
            'fetch_actor_notebook': _fetch_actor_notebook,
            'recover_stale_running_states': _recover_stale_running_states,
        },
    )
    register_notebook_feedback_routes(
        router=router,
        deps={
            'enforce_request_size': _enforce_request_size,
            'default_body_limit_bytes': _default_body_limit_bytes,
            'db_path': _db_path,
            'actor_exists': _actor_exists,
            'normalize_environment_profile': _normalize_environment_profile,
            'upsert_environment_profile': _upsert_environment_profile,
            'load_environment_profile': _load_environment_profile,
            'store_feedback_event': _store_feedback_event,
            'feedback_summary_for_actor': _feedback_summary_for_actor,
            'apply_feedback_to_source_domains': _apply_feedback_to_source_domains,
        },
    )
    register_notebook_operation_routes(
        router=router,
        deps={
            'enforce_request_size': _enforce_request_size,
            'default_body_limit_bytes': _default_body_limit_bytes,
            'generate_actor_requirements': deps['generate_actor_requirements'],
            'db_path': _db_path,
            'utc_now_iso': _utc_now_iso,
            'safe_json_string_list': _safe_json_string_list,
            'actor_exists': _actor_exists,
            'get_tracking_intent': deps['get_tracking_intent'],
            'upsert_tracking_intent': deps['upsert_tracking_intent'],
            'confirm_actor_assessment': deps['confirm_actor_assessment'],
            'dispatch_alert_deliveries': deps.get('dispatch_alert_deliveries'),
        },
    )
    def _enqueue_change_alert_if_needed(
        *,
        connection: sqlite3.Connection,
        actor_id: str,
        change_item_id: str,
        change_summary: str,
        change_type: str,
        confidence: str,
        source_ref: str,
        tags: dict[str, int],
    ) -> dict[str, object]:
        return enqueue_change_alert_if_needed(
            connection=connection,
            actor_id=actor_id,
            change_item_id=change_item_id,
            change_summary=change_summary,
            change_type=change_type,
            confidence=confidence,
            source_ref=source_ref,
            tags=tags,
            safe_json_string_list=_safe_json_string_list,
            utc_now_iso=_utc_now_iso,
        )

    def _fetch_analyst_observations(
        actor_id: str,
        *,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        return fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=limit,
            offset=offset,
            db_path=_db_path,
            actor_exists=_actor_exists,
        )

    def _build_analyst_pack_payload(
        actor_id: str,
        *,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 1000,
        history_limit: int = 1000,
    ) -> dict[str, object]:
        return build_analyst_pack_payload(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            observations_limit=observations_limit,
            history_limit=history_limit,
            fetch_actor_notebook=_fetch_actor_notebook,
            fetch_analyst_observations=_fetch_analyst_observations,
            db_path=_db_path,
            actor_exists=_actor_exists,
            utc_now_iso=_utc_now_iso,
        )

    register_notebook_hunts_routes(
        router=router,
        deps={
            'fetch_actor_notebook': _fetch_actor_notebook,
            'db_path': _db_path,
            'load_environment_profile': _load_environment_profile,
            'generate_ioc_hunt_queries': deps['generate_ioc_hunt_queries'],
            'get_ollama_status': deps['get_ollama_status'],
            'templates': _templates,
            'ioc_value_is_hunt_relevant': ioc_value_is_hunt_relevant,
        },
    )

    register_notebook_observation_routes(
        router=router,
        deps={
            'enforce_request_size': _enforce_request_size,
            'default_body_limit_bytes': _default_body_limit_bytes,
            'db_path': _db_path,
            'actor_exists': _actor_exists,
            'fetch_actor_notebook': _fetch_actor_notebook,
            'utc_now_iso': _utc_now_iso,
            'upsert_observation_with_history': upsert_observation_with_history,
            'fetch_analyst_observations': _fetch_analyst_observations,
        },
    )
    register_notebook_export_routes(
        router=router,
        deps={
            'db_path': _db_path,
            'actor_exists': _actor_exists,
            'build_analyst_pack_payload': _build_analyst_pack_payload,
            'render_simple_text_pdf': render_simple_text_pdf,
        },
    )
    register_notebook_workspace_routes(
        router=router,
        deps={
            'db_path': _db_path,
            'actor_exists': _actor_exists,
            'safe_json_string_list': _safe_json_string_list,
            'templates': _templates,
            'fetch_actor_notebook': _fetch_actor_notebook,
        },
    )


    return router
