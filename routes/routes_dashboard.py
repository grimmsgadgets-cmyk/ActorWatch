from datetime import datetime, timedelta, timezone
import logging

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse
import services.notebook_service as notebook_service

LOGGER = logging.getLogger(__name__)


def render_dashboard_root(
    *,
    request: Request,
    background_tasks: BackgroundTasks,
    actor_id: str | None,
    notice: str | None,
    source_tier: str | None,
    min_confidence_weight: str | None,
    source_days: str | None,
    deps: dict[str, object],
) -> HTMLResponse:
    _list_actor_profiles = deps['list_actor_profiles']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _get_ollama_status = deps['get_ollama_status']
    _get_actor_refresh_stats = deps.get('get_actor_refresh_stats')
    _page_refresh_auto_trigger_minutes = int(deps.get('page_refresh_auto_trigger_minutes', 30))
    _running_stale_recovery_minutes = int(deps.get('running_stale_recovery_minutes', 10))
    _recover_stale_running_states = deps.get('recover_stale_running_states')
    _format_duration_ms = deps['format_duration_ms']
    _templates = deps['templates']

    if _recover_stale_running_states is not None:
        try:
            _recover_stale_running_states()
        except Exception:
            pass

    def _actor_last_updated_label(actor: dict[str, object]) -> str:
        raw_value = str(actor.get('notebook_updated_at') or actor.get('created_at') or '').strip()
        if not raw_value:
            return 'Unknown'
        normalized = raw_value.replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            return raw_value[:10]

    actors_all = _list_actor_profiles()
    actors_by_id = {str(actor.get('id') or ''): actor for actor in actors_all}
    duplicate_actor_groups: list[dict[str, object]] = []
    groups_by_canonical: dict[str, list[dict[str, object]]] = {}
    for actor in actors_all:
        canonical = ' '.join(str(actor.get('display_name') or '').strip().lower().split())
        if not canonical:
            continue
        groups_by_canonical.setdefault(canonical, []).append(actor)
    for canonical, items in groups_by_canonical.items():
        if len(items) <= 1:
            continue
        sorted_items = sorted(
            items,
            key=lambda item: (
                0 if item.get('is_tracked') else 1,
                str(item.get('created_at') or ''),
            ),
        )
        duplicate_actor_groups.append(
            {
                'canonical_name': canonical,
                'target_actor': sorted_items[0],
                'source_actors': sorted_items[1:],
            }
        )
    tracked_actors = [actor for actor in actors_all if actor['is_tracked']]
    for actor in tracked_actors:
        actor['last_updated_label'] = _actor_last_updated_label(actor)

    selected_actor_id = actor_id
    all_actor_ids = {actor['id'] for actor in actors_all}
    if selected_actor_id is None:
        if tracked_actors:
            selected_actor_id = tracked_actors[0]['id']
        elif actors_all:
            selected_actor_id = actors_all[0]['id']

    if selected_actor_id is not None and selected_actor_id not in all_actor_ids:
        selected_actor_id = tracked_actors[0]['id'] if tracked_actors else (actors_all[0]['id'] if actors_all else None)

    notebook: dict[str, object] | None = None
    refresh_stats: dict[str, object] | None = None
    allowed_tiers = {'high', 'medium', 'trusted', 'context', 'unrated'}
    normalized_source_tier = str(source_tier or '').strip().lower() or None
    if normalized_source_tier not in allowed_tiers:
        normalized_source_tier = None
    try:
        normalized_min_confidence_weight = (
            max(0, min(4, int(min_confidence_weight)))
            if min_confidence_weight is not None
            else None
        )
    except Exception:
        normalized_min_confidence_weight = None
    try:
        normalized_source_days = int(source_days) if source_days is not None and int(source_days) > 0 else None
    except Exception:
        normalized_source_days = None
    strict_default_mode = (
        normalized_source_tier is None
        and normalized_min_confidence_weight is None
        and normalized_source_days is None
    )
    # Do not override to explicit values here. The pipeline applies the same
    # defaults (min_conf=1, source_days=365) internally when params are None,
    # and the cache key must stay None/None to match what the generation phase
    # stores. Overriding here creates a different cache key and causes a
    # permanent cache miss even when data exists.

    def _running_notebook_placeholder(actor: dict[str, object]) -> dict[str, object]:
        actor_id_value = str(actor.get('id') or '')
        actor_message = str(actor.get('notebook_message') or '').strip()
        actor_status = str(actor.get('notebook_status') or 'running')
        return notebook_service.finalize_notebook_contract_core({
            'actor': {
                'id': actor_id_value,
                'display_name': str(actor.get('display_name') or actor_id_value),
                'is_tracked': bool(actor.get('is_tracked')),
                'notebook_status': actor_status,
                'notebook_message': actor_message or 'Refreshing notebook...',
                'notebook_updated_at': str(actor.get('notebook_updated_at') or actor.get('created_at') or ''),
                'last_refresh_duration_ms': actor.get('last_refresh_duration_ms'),
                'last_refresh_sources_processed': actor.get('last_refresh_sources_processed'),
            },
            'counts': {'sources': 0},
            'kpis': {
                'activity_30d': 'n/a',
                'new_techniques_30d': 'n/a',
                'last_verified_update': 'n/a',
            },
            'recent_change_summary': {'new_reports': '0', 'targets': 'Pending refresh', 'damage': 'Pending refresh'},
            'timeline_graph': [],
            'priority_questions': [],
            'top_change_signals': [],
            'recent_activity_highlights': [],
            'recent_activity_synthesis': [],
            'top_techniques': [],
            'emerging_techniques': [],
            'ioc_items': [],
            'sources': [],
            'requirements': [],
            'requirements_context': {
                'priority_mode': 'Operational',
                'org_context': '',
            },
            'source_quality_filters': {
                'source_tier': normalized_source_tier or '',
                'min_confidence_weight': normalized_min_confidence_weight if normalized_min_confidence_weight is not None else '',
                'source_days': normalized_source_days if normalized_source_days is not None else '',
                'applied_sources': 0,
                'total_sources': 0,
                'filtered_out_sources': 0,
            },
            'actor_profile_summary': '',
            'actor_profile_group_name': 'Unknown',
            'actor_profile_source_url': '#',
            'actor_profile_source_label': 'Unknown',
            'actor_created_date': '',
            'timeline_compact_rows': [],
            'timeline_window_label': '',
        })

    def _idle_notebook_placeholder(actor: dict[str, object], message: str) -> dict[str, object]:
        actor_id_value = str(actor.get('id') or '')
        actor_status = str(actor.get('notebook_status') or 'idle')
        return notebook_service.finalize_notebook_contract_core({
            'actor': {
                'id': actor_id_value,
                'display_name': str(actor.get('display_name') or actor_id_value),
                'is_tracked': bool(actor.get('is_tracked')),
                'notebook_status': actor_status,
                'notebook_message': message,
                'notebook_updated_at': str(actor.get('notebook_updated_at') or actor.get('created_at') or ''),
                'last_refresh_duration_ms': actor.get('last_refresh_duration_ms'),
                'last_refresh_sources_processed': actor.get('last_refresh_sources_processed'),
            },
            'counts': {'sources': 0},
            'kpis': {
                'activity_30d': 'n/a',
                'new_techniques_30d': 'n/a',
                'last_verified_update': 'n/a',
            },
            'recent_change_summary': {'new_reports': '0', 'targets': 'Preparing notebook', 'damage': 'Preparing notebook'},
            'timeline_graph': [],
            'priority_questions': [],
            'top_change_signals': [],
            'recent_activity_highlights': [],
            'recent_activity_synthesis': [],
            'top_techniques': [],
            'emerging_techniques': [],
            'ioc_items': [],
            'sources': [],
            'requirements': [],
            'requirements_context': {
                'priority_mode': 'Operational',
                'org_context': '',
            },
            'source_quality_filters': {
                'source_tier': normalized_source_tier or '',
                'min_confidence_weight': normalized_min_confidence_weight if normalized_min_confidence_weight is not None else '',
                'source_days': normalized_source_days if normalized_source_days is not None else '',
                'applied_sources': 0,
                'total_sources': 0,
                'filtered_out_sources': 0,
            },
            'actor_profile_summary': '',
            'actor_profile_group_name': 'Unknown',
            'actor_profile_source_url': '#',
            'actor_profile_source_label': 'Unknown',
            'actor_created_date': '',
            'timeline_compact_rows': [],
            'timeline_window_label': '',
        })

    if selected_actor_id is not None:
        selected_actor_summary = actors_by_id.get(str(selected_actor_id), {})
        selected_actor_status = str(selected_actor_summary.get('notebook_status') or '').strip().lower()
        if selected_actor_status == 'running':
            if not notice:
                notice = str(selected_actor_summary.get('notebook_message') or '').strip() or 'Notebook refresh is running.'
        try:
            notebook = _fetch_actor_notebook(
                selected_actor_id,
                source_tier=normalized_source_tier,
                min_confidence_weight=normalized_min_confidence_weight,
                source_days=normalized_source_days,
                build_on_cache_miss=False,
                allow_stale_cache=True,
            )
            if selected_actor_status == 'running' and isinstance(notebook, dict):
                actor_meta = notebook.get('actor', {}) if isinstance(notebook.get('actor'), dict) else {}
                actor_meta = dict(actor_meta)
                actor_meta['notebook_status'] = 'running'
                actor_meta['notebook_message'] = (
                    str(selected_actor_summary.get('notebook_message') or '').strip()
                    or str(actor_meta.get('notebook_message') or '').strip()
                    or 'Refreshing notebook...'
                )
                notebook['actor'] = actor_meta
            if isinstance(notebook, dict) and bool(notebook.get('cache_miss')):
                notebook = _idle_notebook_placeholder(
                    selected_actor_summary,
                    'Preparing notebook snapshot in the background. Refresh will appear automatically.',
                )
                if not notice:
                    notice = 'Preparing notebook snapshot in the background.'
            actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
            is_tracked = bool(actor_meta.get('is_tracked'))
            status = str(actor_meta.get('notebook_status') or '')
            source_count = int(notebook.get('counts', {}).get('sources', 0)) if isinstance(notebook, dict) else 0
            if bool(notebook.get('snapshot_stale')):
                if not notice:
                    notice = 'Showing the last completed snapshot while new synthesis runs in the background.'
            needs_bootstrap = source_count == 0
            if is_tracked and needs_bootstrap and status != 'running':
                if not notice:
                    notice = 'Notebook has no sources yet. Start refresh to collect sources.'
            elif is_tracked and status == 'running':
                running_since_raw = str(actor_meta.get('notebook_updated_at') or actor_meta.get('created_at') or '').strip()
                running_stale = False
                if running_since_raw:
                    try:
                        running_since = datetime.fromisoformat(running_since_raw.replace('Z', '+00:00'))
                        if running_since.tzinfo is None:
                            running_since = running_since.replace(tzinfo=timezone.utc)
                        running_age = datetime.now(timezone.utc) - running_since.astimezone(timezone.utc)
                        running_stale = running_age >= timedelta(minutes=max(5, _running_stale_recovery_minutes))
                    except Exception:
                        running_stale = True
                else:
                    running_stale = True
                if running_stale:
                    if not notice:
                        notice = 'Detected a stalled refresh state. Trigger a manual refresh.'
            elif is_tracked and status != 'running':
                last_run_raw = str(actor_meta.get('notebook_updated_at') or actor_meta.get('created_at') or '').strip()
                should_trigger = False
                if not last_run_raw:
                    should_trigger = True
                else:
                    try:
                        parsed = datetime.fromisoformat(last_run_raw.replace('Z', '+00:00'))
                        if parsed.tzinfo is None:
                            parsed = parsed.replace(tzinfo=timezone.utc)
                        age = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
                        should_trigger = age >= timedelta(minutes=max(0, _page_refresh_auto_trigger_minutes))
                    except Exception:
                        should_trigger = True
                if should_trigger:
                    if not notice:
                        notice = 'Notebook refresh is due. Trigger refresh when ready.'
            if _get_actor_refresh_stats is not None:
                try:
                    refresh_stats = _get_actor_refresh_stats(selected_actor_id)
                except Exception:
                    refresh_stats = None
        except Exception as exc:
            if 'database is locked' in str(exc).lower():
                LOGGER.warning('dashboard_fetch_locked actor_id=%s', selected_actor_id)
            notebook = _running_notebook_placeholder(selected_actor_summary)
            if not notice:
                notice = 'Unable to load notebook for selected actor.'

    try:
        ollama_status = _get_ollama_status()
    except Exception:
        ollama_status = {'available': False, 'base_url': '', 'model': ''}
    notebook_health = {
        'state': 'ready',
        'message': 'Notebook is ready.',
        'last_refresh_duration': 'n/a',
        'last_refresh_sources': 'n/a',
    }
    if notebook is not None:
        actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        status = str(actor_meta.get('notebook_status') or 'idle')
        source_count = int(notebook.get('counts', {}).get('sources', 0)) if isinstance(notebook, dict) else 0
        status_message = str(actor_meta.get('notebook_message') or '').strip()
        refresh_duration_ms_raw = actor_meta.get('last_refresh_duration_ms')
        refresh_sources_raw = actor_meta.get('last_refresh_sources_processed')
        try:
            refresh_duration_ms = int(refresh_duration_ms_raw) if refresh_duration_ms_raw is not None else None
        except Exception:
            refresh_duration_ms = None
        try:
            refresh_sources = int(refresh_sources_raw) if refresh_sources_raw is not None else None
        except Exception:
            refresh_sources = None
        notebook_health['last_refresh_duration'] = _format_duration_ms(refresh_duration_ms)
        notebook_health['last_refresh_sources'] = str(refresh_sources) if refresh_sources is not None else 'n/a'
        if status == 'running':
            notebook_health = {'state': 'running', 'message': status_message or 'Refreshing notebook...'}
        elif status == 'error':
            notebook_health = {'state': 'error', 'message': 'Refresh failed.'}
        elif source_count == 0:
            notebook_health = {'state': 'idle', 'message': 'Needs sources.'}
        elif not bool(ollama_status.get('available')):
            notebook_health = {'state': 'warning', 'message': 'LLM offline.'}
        else:
            notebook_health = {'state': 'ready', 'message': 'Notebook is ready.'}
        notebook_health['last_refresh_duration'] = _format_duration_ms(refresh_duration_ms)
        notebook_health['last_refresh_sources'] = str(refresh_sources) if refresh_sources is not None else 'n/a'

    bastion_nudges = notebook_service.compute_bastion_nudges_core(
        notebook if isinstance(notebook, dict) else None
    )

    return _templates.TemplateResponse(
        request,
        'index.html',
        {
            'actors': tracked_actors,
            'all_actors': actors_all,
            'selected_actor_id': selected_actor_id,
            'notebook': notebook,
            'source_quality_filters': {
                'source_tier': normalized_source_tier or '',
                'min_confidence_weight': str(normalized_min_confidence_weight) if normalized_min_confidence_weight is not None else '',
                'source_days': str(normalized_source_days) if normalized_source_days is not None else '',
                'strict_default_mode': '1' if strict_default_mode else '0',
            },
            'notice': notice,
            'ollama_status': ollama_status,
            'notebook_health': notebook_health,
            'refresh_stats': refresh_stats,
            'duplicate_actor_groups': duplicate_actor_groups,
            'bastion_nudges': bastion_nudges,
        },
    )


def create_dashboard_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    @router.get('/', response_class=HTMLResponse)
    def dashboard_root(
        request: Request,
        background_tasks: BackgroundTasks,
        actor_id: str | None = None,
        notice: str | None = None,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ):
        return render_dashboard_root(
            request=request,
            background_tasks=background_tasks,
            actor_id=actor_id,
            notice=notice,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            deps=deps,
        )

    return router
