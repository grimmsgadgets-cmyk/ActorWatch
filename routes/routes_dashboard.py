from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse
from datetime import datetime


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
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _enqueue_actor_generation = deps.get('enqueue_actor_generation', deps['run_actor_generation'])
    _get_ollama_status = deps['get_ollama_status']
    _format_duration_ms = deps['format_duration_ms']
    _templates = deps['templates']

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
    if strict_default_mode:
        normalized_min_confidence_weight = 3
        normalized_source_days = 90

    if selected_actor_id is not None:
        try:
            notebook = _fetch_actor_notebook(
                selected_actor_id,
                source_tier=normalized_source_tier,
                min_confidence_weight=normalized_min_confidence_weight,
                source_days=normalized_source_days,
            )
            actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
            is_tracked = bool(actor_meta.get('is_tracked'))
            status = str(actor_meta.get('notebook_status') or '')
            source_count = int(notebook.get('counts', {}).get('sources', 0)) if isinstance(notebook, dict) else 0
            needs_bootstrap = source_count == 0
            if is_tracked and needs_bootstrap and status != 'running':
                _set_actor_notebook_status(
                    selected_actor_id,
                    'running',
                    'Collecting actor-specific sources and rebuilding recent activity...',
                )
                _enqueue_actor_generation(selected_actor_id)
                actor_meta['notebook_status'] = 'running'
                actor_meta['notebook_message'] = 'Collecting actor-specific sources and rebuilding recent activity...'
                if not notice:
                    notice = 'Collecting actor-specific sources in the background...'
        except Exception:
            notebook = None
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
    ) -> HTMLResponse:
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
