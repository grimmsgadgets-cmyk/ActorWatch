import html

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def create_ui_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _create_actor_profile = deps['create_actor_profile']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _run_actor_generation = deps['run_actor_generation']
    _list_actor_profiles = deps['list_actor_profiles']

    @router.post('/actors/new')
    async def create_actor_ui(request: Request, background_tasks: BackgroundTasks) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        display_name = str(form_data.get('display_name', '')).strip()
        scope_statement = None
        is_tracked = True
        if not display_name:
            raise HTTPException(status_code=400, detail='display_name is required')
        actor = _create_actor_profile(display_name, scope_statement, is_tracked=is_tracked)
        _set_actor_notebook_status(
            actor['id'],
            'running',
            'Actor added. Importing sources and generating notebook sections...',
        )
        background_tasks.add_task(_run_actor_generation, actor['id'])
        return RedirectResponse(
            url=f'/?actor_id={actor["id"]}&notice=Tracking+started.+Building+notebook+in+the+background.',
            status_code=303,
        )

    @router.get('/actors/ui', response_class=HTMLResponse)
    def actors_ui() -> str:
        actor_items = ''.join(
            (
                f'<li>{html.escape(str(actor["id"]), quote=True)} - '
                f'{html.escape(str(actor["display_name"]), quote=True)}</li>'
            )
            for actor in _list_actor_profiles()
        )
        return (
            '<!doctype html>'
            '<html><body>'
            '<h1>Actors</h1>'
            '<form method="post" action="/actors">'
            '<label for="display_name">Display Name</label>'
            '<input id="display_name" name="display_name" required />'
            '<button type="submit">Create</button>'
            '</form>'
            '<ul>'
            f'{actor_items}'
            '</ul>'
            '</body></html>'
        )

    return router
