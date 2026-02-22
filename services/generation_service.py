from threading import Lock, Thread


_ACTOR_GENERATION_RUNNING: set[str] = set()
_ACTOR_GENERATION_LOCK = Lock()


def mark_actor_generation_started_core(actor_id: str) -> bool:
    with _ACTOR_GENERATION_LOCK:
        if actor_id in _ACTOR_GENERATION_RUNNING:
            return False
        _ACTOR_GENERATION_RUNNING.add(actor_id)
        return True


def mark_actor_generation_finished_core(actor_id: str) -> None:
    with _ACTOR_GENERATION_LOCK:
        _ACTOR_GENERATION_RUNNING.discard(actor_id)


def run_actor_generation_core(*, actor_id: str, deps: dict[str, object]) -> None:
    _mark_started = deps['mark_started']
    _mark_finished = deps['mark_finished']
    _pipeline_run_actor_generation_core = deps['pipeline_run_actor_generation_core']
    _db_path = deps['db_path']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _import_default_feeds_for_actor = deps['import_default_feeds_for_actor']
    _build_notebook = deps['build_notebook']

    if not _mark_started(actor_id):
        return
    try:
        _pipeline_run_actor_generation_core(
            actor_id,
            db_path=_db_path(),
            deps={
                'set_actor_notebook_status': _set_actor_notebook_status,
                'import_default_feeds_for_actor': _import_default_feeds_for_actor,
                'build_notebook': _build_notebook,
            },
        )
    finally:
        _mark_finished(actor_id)


def enqueue_actor_generation_core(*, actor_id: str, deps: dict[str, object]) -> None:
    _run_actor_generation = deps['run_actor_generation']
    worker = Thread(
        target=_run_actor_generation,
        args=(actor_id,),
        daemon=True,
        name=f'actor-generation-{actor_id[:8]}',
    )
    worker.start()
