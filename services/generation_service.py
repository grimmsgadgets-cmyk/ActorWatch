import time
import inspect
from queue import Empty, PriorityQueue, Queue
from threading import Lock, Thread


_ACTOR_GENERATION_RUNNING: set[str] = set()
_ACTOR_GENERATION_LOCK = Lock()
_ACTOR_LLM_ENRICH_RUNNING: set[str] = set()
_ACTOR_LLM_ENRICH_LOCK = Lock()
_GENERATION_QUEUE: PriorityQueue[tuple[int, int, str, str, str]] = PriorityQueue()
_LLM_ENRICH_QUEUE: Queue[tuple[str, str]] = Queue()
_GENERATION_ENQUEUED: set[str] = set()
_LLM_ENRICH_ENQUEUED: set[str] = set()
_QUEUE_LOCK = Lock()
_GENERATION_SEQ = 0
_WORKERS_STARTED = False
_WORKERS_LOCK = Lock()


def mark_actor_generation_started_core(actor_id: str) -> bool:
    with _ACTOR_GENERATION_LOCK:
        if actor_id in _ACTOR_GENERATION_RUNNING:
            return False
        _ACTOR_GENERATION_RUNNING.add(actor_id)
        return True


def mark_actor_generation_finished_core(actor_id: str) -> None:
    with _ACTOR_GENERATION_LOCK:
        _ACTOR_GENERATION_RUNNING.discard(actor_id)


def running_actor_ids_snapshot_core() -> set[str]:
    with _ACTOR_GENERATION_LOCK:
        return set(_ACTOR_GENERATION_RUNNING)


def queue_snapshot_core() -> dict[str, int]:
    with _ACTOR_GENERATION_LOCK:
        running = len(_ACTOR_GENERATION_RUNNING)
    with _ACTOR_LLM_ENRICH_LOCK:
        llm_running = len(_ACTOR_LLM_ENRICH_RUNNING)
    with _QUEUE_LOCK:
        queued = len(_GENERATION_ENQUEUED)
        llm_queued = len(_LLM_ENRICH_ENQUEUED)
    return {
        'generation_queued': queued,
        'generation_running': running,
        'llm_queued': llm_queued,
        'llm_running': llm_running,
    }


def mark_actor_llm_enrichment_started_core(actor_id: str) -> bool:
    with _ACTOR_LLM_ENRICH_LOCK:
        if actor_id in _ACTOR_LLM_ENRICH_RUNNING:
            return False
        _ACTOR_LLM_ENRICH_RUNNING.add(actor_id)
        return True


def mark_actor_llm_enrichment_finished_core(actor_id: str) -> None:
    with _ACTOR_LLM_ENRICH_LOCK:
        _ACTOR_LLM_ENRICH_RUNNING.discard(actor_id)


def run_actor_llm_enrichment_core(*, actor_id: str, deps: dict[str, object]) -> None:
    _mark_started = deps['mark_started']
    _mark_finished = deps['mark_finished']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _refresh_actor_notebook_uncached = deps['refresh_actor_notebook_uncached']
    _max_attempts = max(1, int(deps.get('max_attempts', 2)))
    _retry_sleep_seconds = max(1.0, float(deps.get('retry_sleep_seconds', 2.0)))
    _job_id = str(deps.get('job_id') or '')
    _start_phase = deps.get('start_phase')
    _finish_phase = deps.get('finish_phase')

    if not _mark_started(actor_id):
        return
    try:
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Building AI summary...',
        )
        notebook: dict[str, object] | None = None
        for attempt in range(1, _max_attempts + 1):
            phase_id: str | None = None
            phase_started_at = time.perf_counter()
            if callable(_start_phase):
                phase_id = str(
                    _start_phase(
                        actor_id=actor_id,
                        job_id=_job_id,
                        phase_key='llm_enrichment',
                        phase_label='AI Summary',
                        attempt=attempt,
                        message='Adding AI summary in the background...',
                    )
                )
            notebook = _refresh_actor_notebook_uncached(actor_id)
            if not isinstance(notebook, dict):
                notebook = None
                if callable(_finish_phase) and phase_id:
                    _finish_phase(
                        phase_id=phase_id,
                        status='error',
                        message='AI summary attempt returned no data.',
                        error_detail='empty_notebook_payload',
                        duration_ms=int((time.perf_counter() - phase_started_at) * 1000),
                    )
                if attempt < _max_attempts:
                    time.sleep(_retry_sleep_seconds)
                continue
            degraded_change = bool(notebook.get('llm_change_signals_degraded'))
            degraded_recent = bool(notebook.get('llm_recent_synthesis_degraded'))
            if not degraded_change and not degraded_recent:
                _set_actor_notebook_status(
                    actor_id,
                    'ready',
                    'Notebook is ready and AI summary is up to date.',
                )
                if callable(_finish_phase) and phase_id:
                    _finish_phase(
                        phase_id=phase_id,
                        status='completed',
                        message='AI summary finished successfully.',
                        duration_ms=int((time.perf_counter() - phase_started_at) * 1000),
                    )
                return
            if callable(_finish_phase) and phase_id:
                _finish_phase(
                    phase_id=phase_id,
                    status='error',
                    message='AI summary attempt did not produce complete results.',
                    error_detail='llm_degraded_output',
                    duration_ms=int((time.perf_counter() - phase_started_at) * 1000),
                )
            if attempt < _max_attempts:
                time.sleep(_retry_sleep_seconds)
        _set_actor_notebook_status(
            actor_id,
            'ready',
            'Notebook is ready. AI summary is still loading; source-based summary is shown for now.',
        )
    except Exception:
        _set_actor_notebook_status(
            actor_id,
            'ready',
            'Notebook is ready. AI summary is temporarily unavailable; source-based summary is shown.',
        )
        if callable(_finish_phase) and locals().get('phase_id'):
            _finish_phase(
                phase_id=str(locals().get('phase_id')),
                status='error',
                message='AI summary failed.',
                error_detail='enrichment_exception',
                duration_ms=int((time.perf_counter() - locals().get('phase_started_at', time.perf_counter())) * 1000),
            )
    finally:
        _mark_finished(actor_id)


def run_actor_generation_core(*, actor_id: str, deps: dict[str, object]) -> None:
    _mark_started = deps['mark_started']
    _mark_finished = deps['mark_finished']
    _pipeline_run_actor_generation_core = deps['pipeline_run_actor_generation_core']
    _db_path = deps['db_path']
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _import_default_feeds_for_actor = deps['import_default_feeds_for_actor']
    _build_notebook = deps['build_notebook']
    _enqueue_actor_llm_enrichment = deps.get('enqueue_actor_llm_enrichment')
    _create_generation_job = deps.get('create_generation_job')
    _start_generation_phase = deps.get('start_generation_phase')
    _finish_generation_phase = deps.get('finish_generation_phase')
    _finalize_generation_job = deps.get('finalize_generation_job')
    _mark_generation_job_started = deps.get('mark_generation_job_started')
    _trigger_type = str(deps.get('trigger_type') or 'manual_refresh')
    _job_id = str(deps.get('job_id') or '')

    if not _mark_started(actor_id):
        return
    job_id = _job_id
    try:
        if job_id and callable(_mark_generation_job_started):
            _mark_generation_job_started(job_id=job_id)
        elif callable(_create_generation_job):
            job_id = str(_create_generation_job(actor_id=actor_id, trigger_type=_trigger_type))
        result = _pipeline_run_actor_generation_core(
            actor_id,
            db_path=_db_path(),
            deps={
                'set_actor_notebook_status': _set_actor_notebook_status,
                'import_default_feeds_for_actor': _import_default_feeds_for_actor,
                'build_notebook': _build_notebook,
                'enqueue_actor_llm_enrichment': _enqueue_actor_llm_enrichment,
                'job_id': job_id,
                'trigger_type': _trigger_type,
                'start_phase': _start_generation_phase,
                'finish_phase': _finish_generation_phase,
            },
        )
        if callable(_finalize_generation_job) and job_id:
            _finalize_generation_job(
                job_id=job_id,
                status='completed',
                imported_sources=int((result or {}).get('imported') or 0),
                duration_ms=int((result or {}).get('duration_ms') or 0),
                final_message=str((result or {}).get('message') or ''),
                error_message='',
            )
    except Exception as exc:
        if callable(_finalize_generation_job) and job_id:
            _finalize_generation_job(
                job_id=job_id,
                status='error',
                imported_sources=0,
                duration_ms=0,
                final_message='',
                error_message=str(exc),
            )
        raise
    finally:
        _mark_finished(actor_id)


def enqueue_actor_generation_core(*, actor_id: str, deps: dict[str, object]) -> bool:
    global _GENERATION_SEQ
    trigger_type = str(deps.get('trigger_type') or 'manual_refresh')
    job_id = str(deps.get('job_id') or '')
    priority = int(deps.get('priority') or (2 if trigger_type == 'auto_refresh' else 0))
    with _QUEUE_LOCK:
        if actor_id in _GENERATION_ENQUEUED:
            return False
        _GENERATION_ENQUEUED.add(actor_id)
        _GENERATION_SEQ += 1
        sequence = _GENERATION_SEQ
    _GENERATION_QUEUE.put((priority, sequence, actor_id, trigger_type, job_id))
    return True


def enqueue_actor_llm_enrichment_core(*, actor_id: str, deps: dict[str, object]) -> bool:
    job_id = str(deps.get('job_id') or '')
    with _QUEUE_LOCK:
        if actor_id in _LLM_ENRICH_ENQUEUED:
            return False
        _LLM_ENRICH_ENQUEUED.add(actor_id)
    _LLM_ENRICH_QUEUE.put((actor_id, job_id))
    return True


def start_generation_workers_core(*, deps: dict[str, object]) -> None:
    global _WORKERS_STARTED
    _run_actor_generation = deps['run_actor_generation']
    _run_actor_llm_enrichment = deps['run_actor_llm_enrichment']
    _stop_event = deps['stop_event']
    with _WORKERS_LOCK:
        if _WORKERS_STARTED:
            return
        _WORKERS_STARTED = True

    def _generation_worker() -> None:
        while not _stop_event.is_set():
            try:
                _priority, _seq, actor_id, trigger_type, job_id = _GENERATION_QUEUE.get(timeout=0.5)
            except Empty:
                continue
            try:
                # Tests may monkeypatch run_actor_generation with a simplified
                # callable that only accepts actor_id.
                try:
                    run_sig = inspect.signature(_run_actor_generation)
                    param_names = set(run_sig.parameters.keys())
                except (TypeError, ValueError):
                    param_names = {'actor_id', 'trigger_type', 'job_id'}

                if 'trigger_type' in param_names or 'job_id' in param_names:
                    _run_actor_generation(actor_id, trigger_type=trigger_type, job_id=job_id)
                else:
                    _run_actor_generation(actor_id)
            finally:
                with _QUEUE_LOCK:
                    _GENERATION_ENQUEUED.discard(actor_id)
                _GENERATION_QUEUE.task_done()

    def _llm_worker() -> None:
        while not _stop_event.is_set():
            try:
                actor_id, job_id = _LLM_ENRICH_QUEUE.get(timeout=0.5)
            except Empty:
                continue
            try:
                _run_actor_llm_enrichment(actor_id, job_id=job_id)
            finally:
                with _QUEUE_LOCK:
                    _LLM_ENRICH_ENQUEUED.discard(actor_id)
                _LLM_ENRICH_QUEUE.task_done()

    Thread(target=_generation_worker, daemon=True, name='actor-generation-worker').start()
    Thread(target=_llm_worker, daemon=True, name='actor-llm-worker').start()


def stop_generation_workers_core() -> None:
    global _WORKERS_STARTED
    with _WORKERS_LOCK:
        _WORKERS_STARTED = False
