import sqlite3
import time


def run_actor_generation_core(
    actor_id: str,
    *,
    db_path: str,
    deps: dict[str, object],
) -> dict[str, object]:
    _set_actor_notebook_status = deps['set_actor_notebook_status']
    _import_default_feeds_for_actor = deps['import_default_feeds_for_actor']
    _build_notebook = deps['build_notebook']
    _enqueue_actor_llm_enrichment = deps.get('enqueue_actor_llm_enrichment')
    _job_id = str(deps.get('job_id') or '')
    _trigger_type = str(deps.get('trigger_type') or 'manual_refresh')
    _interactive_feed_import_max_seconds = int(deps.get('interactive_feed_import_max_seconds', 25) or 25)
    _interactive_high_signal_target = int(deps.get('interactive_high_signal_target', 2) or 2)
    _start_phase = deps.get('start_phase')
    _finish_phase = deps.get('finish_phase')

    def _phase_start(*, key: str, label: str, message: str, attempt: int = 1) -> str | None:
        if not callable(_start_phase):
            return None
        return str(
            _start_phase(
                actor_id=actor_id,
                job_id=_job_id,
                phase_key=key,
                phase_label=label,
                attempt=attempt,
                message=message,
            )
        )

    def _phase_finish(phase_id: str | None, *, status: str, message: str = '', error_detail: str = '', duration_ms: int | None = None) -> None:
        if not phase_id or not callable(_finish_phase):
            return
        _finish_phase(
            phase_id=phase_id,
            status=status,
            message=message,
            error_detail=error_detail,
            duration_ms=duration_ms,
        )

    started_at = time.perf_counter()
    try:
        phase_started_at = time.perf_counter()
        source_phase_id = _phase_start(
            key='source_collection',
            label='Source Updates',
            message='Checking trusted sources for new updates...',
        )
        _set_actor_notebook_status(
            actor_id,
            'running',
            'Checking trusted sources for new updates...',
        )
        is_interactive_trigger = _trigger_type in {'manual_refresh', 'page_load', 'manual'}
        import_kwargs: dict[str, object] = {}
        if is_interactive_trigger:
            import_kwargs = {
                'max_seconds': max(10, int(_interactive_feed_import_max_seconds)),
                'import_mode': 'interactive',
                'high_signal_target': max(1, int(_interactive_high_signal_target)),
            }
        else:
            import_kwargs = {'import_mode': 'background'}
        try:
            imported = int(_import_default_feeds_for_actor(actor_id, **import_kwargs))
        except TypeError:
            imported = int(_import_default_feeds_for_actor(actor_id))
        _phase_finish(
            source_phase_id,
            status='completed',
            message=f'Checked sources and imported {imported} update(s).',
            duration_ms=int((time.perf_counter() - phase_started_at) * 1000),
        )
        skip_heavy_recompute = imported == 0 and _trigger_type == 'auto_refresh'
        phase_started_at = time.perf_counter()
        deterministic_phase_id = _phase_start(
            key='deterministic_build',
            label='Source-Based Notebook',
            message='Updating notebook from source evidence...',
        )
        if skip_heavy_recompute:
            _set_actor_notebook_status(
                actor_id,
                'running',
                'No new source updates found. Running quick consistency check...',
            )
            _build_notebook(actor_id, generate_questions=False, rebuild_timeline=False)
        else:
            _set_actor_notebook_status(
                actor_id,
                'running',
                f'Sources collected ({imported}). Building timeline preview...',
            )
            _build_notebook(actor_id, generate_questions=False, rebuild_timeline=True)
            _set_actor_notebook_status(
                actor_id,
                'running',
                'Timeline ready. Generating question threads and guidance...',
            )
            _build_notebook(actor_id, generate_questions=True, rebuild_timeline=False)
        _phase_finish(
            deterministic_phase_id,
            status='completed',
            message='Notebook sections are ready.',
            duration_ms=int((time.perf_counter() - phase_started_at) * 1000),
        )
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                '''
                UPDATE actor_profiles
                SET last_refresh_duration_ms = ?,
                    last_refresh_sources_processed = ?,
                    auto_refresh_last_status = 'completed'
                WHERE id = ?
                ''',
                (elapsed_ms, imported, actor_id),
            )
            connection.commit()
        if callable(_enqueue_actor_llm_enrichment) and not skip_heavy_recompute:
            # Stay 'running' — the LLM worker will write notebook_cache and then
            # set status to 'ready'. Marking 'ready' here causes the UI to show
            # a cache-miss placeholder while the LLM is still working.
            _set_actor_notebook_status(
                actor_id,
                'running',
                f'Sources collected ({imported} update(s)). Building AI summary...',
            )
            _enqueue_actor_llm_enrichment(actor_id, job_id=_job_id)
        else:
            _set_actor_notebook_status(
                actor_id,
                'ready',
                f'Notebook is ready with source-based analysis. Imported {imported} source update(s).',
            )
        return {
            'success': True,
            'imported': int(imported),
            'duration_ms': int(elapsed_ms),
            'message': f'Notebook is ready with source-based analysis. Imported {imported} source update(s).',
        }
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        _phase_finish(
            locals().get('source_phase_id'),
            status='error',
            message='Source collection failed.',
            error_detail=str(exc),
            duration_ms=int((time.perf_counter() - locals().get('phase_started_at', started_at)) * 1000),
        )
        _phase_finish(
            locals().get('deterministic_phase_id'),
            status='error',
            message='Source-based notebook build failed.',
            error_detail=str(exc),
            duration_ms=int((time.perf_counter() - locals().get('phase_started_at', started_at)) * 1000),
        )
        try:
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    '''
                    UPDATE actor_profiles
                    SET last_refresh_duration_ms = ?,
                        last_refresh_sources_processed = ?,
                        auto_refresh_last_status = 'error'
                    WHERE id = ?
                    ''',
                    (elapsed_ms, 0, actor_id),
                )
                connection.commit()
        except Exception:
            pass
        _set_actor_notebook_status(actor_id, 'error', f'Notebook generation failed: {exc}')
        return {
            'success': False,
            'imported': 0,
            'duration_ms': int(elapsed_ms),
            'message': f'Notebook generation failed: {exc}',
            'error': str(exc),
        }
