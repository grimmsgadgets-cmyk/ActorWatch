import sqlite3

from pipelines.generation_runner import run_actor_generation_core


def _setup_actor_db(db_path: str, actor_id: str) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            '''
            CREATE TABLE actor_profiles (
                id TEXT PRIMARY KEY,
                last_refresh_duration_ms INTEGER,
                last_refresh_sources_processed INTEGER,
                auto_refresh_last_status TEXT
            )
            '''
        )
        connection.execute(
            '''
            INSERT INTO actor_profiles (
                id, last_refresh_duration_ms, last_refresh_sources_processed, auto_refresh_last_status
            ) VALUES (?, 0, 0, 'idle')
            ''',
            (actor_id,),
        )
        connection.commit()


def test_generation_runner_uses_interactive_import_for_manual_trigger(tmp_path):
    db_path = str(tmp_path / 'generation.db')
    actor_id = 'actor-manual'
    _setup_actor_db(db_path, actor_id)
    captured: dict[str, object] = {}

    def _import(actor: str, **kwargs):
        captured['actor_id'] = actor
        captured['kwargs'] = dict(kwargs)
        return 1

    run_actor_generation_core(
        actor_id,
        db_path=db_path,
        deps={
            'set_actor_notebook_status': lambda *_args, **_kwargs: None,
            'import_default_feeds_for_actor': _import,
            'build_notebook': lambda *_args, **_kwargs: None,
            'trigger_type': 'manual_refresh',
            'interactive_feed_import_max_seconds': 22,
            'interactive_high_signal_target': 2,
        },
    )

    kwargs = captured.get('kwargs', {})
    assert captured.get('actor_id') == actor_id
    assert kwargs.get('import_mode') == 'interactive'
    assert kwargs.get('max_seconds') == 22
    assert kwargs.get('high_signal_target') == 2


def test_generation_runner_uses_background_import_for_auto_trigger(tmp_path):
    db_path = str(tmp_path / 'generation.db')
    actor_id = 'actor-auto'
    _setup_actor_db(db_path, actor_id)
    captured: dict[str, object] = {}

    def _import(actor: str, **kwargs):
        captured['actor_id'] = actor
        captured['kwargs'] = dict(kwargs)
        return 0

    run_actor_generation_core(
        actor_id,
        db_path=db_path,
        deps={
            'set_actor_notebook_status': lambda *_args, **_kwargs: None,
            'import_default_feeds_for_actor': _import,
            'build_notebook': lambda *_args, **_kwargs: None,
            'trigger_type': 'auto_refresh',
        },
    )

    kwargs = captured.get('kwargs', {})
    assert captured.get('actor_id') == actor_id
    assert kwargs.get('import_mode') == 'background'
    assert 'max_seconds' not in kwargs


def test_generation_runner_stays_running_when_llm_enrichment_queued(tmp_path):
    """When LLM enrichment is enqueued, status should stay 'running' so the UI
    doesn't show a placeholder while the LLM worker is still building the cache."""
    db_path = str(tmp_path / 'generation.db')
    actor_id = 'actor-llm'
    _setup_actor_db(db_path, actor_id)

    status_calls: list[tuple[str, str, str]] = []
    enrichment_calls: list[str] = []

    def _import(actor: str, **kwargs):
        return 3  # non-zero → skip_heavy_recompute=False

    run_actor_generation_core(
        actor_id,
        db_path=db_path,
        deps={
            'set_actor_notebook_status': lambda a, s, m: status_calls.append((a, s, m)),
            'import_default_feeds_for_actor': _import,
            'build_notebook': lambda *_args, **_kwargs: None,
            'enqueue_actor_llm_enrichment': lambda a, **_kw: enrichment_calls.append(a),
            'trigger_type': 'manual_refresh',
        },
    )

    # The final status call before returning should be 'running', not 'ready'
    final_status = status_calls[-1][1] if status_calls else None
    assert final_status == 'running', f'Expected running, got {final_status!r}'
    assert enrichment_calls == [actor_id]


def test_generation_runner_sets_ready_when_no_llm_enrichment(tmp_path):
    """When no LLM enrichment is configured, status should be set to 'ready'
    after the deterministic build completes."""
    db_path = str(tmp_path / 'generation.db')
    actor_id = 'actor-no-llm'
    _setup_actor_db(db_path, actor_id)

    status_calls: list[tuple[str, str, str]] = []

    def _import(actor: str, **kwargs):
        return 2

    run_actor_generation_core(
        actor_id,
        db_path=db_path,
        deps={
            'set_actor_notebook_status': lambda a, s, m: status_calls.append((a, s, m)),
            'import_default_feeds_for_actor': _import,
            'build_notebook': lambda *_args, **_kwargs: None,
            # no enqueue_actor_llm_enrichment
            'trigger_type': 'manual_refresh',
        },
    )

    final_status = status_calls[-1][1] if status_calls else None
    assert final_status == 'ready', f'Expected ready, got {final_status!r}'


def test_generation_runner_sets_ready_when_auto_refresh_with_no_imports(tmp_path):
    """Auto-refresh with no new imports triggers skip_heavy_recompute=True,
    so LLM enrichment is skipped even if callable and status becomes 'ready'."""
    db_path = str(tmp_path / 'generation.db')
    actor_id = 'actor-skip'
    _setup_actor_db(db_path, actor_id)

    status_calls: list[tuple[str, str, str]] = []
    enrichment_calls: list[str] = []

    def _import(actor: str, **kwargs):
        return 0  # zero imports → skip_heavy_recompute=True for auto_refresh

    run_actor_generation_core(
        actor_id,
        db_path=db_path,
        deps={
            'set_actor_notebook_status': lambda a, s, m: status_calls.append((a, s, m)),
            'import_default_feeds_for_actor': _import,
            'build_notebook': lambda *_args, **_kwargs: None,
            'enqueue_actor_llm_enrichment': lambda a, **_kw: enrichment_calls.append(a),
            'trigger_type': 'auto_refresh',
        },
    )

    final_status = status_calls[-1][1] if status_calls else None
    assert final_status == 'ready', f'Expected ready, got {final_status!r}'
    # LLM enrichment must NOT be enqueued when skipping heavy recompute
    assert enrichment_calls == []
