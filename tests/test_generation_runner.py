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
