import sqlite3
from datetime import datetime, timedelta, timezone

import app as app_module
from services import db_schema_service


def _init_db(path: str) -> None:
    with sqlite3.connect(path) as connection:
        db_schema_service.ensure_schema(connection)


def test_run_tracked_actor_auto_refresh_once_queues_only_stale(monkeypatch, tmp_path):
    db_path = str(tmp_path / 'auto_refresh.db')
    _init_db(db_path)
    now_utc = datetime.now(timezone.utc)
    stale = (now_utc - timedelta(hours=50)).isoformat()
    fresh = (now_utc - timedelta(hours=2)).isoformat()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (
                id, display_name, canonical_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, auto_refresh_last_run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('actor-stale', 'Akira', 'akira', None, stale, 1, 'ready', 'ok', stale),
        )
        connection.execute(
            '''
            INSERT INTO actor_profiles (
                id, display_name, canonical_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, auto_refresh_last_run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('actor-fresh', 'Qilin', 'qilin', None, fresh, 1, 'ready', 'ok', fresh),
        )
        connection.execute(
            '''
            INSERT INTO actor_profiles (
                id, display_name, canonical_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, auto_refresh_last_run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('actor-running', 'Spider', 'spider', None, stale, 1, 'running', 'busy', stale),
        )
        connection.commit()

    queued: list[str] = []
    monkeypatch.setattr(app_module, 'DB_PATH', db_path)
    monkeypatch.setattr(app_module, 'enqueue_actor_generation', lambda actor_id, **_kwargs: queued.append(actor_id))
    monkeypatch.setattr(app_module, 'AUTO_REFRESH_MIN_INTERVAL_HOURS', 24)

    queued_count = app_module._run_tracked_actor_auto_refresh_once(limit=3)

    assert queued_count == 1
    assert queued == ['actor-stale']
    with sqlite3.connect(db_path) as connection:
        status = connection.execute(
            "SELECT auto_refresh_last_status FROM actor_profiles WHERE id = 'actor-stale'"
        ).fetchone()[0]
    assert status == 'queued'


def test_get_actor_refresh_stats_aggregates_state(monkeypatch, tmp_path):
    db_path = str(tmp_path / 'refresh_stats.db')
    _init_db(db_path)
    now_utc = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (
                id, display_name, canonical_name, scope_statement, created_at, is_tracked,
                notebook_status, notebook_message, auto_refresh_last_run_at, auto_refresh_last_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('actor-1', 'Akira', 'akira', None, now_utc, 1, 'ready', 'ok', now_utc, 'completed'),
        )
        connection.execute(
            '''
            INSERT INTO actor_feed_state (
                actor_id, feed_name, feed_url, last_checked_at, last_success_at,
                last_imported_count, total_imported, consecutive_failures, total_failures, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'actor-1',
                'Primary Feed',
                'https://example.com/feed.xml',
                now_utc,
                now_utc,
                2,
                10,
                3,
                5,
                'timeout',
            ),
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, retrieved_at, pasted_text, confidence_weight
            ) VALUES
            ('src-1', 'actor-1', 'Example', 'https://example.com/1', ?, ?, 'text', 3),
            ('src-2', 'actor-1', 'Example', 'https://example.com/2', ?, ?, 'text', 1)
            ''',
            (now_utc, now_utc, now_utc, now_utc),
        )
        connection.commit()

    monkeypatch.setattr(app_module, 'DB_PATH', db_path)
    result = app_module.get_actor_refresh_stats('actor-1')

    assert result['actor_name'] == 'Akira'
    assert result['feed_state']['total_feeds'] == 1
    assert result['feed_state']['backoff_feeds'] == 1
    assert result['source_state']['total_sources'] == 2
    assert result['source_state']['high_confidence_sources'] == 1
