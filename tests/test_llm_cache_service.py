import hashlib
import sqlite3
from datetime import datetime, timezone

import services.db_schema_service as db_schema_service
import services.llm_cache_service as llm_cache_service


def _setup_db(tmp_path):
    db_path = str(tmp_path / 'llm_cache_test.db')
    with sqlite3.connect(db_path) as connection:
        db_schema_service.ensure_schema(connection)
    return db_path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_llm_cache_roundtrip_and_saved_time_stats(tmp_path):
    db_path = _setup_db(tmp_path)
    deps = {'db_path': lambda: db_path, 'utc_now_iso': _utc_now_iso}
    actor_key = llm_cache_service.actor_key_core('Example Actor')
    fingerprint = llm_cache_service.input_fingerprint_core(
        {'x': 1, 'y': ['a', 'b']},
        deps={'sha256': hashlib.sha256},
    )

    llm_cache_service.save_cached_payload_core(
        actor_key=actor_key,
        cache_kind='recent_activity_synthesis',
        input_fingerprint=fingerprint,
        payload=[{'summary': 'cached'}],
        estimated_cost_ms=1300,
        deps=deps,
    )
    loaded = llm_cache_service.load_cached_payload_core(
        actor_key=actor_key,
        cache_kind='recent_activity_synthesis',
        input_fingerprint=fingerprint,
        deps=deps,
    )
    stats = llm_cache_service.cache_stats_for_actor_core(actor_key=actor_key, deps=deps)

    assert isinstance(loaded, list)
    assert loaded and loaded[0].get('summary') == 'cached'
    assert stats['cache_hits'] >= 1
    assert stats['saved_ms_total'] >= 1300


def test_llm_cache_prunes_rows_per_actor_kind(tmp_path):
    db_path = _setup_db(tmp_path)
    deps = {'db_path': lambda: db_path, 'utc_now_iso': _utc_now_iso}
    actor_key = llm_cache_service.actor_key_core('Prune Actor')

    for idx in range(3):
        llm_cache_service.save_cached_payload_core(
            actor_key=actor_key,
            cache_kind='review_change_signals',
            input_fingerprint=f'fp-{idx}',
            payload=[{'row': idx}],
            estimated_cost_ms=100,
            deps={**deps, 'max_rows_per_actor_kind': 1},
        )

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            '''
            SELECT COUNT(*)
            FROM llm_synthesis_cache
            WHERE actor_key = ? AND cache_kind = 'review_change_signals'
            ''',
            (actor_key,),
        ).fetchone()
    assert int(row[0] or 0) == 1

