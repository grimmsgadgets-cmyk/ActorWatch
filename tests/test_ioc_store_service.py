import sqlite3

import services.db_schema_service as db_schema_service
import services.ioc_store_service as ioc_store_service
import services.ioc_validation_service as ioc_validation_service


def test_upsert_ioc_item_tracks_seen_count_and_history(tmp_path):
    db_path = tmp_path / 'ioc.db'
    with sqlite3.connect(db_path) as connection:
        db_schema_service.ensure_schema(connection)

    now1 = '2026-02-23T00:00:00+00:00'
    now2 = '2026-02-23T00:05:00+00:00'

    with sqlite3.connect(db_path) as connection:
        first = ioc_store_service.upsert_ioc_item_core(
            connection,
            actor_id='actor-1',
            raw_ioc_type='domain',
            raw_ioc_value='evil.example',
            source_ref='test-source',
            source_id='src-1',
            source_tier='high',
            extraction_method='manual',
            now_iso=now1,
            deps={'validate_ioc_candidate': ioc_validation_service.validate_ioc_candidate_core},
        )
        second = ioc_store_service.upsert_ioc_item_core(
            connection,
            actor_id='actor-1',
            raw_ioc_type='domain',
            raw_ioc_value='evil.example',
            source_ref='test-source',
            source_id='src-1',
            source_tier='high',
            extraction_method='manual',
            now_iso=now2,
            deps={'validate_ioc_candidate': ioc_validation_service.validate_ioc_candidate_core},
        )
        connection.commit()

        row = connection.execute(
            'SELECT seen_count, first_seen_at, last_seen_at, validation_status FROM ioc_items WHERE actor_id = ? AND ioc_type = ? AND normalized_value = ?',
            ('actor-1', 'domain', 'evil.example'),
        ).fetchone()
        assert row is not None
        assert int(row[0]) == 2
        assert str(row[1]) == now1
        assert str(row[2]) == now2
        assert str(row[3]) == 'valid'

        history_count = connection.execute(
            'SELECT COUNT(*) FROM ioc_history WHERE actor_id = ?',
            ('actor-1',),
        ).fetchone()
        assert history_count is not None
        assert int(history_count[0]) == 2

    assert first['stored'] is True
    assert second['stored'] is True
