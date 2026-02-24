import sqlite3

from services import data_retention_service
from services import db_schema_service


def test_prune_data_core_deletes_old_rows_and_preserves_minimum():
    connection = sqlite3.connect(':memory:')
    db_schema_service.ensure_schema(connection)
    connection.execute(
        '''
        INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
        VALUES ('actor-1', 'Actor One', 'actor one', '', '2026-01-01T00:00:00Z', 1)
        '''
    )
    for i in range(20):
        connection.execute(
            '''
            INSERT INTO analyst_feedback_events (
                id, actor_id, item_type, item_id, feedback_label, rating_score, reason, source_id, metadata_json, created_at
            ) VALUES (?, 'actor-1', 'priority_question', 'q-1', 'useful', 1, '', NULL, '{}', datetime('now', ?))
            ''',
            (f'fb-{i}', f'-{500 + i} days'),
        )
    for i in range(3):
        connection.execute(
            '''
            INSERT INTO analyst_feedback_events (
                id, actor_id, item_type, item_id, feedback_label, rating_score, reason, source_id, metadata_json, created_at
            ) VALUES (?, 'actor-1', 'priority_question', 'q-1', 'useful', 1, '', NULL, '{}', datetime('now'))
            ''',
            (f'fb-new-{i}',),
        )
    connection.commit()

    result = data_retention_service.prune_data_core(
        connection,
        retention_days=180,
        keep_min_rows_per_table=2,
    )
    connection.commit()

    remaining = connection.execute(
        'SELECT COUNT(*) FROM analyst_feedback_events'
    ).fetchone()
    assert remaining is not None
    assert int(remaining[0]) >= 2
    assert int(result.get('feedback_events_deleted') or 0) > 0
