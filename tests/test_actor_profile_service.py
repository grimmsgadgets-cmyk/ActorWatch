import sqlite3
from pathlib import Path

from services import actor_profile_service
from services import db_schema_service


def _init_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as connection:
        db_schema_service.ensure_schema(connection)


def test_create_actor_profile_reuses_canonical_duplicates(tmp_path):
    db_path = tmp_path / 'actors.db'
    _init_db(db_path)
    deps = {
        'db_path': lambda: str(db_path),
        'new_id': lambda: 'actor-1',
        'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
        'normalize_actor_name': actor_profile_service.normalize_actor_name_core,
    }
    actor_profile_service.create_actor_profile_core(
        display_name='Akira',
        scope_statement=None,
        is_tracked=True,
        deps=deps,
    )
    duplicate = actor_profile_service.create_actor_profile_core(
        display_name='  akira  ',
        scope_statement=None,
        is_tracked=False,
        deps={
            **deps,
            'new_id': lambda: 'actor-2',
        },
    )
    assert duplicate['id'] == 'actor-1'
    with sqlite3.connect(str(db_path)) as connection:
        count = connection.execute('SELECT COUNT(*) FROM actor_profiles').fetchone()[0]
    assert count == 1


def test_merge_actor_profiles_moves_records_and_removes_source(tmp_path):
    db_path = tmp_path / 'actors.db'
    _init_db(db_path)
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES ('target', 'Akira', 'akira', 'target-scope', '2026-02-20T00:00:00+00:00', 1)
            '''
        )
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES ('source', 'AKIRA Team', 'akira team', 'source-scope', '2026-02-21T00:00:00+00:00', 0)
            '''
        )
        connection.execute(
            '''
            INSERT INTO sources (id, actor_id, source_name, url, published_at, retrieved_at, pasted_text)
            VALUES ('src-1', 'source', 'Example', 'https://example.com/a', '2026-02-22T00:00:00Z', '2026-02-22T00:00:00Z', 'text')
            '''
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (id, actor_id, occurred_at, category, title, summary, source_id)
            VALUES ('ev-1', 'source', '2026-02-22T00:00:00Z', 'execution', 'event', 'summary', 'src-1')
            '''
        )
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref, confidence,
                source_reliability, information_credibility, updated_by, updated_at
            ) VALUES
            ('obs-target', 'target', 'tool', 'mimikatz', 'old', 'ref-old', 'moderate', '', '', 'a', '2026-02-20T00:00:00Z'),
            ('obs-source', 'source', 'tool', 'mimikatz', 'new', 'ref-new', 'high', '', '', 'b', '2026-02-22T00:00:00Z')
            '''
        )
        connection.execute(
            '''
            INSERT INTO actor_feed_state (
                actor_id, feed_name, feed_url, last_checked_at, total_imported, total_failures
            ) VALUES
            ('target', 'Feed A', 'https://example.com/feed', '2026-02-20T00:00:00Z', 3, 1),
            ('source', 'Feed A', 'https://example.com/feed', '2026-02-22T00:00:00Z', 2, 2)
            '''
        )
        connection.commit()

    ids = iter(['new-obs'])
    result = actor_profile_service.merge_actor_profiles_core(
        target_actor_id='target',
        source_actor_id='source',
        deps={
            'db_path': lambda: str(db_path),
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
            'new_id': lambda: next(ids),
        },
    )

    assert result['target_actor_id'] == 'target'
    assert result['source_actor_id'] == 'source'
    with sqlite3.connect(str(db_path)) as connection:
        assert connection.execute("SELECT id FROM actor_profiles WHERE id = 'source'").fetchone() is None
        assert connection.execute(
            "SELECT actor_id FROM sources WHERE id = 'src-1'"
        ).fetchone()[0] == 'target'
        assert connection.execute(
            "SELECT actor_id FROM timeline_events WHERE id = 'ev-1'"
        ).fetchone()[0] == 'target'
        merged_obs = connection.execute(
            '''
            SELECT note, source_ref, confidence
            FROM analyst_observations
            WHERE actor_id = 'target' AND item_type = 'tool' AND item_key = 'mimikatz'
            '''
        ).fetchone()
        assert merged_obs == ('new', 'ref-new', 'high')
        feed_state = connection.execute(
            '''
            SELECT total_imported, total_failures
            FROM actor_feed_state
            WHERE actor_id = 'target' AND feed_name = 'Feed A'
            '''
        ).fetchone()
        assert feed_state == (5, 3)


def test_auto_merge_duplicate_actors_collapses_duplicate_sets(tmp_path):
    db_path = tmp_path / 'actors.db'
    _init_db(db_path)
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, canonical_name, scope_statement, created_at, is_tracked)
            VALUES
            ('a1', 'Akira', 'akira', NULL, '2026-02-20T00:00:00+00:00', 1),
            ('a2', 'AKIRA', 'akira', NULL, '2026-02-21T00:00:00+00:00', 0),
            ('q1', 'Qilin', 'qilin', NULL, '2026-02-20T00:00:00+00:00', 1),
            ('q2', 'QILIN', 'qilin', NULL, '2026-02-22T00:00:00+00:00', 0)
            '''
        )
        connection.execute(
            '''
            INSERT INTO sources (id, actor_id, source_name, url, published_at, retrieved_at, pasted_text)
            VALUES
            ('s1', 'a1', 'Example', 'https://example.com/a1', '2026-02-22T00:00:00Z', '2026-02-22T00:00:00Z', 'text'),
            ('s2', 'a2', 'Example', 'https://example.com/a2', '2026-02-22T00:00:00Z', '2026-02-22T00:00:00Z', 'text')
            '''
        )
        connection.commit()

    merged = actor_profile_service.auto_merge_duplicate_actors_core(
        deps={
            'db_path': lambda: str(db_path),
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
            'new_id': lambda: 'obs-1',
        }
    )
    assert merged == 2
    with sqlite3.connect(str(db_path)) as connection:
        akira_count = connection.execute(
            "SELECT COUNT(*) FROM actor_profiles WHERE canonical_name = 'akira'"
        ).fetchone()[0]
        qilin_count = connection.execute(
            "SELECT COUNT(*) FROM actor_profiles WHERE canonical_name = 'qilin'"
        ).fetchone()[0]
        assert akira_count == 1
        assert qilin_count == 1


def test_seed_actor_profiles_from_mitre_groups_adds_untracked_catalog(tmp_path):
    db_path = tmp_path / 'actors.db'
    _init_db(db_path)

    result = actor_profile_service.seed_actor_profiles_from_mitre_groups_core(
        deps={
            'db_path': lambda: str(db_path),
            'utc_now_iso': lambda: '2026-02-23T00:00:00+00:00',
            'new_id': lambda: 'seeded-1',
            'normalize_actor_name': actor_profile_service.normalize_actor_name_core,
            'load_mitre_groups': lambda: [
                {
                    'name': 'APT Test Group',
                    'description': 'MITRE ATT&CK group test description.',
                    'aliases': ['TestAlias'],
                }
            ],
        },
    )

    assert result['total'] == 1
    assert result['seeded'] == 1
    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            '''
            SELECT display_name, is_tracked, notebook_status, notebook_message
            FROM actor_profiles
            WHERE canonical_name = 'apt test group'
            '''
        ).fetchone()
    assert row == (
        'APT Test Group',
        0,
        'idle',
        'Waiting for tracking action.',
    )
