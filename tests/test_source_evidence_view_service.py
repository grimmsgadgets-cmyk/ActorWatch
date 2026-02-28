import sqlite3

from services.source_evidence_view_service import list_ranked_evidence_core


def test_ranked_evidence_uses_scoring_and_resolution():
    with sqlite3.connect(':memory:') as connection:
        connection.execute(
            '''
            CREATE TABLE sources (
                id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT,
                ingested_at TEXT,
                retrieved_at TEXT NOT NULL,
                pasted_text TEXT NOT NULL,
                source_type TEXT,
                source_tier TEXT,
                confidence_weight INTEGER
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE source_scoring (
                source_id TEXT PRIMARY KEY,
                relevance_score REAL NOT NULL DEFAULT 0.0,
                trust_score REAL NOT NULL DEFAULT 0.0,
                recency_score REAL NOT NULL DEFAULT 0.0,
                novelty_score REAL NOT NULL DEFAULT 0.0,
                final_score REAL NOT NULL DEFAULT 0.0,
                scored_at TEXT NOT NULL,
                features_json TEXT NOT NULL DEFAULT '{}'
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE actor_resolution (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                match_type TEXT NOT NULL DEFAULT '',
                matched_term TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0.0,
                explanation_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE source_entities (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_value TEXT NOT NULL,
                normalized_value TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0.0,
                extractor TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, retrieved_at, pasted_text, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                's1',
                'a1',
                'Example',
                'https://intel.example/report',
                '2026-02-27T00:00:00+00:00',
                '2026-02-27T00:00:00+00:00',
                '2026-02-27T00:00:00+00:00',
                'text',
                'feed_partial_match',
                'trusted',
                2,
            ),
        )
        connection.execute(
            '''
            INSERT INTO source_scoring (
                source_id, relevance_score, trust_score, recency_score, novelty_score, final_score, scored_at, features_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('s1', 0.8, 1.0, 0.9, 0.6, 0.84, '2026-02-27T00:00:00+00:00', '{}'),
        )
        connection.execute(
            '''
            INSERT INTO actor_resolution (
                id, source_id, actor_id, match_type, matched_term, confidence, explanation_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('r1', 's1', 'a1', 'exact_actor_term', 'A1', 0.8, '{}', '2026-02-27T00:00:00+00:00'),
        )
        connection.execute(
            '''
            INSERT INTO source_entities (
                id, source_id, entity_type, entity_value, normalized_value, confidence, extractor, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('e1', 's1', 'domain', 'bad.example', 'bad.example', 0.8, 'test', '2026-02-27T00:00:00+00:00'),
        )
        items = list_ranked_evidence_core(connection, actor_id='a1', limit=10)

    assert len(items) == 1
    assert items[0]['source_id'] == 's1'
    assert float(items[0]['scores']['final']) == 0.84
    assert int(items[0]['entity_count']) == 1


def test_ranked_evidence_filters_and_corroboration():
    with sqlite3.connect(':memory:') as connection:
        connection.execute(
            '''
            CREATE TABLE sources (
                id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT,
                ingested_at TEXT,
                retrieved_at TEXT NOT NULL,
                pasted_text TEXT NOT NULL,
                source_type TEXT,
                source_tier TEXT,
                confidence_weight INTEGER
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE source_scoring (
                source_id TEXT PRIMARY KEY,
                relevance_score REAL NOT NULL DEFAULT 0.0,
                trust_score REAL NOT NULL DEFAULT 0.0,
                recency_score REAL NOT NULL DEFAULT 0.0,
                novelty_score REAL NOT NULL DEFAULT 0.0,
                final_score REAL NOT NULL DEFAULT 0.0,
                scored_at TEXT NOT NULL,
                features_json TEXT NOT NULL DEFAULT '{}'
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE actor_resolution (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                match_type TEXT NOT NULL DEFAULT '',
                matched_term TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0.0,
                explanation_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.execute(
            '''
            CREATE TABLE source_entities (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_value TEXT NOT NULL,
                normalized_value TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0.0,
                extractor TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            '''
        )
        connection.executemany(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, retrieved_at, pasted_text, source_type, source_tier, confidence_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                ('s1', 'a2', 'One', 'https://one.test/a', '2026-02-27T00:00:00+00:00', '2026-02-27T00:00:00+00:00', '2026-02-27T00:00:00+00:00', 'text', 'feed_partial_match', 'trusted', 2),
                ('s2', 'a2', 'Two', 'https://two.test/b', '2026-02-27T00:00:00+00:00', '2026-02-27T00:00:00+00:00', '2026-02-27T00:00:00+00:00', 'text', 'feed_soft_match', 'context', 1),
            ],
        )
        connection.executemany(
            '''
            INSERT INTO source_scoring (
                source_id, relevance_score, trust_score, recency_score, novelty_score, final_score, scored_at, features_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                ('s1', 0.8, 1.0, 0.9, 0.6, 0.85, '2026-02-27T00:00:00+00:00', '{"corroboration_sources":2}'),
                ('s2', 0.5, 0.3, 0.8, 0.5, 0.52, '2026-02-27T00:00:00+00:00', '{"corroboration_sources":0}'),
            ],
        )
        connection.executemany(
            '''
            INSERT INTO actor_resolution (
                id, source_id, actor_id, match_type, matched_term, confidence, explanation_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                ('r1', 's1', 'a2', 'exact_actor_term', 'A2', 0.9, '{}', '2026-02-27T00:00:00+00:00'),
                ('r2', 's2', 'a2', 'soft_actor_match', 'A2', 0.5, '{}', '2026-02-27T00:00:00+00:00'),
            ],
        )
        filtered = list_ranked_evidence_core(
            connection,
            actor_id='a2',
            min_final_score=0.6,
            source_tier='trusted',
            match_type='exact_actor_term',
            require_corroboration=True,
        )

    assert len(filtered) == 1
    assert filtered[0]['source_id'] == 's1'
    assert int(filtered[0]['corroboration_sources']) == 2
