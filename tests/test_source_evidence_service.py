import sqlite3

from services.source_evidence_service import persist_source_evidence_core


def _setup_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        '''
        CREATE TABLE source_documents (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            raw_text TEXT NOT NULL DEFAULT '',
            html_text TEXT NOT NULL DEFAULT '',
            fetched_at TEXT NOT NULL,
            http_status INTEGER,
            content_type TEXT NOT NULL DEFAULT '',
            parse_status TEXT NOT NULL DEFAULT 'unknown',
            parse_error TEXT NOT NULL DEFAULT ''
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


def test_persist_source_evidence_writes_artifacts():
    with sqlite3.connect(':memory:') as connection:
        _setup_tables(connection)
        persist_source_evidence_core(
            connection,
            source_id='source-1',
            actor_id='actor-1',
            source_url='https://example.org/report',
            source_text='APT29 used T1059 and C2 at bad.example plus 203.0.113.10.',
            raw_html='<html><body>APT29 report</body></html>',
            fetched_at='2026-02-28T00:00:00+00:00',
            published_at='2026-02-20T00:00:00+00:00',
            http_status=200,
            content_type='text/html',
            parse_status='parsed',
            parse_error='',
            actor_terms=['APT29', 'Cozy Bear'],
            relevance_score=0.9,
            match_type='exact_actor_term',
            match_reasons=['actor_term_exact'],
            matched_terms=['APT29'],
            source_trust_score=4,
            novelty_score=0.7,
            extractor='feed_ingest_v2',
        )
        doc_count = int(connection.execute('SELECT COUNT(*) FROM source_documents').fetchone()[0])
        entity_count = int(connection.execute('SELECT COUNT(*) FROM source_entities').fetchone()[0])
        resolution_row = connection.execute(
            'SELECT match_type, matched_term, confidence FROM actor_resolution WHERE actor_id = ?',
            ('actor-1',),
        ).fetchone()
        score_row = connection.execute(
            'SELECT relevance_score, trust_score, final_score FROM source_scoring WHERE source_id = ?',
            ('source-1',),
        ).fetchone()

    assert doc_count == 1
    assert entity_count >= 3
    assert resolution_row is not None
    assert str(resolution_row[0]) == 'exact_actor_term'
    assert str(resolution_row[1]) == 'APT29'
    assert float(resolution_row[2]) > 0.0
    assert score_row is not None
    assert float(score_row[0]) == 0.9
    assert float(score_row[1]) == 1.0
    assert float(score_row[2]) > 0.0


def test_persist_source_evidence_normalizes_url_entities():
    with sqlite3.connect(':memory:') as connection:
        _setup_tables(connection)
        persist_source_evidence_core(
            connection,
            source_id='source-2',
            actor_id='actor-2',
            source_url='https://example.org/report',
            source_text='Observed callback https://Intel.badcorp.com/path?utm_source=x&id=1 and domain www.bad.example',
            raw_html='',
            fetched_at='2026-02-28T00:00:00+00:00',
            published_at='2026-02-20T00:00:00+00:00',
            http_status=200,
            content_type='text/html',
            parse_status='parsed',
            parse_error='',
            actor_terms=['Actor-2'],
            relevance_score=0.6,
            match_type='soft_actor_match',
            match_reasons=['technical_linkage'],
            matched_terms=[],
            source_trust_score=2,
            novelty_score=0.6,
            extractor='feed_ingest_v2',
        )
        rows = connection.execute(
            '''
            SELECT entity_type, normalized_value
            FROM source_entities
            WHERE source_id = ?
            ORDER BY entity_type, normalized_value
            ''',
            ('source-2',),
        ).fetchall()

    normalized_pairs = {(str(row[0]), str(row[1])) for row in rows}
    assert ('url', 'https://intel.badcorp.com/path?id=1') in normalized_pairs
    assert ('domain', 'bad.example') in normalized_pairs
