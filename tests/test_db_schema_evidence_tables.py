import sqlite3

from services import db_schema_service


def test_schema_creates_evidence_pipeline_tables():
    with sqlite3.connect(':memory:') as connection:
        db_schema_service.ensure_schema(connection)
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert 'source_documents' in tables
    assert 'source_entities' in tables
    assert 'actor_resolution' in tables
    assert 'source_scoring' in tables
    assert 'ingest_decisions' in tables
