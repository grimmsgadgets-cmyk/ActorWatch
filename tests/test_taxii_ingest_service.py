import sqlite3

from services.taxii_ingest_service import list_taxii_sync_runs_core, sync_taxii_collection_core


class _Resp:
    def __init__(self, status_code: int, payload: object):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_taxii_sync_imports_bundle_and_records_run():
    with sqlite3.connect(':memory:') as connection:
        imported = {'iocs': 0, 'notes': 0, 'skipped': 0}

        def _http_get(_url, timeout=20.0, follow_redirects=True, headers=None, params=None):
            _ = timeout
            _ = follow_redirects
            _ = headers
            _ = params
            return _Resp(
                200,
                {
                    'objects': [
                        {
                            'type': 'indicator',
                            'pattern': "[domain-name:value = 'bad.example']",
                            'confidence': 80,
                        }
                    ]
                },
            )

        def _import_actor_stix_bundle(conn, *, actor_id, bundle, now_iso, upsert_ioc_item):
            _ = conn
            _ = actor_id
            _ = now_iso
            _ = upsert_ioc_item
            objects = bundle.get('objects', []) if isinstance(bundle, dict) else []
            imported['iocs'] = len(objects)
            return {'imported_iocs': len(objects), 'imported_notes': 0, 'skipped': 0}

        result = sync_taxii_collection_core(
            connection,
            actor_id='actor-1',
            collection_url='https://taxii.example/collections/abc',
            auth_token='',
            now_iso='2026-02-28T00:00:00+00:00',
            lookback_hours=72,
            deps={
                'http_get': _http_get,
                'import_actor_stix_bundle': _import_actor_stix_bundle,
                'upsert_ioc_item': lambda *_args, **_kwargs: {'stored': True},
            },
        )
        row = connection.execute(
            '''
            SELECT status, objects_received, imported_iocs
            FROM taxii_sync_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            ('actor-1',),
        ).fetchone()

    assert bool(result.get('ok'))
    assert int(result.get('objects_received') or 0) == 1
    assert int(result.get('imported_iocs') or 0) == 1
    assert row is not None
    assert str(row[0]) == 'completed'
    assert int(row[1] or 0) == 1
    assert int(row[2] or 0) == 1


def test_taxii_sync_run_listing():
    with sqlite3.connect(':memory:') as connection:
        connection.execute(
            '''
            CREATE TABLE taxii_sync_runs (
                id TEXT PRIMARY KEY,
                actor_id TEXT NOT NULL,
                collection_url TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                objects_received INTEGER NOT NULL DEFAULT 0,
                imported_iocs INTEGER NOT NULL DEFAULT 0,
                imported_notes INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                error_detail TEXT NOT NULL DEFAULT ''
            )
            '''
        )
        connection.execute(
            '''
            INSERT INTO taxii_sync_runs (
                id, actor_id, collection_url, started_at, finished_at, status, objects_received, imported_iocs, imported_notes, skipped
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'run-1',
                'actor-2',
                'https://taxii.example/collections/x',
                '2026-02-28T00:00:00+00:00',
                '2026-02-28T00:01:00+00:00',
                'completed',
                4,
                2,
                1,
                1,
            ),
        )
        runs = list_taxii_sync_runs_core(connection, actor_id='actor-2', limit=10)

    assert len(runs) == 1
    assert runs[0]['run_id'] == 'run-1'
    assert int(runs[0]['objects_received']) == 4
