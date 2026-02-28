import sqlite3
import uuid
from datetime import datetime, timedelta, timezone


def _normalize_taxii_objects_payload(payload: object) -> dict[str, object]:
    if isinstance(payload, dict):
        objects = payload.get('objects')
        if isinstance(objects, list):
            return {
                'type': 'bundle',
                'id': str(payload.get('id') or f"bundle--{uuid.uuid4()}"),
                'spec_version': str(payload.get('spec_version') or '2.1'),
                'objects': objects,
            }
    if isinstance(payload, list):
        return {
            'type': 'bundle',
            'id': f'bundle--{uuid.uuid4()}',
            'spec_version': '2.1',
            'objects': payload,
        }
    return {
        'type': 'bundle',
        'id': f'bundle--{uuid.uuid4()}',
        'spec_version': '2.1',
        'objects': [],
    }


def _added_after_from_hours(now_iso: str, lookback_hours: int) -> str:
    try:
        now = datetime.fromisoformat(str(now_iso).replace('Z', '+00:00'))
    except Exception:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    lookback = max(1, int(lookback_hours))
    return (now - timedelta(hours=lookback)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _ensure_taxii_sync_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS taxii_sync_runs (
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
        CREATE INDEX IF NOT EXISTS idx_taxii_sync_runs_actor_started
        ON taxii_sync_runs(actor_id, started_at DESC)
        '''
    )


def sync_taxii_collection_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    collection_url: str,
    auth_token: str | None,
    now_iso: str,
    lookback_hours: int,
    deps: dict[str, object],
) -> dict[str, object]:
    _http_get = deps['http_get']
    _import_actor_stix_bundle = deps['import_actor_stix_bundle']
    _upsert_ioc_item = deps['upsert_ioc_item']

    _ensure_taxii_sync_schema(connection)
    run_id = str(uuid.uuid4())
    safe_collection = str(collection_url or '').strip()
    if not safe_collection:
        return {
            'ok': False,
            'actor_id': actor_id,
            'collection_url': '',
            'run_id': '',
            'objects_received': 0,
            'imported_iocs': 0,
            'imported_notes': 0,
            'skipped': 0,
            'error': 'collection URL is required',
        }

    connection.execute(
        '''
        INSERT INTO taxii_sync_runs (
            id, actor_id, collection_url, started_at, status
        ) VALUES (?, ?, ?, ?, ?)
        ''',
        (run_id, actor_id, safe_collection, now_iso, 'running'),
    )

    headers = {'Accept': 'application/taxii+json;version=2.1'}
    token = str(auth_token or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'
    request_url = safe_collection.rstrip('/') + '/objects/'
    added_after = _added_after_from_hours(now_iso, lookback_hours)
    params = {'added_after': added_after}

    try:
        response = _http_get(
            request_url,
            timeout=20.0,
            follow_redirects=True,
            headers=headers,
            params=params,
        )
        status_code = int(getattr(response, 'status_code', 0) or 0)
        if status_code != 200:
            raise RuntimeError(f'TAXII fetch failed with status {status_code}')
        payload = response.json()
        bundle = _normalize_taxii_objects_payload(payload)
        result = _import_actor_stix_bundle(
            connection,
            actor_id=actor_id,
            bundle=bundle,
            now_iso=now_iso,
            upsert_ioc_item=_upsert_ioc_item,
        )
        imported_iocs = int(result.get('imported_iocs') or 0)
        imported_notes = int(result.get('imported_notes') or 0)
        skipped = int(result.get('skipped') or 0)
        objects_received = len(bundle.get('objects', [])) if isinstance(bundle.get('objects'), list) else 0
        connection.execute(
            '''
            UPDATE taxii_sync_runs
            SET finished_at = ?, status = ?, objects_received = ?, imported_iocs = ?, imported_notes = ?, skipped = ?
            WHERE id = ?
            ''',
            (now_iso, 'completed', objects_received, imported_iocs, imported_notes, skipped, run_id),
        )
        return {
            'ok': True,
            'actor_id': actor_id,
            'collection_url': safe_collection,
            'run_id': run_id,
            'added_after': added_after,
            'objects_received': objects_received,
            'imported_iocs': imported_iocs,
            'imported_notes': imported_notes,
            'skipped': skipped,
        }
    except Exception as exc:
        error = str(exc)[:500]
        connection.execute(
            '''
            UPDATE taxii_sync_runs
            SET finished_at = ?, status = ?, error_detail = ?
            WHERE id = ?
            ''',
            (now_iso, 'failed', error, run_id),
        )
        return {
            'ok': False,
            'actor_id': actor_id,
            'collection_url': safe_collection,
            'run_id': run_id,
            'objects_received': 0,
            'imported_iocs': 0,
            'imported_notes': 0,
            'skipped': 0,
            'error': error,
        }


def list_taxii_sync_runs_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    limit: int = 20,
) -> list[dict[str, object]]:
    _ensure_taxii_sync_schema(connection)
    safe_limit = max(1, min(200, int(limit)))
    rows = connection.execute(
        '''
        SELECT
            id,
            collection_url,
            started_at,
            finished_at,
            status,
            objects_received,
            imported_iocs,
            imported_notes,
            skipped,
            error_detail
        FROM taxii_sync_runs
        WHERE actor_id = ?
        ORDER BY started_at DESC
        LIMIT ?
        ''',
        (actor_id, safe_limit),
    ).fetchall()
    return [
        {
            'run_id': str(row[0] or ''),
            'collection_url': str(row[1] or ''),
            'started_at': str(row[2] or ''),
            'finished_at': str(row[3] or ''),
            'status': str(row[4] or ''),
            'objects_received': int(row[5] or 0),
            'imported_iocs': int(row[6] or 0),
            'imported_notes': int(row[7] or 0),
            'skipped': int(row[8] or 0),
            'error_detail': str(row[9] or ''),
        }
        for row in rows
    ]
