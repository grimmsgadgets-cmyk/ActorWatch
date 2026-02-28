import hashlib
import json
import sqlite3
from datetime import datetime, timezone


NOTEBOOK_CACHE_FORMAT_VERSION = '2026-02-27.3'


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def cache_key_core(
    *,
    source_tier: object = None,
    min_confidence_weight: object = None,
    source_days: object = None,
    enforce_ollama_synthesis: object = None,
    backfill_debug_ui_enabled: object = None,
) -> str:
    parts = [
        f'fmt={NOTEBOOK_CACHE_FORMAT_VERSION}',
        f'tier={str(source_tier or "").strip().lower()}',
        f'min_conf={str(min_confidence_weight or "").strip()}',
        f'source_days={str(source_days or "").strip()}',
        f'enforce_ollama={1 if bool(enforce_ollama_synthesis) else 0}',
        f'backfill_debug={1 if bool(backfill_debug_ui_enabled) else 0}',
    ]
    return '|'.join(parts)


def actor_data_fingerprint_core(connection: sqlite3.Connection, actor_id: str) -> str:
    row = connection.execute(
        '''
        SELECT
            COALESCE((SELECT MAX(COALESCE(published_at, ingested_at, retrieved_at, '')) FROM sources WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM sources WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(occurred_at, '')) FROM timeline_events WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM timeline_events WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(updated_at, created_at, '')) FROM question_threads WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM question_threads WHERE actor_id = ?), 0),
            COALESCE((
                SELECT MAX(COALESCE(qu.created_at, ''))
                FROM question_updates qu
                JOIN question_threads qt ON qt.id = qu.thread_id
                WHERE qt.actor_id = ?
            ), ''),
            COALESCE((
                SELECT COUNT(*)
                FROM question_updates qu
                JOIN question_threads qt ON qt.id = qu.thread_id
                WHERE qt.actor_id = ?
            ), 0),
            COALESCE((SELECT MAX(COALESCE(updated_at, last_seen_at, created_at, '')) FROM ioc_items WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM ioc_items WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(created_at, '')) FROM requirement_items WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM requirement_items WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(updated_at, '')) FROM analyst_observations WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM analyst_observations WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(generated_at, '')) FROM quick_check_overrides WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM quick_check_overrides WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(updated_at, '')) FROM tracking_intent_register WHERE actor_id = ?), ''),
            COALESCE((SELECT priority FROM tracking_intent_register WHERE actor_id = ?), ''),
            COALESCE((SELECT impact FROM tracking_intent_register WHERE actor_id = ?), ''),
            COALESCE((SELECT MAX(COALESCE(updated_at, '')) FROM actor_collection_plans WHERE actor_id = ?), ''),
            COALESCE((SELECT MAX(COALESCE(created_at, '')) FROM actor_change_items WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM actor_change_items WHERE actor_id = ?), 0),
            COALESCE((SELECT MAX(COALESCE(created_at, '')) FROM actor_alert_events WHERE actor_id = ?), ''),
            COALESCE((SELECT COUNT(*) FROM actor_alert_events WHERE actor_id = ? AND status = 'open'), 0),
            COALESCE((SELECT MAX(COALESCE(updated_at, '')) FROM actor_report_preferences WHERE actor_id = ?), ''),
            COALESCE((SELECT MAX(COALESCE(last_confirmed_at, '')) FROM actor_profiles WHERE id = ?), ''),
            COALESCE((SELECT MAX(COALESCE(last_confirmed_by, '')) FROM actor_profiles WHERE id = ?), '')
        ''',
        (
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
            actor_id,
        ),
    ).fetchone()
    fingerprint_source = '|'.join(str(value or '') for value in (row or ()))
    return hashlib.sha256(fingerprint_source.encode('utf-8')).hexdigest()


def load_cached_notebook_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    cache_key: str,
    data_fingerprint: str,
) -> dict[str, object] | None:
    row = connection.execute(
        '''
        SELECT payload_json
        FROM notebook_cache
        WHERE actor_id = ? AND cache_key = ? AND data_fingerprint = ?
        ''',
        (actor_id, cache_key, data_fingerprint),
    ).fetchone()
    if row is None:
        return None
    raw = str(row[0] or '').strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def load_latest_cached_notebook_for_key_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    cache_key: str,
) -> dict[str, object] | None:
    row = connection.execute(
        '''
        SELECT payload_json
        FROM notebook_cache
        WHERE actor_id = ? AND cache_key = ?
        ORDER BY updated_at DESC
        LIMIT 1
        ''',
        (actor_id, cache_key),
    ).fetchone()
    if row is None:
        return None
    raw = str(row[0] or '').strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def save_cached_notebook_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    cache_key: str,
    data_fingerprint: str,
    payload: dict[str, object],
) -> None:
    now = _utc_now_iso()
    payload_json = json.dumps(payload, ensure_ascii=True, separators=(',', ':'), sort_keys=True)
    connection.execute(
        '''
        INSERT INTO notebook_cache (
            actor_id, cache_key, data_fingerprint, payload_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_id, cache_key) DO UPDATE SET
            data_fingerprint = excluded.data_fingerprint,
            payload_json = excluded.payload_json,
            updated_at = excluded.updated_at
        ''',
        (actor_id, cache_key, data_fingerprint, payload_json, now, now),
    )
