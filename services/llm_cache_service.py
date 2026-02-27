import json
import sqlite3


def actor_key_core(actor_name: str) -> str:
    return ' '.join(str(actor_name or '').strip().lower().split())


def input_fingerprint_core(payload: object, *, deps: dict[str, object]) -> str:
    _sha256 = deps['sha256']
    serialized = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    return _sha256(serialized.encode('utf-8')).hexdigest()


def load_cached_payload_core(
    *,
    actor_key: str,
    cache_kind: str,
    input_fingerprint: str,
    deps: dict[str, object],
) -> object | None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    row = None
    with sqlite3.connect(_db_path()) as connection:
        row = connection.execute(
            '''
            SELECT payload_json, estimated_cost_ms
            FROM llm_synthesis_cache
            WHERE actor_key = ? AND cache_kind = ? AND input_fingerprint = ?
            ''',
            (str(actor_key), str(cache_kind), str(input_fingerprint)),
        ).fetchone()
        if row is None:
            return None
        payload_json = str(row[0] or '')
        estimated_cost_ms = max(0, int(row[1] or 0))
        connection.execute(
            '''
            UPDATE llm_synthesis_cache
            SET hit_count = hit_count + 1,
                saved_ms_total = saved_ms_total + ?,
                updated_at = ?
            WHERE actor_key = ? AND cache_kind = ? AND input_fingerprint = ?
            ''',
            (
                estimated_cost_ms,
                _utc_now_iso(),
                str(actor_key),
                str(cache_kind),
                str(input_fingerprint),
            ),
        )
        connection.commit()
    try:
        return json.loads(payload_json)
    except Exception:
        return None


def save_cached_payload_core(
    *,
    actor_key: str,
    cache_kind: str,
    input_fingerprint: str,
    payload: object,
    estimated_cost_ms: int,
    deps: dict[str, object],
) -> None:
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    max_age_days = max(1, int(deps.get('max_age_days', 30)))
    max_rows_per_actor_kind = max(10, int(deps.get('max_rows_per_actor_kind', 300)))
    now_iso = _utc_now_iso()
    payload_json = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    with sqlite3.connect(_db_path()) as connection:
        connection.execute(
            '''
            INSERT INTO llm_synthesis_cache (
                actor_key, cache_kind, input_fingerprint, payload_json,
                estimated_cost_ms, hit_count, saved_ms_total, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
            ON CONFLICT(actor_key, cache_kind, input_fingerprint)
            DO UPDATE SET
                payload_json = excluded.payload_json,
                estimated_cost_ms = excluded.estimated_cost_ms,
                updated_at = excluded.updated_at
            ''',
            (
                str(actor_key),
                str(cache_kind),
                str(input_fingerprint),
                payload_json,
                max(0, int(estimated_cost_ms)),
                now_iso,
                now_iso,
            ),
        )
        connection.execute(
            '''
            DELETE FROM llm_synthesis_cache
            WHERE updated_at < datetime('now', ?)
            ''',
            (f'-{max_age_days} days',),
        )
        connection.execute(
            '''
            DELETE FROM llm_synthesis_cache
            WHERE actor_key = ?
              AND cache_kind = ?
              AND rowid IN (
                  SELECT rowid
                  FROM llm_synthesis_cache
                  WHERE actor_key = ? AND cache_kind = ?
                  ORDER BY updated_at DESC, rowid DESC
                  LIMIT -1 OFFSET ?
              )
            ''',
            (
                str(actor_key),
                str(cache_kind),
                str(actor_key),
                str(cache_kind),
                max_rows_per_actor_kind,
            ),
        )
        connection.commit()


def cache_stats_for_actor_core(*, actor_key: str, deps: dict[str, object]) -> dict[str, int]:
    _db_path = deps['db_path']
    with sqlite3.connect(_db_path()) as connection:
        row = connection.execute(
            '''
            SELECT
                COALESCE(SUM(hit_count), 0),
                COALESCE(SUM(saved_ms_total), 0)
            FROM llm_synthesis_cache
            WHERE actor_key = ?
            ''',
            (str(actor_key),),
        ).fetchone()
    return {
        'cache_hits': int(row[0] or 0) if row else 0,
        'saved_ms_total': int(row[1] or 0) if row else 0,
    }
