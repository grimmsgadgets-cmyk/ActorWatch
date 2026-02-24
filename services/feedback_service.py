import json
import uuid
from collections import defaultdict


def normalize_feedback_label(value: str) -> tuple[str, int]:
    lowered = str(value or '').strip().lower()
    if lowered in {'useful', 'positive', 'good'}:
        return ('useful', 1)
    if lowered in {'partial', 'mixed', 'neutral'}:
        return ('partial', 0)
    if lowered in {'not_useful', 'negative', 'bad'}:
        return ('not_useful', -1)
    return ('partial', 0)


def store_feedback_event_core(
    connection,
    *,
    actor_id: str,
    item_type: str,
    item_id: str,
    feedback_label: str,
    reason: str,
    source_id: str | None,
    metadata: dict[str, object] | None,
    now_iso: str,
) -> dict[str, object]:
    normalized_type = str(item_type or '').strip().lower()[:40]
    normalized_id = str(item_id or '').strip()[:200]
    if not normalized_type or not normalized_id:
        return {'stored': False, 'reason': 'item_type and item_id are required'}
    label, score = normalize_feedback_label(feedback_label)
    connection.execute(
        '''
        INSERT INTO analyst_feedback_events (
            id, actor_id, item_type, item_id, feedback_label, rating_score,
            reason, source_id, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            str(uuid.uuid4()),
            actor_id,
            normalized_type,
            normalized_id,
            label,
            score,
            str(reason or '').strip()[:400],
            str(source_id or '').strip() or None,
            json.dumps(metadata or {}),
            now_iso,
        ),
    )
    return {
        'stored': True,
        'item_type': normalized_type,
        'item_id': normalized_id,
        'feedback_label': label,
        'rating_score': score,
    }


def feedback_summary_for_actor_core(connection, *, actor_id: str, item_type: str | None = None) -> dict[str, object]:
    where = ['actor_id = ?']
    params: list[object] = [actor_id]
    if item_type:
        where.append('item_type = ?')
        params.append(str(item_type).strip().lower())
    rows = connection.execute(
        f'''
        SELECT item_type, item_id, COUNT(*), SUM(rating_score)
        FROM analyst_feedback_events
        WHERE {' AND '.join(where)}
        GROUP BY item_type, item_id
        ''',
        params,
    ).fetchall()
    by_item_type: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        votes = int(row[2] or 0)
        score = int(row[3] or 0)
        by_item_type[str(row[0])].append(
            {
                'item_id': str(row[1]),
                'votes': votes,
                'score': score,
                'avg': round(score / votes, 3) if votes > 0 else 0.0,
            }
        )
    return {
        'actor_id': actor_id,
        'items': dict(by_item_type),
    }
