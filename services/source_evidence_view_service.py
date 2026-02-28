import json
import sqlite3
from urllib.parse import urlparse


def _domain_from_url(url_value: str) -> str:
    try:
        return (urlparse(url_value).hostname or '').strip('.').lower()
    except Exception:
        return ''


def list_ranked_evidence_core(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    limit: int = 100,
    entity_type: str | None = None,
    min_final_score: float = 0.0,
    source_tier: str | None = None,
    match_type: str | None = None,
    require_corroboration: bool = False,
) -> list[dict[str, object]]:
    safe_limit = max(1, min(200, int(limit)))
    safe_entity = str(entity_type or '').strip().lower()
    safe_min_score = max(0.0, min(1.0, float(min_final_score or 0.0)))
    safe_tier = str(source_tier or '').strip().lower()
    safe_match_type = str(match_type or '').strip().lower()
    safe_require_corroboration = bool(require_corroboration)

    try:
        query = '''
            SELECT
                s.id,
                s.source_name,
                s.url,
                s.published_at,
                s.ingested_at,
                s.source_type,
                s.source_tier,
                s.confidence_weight,
                COALESCE(sc.relevance_score, 0.0),
                COALESCE(sc.trust_score, 0.0),
                COALESCE(sc.recency_score, 0.0),
                COALESCE(sc.novelty_score, 0.0),
                COALESCE(sc.final_score, 0.0),
                COALESCE(ar.match_type, ''),
                COALESCE(ar.matched_term, ''),
                COALESCE(ar.confidence, 0.0),
                COALESCE(sc.features_json, '{}'),
                COUNT(se.id) AS entity_count
            FROM sources s
            LEFT JOIN source_scoring sc ON sc.source_id = s.id
            LEFT JOIN actor_resolution ar ON ar.source_id = s.id AND ar.actor_id = s.actor_id
            LEFT JOIN source_entities se ON se.source_id = s.id
            WHERE s.actor_id = ?
        '''
        params: list[object] = [actor_id]
        query += ' AND COALESCE(sc.final_score, 0.0) >= ?'
        params.append(safe_min_score)
        if safe_entity:
            query += ' AND se.entity_type = ?'
            params.append(safe_entity)
        if safe_tier:
            query += ' AND LOWER(COALESCE(s.source_tier, \'\')) = ?'
            params.append(safe_tier)
        if safe_match_type:
            query += ' AND LOWER(COALESCE(ar.match_type, \'\')) = ?'
            params.append(safe_match_type)
        query += '''
            GROUP BY
                s.id, s.source_name, s.url, s.published_at, s.ingested_at, s.source_type, s.source_tier,
                s.confidence_weight, sc.relevance_score, sc.trust_score, sc.recency_score, sc.novelty_score,
                sc.final_score, ar.match_type, ar.matched_term, ar.confidence, sc.features_json
            ORDER BY COALESCE(sc.final_score, 0.0) DESC, COALESCE(s.published_at, s.ingested_at, s.retrieved_at) DESC
            LIMIT ?
        '''
        params.append(safe_limit)
        rows = connection.execute(query, tuple(params)).fetchall()
    except sqlite3.OperationalError:
        fallback_rows = connection.execute(
            '''
            SELECT id, source_name, url, published_at, ingested_at, source_type, source_tier, confidence_weight
            FROM sources
            WHERE actor_id = ?
            ORDER BY COALESCE(published_at, ingested_at, retrieved_at) DESC
            LIMIT ?
            ''',
            (actor_id, safe_limit),
        ).fetchall()
        return [
            {
                'source_id': str(row[0] or ''),
                'source_name': str(row[1] or ''),
                'url': str(row[2] or ''),
                'domain': _domain_from_url(str(row[2] or '')),
                'published_at': str(row[3] or ''),
                'ingested_at': str(row[4] or ''),
                'source_type': str(row[5] or ''),
                'source_tier': str(row[6] or ''),
                'confidence_weight': int(row[7] or 0),
                'scores': {
                    'relevance': 0.0,
                    'trust': 0.0,
                    'recency': 0.0,
                    'novelty': 0.0,
                    'final': 0.0,
                },
                'match': {'type': '', 'matched_term': '', 'confidence': 0.0},
                'entity_count': 0,
                'corroboration_sources': 0,
                'features': {},
            }
            for row in fallback_rows
        ]

    results: list[dict[str, object]] = []
    for row in rows:
        features_raw = str(row[16] or '{}')
        try:
            features = json.loads(features_raw)
        except Exception:
            features = {}
        corroboration_sources = 0
        if isinstance(features, dict):
            corroboration_sources = int(features.get('corroboration_sources') or 0)
        if safe_require_corroboration and corroboration_sources < 1:
            continue
        results.append(
            {
                'source_id': str(row[0] or ''),
                'source_name': str(row[1] or ''),
                'url': str(row[2] or ''),
                'domain': _domain_from_url(str(row[2] or '')),
                'published_at': str(row[3] or ''),
                'ingested_at': str(row[4] or ''),
                'source_type': str(row[5] or ''),
                'source_tier': str(row[6] or ''),
                'confidence_weight': int(row[7] or 0),
                'scores': {
                    'relevance': float(row[8] or 0.0),
                    'trust': float(row[9] or 0.0),
                    'recency': float(row[10] or 0.0),
                    'novelty': float(row[11] or 0.0),
                    'final': float(row[12] or 0.0),
                },
                'match': {
                    'type': str(row[13] or ''),
                    'matched_term': str(row[14] or ''),
                    'confidence': float(row[15] or 0.0),
                },
                'entity_count': int(row[17] or 0),
                'corroboration_sources': corroboration_sources,
                'features': features if isinstance(features, dict) else {},
            }
        )
    return results
