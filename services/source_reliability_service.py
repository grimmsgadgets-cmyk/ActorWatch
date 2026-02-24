from services.environment_profile_service import domain_from_url_core


def apply_feedback_to_source_domains_core(
    connection,
    *,
    actor_id: str,
    source_urls: list[str],
    rating_score: int,
    now_iso: str,
) -> int:
    domains = [domain_from_url_core(url) for url in source_urls]
    unique_domains = sorted({domain for domain in domains if domain})
    updated = 0
    for domain in unique_domains:
        row = connection.execute(
            '''
            SELECT helpful_count, unhelpful_count
            FROM source_reliability
            WHERE actor_id = ? AND domain = ?
            ''',
            (actor_id, domain),
        ).fetchone()
        helpful = int(row[0] or 0) if row else 0
        unhelpful = int(row[1] or 0) if row else 0
        if rating_score > 0:
            helpful += 1
        elif rating_score < 0:
            unhelpful += 1
        total = max(1, helpful + unhelpful)
        score = round(helpful / total, 3)
        connection.execute(
            '''
            INSERT INTO source_reliability (
                actor_id, domain, helpful_count, unhelpful_count, reliability_score, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(actor_id, domain) DO UPDATE SET
                helpful_count = excluded.helpful_count,
                unhelpful_count = excluded.unhelpful_count,
                reliability_score = excluded.reliability_score,
                updated_at = excluded.updated_at
            ''',
            (actor_id, domain, helpful, unhelpful, score, now_iso),
        )
        updated += 1
    return updated


def load_reliability_map_core(connection, *, actor_id: str) -> dict[str, dict[str, object]]:
    rows = connection.execute(
        '''
        SELECT domain, helpful_count, unhelpful_count, reliability_score, updated_at
        FROM source_reliability
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchall()
    return {
        str(row[0]): {
            'helpful_count': int(row[1] or 0),
            'unhelpful_count': int(row[2] or 0),
            'reliability_score': float(row[3] or 0.5),
            'updated_at': str(row[4] or ''),
        }
        for row in rows
    }


def confidence_weight_adjustment_core(reliability_score: float) -> int:
    if reliability_score >= 0.8:
        return 1
    if reliability_score <= 0.2:
        return -1
    return 0
