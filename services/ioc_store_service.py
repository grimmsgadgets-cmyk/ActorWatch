import uuid


def upsert_ioc_item_core(
    connection,
    *,
    actor_id: str,
    raw_ioc_type: str,
    raw_ioc_value: str,
    source_ref: str | None,
    source_id: str | None,
    source_tier: str | None,
    extraction_method: str,
    now_iso: str,
    lifecycle_status: str = 'active',
    handling_tlp: str = 'TLP:CLEAR',
    confidence_score_override: int | None = None,
    deps: dict[str, object],
) -> dict[str, object]:
    _validate_ioc_candidate = deps['validate_ioc_candidate']

    validated = _validate_ioc_candidate(
        raw_value=raw_ioc_value,
        raw_type=raw_ioc_type,
        source_tier=source_tier,
        extraction_method=extraction_method,
    )
    if not bool(validated.get('valid')):
        return {
            'stored': False,
            'status': str(validated.get('validation_status') or 'invalid'),
            'reason': str(validated.get('validation_reason') or ''),
            'ioc_type': str(validated.get('ioc_type') or ''),
            'ioc_value': str(validated.get('ioc_value') or raw_ioc_value),
        }

    ioc_type = str(validated.get('ioc_type') or '').strip().lower()
    ioc_value = str(validated.get('ioc_value') or raw_ioc_value).strip()
    normalized_value = str(validated.get('normalized_value') or '').strip().lower()
    validation_status = str(validated.get('validation_status') or 'valid')
    validation_reason = str(validated.get('validation_reason') or '')
    confidence_score = int(validated.get('confidence_score') or 0)
    if confidence_score_override is not None:
        confidence_score = max(0, min(5, int(confidence_score_override)))
    normalized_lifecycle = str(lifecycle_status or 'active').strip().lower()
    if normalized_lifecycle not in {'active', 'monitor', 'superseded', 'revoked', 'false_positive'}:
        normalized_lifecycle = 'active'
    normalized_tlp = str(handling_tlp or 'TLP:CLEAR').strip().upper()
    if normalized_tlp not in {'TLP:CLEAR', 'TLP:GREEN', 'TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:RED'}:
        normalized_tlp = 'TLP:CLEAR'
    is_active = 1 if (int(validated.get('is_active') or 0) == 1 and normalized_lifecycle in {'active', 'monitor'}) else 0

    existing = connection.execute(
        '''
        SELECT id, seen_count
        FROM ioc_items
        WHERE actor_id = ? AND ioc_type = ? AND normalized_value = ?
        ''',
        (actor_id, ioc_type, normalized_value),
    ).fetchone()

    if existing is None:
        item_id = str(uuid.uuid4())
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, normalized_value,
                validation_status, validation_reason, confidence_score,
                source_id, source_ref, extraction_method,
                lifecycle_status, handling_tlp,
                first_seen_at, last_seen_at, seen_count, is_active, updated_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                item_id,
                actor_id,
                ioc_type,
                ioc_value,
                normalized_value,
                validation_status,
                validation_reason,
                confidence_score,
                source_id,
                source_ref,
                extraction_method,
                normalized_lifecycle,
                normalized_tlp,
                now_iso,
                now_iso,
                1,
                is_active,
                now_iso,
                now_iso,
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_history (
                id, ioc_item_id, actor_id, event_type, ioc_type, ioc_value, normalized_value,
                validation_status, validation_reason, confidence_score, source_id, source_ref,
                extraction_method, lifecycle_status, handling_tlp, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(uuid.uuid4()),
                item_id,
                actor_id,
                'created',
                ioc_type,
                ioc_value,
                normalized_value,
                validation_status,
                validation_reason,
                confidence_score,
                source_id,
                source_ref,
                extraction_method,
                normalized_lifecycle,
                normalized_tlp,
                now_iso,
            ),
        )
        return {
            'stored': True,
            'status': validation_status,
            'reason': validation_reason,
            'ioc_type': ioc_type,
            'ioc_value': ioc_value,
            'created': True,
        }

    item_id = str(existing[0])
    seen_count = int(existing[1] or 0) + 1
    connection.execute(
        '''
        UPDATE ioc_items
        SET
            ioc_value = ?,
            validation_status = ?,
            validation_reason = ?,
            confidence_score = CASE WHEN confidence_score < ? THEN ? ELSE confidence_score END,
            source_id = COALESCE(?, source_id),
            source_ref = COALESCE(?, source_ref),
            extraction_method = COALESCE(?, extraction_method),
            lifecycle_status = ?,
            handling_tlp = ?,
            last_seen_at = ?,
            seen_count = ?,
            is_active = ?,
            updated_at = ?
        WHERE id = ?
        ''',
        (
            ioc_value,
            validation_status,
            validation_reason,
            confidence_score,
            confidence_score,
            source_id,
            source_ref,
            extraction_method,
            normalized_lifecycle,
            normalized_tlp,
            now_iso,
            seen_count,
            is_active,
            now_iso,
            item_id,
        ),
    )
    connection.execute(
        '''
        INSERT INTO ioc_history (
            id, ioc_item_id, actor_id, event_type, ioc_type, ioc_value, normalized_value,
            validation_status, validation_reason, confidence_score, source_id, source_ref,
            extraction_method, lifecycle_status, handling_tlp, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            str(uuid.uuid4()),
            item_id,
            actor_id,
            'seen',
            ioc_type,
            ioc_value,
            normalized_value,
            validation_status,
            validation_reason,
            confidence_score,
            source_id,
            source_ref,
            extraction_method,
            normalized_lifecycle,
            normalized_tlp,
            now_iso,
        ),
    )
    return {
        'stored': True,
        'status': validation_status,
        'reason': validation_reason,
        'ioc_type': ioc_type,
        'ioc_value': ioc_value,
        'created': False,
    }
