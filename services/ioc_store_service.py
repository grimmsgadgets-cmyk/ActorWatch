import uuid
from datetime import datetime, timedelta, timezone


def _parse_iso(value: str | None) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _choose_latest_iso(*values: str | None) -> str | None:
    parsed = [(dt, str(value)) for value in values if (dt := _parse_iso(value)) is not None]
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0], reverse=True)
    return parsed[0][1]


def _choose_earliest_iso(*values: str | None) -> str | None:
    parsed = [(dt, str(value)) for value in values if (dt := _parse_iso(value)) is not None]
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0])
    return parsed[0][1]


def _default_valid_until(*, ioc_type: str, valid_from: str) -> str | None:
    base_dt = _parse_iso(valid_from)
    if base_dt is None:
        return None
    ttl_days = 180
    if ioc_type == 'hash':
        ttl_days = 365
    elif ioc_type in {'domain', 'url', 'ip'}:
        ttl_days = 120
    elif ioc_type == 'email':
        ttl_days = 180
    return (base_dt + timedelta(days=ttl_days)).isoformat()


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
    observed_at: str | None = None,
    valid_from: str | None = None,
    valid_until: str | None = None,
    revoked: bool = False,
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
    effective_seen_at = str(observed_at).strip() if _parse_iso(observed_at) is not None else now_iso
    effective_valid_from = str(_choose_earliest_iso(valid_from, effective_seen_at) or effective_seen_at)
    effective_valid_until = str(
        _choose_latest_iso(valid_until, _default_valid_until(ioc_type=ioc_type, valid_from=effective_valid_from))
        or ''
    ) or None
    revoked_flag = bool(revoked) or normalized_lifecycle in {'revoked', 'false_positive'}
    revoked_int = 1 if revoked_flag else 0
    revoked_at = now_iso if revoked_flag else None
    is_active = 1 if (int(validated.get('is_active') or 0) == 1 and normalized_lifecycle in {'active', 'monitor'} and revoked_int == 0) else 0

    existing = connection.execute(
        '''
        SELECT id, seen_count, confidence_score, valid_until, last_seen_at, first_seen_at
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
                valid_from, valid_until, revoked, revoked_at,
                first_seen_at, last_seen_at, seen_count, is_active, updated_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                effective_valid_from,
                effective_valid_until,
                revoked_int,
                revoked_at,
                effective_seen_at,
                effective_seen_at,
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
                extraction_method, lifecycle_status, handling_tlp, valid_from, valid_until, revoked, revoked_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                effective_valid_from,
                effective_valid_until,
                revoked_int,
                revoked_at,
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
    previous_confidence = int(existing[2] or 0)
    existing_valid_until = str(existing[3] or '').strip() or None
    existing_last_seen = str(existing[4] or '').strip() or None
    existing_first_seen = str(existing[5] or '').strip() or None
    blended_confidence = max(0, min(5, int(round((float(previous_confidence) + float(confidence_score)) / 2.0))))
    effective_valid_until = str(_choose_latest_iso(existing_valid_until, effective_valid_until) or '') or None
    effective_last_seen = str(_choose_latest_iso(existing_last_seen, effective_seen_at) or effective_seen_at)
    effective_first_seen = str(_choose_earliest_iso(existing_first_seen, effective_seen_at) or effective_seen_at)
    connection.execute(
        '''
        UPDATE ioc_items
        SET
            ioc_value = ?,
            validation_status = ?,
            validation_reason = ?,
            confidence_score = ?,
            source_id = COALESCE(?, source_id),
            source_ref = COALESCE(?, source_ref),
            extraction_method = COALESCE(?, extraction_method),
            lifecycle_status = ?,
            handling_tlp = ?,
            valid_from = COALESCE(valid_from, ?),
            valid_until = ?,
            revoked = ?,
            revoked_at = CASE WHEN ? = 1 THEN COALESCE(revoked_at, ?) ELSE NULL END,
            first_seen_at = ?,
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
            blended_confidence,
            source_id,
            source_ref,
            extraction_method,
            normalized_lifecycle,
            normalized_tlp,
            effective_valid_from,
            effective_valid_until,
            revoked_int,
            revoked_int,
            revoked_at,
            effective_first_seen,
            effective_last_seen,
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
            extraction_method, lifecycle_status, handling_tlp, valid_from, valid_until, revoked, revoked_at, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            blended_confidence,
            source_id,
            source_ref,
            extraction_method,
            normalized_lifecycle,
            normalized_tlp,
            effective_valid_from,
            effective_valid_until,
            revoked_int,
            revoked_at,
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
