import json
import re
import sqlite3
import uuid
import zlib

from fastapi import HTTPException

import services.observation_service as observation_service


def _pdf_escape_text(value: str) -> str:
    return str(value or '').replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')


def render_simple_text_pdf(*, title: str, lines: list[str]) -> bytes:
    safe_title = str(title or 'Analyst Pack')
    safe_lines = [str(line or '')[:220] for line in lines if str(line or '').strip()]
    pages: list[list[str]] = []
    lines_per_page = 46
    if not safe_lines:
        safe_lines = ['(no content)']
    for index in range(0, len(safe_lines), lines_per_page):
        pages.append(safe_lines[index:index + lines_per_page])

    objects: dict[int, bytes] = {}
    objects[1] = b'<< /Type /Catalog /Pages 2 0 R >>'
    font_obj_id = 3
    objects[font_obj_id] = b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>'

    kids_refs: list[str] = []
    page_obj_id = 4
    for page_idx, page_lines in enumerate(pages, start=1):
        content_rows: list[str] = [
            'BT',
            '/F1 11 Tf',
            '72 760 Td',
            f'({_pdf_escape_text(f"{safe_title} (page {page_idx}/{len(pages)})")}) Tj',
            '0 -18 Td',
        ]
        for line in page_lines:
            content_rows.append(f'({_pdf_escape_text(line)}) Tj')
            content_rows.append('0 -14 Td')
        content_rows.append('ET')
        content_stream = '\n'.join(content_rows).encode('latin-1', 'replace')
        compressed = zlib.compress(content_stream)
        content_obj_id = page_obj_id + 1
        objects[content_obj_id] = (
            f'<< /Length {len(compressed)} /Filter /FlateDecode >>\nstream\n'.encode('ascii')
            + compressed
            + b'\nendstream'
        )
        objects[page_obj_id] = (
            f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] '
            f'/Resources << /Font << /F1 {font_obj_id} 0 R >> >> '
            f'/Contents {content_obj_id} 0 R >>'
        ).encode('ascii')
        kids_refs.append(f'{page_obj_id} 0 R')
        page_obj_id += 2

    objects[2] = f'<< /Type /Pages /Count {len(kids_refs)} /Kids [{" ".join(kids_refs)}] >>'.encode('ascii')

    output = bytearray()
    output.extend(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n')
    offsets: dict[int, int] = {}
    max_id = max(objects.keys())
    for obj_id in sorted(objects.keys()):
        offsets[obj_id] = len(output)
        output.extend(f'{obj_id} 0 obj\n'.encode('ascii'))
        output.extend(objects[obj_id])
        output.extend(b'\nendobj\n')

    xref_start = len(output)
    output.extend(f'xref\n0 {max_id + 1}\n'.encode('ascii'))
    output.extend(b'0000000000 65535 f \n')
    for obj_id in range(1, max_id + 1):
        offset = offsets.get(obj_id, 0)
        in_use = 'n' if obj_id in offsets else 'f'
        output.extend(f'{offset:010d} 00000 {in_use} \n'.encode('ascii'))
    output.extend(
        (
            'trailer\n'
            f'<< /Size {max_id + 1} /Root 1 0 R >>\n'
            f'startxref\n{xref_start}\n%%EOF\n'
        ).encode('ascii')
    )
    return bytes(output)


def _change_matches_trigger_conditions(
    *,
    trigger_conditions: list[str],
    change_summary: str,
    change_type: str,
    tag_tokens: list[str],
) -> bool:
    normalized_conditions = [
        str(item or '').strip().lower()
        for item in trigger_conditions
        if str(item or '').strip()
    ]
    if not normalized_conditions:
        return True
    haystack = ' '.join(
        [
            str(change_summary or '').lower(),
            str(change_type or '').lower(),
            ' '.join([str(token or '').lower() for token in tag_tokens if str(token or '').strip()]),
        ]
    )
    return any(condition in haystack for condition in normalized_conditions)


def _severity_from_change(*, confidence: str, change_type: str, tag_count: int) -> str:
    normalized_confidence = str(confidence or 'moderate').strip().lower()
    normalized_type = str(change_type or '').strip().lower()
    if normalized_confidence == 'high' and tag_count >= 2:
        return 'high'
    if normalized_type in {'infra', 'access_vector', 'targeting'} and normalized_confidence in {'moderate', 'high'}:
        return 'high'
    if normalized_confidence == 'low':
        return 'low'
    return 'medium'


def enqueue_change_alert_if_needed(
    *,
    connection: sqlite3.Connection,
    actor_id: str,
    change_item_id: str,
    change_summary: str,
    change_type: str,
    confidence: str,
    source_ref: str,
    tags: dict[str, int],
    safe_json_string_list,
    utc_now_iso,
) -> dict[str, object]:
    plan_row = connection.execute(
        '''
        SELECT trigger_conditions_json, alert_subscriptions_json, alert_notifications_enabled
        FROM actor_collection_plans
        WHERE actor_id = ?
        ''',
        (actor_id,),
    ).fetchone()
    if plan_row:
        trigger_conditions = safe_json_string_list(str(plan_row[0] or '[]'))
        alert_subscriptions = safe_json_string_list(str(plan_row[1] or '[]'))
        notifications_enabled = int(plan_row[2] or 0) == 1
    else:
        trigger_conditions = []
        alert_subscriptions = []
        notifications_enabled = False
    tag_tokens = [name for name, enabled in tags.items() if int(enabled or 0) == 1]
    if not _change_matches_trigger_conditions(
        trigger_conditions=trigger_conditions,
        change_summary=change_summary,
        change_type=change_type,
        tag_tokens=tag_tokens,
    ):
        return {'created': False, 'delivered': False, 'notifications_enabled': notifications_enabled}
    duplicate_row = connection.execute(
        '''
        SELECT id
        FROM actor_alert_events
        WHERE actor_id = ? AND change_item_id = ? AND status = 'open'
        LIMIT 1
        ''',
        (actor_id, change_item_id),
    ).fetchone()
    if duplicate_row:
        return {'created': False, 'delivered': False, 'notifications_enabled': notifications_enabled}
    now_iso = utc_now_iso()
    tag_count = sum(int(value or 0) for value in tags.values())
    severity = _severity_from_change(confidence=confidence, change_type=change_type, tag_count=tag_count)
    readable_type = str(change_type or 'change').replace('_', ' ').title()
    title = f'{readable_type} change detected'
    detail = str(change_summary or '').strip()[:1200]
    alert_id = str(uuid.uuid4())
    connection.execute(
        '''
        INSERT INTO actor_alert_events (
            id, actor_id, alert_type, severity, title, detail, status,
            source_ref, channel_targets_json, change_item_id, created_at,
            acknowledged_at, acknowledged_by
        )
        VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, '', '')
        ''',
        (
            alert_id,
            actor_id,
            'change_detection',
            severity,
            title,
            detail,
            str(source_ref or '')[:500],
            str(json.dumps(alert_subscriptions)),
            change_item_id,
            now_iso,
        ),
    )
    return {
        'created': True,
        'notifications_enabled': notifications_enabled,
        'alert_id': alert_id,
        'title': title,
        'detail': detail,
        'severity': severity,
        'alert_subscriptions': alert_subscriptions,
    }


def build_analyst_pack_payload(
    actor_id: str,
    *,
    source_tier: str | None = None,
    min_confidence_weight: str | None = None,
    source_days: str | None = None,
    observations_limit: int = 1000,
    history_limit: int = 1000,
    fetch_actor_notebook,
    fetch_analyst_observations,
    db_path,
    actor_exists,
    utc_now_iso,
) -> dict[str, object]:
    safe_observations_limit = max(1, min(5000, int(observations_limit)))
    safe_history_limit = max(1, min(5000, int(history_limit)))
    notebook = fetch_actor_notebook(
        actor_id,
        source_tier=source_tier,
        min_confidence_weight=min_confidence_weight,
        source_days=source_days,
    )
    observations = fetch_analyst_observations(actor_id, limit=safe_observations_limit, offset=0)
    with sqlite3.connect(db_path()) as connection:
        if not actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')
        history_rows = connection.execute(
            '''
            SELECT item_type, item_key, note, source_ref, confidence,
                   source_reliability, information_credibility, claim_type, citation_url, observed_on,
                   updated_by, updated_at
            FROM analyst_observation_history
            WHERE actor_id = ?
            ORDER BY updated_at DESC
            LIMIT ?
            ''',
            (actor_id, safe_history_limit),
        ).fetchall()
    history_items = [
        {
            'item_type': str(row[0] or ''),
            'item_key': str(row[1] or ''),
            'note': str(row[2] or ''),
            'source_ref': str(row[3] or ''),
            'confidence': str(row[4] or 'moderate'),
            'source_reliability': str(row[5] or ''),
            'information_credibility': str(row[6] or ''),
            'claim_type': str(row[7] or 'assessment'),
            'citation_url': str(row[8] or ''),
            'observed_on': str(row[9] or ''),
            'updated_by': str(row[10] or ''),
            'updated_at': str(row[11] or ''),
        }
        for row in history_rows
    ]
    quality_filters = notebook.get('source_quality_filters', {})
    quality_filters_dict = quality_filters if isinstance(quality_filters, dict) else {}
    source_scope_active = any(
        str(quality_filters_dict.get(key) or '').strip()
        for key in ('source_tier', 'min_confidence_weight', 'source_days')
    )
    if source_scope_active:
        allowed_source_ids = {
            str(item.get('id') or '').strip()
            for item in (notebook.get('sources', []) if isinstance(notebook.get('sources', []), list) else [])
            if isinstance(item, dict) and str(item.get('id') or '').strip()
        }
        observations = [
            item
            for item in observations
            if str(item.get('item_type') or '').strip().lower() != 'source'
            or str(item.get('item_key') or '').strip() in allowed_source_ids
        ]
        history_items = [
            item
            for item in history_items
            if str(item.get('item_type') or '').strip().lower() != 'source'
            or str(item.get('item_key') or '').strip() in allowed_source_ids
        ]
    return {
        'actor_id': actor_id,
        'exported_at': utc_now_iso(),
        'limits': {
            'observations': safe_observations_limit,
            'history': safe_history_limit,
        },
        'source_quality_filters': quality_filters_dict,
        'actor': notebook.get('actor', {}),
        'recent_change_summary': notebook.get('recent_change_summary', {}),
        'priority_questions': notebook.get('priority_questions', [])[:3],
        'ioc_items': notebook.get('ioc_items', []),
        'observations': observations,
        'observation_history': history_items,
    }


def ioc_value_is_hunt_relevant(ioc_type: str, ioc_value: str) -> bool:
    value = str(ioc_value or '').strip().lower()
    indicator_type = str(ioc_type or '').strip().lower()
    if not value or not indicator_type:
        return False
    if len(value) < 4:
        return False
    if indicator_type == 'domain':
        if re.fullmatch(r'^[a-z0-9-]+\.(js|json|css|html|xml|yaml|yml|md|txt|jsx|tsx)$', value):
            return False
    return True


def upsert_observation_with_history(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    item_type: str,
    item_key: str,
    note: str,
    source_ref: str,
    confidence: str,
    source_reliability: str,
    information_credibility: str,
    claim_type: str,
    citation_url: str,
    observed_on: str,
    updated_by: str,
    updated_at: str,
) -> None:
    connection.execute(
        '''
        INSERT INTO analyst_observations (
            id, actor_id, item_type, item_key, note, source_ref,
            confidence, source_reliability, information_credibility, claim_type, citation_url, observed_on,
            updated_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(actor_id, item_type, item_key)
        DO UPDATE SET
            note = excluded.note,
            source_ref = excluded.source_ref,
            confidence = excluded.confidence,
            source_reliability = excluded.source_reliability,
            information_credibility = excluded.information_credibility,
            claim_type = excluded.claim_type,
            citation_url = excluded.citation_url,
            observed_on = excluded.observed_on,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        ''',
        (
            str(uuid.uuid4()),
            actor_id,
            item_type,
            item_key,
            note,
            source_ref,
            confidence,
            source_reliability,
            information_credibility,
            claim_type,
            citation_url,
            observed_on,
            updated_by,
            updated_at,
        ),
    )
    connection.execute(
        '''
        INSERT INTO analyst_observation_history (
            id, actor_id, item_type, item_key, note, source_ref,
            confidence, source_reliability, information_credibility, claim_type, citation_url, observed_on,
            updated_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            str(uuid.uuid4()),
            actor_id,
            item_type,
            item_key,
            note,
            source_ref,
            confidence,
            source_reliability,
            information_credibility,
            claim_type,
            citation_url,
            observed_on,
            updated_by,
            updated_at,
        ),
    )


def fetch_analyst_observations(
    actor_id: str,
    *,
    analyst: str | None = None,
    confidence: str | None = None,
    updated_from: str | None = None,
    updated_to: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    db_path,
    actor_exists,
) -> list[dict[str, object]]:
    normalized_filters = observation_service.normalize_observation_filters_core(
        analyst=analyst,
        confidence=confidence,
        updated_from=updated_from,
        updated_to=updated_to,
    )

    # Build fully static parameterized query — no string concatenation from user input.
    # Optional filters use the IS NULL sentinel so the query text is always identical.
    analyst_param = f'%{normalized_filters["analyst"]}%' if normalized_filters.get('analyst') else None
    confidence_param = normalized_filters.get('confidence') or None
    from_param = normalized_filters.get('updated_from') or None
    to_param = normalized_filters.get('updated_to') or None

    try:
        safe_limit: int = max(1, min(500, int(limit))) if limit is not None else -1
    except Exception:
        safe_limit = 100
    try:
        safe_offset = max(0, int(offset))
    except Exception:
        safe_offset = 0

    with sqlite3.connect(db_path()) as connection:
        if not actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')
        rows = connection.execute(
            '''
            SELECT item_type, item_key, note, source_ref, confidence,
                   source_reliability, information_credibility, claim_type, citation_url, observed_on,
                   updated_by, updated_at
            FROM analyst_observations
            WHERE actor_id = ?
              AND (? IS NULL OR LOWER(updated_by) LIKE ?)
              AND (? IS NULL OR confidence = ?)
              AND (? IS NULL OR substr(updated_at, 1, 10) >= ?)
              AND (? IS NULL OR substr(updated_at, 1, 10) <= ?)
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
            ''',
            (
                actor_id,
                analyst_param, analyst_param,        # ? IS NULL OR LOWER(updated_by) LIKE ?
                confidence_param, confidence_param,  # ? IS NULL OR confidence = ?
                from_param, from_param,              # ? IS NULL OR substr(updated_at, 1, 10) >= ?
                to_param, to_param,                  # ? IS NULL OR substr(updated_at, 1, 10) <= ?
                safe_limit, safe_offset,
            ),
        ).fetchall()
        source_keys = observation_service.observation_source_keys_core(rows)
        source_lookup: dict[str, dict[str, str]] = {}
        if source_keys:
            for key_chunk in observation_service.source_lookup_chunks_core(source_keys, chunk_size=800):
                placeholders = ','.join('?' for _ in key_chunk)
                source_rows = connection.execute(
                    f'''
                    SELECT id, source_name, url, title, published_at, retrieved_at
                    FROM sources
                    WHERE actor_id = ? AND id IN ({placeholders})
                    ''',
                    (actor_id, *key_chunk),
                ).fetchall()
                source_lookup.update(
                    {
                        str(source_row[0]): {
                            'source_name': str(source_row[1] or ''),
                            'source_url': str(source_row[2] or ''),
                            'source_title': str(source_row[3] or ''),
                            'source_date': str(source_row[4] or source_row[5] or ''),
                        }
                        for source_row in source_rows
                    }
                )
    return observation_service.map_observation_rows_core(rows, source_lookup=source_lookup)
