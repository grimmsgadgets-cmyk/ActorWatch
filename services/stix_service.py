import re
import uuid
from datetime import datetime, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _indicator_pattern(ioc_type: str, ioc_value: str) -> str | None:
    value = ioc_value.replace("'", "\\'")
    lowered = ioc_type.strip().lower()
    if lowered == 'domain':
        return f"[domain-name:value = '{value}']"
    if lowered == 'ipv4':
        return f"[ipv4-addr:value = '{value}']"
    if lowered == 'ipv6':
        return f"[ipv6-addr:value = '{value}']"
    if lowered in {'url', 'uri'}:
        return f"[url:value = '{value}']"
    if lowered == 'email':
        return f"[email-addr:value = '{value}']"
    if lowered in {'md5', 'sha1', 'sha256', 'hash'}:
        return f"[file:hashes.'{lowered if lowered != 'hash' else 'SHA-256'}' = '{value}']"
    return None


def _extract_indicator_from_pattern(pattern: str) -> tuple[str, str] | None:
    text = str(pattern or '').strip()
    checks = [
        (r"^\[domain-name:value = '([^']+)'\]$", 'domain'),
        (r"^\[ipv4-addr:value = '([^']+)'\]$", 'ipv4'),
        (r"^\[ipv6-addr:value = '([^']+)'\]$", 'ipv6'),
        (r"^\[url:value = '([^']+)'\]$", 'url'),
        (r"^\[email-addr:value = '([^']+)'\]$", 'email'),
        (r"^\[file:hashes\.'(?:MD5|SHA-1|SHA-256|md5|sha1|sha256)' = '([^']+)'\]$", 'hash'),
    ]
    for regex, ioc_type in checks:
        match = re.fullmatch(regex, text)
        if match:
            return (ioc_type, str(match.group(1)))
    return None


def _confidence_score_from_stix(confidence: object) -> int:
    try:
        value = int(confidence)
    except Exception:
        return 0
    if value >= 80:
        return 5
    if value >= 60:
        return 4
    if value >= 40:
        return 3
    if value >= 20:
        return 2
    return 1


def export_actor_bundle_core(connection, *, actor_id: str, actor_name: str) -> dict[str, object]:
    now = _utc_now()
    identity_id = f'identity--{uuid.uuid4()}'
    intrusion_set_id = f'intrusion-set--{uuid.uuid4()}'
    objects: list[dict[str, object]] = [
        {
            'type': 'identity',
            'spec_version': '2.1',
            'id': identity_id,
            'created': now,
            'modified': now,
            'name': 'ActorWatch Community',
            'identity_class': 'organization',
        },
        {
            'type': 'intrusion-set',
            'spec_version': '2.1',
            'id': intrusion_set_id,
            'created': now,
            'modified': now,
            'name': actor_name,
            'description': f'Actor profile exported from ActorWatch community edition for {actor_name}.',
            'created_by_ref': identity_id,
        },
    ]

    ioc_rows = connection.execute(
        '''
        SELECT id, ioc_type, ioc_value, confidence_score, source_ref, first_seen_at, last_seen_at
        FROM ioc_items
        WHERE actor_id = ?
        ORDER BY last_seen_at DESC, created_at DESC
        ''',
        (actor_id,),
    ).fetchall()
    for row in ioc_rows:
        pattern = _indicator_pattern(str(row[1] or ''), str(row[2] or ''))
        if not pattern:
            continue
        created = str(row[5] or now).replace('+00:00', 'Z')
        modified = str(row[6] or row[5] or now).replace('+00:00', 'Z')
        source_ref = str(row[4] or '').strip()
        indicator_id = f"indicator--{uuid.uuid5(uuid.NAMESPACE_URL, f'{actor_id}:{row[0]}')}"
        objects.append(
            {
                'type': 'indicator',
                'spec_version': '2.1',
                'id': indicator_id,
                'created': created,
                'modified': modified,
                'name': f'{actor_name} IOC {str(row[1] or "").upper()}',
                'description': source_ref or f'IOC associated with {actor_name}.',
                'indicator_types': ['malicious-activity'],
                'pattern': pattern,
                'pattern_type': 'stix',
                'valid_from': created,
                'created_by_ref': identity_id,
                'confidence': max(0, min(100, int(row[3] or 0) * 20)),
                'labels': ['actorwatch', 'community-edition'],
                'object_marking_refs': ['marking-definition--613f2e26-407d-48c7-9eca-b8e91df99dc9'],
            }
        )
        objects.append(
            {
                'type': 'relationship',
                'spec_version': '2.1',
                'id': f'relationship--{uuid.uuid4()}',
                'created': now,
                'modified': now,
                'relationship_type': 'indicates',
                'source_ref': indicator_id,
                'target_ref': intrusion_set_id,
                'created_by_ref': identity_id,
            }
        )

    note_rows = connection.execute(
        '''
        SELECT id, item_type, item_key, note, confidence, source_ref, updated_at
        FROM analyst_observations
        WHERE actor_id = ? AND TRIM(COALESCE(note, '')) != ''
        ORDER BY updated_at DESC
        LIMIT 500
        ''',
        (actor_id,),
    ).fetchall()
    for row in note_rows:
        objects.append(
            {
                'type': 'note',
                'spec_version': '2.1',
                'id': f"note--{uuid.uuid5(uuid.NAMESPACE_URL, f'{actor_id}:{row[0]}')}",
                'created': str(row[6] or now).replace('+00:00', 'Z'),
                'modified': str(row[6] or now).replace('+00:00', 'Z'),
                'created_by_ref': identity_id,
                'object_refs': [intrusion_set_id],
                'content': str(row[3] or ''),
                'abstract': f"{str(row[1] or 'observation')}:{str(row[2] or '')}",
                'labels': ['analyst-observation', str(row[4] or 'moderate')],
                'external_references': (
                    [{'source_name': 'source_ref', 'description': str(row[5] or '')}]
                    if str(row[5] or '').strip()
                    else []
                ),
            }
        )

    return {
        'type': 'bundle',
        'id': f'bundle--{uuid.uuid4()}',
        'spec_version': '2.1',
        'objects': objects,
    }


def import_actor_bundle_core(
    connection,
    *,
    actor_id: str,
    bundle: dict[str, object],
    now_iso: str,
    upsert_ioc_item,
) -> dict[str, int]:
    objects = bundle.get('objects')
    if not isinstance(objects, list):
        return {'imported_iocs': 0, 'imported_notes': 0, 'skipped': 0}

    imported_iocs = 0
    imported_notes = 0
    skipped = 0
    for item in objects:
        if not isinstance(item, dict):
            skipped += 1
            continue
        stix_type = str(item.get('type') or '').strip().lower()
        if stix_type == 'indicator':
            parsed = _extract_indicator_from_pattern(str(item.get('pattern') or ''))
            if parsed is None:
                skipped += 1
                continue
            ioc_type, ioc_value = parsed
            confidence_score = _confidence_score_from_stix(item.get('confidence'))
            result = upsert_ioc_item(
                connection,
                actor_id=actor_id,
                raw_ioc_type=ioc_type,
                raw_ioc_value=ioc_value,
                source_ref=str(item.get('id') or ''),
                source_id=None,
                source_tier='high',
                extraction_method='stix-import',
                now_iso=now_iso,
                lifecycle_status='active',
                handling_tlp='TLP:CLEAR',
                confidence_score_override=confidence_score,
                observed_at=str(item.get('valid_from') or now_iso),
                valid_from=str(item.get('valid_from') or ''),
                valid_until=str(item.get('valid_until') or ''),
                revoked=bool(item.get('revoked')),
            )
            if bool(result.get('stored')):
                imported_iocs += 1
            else:
                skipped += 1
            continue
        if stix_type == 'note':
            content = str(item.get('content') or '').strip()
            if not content:
                skipped += 1
                continue
            note_key = str(item.get('id') or f'note--{uuid.uuid4()}')
            abstract = str(item.get('abstract') or 'stix-note').strip()[:200]
            source_ref = ''
            ext_refs = item.get('external_references')
            if isinstance(ext_refs, list):
                for ref in ext_refs:
                    if isinstance(ref, dict) and str(ref.get('description') or '').strip():
                        source_ref = str(ref.get('description') or '').strip()[:500]
                        break
            connection.execute(
                '''
                INSERT INTO analyst_observations (
                    id, actor_id, item_type, item_key, note, source_ref, confidence,
                    source_reliability, information_credibility, updated_by, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(actor_id, item_type, item_key)
                DO UPDATE SET
                    note = excluded.note,
                    source_ref = excluded.source_ref,
                    confidence = excluded.confidence,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                ''',
                (
                    str(uuid.uuid4()),
                    actor_id,
                    'stix-note',
                    note_key[:200],
                    content[:4000],
                    source_ref,
                    'moderate',
                    '',
                    '',
                    f'stix-import:{abstract[:80]}',
                    now_iso,
                ),
            )
            connection.execute(
                '''
                INSERT INTO analyst_observation_history (
                    id, actor_id, item_type, item_key, note, source_ref, confidence,
                    source_reliability, information_credibility, updated_by, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    str(uuid.uuid4()),
                    actor_id,
                    'stix-note',
                    note_key[:200],
                    content[:4000],
                    source_ref,
                    'moderate',
                    '',
                    '',
                    f'stix-import:{abstract[:80]}',
                    now_iso,
                ),
            )
            imported_notes += 1
            continue
        skipped += 1

    return {
        'imported_iocs': imported_iocs,
        'imported_notes': imported_notes,
        'skipped': skipped,
    }
