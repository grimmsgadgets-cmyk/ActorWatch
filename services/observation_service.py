from datetime import date
from typing import Sequence


def normalize_observation_filters_core(
    *,
    analyst: str | None,
    confidence: str | None,
    updated_from: str | None,
    updated_to: str | None,
) -> dict[str, str]:
    analyst_text = str(analyst or '').strip().lower()

    confidence_value = str(confidence or '').strip().lower()
    if confidence_value not in {'low', 'moderate', 'high'}:
        confidence_value = ''

    from_value = str(updated_from or '').strip()
    normalized_from = ''
    if from_value:
        try:
            normalized_from = date.fromisoformat(from_value).isoformat()
        except ValueError:
            normalized_from = ''

    to_value = str(updated_to or '').strip()
    normalized_to = ''
    if to_value:
        try:
            normalized_to = date.fromisoformat(to_value).isoformat()
        except ValueError:
            normalized_to = ''

    return {
        'analyst': analyst_text,
        'confidence': confidence_value,
        'updated_from': normalized_from,
        'updated_to': normalized_to,
    }


def build_observation_where_clause_core(
    actor_id: str,
    *,
    filters: dict[str, str],
) -> tuple[str, list[object]]:
    where_clauses = ['actor_id = ?']
    params: list[object] = [actor_id]

    analyst_text = str(filters.get('analyst', '')).strip().lower()
    if analyst_text:
        where_clauses.append('LOWER(updated_by) LIKE ?')
        params.append(f'%{analyst_text}%')

    confidence_value = str(filters.get('confidence', '')).strip().lower()
    if confidence_value:
        where_clauses.append('confidence = ?')
        params.append(confidence_value)

    from_value = str(filters.get('updated_from', '')).strip()
    if from_value:
        where_clauses.append('substr(updated_at, 1, 10) >= ?')
        params.append(from_value)

    to_value = str(filters.get('updated_to', '')).strip()
    if to_value:
        where_clauses.append('substr(updated_at, 1, 10) <= ?')
        params.append(to_value)

    return (' AND '.join(where_clauses), params)


def observation_source_keys_core(rows: Sequence[tuple[object, ...]]) -> list[str]:
    return sorted(
        {
            str(row[1])
            for row in rows
            if str(row[0] or '').strip().lower() == 'source' and str(row[1] or '').strip()
        }
    )


def source_lookup_chunks_core(source_keys: Sequence[str], *, chunk_size: int = 800) -> list[list[str]]:
    safe_chunk_size = max(1, int(chunk_size))
    keys = list(source_keys)
    return [keys[idx: idx + safe_chunk_size] for idx in range(0, len(keys), safe_chunk_size)]


def observation_quality_guidance_core(
    *,
    note: str,
    source_ref: str,
    confidence: str,
    source_reliability: str,
    information_credibility: str,
    claim_type: str = 'assessment',
    citation_url: str = '',
    observed_on: str = '',
) -> list[str]:
    cleaned_note = ' '.join(str(note or '').split())
    lowered_note = cleaned_note.lower()
    confidence_value = str(confidence or '').strip().lower()
    source_ref_value = str(source_ref or '').strip()
    source_reliability_value = str(source_reliability or '').strip().upper()
    info_credibility_value = str(information_credibility or '').strip()
    claim_type_value = str(claim_type or 'assessment').strip().lower()
    citation_url_value = str(citation_url or '').strip()
    observed_on_value = str(observed_on or '').strip()

    guidance: list[str] = []
    if confidence_value == 'high' and not source_ref_value:
        guidance.append('High confidence should include a source reference (case/report/ticket id).')

    if claim_type_value == 'evidence' and not citation_url_value:
        guidance.append('Evidence-backed claims should include a citation URL.')
    if claim_type_value == 'evidence' and not observed_on_value:
        guidance.append('Evidence-backed claims should include an observed date (YYYY-MM-DD).')

    if confidence_value == 'high' and (not source_reliability_value or not info_credibility_value):
        guidance.append('High confidence should include source reliability (A-F) and information credibility (1-6).')

    words = [token for token in lowered_note.split(' ') if token]
    if not words:
        guidance.append('Add a short note describing what changed versus prior assessment.')
    elif len(words) < 6:
        guidance.append('Add one concrete detail (behavior, target, or timeframe) to reduce ambiguity.')

    vague_phrases = (
        'looks bad',
        'suspicious activity',
        'monitor this',
        'needs review',
        'check this',
        'watch this',
    )
    if lowered_note and len(words) <= 12 and any(phrase in lowered_note for phrase in vague_phrases):
        guidance.append('Replace vague wording with one evidence-backed observation from this source.')

    rationale_tokens = ('because', 'since', 'due to', 'based on', 'confirmed', 'observed', 'evidence')
    if confidence_value in {'high', 'moderate'} and lowered_note and not any(token in lowered_note for token in rationale_tokens):
        guidance.append('Add a short confidence rationale (for example: based on two corroborating reports).')

    return guidance[:3]


def map_observation_rows_core(
    rows: Sequence[tuple[object, ...]],
    *,
    source_lookup: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    def _row_value(row: tuple[object, ...], index: int, default: object = '') -> object:
        return row[index] if index < len(row) else default

    def _mapped_row(row: tuple[object, ...]) -> dict[str, object]:
        item_type = _row_value(row, 0, '')
        item_key = _row_value(row, 1, '')
        note = _row_value(row, 2, '')
        source_ref = _row_value(row, 3, '')
        confidence = _row_value(row, 4, 'moderate')
        source_reliability = _row_value(row, 5, '')
        information_credibility = _row_value(row, 6, '')
        # Backward-compatible shape support:
        # old rows had: updated_by, updated_at at positions 7,8.
        # new rows have: claim_type, citation_url, observed_on, updated_by, updated_at at 7..11.
        if len(row) >= 12:
            claim_type = _row_value(row, 7, 'assessment')
            citation_url = _row_value(row, 8, '')
            observed_on = _row_value(row, 9, '')
            updated_by = _row_value(row, 10, '')
            updated_at = _row_value(row, 11, '')
        else:
            claim_type = 'assessment'
            citation_url = ''
            observed_on = ''
            updated_by = _row_value(row, 7, '')
            updated_at = _row_value(row, 8, '')
        return {
            'item_type': item_type,
            'item_key': item_key,
            'note': note or '',
            'source_ref': source_ref or '',
            'confidence': confidence or 'moderate',
            'source_reliability': source_reliability or '',
            'information_credibility': information_credibility or '',
            'claim_type': claim_type or 'assessment',
            'citation_url': citation_url or '',
            'observed_on': observed_on or '',
            'updated_by': updated_by or '',
            'updated_at': updated_at or '',
            'quality_guidance': observation_quality_guidance_core(
                note=str(note or ''),
                source_ref=str(source_ref or ''),
                confidence=str(confidence or 'moderate'),
                source_reliability=str(source_reliability or ''),
                information_credibility=str(information_credibility or ''),
                claim_type=str(claim_type or 'assessment'),
                citation_url=str(citation_url or ''),
                observed_on=str(observed_on or ''),
            ),
            'source_name': source_lookup.get(str(item_key), {}).get('source_name', ''),
            'source_url': source_lookup.get(str(item_key), {}).get('source_url', ''),
            'source_title': source_lookup.get(str(item_key), {}).get('source_title', ''),
            'source_date': source_lookup.get(str(item_key), {}).get('source_date', ''),
        }

    return [
        _mapped_row(row)
        for row in rows
    ]
