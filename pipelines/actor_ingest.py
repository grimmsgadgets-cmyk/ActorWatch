import hashlib
import sqlite3
from collections.abc import Callable


def source_fingerprint(
    title: str | None,
    headline: str | None,
    og_title: str | None,
    html_title: str | None,
    pasted_text: str,
    *,
    normalize_text: Callable[[str], str],
    first_sentences: Callable[[str, int], str],
) -> str:
    title_candidate = (
        str(title or '').strip()
        or str(headline or '').strip()
        or str(og_title or '').strip()
        or str(html_title or '').strip()
    )
    normalized_title = normalize_text(title_candidate)[:220]
    excerpt = first_sentences(pasted_text or '', 2)
    normalized_excerpt = normalize_text(excerpt)[:420]
    if not normalized_title and not normalized_excerpt:
        return ''
    raw = f'{normalized_title}|{normalized_excerpt}'
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def upsert_source_for_actor(
    connection: sqlite3.Connection,
    actor_id: str,
    source_name: str,
    source_url: str,
    published_at: str | None,
    pasted_text: str,
    trigger_excerpt: str | None = None,
    title: str | None = None,
    headline: str | None = None,
    og_title: str | None = None,
    html_title: str | None = None,
    publisher: str | None = None,
    site_name: str | None = None,
    source_tier: str | None = None,
    confidence_weight: int | None = None,
    overwrite_source_quality: bool = False,
    refresh_existing_content: bool = False,
    *,
    build_fingerprint: Callable[[str | None, str | None, str | None, str | None, str], str],
    new_id: Callable[[], str],
    now_iso: Callable[[], str],
) -> str:
    fingerprint = build_fingerprint(title, headline, og_title, html_title, pasted_text)
    final_text = pasted_text
    if trigger_excerpt and trigger_excerpt not in final_text:
        final_text = f'{trigger_excerpt}\n\n{pasted_text}'
    existing_rows = connection.execute(
        '''
        SELECT id
        FROM sources
        WHERE actor_id = ? AND url = ?
        ORDER BY COALESCE(published_at, retrieved_at) DESC, retrieved_at DESC, id DESC
        ''',
        (actor_id, source_url),
    ).fetchall()
    if existing_rows:
        existing_id = str(existing_rows[0][0])
        duplicate_ids = [str(row[0]) for row in existing_rows[1:]]
        for duplicate_id in duplicate_ids:
            connection.execute(
                'UPDATE timeline_events SET source_id = ? WHERE source_id = ?',
                (existing_id, duplicate_id),
            )
            connection.execute(
                'UPDATE question_updates SET source_id = ? WHERE source_id = ?',
                (existing_id, duplicate_id),
            )
            connection.execute(
                'DELETE FROM sources WHERE id = ?',
                (duplicate_id,),
            )
        if refresh_existing_content:
            title_value = str(title or '').strip() or None
            headline_value = str(headline or '').strip() or None
            og_title_value = str(og_title or '').strip() or None
            html_title_value = str(html_title or '').strip() or None
            publisher_value = str(publisher or '').strip() or None
            site_name_value = str(site_name or '').strip() or None
            source_tier_value = str(source_tier or '').strip() or None
            connection.execute(
                '''
                UPDATE sources
                SET pasted_text = CASE
                        WHEN length(COALESCE(?, '')) >= 120 THEN ?
                        WHEN length(COALESCE(pasted_text, '')) = 0 THEN ?
                        ELSE pasted_text
                    END,
                    published_at = COALESCE(?, published_at),
                    retrieved_at = ?,
                    title = COALESCE(?, title),
                    headline = COALESCE(?, headline),
                    og_title = COALESCE(?, og_title),
                    html_title = COALESCE(?, html_title),
                    publisher = COALESCE(?, publisher),
                    site_name = COALESCE(?, site_name),
                    source_tier = COALESCE(?, source_tier)
                WHERE id = ?
                ''',
                (
                    final_text,
                    final_text,
                    final_text,
                    published_at,
                    now_iso(),
                    title_value,
                    headline_value,
                    og_title_value,
                    html_title_value,
                    publisher_value,
                    site_name_value,
                    source_tier_value,
                    existing_id,
                ),
            )
        else:
            metadata_values = [title, headline, og_title, html_title, publisher, site_name, source_tier]
            if any(str(value or '').strip() for value in metadata_values):
                if overwrite_source_quality:
                    connection.execute(
                        '''
                        UPDATE sources
                        SET title = COALESCE(NULLIF(title, ''), ?),
                            headline = COALESCE(NULLIF(headline, ''), ?),
                            og_title = COALESCE(NULLIF(og_title, ''), ?),
                            html_title = COALESCE(NULLIF(html_title, ''), ?),
                            publisher = COALESCE(NULLIF(publisher, ''), ?),
                            site_name = COALESCE(NULLIF(site_name, ''), ?),
                            source_tier = COALESCE(NULLIF(?, ''), source_tier)
                        WHERE id = ?
                        ''',
                        (
                            str(title or '').strip() or None,
                            str(headline or '').strip() or None,
                            str(og_title or '').strip() or None,
                            str(html_title or '').strip() or None,
                            str(publisher or '').strip() or None,
                            str(site_name or '').strip() or None,
                            str(source_tier or '').strip() or None,
                            existing_id,
                        ),
                    )
                else:
                    connection.execute(
                        '''
                        UPDATE sources
                        SET title = COALESCE(NULLIF(title, ''), ?),
                            headline = COALESCE(NULLIF(headline, ''), ?),
                            og_title = COALESCE(NULLIF(og_title, ''), ?),
                            html_title = COALESCE(NULLIF(html_title, ''), ?),
                            publisher = COALESCE(NULLIF(publisher, ''), ?),
                            site_name = COALESCE(NULLIF(site_name, ''), ?),
                            source_tier = COALESCE(NULLIF(source_tier, ''), ?)
                        WHERE id = ?
                        ''',
                        (
                            str(title or '').strip() or None,
                            str(headline or '').strip() or None,
                            str(og_title or '').strip() or None,
                            str(html_title or '').strip() or None,
                            str(publisher or '').strip() or None,
                            str(site_name or '').strip() or None,
                            str(source_tier or '').strip() or None,
                            existing_id,
                        ),
                    )
        if confidence_weight is not None:
            if overwrite_source_quality:
                connection.execute(
                    '''
                    UPDATE sources
                    SET confidence_weight = ?
                    WHERE id = ?
                    ''',
                    (int(confidence_weight), existing_id),
                )
            else:
                connection.execute(
                    '''
                    UPDATE sources
                    SET confidence_weight = COALESCE(confidence_weight, ?)
                    WHERE id = ?
                    ''',
                    (int(confidence_weight), existing_id),
                )
        if fingerprint:
            if refresh_existing_content:
                connection.execute(
                    '''
                    UPDATE sources
                    SET source_fingerprint = ?
                    WHERE id = ?
                    ''',
                    (fingerprint, existing_id),
                )
            else:
                connection.execute(
                    '''
                    UPDATE sources
                    SET source_fingerprint = COALESCE(NULLIF(source_fingerprint, ''), ?)
                    WHERE id = ?
                    ''',
                    (fingerprint, existing_id),
                )
        return existing_id

    if fingerprint:
        fingerprint_existing = connection.execute(
            '''
            SELECT id
            FROM sources
            WHERE actor_id = ? AND source_fingerprint = ?
            LIMIT 1
            ''',
            (actor_id, fingerprint),
        ).fetchone()
        if fingerprint_existing is not None:
            return str(fingerprint_existing[0])

    source_id = new_id()
    connection.execute(
        '''
        INSERT INTO sources (
            id, actor_id, source_name, url, published_at, retrieved_at, pasted_text,
            source_fingerprint, title, headline, og_title, html_title, publisher, site_name,
            source_tier, confidence_weight
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            source_id,
            actor_id,
            source_name,
            source_url,
            published_at,
            now_iso(),
            final_text,
            fingerprint or None,
            str(title or '').strip() or None,
            str(headline or '').strip() or None,
            str(og_title or '').strip() or None,
            str(html_title or '').strip() or None,
            str(publisher or '').strip() or None,
            str(site_name or '').strip() or None,
            str(source_tier or '').strip() or None,
            int(confidence_weight) if confidence_weight is not None else None,
        ),
    )
    return source_id
