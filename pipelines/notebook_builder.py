import json
import sqlite3
import uuid
from collections.abc import Callable

from fastapi import HTTPException


def build_notebook_core(
    actor_id: str,
    *,
    db_path: str,
    generate_questions: bool,
    rebuild_timeline: bool,
    now_iso: Callable[[], str],
    actor_exists: Callable[[sqlite3.Connection, str], bool],
    build_actor_profile_from_mitre: Callable[[str], dict[str, object]],
    actor_terms_fn: Callable[[str, str, str], list[str]],
    extract_major_move_events: Callable[[str, str, str, str, list[str], str | None], list[dict[str, object]]],
    normalize_text: Callable[[str], str],
    token_overlap: Callable[[str, str], float],
    extract_question_sentences: Callable[[str], list[str]],
    sentence_mentions_actor_terms: Callable[[str, list[str]], bool],
    sanitize_question_text: Callable[[str], str],
    question_from_sentence: Callable[[str], str],
    ollama_generate_questions: Callable[[str, str | None, list[str]], list[str]],
    platforms_for_question: Callable[[str], list[str]],
    guidance_for_platform: Callable[[str, str], dict[str, str]],
) -> None:
    now = now_iso()
    with sqlite3.connect(db_path) as connection:
        if not actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')

        actor_row = connection.execute(
            'SELECT display_name, scope_statement FROM actor_profiles WHERE id = ?',
            (actor_id,),
        ).fetchone()
        actor_name = actor_row[0] if actor_row else 'actor'
        actor_scope = actor_row[1] if actor_row else None
        mitre_profile = build_actor_profile_from_mitre(actor_name)
        actor_terms = actor_terms_fn(
            actor_name,
            str(mitre_profile.get('group_name') or ''),
            str(mitre_profile.get('aliases_csv') or ''),
        )

        sources = connection.execute(
            '''
            SELECT id, source_name, url, published_at, retrieved_at, pasted_text, title, headline, og_title, html_title
            FROM sources
            WHERE actor_id = ?
            ORDER BY retrieved_at ASC
            ''',
            (actor_id,),
        ).fetchall()

        if rebuild_timeline:
            connection.execute('DELETE FROM timeline_events WHERE actor_id = ?', (actor_id,))
            timeline_candidates: list[dict[str, object]] = []
            for source in sources:
                occurred_at = source[3] or source[4]
                text = source[5] or ''
                source_title = str(source[6] or source[7] or source[8] or source[9] or '').strip() or None
                moves = extract_major_move_events(source[1], source[0], occurred_at, text, actor_terms, source_title)
                if moves:
                    timeline_candidates.extend(moves[:6])

            deduped_timeline: list[dict[str, object]] = []
            seen_summaries: list[str] = []
            for event in sorted(timeline_candidates, key=lambda item: str(item['occurred_at'])):
                norm = normalize_text(str(event['summary']))
                if any(token_overlap(norm, existing) >= 0.75 for existing in seen_summaries):
                    continue
                deduped_timeline.append(event)
                seen_summaries.append(norm)

            for event in deduped_timeline:
                connection.execute(
                    '''
                    INSERT INTO timeline_events (
                        id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        str(event['id']),
                        actor_id,
                        str(event['occurred_at']),
                        str(event['category']),
                        str(event['title']),
                        str(event['summary']),
                        str(event['source_id']),
                        str(event.get('target_text') or ''),
                        json.dumps(event.get('ttp_ids') or []),
                    ),
                )

        if not generate_questions:
            connection.commit()
            return

        thread_rows = connection.execute(
            '''
            SELECT id, question_text, status, created_at, updated_at
            FROM question_threads
            WHERE actor_id = ?
            ORDER BY created_at ASC
            ''',
            (actor_id,),
        ).fetchall()
        thread_cache: list[dict[str, str]] = [
            {
                'id': row[0],
                'question_text': row[1],
                'status': row[2],
                'created_at': row[3],
                'updated_at': row[4],
            }
            for row in thread_rows
        ]

        source_sentence_records: list[dict[str, str]] = []
        for source in sources:
            source_id = source[0]
            text = source[5] or ''
            for sentence in extract_question_sentences(text):
                if actor_terms and not sentence_mentions_actor_terms(sentence, actor_terms):
                    continue
                source_sentence_records.append(
                    {
                        'source_id': source_id,
                        'sentence': sentence,
                        'question_text': sanitize_question_text(question_from_sentence(sentence)),
                    }
                )

        llm_candidates = ollama_generate_questions(
            actor_name,
            actor_scope,
            [record['sentence'] for record in source_sentence_records],
        )
        for candidate in llm_candidates:
            best_sentence = None
            best_source = None
            best_score = 0.0
            for record in source_sentence_records:
                score = token_overlap(candidate, record['sentence'])
                if score > best_score:
                    best_score = score
                    best_sentence = record['sentence']
                    best_source = record['source_id']
            if best_sentence and best_source and best_score >= 0.20:
                source_sentence_records.append(
                    {
                        'source_id': best_source,
                        'sentence': best_sentence,
                        'question_text': candidate,
                    }
                )

        for record in source_sentence_records:
            source_id = record['source_id']
            sentence = record['sentence']
            question_text = record['question_text']
            best_thread: dict[str, str] | None = None
            best_score = 0.0
            for candidate in thread_cache:
                score = token_overlap(question_text, candidate['question_text'])
                if score > best_score:
                    best_score = score
                    best_thread = candidate

            if best_thread is not None and best_score >= 0.45:
                thread_id = best_thread['id']
                connection.execute(
                    'UPDATE question_threads SET updated_at = ? WHERE id = ?',
                    (now, thread_id),
                )
            else:
                thread_id = str(uuid.uuid4())
                connection.execute(
                    '''
                    INSERT INTO question_threads (
                        id, actor_id, question_text, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, 'open', ?, ?)
                    ''',
                    (thread_id, actor_id, question_text, now, now),
                )
                thread_cache.append(
                    {
                        'id': thread_id,
                        'question_text': question_text,
                        'status': 'open',
                        'created_at': now,
                        'updated_at': now,
                    }
                )

            existing_update = connection.execute(
                '''
                SELECT id
                FROM question_updates
                WHERE thread_id = ? AND source_id = ? AND trigger_excerpt = ?
                ''',
                (thread_id, source_id, sentence),
            ).fetchone()
            if existing_update is None:
                connection.execute(
                    '''
                    INSERT INTO question_updates (
                        id, thread_id, source_id, trigger_excerpt, update_note, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (str(uuid.uuid4()), thread_id, source_id, sentence, None, now),
                )

        connection.execute('DELETE FROM environment_guidance WHERE actor_id = ?', (actor_id,))
        open_threads = connection.execute(
            '''
            SELECT id, question_text
            FROM question_threads
            WHERE actor_id = ? AND status = 'open'
            ORDER BY created_at ASC
            ''',
            (actor_id,),
        ).fetchall()
        for thread in open_threads:
            thread_id = thread[0]
            question_text = thread[1]
            for platform in platforms_for_question(question_text):
                guidance = guidance_for_platform(platform, question_text)
                connection.execute(
                    '''
                    INSERT INTO environment_guidance (
                        id, actor_id, thread_id, platform,
                        what_to_look_for, where_to_look, query_hint, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        str(uuid.uuid4()),
                        actor_id,
                        thread_id,
                        guidance['platform'],
                        guidance['what_to_look_for'],
                        guidance['where_to_look'],
                        guidance['query_hint'],
                        now,
                    ),
                )

        connection.commit()
