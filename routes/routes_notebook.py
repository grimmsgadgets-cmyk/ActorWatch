import io
import sqlite3
import uuid
import csv
import zlib
import re

import services.observation_service as observation_service
import route_paths
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response


def create_notebook_router(*, deps: dict[str, object]) -> APIRouter:
    router = APIRouter()

    _enforce_request_size = deps['enforce_request_size']
    _default_body_limit_bytes = deps['default_body_limit_bytes']
    _generate_actor_requirements = deps['generate_actor_requirements']
    _db_path = deps['db_path']
    _utc_now_iso = deps['utc_now_iso']
    _safe_json_string_list = deps['safe_json_string_list']
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _templates = deps['templates']
    _actor_exists = deps['actor_exists']
    _generate_ioc_hunt_queries = deps['generate_ioc_hunt_queries']
    _get_ollama_status = deps['get_ollama_status']
    _store_feedback_event = deps['store_feedback_event']
    _feedback_summary_for_actor = deps['feedback_summary_for_actor']
    _normalize_environment_profile = deps['normalize_environment_profile']
    _upsert_environment_profile = deps['upsert_environment_profile']
    _load_environment_profile = deps['load_environment_profile']
    _apply_feedback_to_source_domains = deps['apply_feedback_to_source_domains']

    def _pdf_escape_text(value: str) -> str:
        return str(value or '').replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')

    def _render_simple_text_pdf(*, title: str, lines: list[str]) -> bytes:
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

    def _build_analyst_pack_payload(
        actor_id: str,
        *,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 1000,
        history_limit: int = 1000,
    ) -> dict[str, object]:
        safe_observations_limit = max(1, min(5000, int(observations_limit)))
        safe_history_limit = max(1, min(5000, int(history_limit)))
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        observations = _fetch_analyst_observations(actor_id, limit=safe_observations_limit, offset=0)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            history_rows = connection.execute(
                '''
                SELECT item_type, item_key, note, source_ref, confidence,
                       source_reliability, information_credibility, updated_by, updated_at
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
                'updated_by': str(row[7] or ''),
                'updated_at': str(row[8] or ''),
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
            'exported_at': _utc_now_iso(),
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

    def _ioc_value_is_hunt_relevant(ioc_type: str, ioc_value: str) -> bool:
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

    def _upsert_observation_with_history(
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
        updated_by: str,
        updated_at: str,
    ) -> None:
        connection.execute(
            '''
            INSERT INTO analyst_observations (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(actor_id, item_type, item_key)
            DO UPDATE SET
                note = excluded.note,
                source_ref = excluded.source_ref,
                confidence = excluded.confidence,
                source_reliability = excluded.source_reliability,
                information_credibility = excluded.information_credibility,
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
                updated_by,
                updated_at,
            ),
        )
        connection.execute(
            '''
            INSERT INTO analyst_observation_history (
                id, actor_id, item_type, item_key, note, source_ref,
                confidence, source_reliability, information_credibility,
                updated_by, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                updated_by,
                updated_at,
            ),
        )

    def _fetch_analyst_observations(
        actor_id: str,
        *,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        normalized_filters = observation_service.normalize_observation_filters_core(
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
        )
        where_sql, params = observation_service.build_observation_where_clause_core(
            actor_id,
            filters=normalized_filters,
        )

        safe_limit: int | None = None
        if limit is not None:
            try:
                safe_limit = max(1, min(500, int(limit)))
            except Exception:
                safe_limit = 100
        try:
            safe_offset = max(0, int(offset))
        except Exception:
            safe_offset = 0

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            query = (
                '''
                SELECT item_type, item_key, note, source_ref, confidence,
                       source_reliability, information_credibility, updated_by, updated_at
                FROM analyst_observations
                WHERE '''
                + where_sql
                + '\nORDER BY updated_at DESC'
            )
            query_params: list[object] = list(params)
            if safe_limit is not None:
                query += '\nLIMIT ? OFFSET ?'
                query_params.extend([safe_limit, safe_offset])
            rows = connection.execute(query, query_params).fetchall()
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

    @router.post(route_paths.ACTOR_NOTEBOOK_REQUIREMENTS_GENERATE)
    async def generate_requirements(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        org_context = str(form_data.get('org_context', '')).strip()
        priority_mode = str(form_data.get('priority_mode', 'Operational')).strip()
        if priority_mode not in {'Strategic', 'Operational', 'Tactical'}:
            priority_mode = 'Operational'
        count = _generate_actor_requirements(actor_id, org_context, priority_mode)
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Generated+{count}+requirements',
            status_code=303,
        )

    @router.post('/requirements/{requirement_id}/resolve')
    async def resolve_requirement(requirement_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        actor_id = str(form_data.get('actor_id', '')).strip()
        with sqlite3.connect(_db_path()) as connection:
            row = connection.execute(
                'SELECT actor_id FROM requirement_items WHERE id = ?',
                (requirement_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='requirement not found')
            resolved_actor_id = str(row[0])
            connection.execute(
                'UPDATE requirement_items SET status = ? WHERE id = ?',
                ('resolved', requirement_id),
            )
            connection.commit()
        return RedirectResponse(url=f'/?actor_id={actor_id or resolved_actor_id}', status_code=303)

    @router.post('/questions/{thread_id}/resolve')
    async def resolve_question_thread(thread_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        form_data = await request.form()
        actor_id = str(form_data.get('actor_id', '')).strip()

        with sqlite3.connect(_db_path()) as connection:
            row = connection.execute(
                'SELECT actor_id, status FROM question_threads WHERE id = ?',
                (thread_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail='question thread not found')
            db_actor_id = row[0]
            if row[1] != 'resolved':
                connection.execute(
                    'UPDATE question_threads SET status = ?, updated_at = ? WHERE id = ?',
                    ('resolved', _utc_now_iso(), thread_id),
                )
            connection.commit()

        return RedirectResponse(url=f'/?actor_id={actor_id or db_actor_id}', status_code=303)

    @router.get(route_paths.ACTOR_TIMELINE_DETAILS, response_class=HTMLResponse)
    def actor_timeline_details(request: Request, actor_id: str, limit: int = 300, offset: int = 0) -> HTMLResponse:
        safe_limit = max(1, min(1000, int(limit)))
        safe_offset = max(0, int(offset))
        with sqlite3.connect(_db_path()) as connection:
            actor_row = connection.execute(
                'SELECT id, display_name FROM actor_profiles WHERE id = ?',
                (actor_id,),
            ).fetchone()
            if actor_row is None:
                raise HTTPException(status_code=404, detail='actor not found')

            rows = connection.execute(
                '''
                SELECT
                    te.occurred_at, te.category, te.title, te.summary, te.target_text, te.ttp_ids_json,
                    s.source_name, s.url, s.published_at, s.title, s.headline, s.og_title, s.html_title
                FROM timeline_events te
                LEFT JOIN sources s ON s.id = te.source_id
                WHERE te.actor_id = ?
                ORDER BY te.occurred_at DESC
                LIMIT ? OFFSET ?
                ''',
                (actor_id, safe_limit, safe_offset),
            ).fetchall()

        detail_rows: list[dict[str, object]] = []
        for row in rows:
            event_title = str(row[2] or row[9] or row[3] or '').strip()
            if event_title.startswith(('http://', 'https://')):
                event_title = str(row[3] or row[2] or 'Untitled report').strip()
            if 'who/what/when/where/how' in event_title.lower():
                fallback_title = str(row[2] or '').strip()
                if fallback_title and 'who/what/when/where/how' not in fallback_title.lower():
                    event_title = fallback_title
                else:
                    event_title = 'Ransomware disclosure and targeting update'
            detail_rows.append(
                {
                    'occurred_at': row[0],
                    'category': str(row[1]).replace('_', ' '),
                    'title': event_title,
                    'summary': row[3],
                    'target_text': row[4] or '',
                    'ttp_ids': _safe_json_string_list(row[5]),
                    'source_name': row[6] or '',
                    'source_url': row[7] or '',
                    'source_published_at': row[8] or '',
                    'source_title': row[9] or row[10] or row[11] or row[12] or '',
                }
            )
        return _templates.TemplateResponse(
            request,
            'timeline_details.html',
            {
                'actor_id': actor_id,
                'actor_name': str(actor_row[1]),
                'detail_rows': detail_rows,
                'limit': safe_limit,
                'offset': safe_offset,
            },
        )

    @router.get(route_paths.ACTOR_QUESTIONS_WORKSPACE, response_class=HTMLResponse)
    def actor_questions_workspace(
        request: Request,
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> HTMLResponse:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        return _templates.TemplateResponse(
            request,
            'questions.html',
            {
                'actor_id': actor_id,
                'notebook': notebook,
            },
        )

    @router.get(route_paths.ACTOR_IOC_HUNT_QUERIES, response_class=HTMLResponse)
    def actor_ioc_hunt_queries(
        request: Request,
        actor_id: str,
        thread_id: str | None = None,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> HTMLResponse:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        actor_name = str(actor_meta.get('display_name') or actor_id)
        actor_terms = {
            token
            for token in re.findall(r'[a-z0-9][a-z0-9._-]+', actor_name.lower())
            if len(token) >= 3
        }
        actor_terms.add(actor_id.lower())
        cards_raw = notebook.get('priority_questions', []) if isinstance(notebook, dict) else []
        cards_list = cards_raw if isinstance(cards_raw, list) else []
        if thread_id:
            cards_list = [card for card in cards_list if str(card.get('id') or '').strip() == thread_id]

        cards_for_hunts: list[dict[str, object]] = []
        environment_profile: dict[str, object] = {}
        with sqlite3.connect(_db_path()) as connection:
            environment_profile = _load_environment_profile(connection, actor_id=actor_id)
            for card in cards_list:
                if not isinstance(card, dict):
                    continue
                card_id = str(card.get('id') or '').strip()
                if not card_id:
                    continue
                related_iocs_raw = card.get('related_iocs')
                related_iocs = related_iocs_raw if isinstance(related_iocs_raw, list) else []
                if not related_iocs:
                    continue
                evidence_rows = connection.execute(
                    '''
                    SELECT qu.source_id, qu.trigger_excerpt, s.url, s.title, s.headline, s.og_title, s.html_title, s.published_at
                    FROM question_updates qu
                    JOIN sources s ON s.id = qu.source_id
                    WHERE qu.thread_id = ?
                    ORDER BY qu.created_at DESC
                    LIMIT 8
                    ''',
                    (card_id,),
                ).fetchall()
                evidence_items: list[dict[str, str]] = []
                seen_evidence_ids: set[str] = set()
                for row in evidence_rows:
                    evidence_id = str(row[0] or '').strip()
                    source_url = str(row[2] or '').strip()
                    if not evidence_id or not source_url or evidence_id in seen_evidence_ids:
                        continue
                    seen_evidence_ids.add(evidence_id)
                    evidence_items.append(
                        {
                            'id': evidence_id,
                            'source_url': source_url,
                            'source_title': str(row[3] or row[4] or row[5] or row[6] or source_url),
                            'source_date': str(row[7] or ''),
                            'excerpt': str(row[1] or '')[:320],
                        }
                    )

                if not evidence_items:
                    continue

                cards_for_hunts.append(
                    {
                        'id': card_id,
                        'quick_check_title': str(card.get('quick_check_title') or card.get('question_text') or ''),
                        'question_text': str(card.get('question_text') or ''),
                        'related_iocs': related_iocs[:8],
                        'evidence': evidence_items[:10],
                    }
                )

        hunt_payload = _generate_ioc_hunt_queries(
            actor_name,
            cards_for_hunts,
            environment_profile=environment_profile,
        )
        hunt_by_card = hunt_payload.get('items_by_card', {}) if isinstance(hunt_payload, dict) else {}
        reason = str(hunt_payload.get('reason') or '') if isinstance(hunt_payload, dict) else ''
        ollama_status = _get_ollama_status()
        card_views: list[dict[str, object]] = []
        used_ioc_pairs: set[tuple[str, str]] = set()
        actor_context_chunks: list[str] = []
        for card in cards_for_hunts:
            card_id = str(card.get('id') or '')
            evidence_items_raw = card.get('evidence')
            evidence_items = evidence_items_raw if isinstance(evidence_items_raw, list) else []
            evidence_lookup = {
                str(item.get('id') or ''): item
                for item in evidence_items
                if isinstance(item, dict) and str(item.get('id') or '').strip()
            }
            query_items_raw = hunt_by_card.get(card_id, []) if isinstance(hunt_by_card, dict) else []
            query_items = query_items_raw if isinstance(query_items_raw, list) else []
            query_feedback_map: dict[str, dict[str, object]] = {}
            with sqlite3.connect(_db_path()) as connection:
                feedback_rows = connection.execute(
                    '''
                    SELECT item_id, COUNT(*), SUM(rating_score)
                    FROM analyst_feedback_events
                    WHERE actor_id = ? AND item_type = 'hunt_query'
                    GROUP BY item_id
                    ''',
                    (actor_id,),
                ).fetchall()
                query_feedback_map = {
                    str(row[0]): {
                        'votes': int(row[1] or 0),
                        'score': int(row[2] or 0),
                    }
                    for row in feedback_rows
                }
            for query_item in query_items:
                if not isinstance(query_item, dict):
                    continue
                query_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{card_id}:{str(query_item.get('query') or '')}"))
                query_item['query_id'] = query_id
                query_feedback = query_feedback_map.get(query_id, {'votes': 0, 'score': 0})
                query_item['feedback_votes'] = int(query_feedback.get('votes') or 0)
                query_item['feedback_score'] = int(query_feedback.get('score') or 0)
                refs_raw = query_item.get('evidence_source_ids')
                refs = refs_raw if isinstance(refs_raw, list) else []
                query_item['evidence_sources'] = [
                    evidence_lookup.get(str(ref_id), {'id': str(ref_id), 'source_title': str(ref_id), 'source_url': '', 'source_date': ''})
                    for ref_id in refs
                ]
            query_items = sorted(
                query_items,
                key=lambda item: (
                    int(item.get('feedback_score') or 0),
                    int(item.get('feedback_votes') or 0),
                ),
                reverse=True,
            )
            card_views.append(
                {
                    'id': card_id,
                    'title': str(card.get('quick_check_title') or card.get('question_text') or ''),
                    'iocs': card.get('related_iocs', []),
                    'queries': query_items,
                    'evidence': evidence_items,
                }
            )
            actor_context_chunks.append(str(card.get('quick_check_title') or ''))
            actor_context_chunks.append(str(card.get('question_text') or ''))
            for ioc in (card.get('related_iocs', []) if isinstance(card.get('related_iocs', []), list) else []):
                if not isinstance(ioc, dict):
                    continue
                ioc_type = str(ioc.get('ioc_type') or '').strip().lower()
                ioc_value = str(ioc.get('ioc_value') or '').strip().lower()
                if ioc_type and ioc_value:
                    used_ioc_pairs.add((ioc_type, ioc_value))
            for evidence in evidence_items:
                if not isinstance(evidence, dict):
                    continue
                actor_context_chunks.extend(
                    [
                        str(evidence.get('excerpt') or ''),
                        str(evidence.get('source_title') or ''),
                        str(evidence.get('source_url') or ''),
                    ]
                )

        actor_context_text = ' '.join(actor_context_chunks).lower()

        def _is_actor_related_unmatched(ioc_item: dict[str, object]) -> bool:
            ioc_value = str(ioc_item.get('ioc_value') or '').strip().lower()
            source_ref = str(ioc_item.get('source_ref') or '').strip().lower()
            if not ioc_value:
                return False
            if ioc_value in actor_context_text:
                return True
            if source_ref and source_ref in actor_context_text:
                return True
            if any(term in ioc_value for term in actor_terms):
                return True
            if source_ref and any(term in source_ref for term in actor_terms):
                return True
            return False

        misc_iocs_by_type: dict[str, list[dict[str, object]]] = {}
        ioc_items_raw = notebook.get('ioc_items', []) if isinstance(notebook, dict) else []
        ioc_items = ioc_items_raw if isinstance(ioc_items_raw, list) else []
        for ioc in ioc_items:
            if not isinstance(ioc, dict):
                continue
            ioc_type = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type or not ioc_value:
                continue
            if not _ioc_value_is_hunt_relevant(ioc_type, ioc_value):
                continue
            key = (ioc_type, ioc_value.lower())
            if key in used_ioc_pairs:
                continue
            if not _is_actor_related_unmatched(ioc):
                continue
            misc_iocs_by_type.setdefault(ioc_type, []).append(
                {
                    'ioc_type': ioc_type,
                    'ioc_value': ioc_value,
                    'source_ref': str(ioc.get('source_ref') or ''),
                    'last_seen_at': str(ioc.get('last_seen_at') or ioc.get('created_at') or ''),
                    'confidence_score': int(ioc.get('confidence_score') or 0),
                }
            )
        for ioc_type in list(misc_iocs_by_type.keys()):
            misc_iocs_by_type[ioc_type] = sorted(
                misc_iocs_by_type[ioc_type],
                key=lambda item: (
                    int(item.get('confidence_score') or 0),
                    str(item.get('last_seen_at') or ''),
                ),
                reverse=True,
            )[:25]

        return _templates.TemplateResponse(
            request,
            'ioc_hunts.html',
            {
                'actor_id': actor_id,
                'actor_name': actor_name,
                'cards': card_views,
                'thread_id': thread_id or '',
                'reason': reason,
                'ollama_status': ollama_status,
                'environment_profile': environment_profile,
                'misc_iocs_by_type': misc_iocs_by_type,
            },
        )

    @router.get(route_paths.ACTOR_ENVIRONMENT_PROFILE, response_class=JSONResponse)
    def actor_environment_profile(actor_id: str) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            return _load_environment_profile(connection, actor_id=actor_id)

    @router.post(route_paths.ACTOR_ENVIRONMENT_PROFILE, response_class=JSONResponse)
    async def upsert_actor_environment_profile(actor_id: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail='invalid profile payload')
        profile = _normalize_environment_profile(payload)
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            response = _upsert_environment_profile(connection, actor_id=actor_id, profile=profile)
            connection.commit()
        return response

    @router.post(route_paths.ACTOR_FEEDBACK, response_class=JSONResponse)
    async def submit_feedback(actor_id: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail='invalid feedback payload')
        item_type = str(payload.get('item_type') or '').strip()
        item_id = str(payload.get('item_id') or '').strip()
        feedback_label = str(payload.get('feedback') or payload.get('feedback_label') or 'partial').strip()
        reason = str(payload.get('reason') or '').strip()
        source_id = str(payload.get('source_id') or '').strip() or None
        metadata_raw = payload.get('metadata')
        metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
        source_reliability_updates = 0
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            stored = _store_feedback_event(
                connection,
                actor_id=actor_id,
                item_type=item_type,
                item_id=item_id,
                feedback_label=feedback_label,
                reason=reason,
                source_id=source_id,
                metadata=metadata,
            )
            if not bool(stored.get('stored')):
                raise HTTPException(status_code=400, detail=str(stored.get('reason') or 'failed to store feedback'))
            evidence_ids_raw = metadata.get('evidence_source_ids')
            evidence_ids = [str(item).strip() for item in evidence_ids_raw if str(item).strip()] if isinstance(evidence_ids_raw, list) else []
            if evidence_ids:
                placeholders = ','.join('?' for _ in evidence_ids)
                rows = connection.execute(
                    f'''
                    SELECT url
                    FROM sources
                    WHERE actor_id = ? AND id IN ({placeholders})
                    ''',
                    (actor_id, *evidence_ids),
                ).fetchall()
                urls = [str(row[0] or '').strip() for row in rows if str(row[0] or '').strip()]
                source_reliability_updates = _apply_feedback_to_source_domains(
                    connection,
                    actor_id=actor_id,
                    source_urls=urls,
                    rating_score=int(stored.get('rating_score') or 0),
                )
            connection.commit()
        return {
            'actor_id': actor_id,
            **stored,
            'source_reliability_updates': source_reliability_updates,
        }

    @router.get(route_paths.ACTOR_FEEDBACK_SUMMARY, response_class=JSONResponse)
    def feedback_summary(actor_id: str, item_type: str | None = None) -> dict[str, object]:
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            return _feedback_summary_for_actor(connection, actor_id=actor_id, item_type=item_type)

    @router.get(route_paths.ACTOR_UI_LIVE, response_class=JSONResponse)
    def actor_live_state(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
    ) -> dict[str, object]:
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
        )
        return {
            'actor_id': actor_id,
            'notebook_status': str(notebook.get('actor', {}).get('notebook_status') or 'idle'),
            'notebook_message': str(notebook.get('actor', {}).get('notebook_message') or ''),
            'kpis': notebook.get('kpis', {}),
            'recent_change_summary': notebook.get('recent_change_summary', {}),
            'priority_questions': notebook.get('priority_questions', []),
            'timeline_compact_rows': notebook.get('timeline_compact_rows', []),
            'timeline_window_label': notebook.get('timeline_window_label', ''),
        }

    @router.get(route_paths.ACTOR_OBSERVATIONS, response_class=JSONResponse)
    def list_observations(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=limit,
            offset=offset,
        )
        return {
            'actor_id': actor_id,
            'limit': max(1, min(500, int(limit))),
            'offset': max(0, int(offset)),
            'items': items,
        }

    @router.get(route_paths.ACTOR_OBSERVATIONS_EXPORT_JSON, response_class=JSONResponse)
    def export_observations_json(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, object]:
        safe_limit = max(1, min(5000, int(limit)))
        safe_offset = max(0, int(offset))
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=safe_limit,
            offset=safe_offset,
        )
        return {
            'actor_id': actor_id,
            'count': len(items),
            'limit': safe_limit,
            'offset': safe_offset,
            'filters': {
                'analyst': analyst or '',
                'confidence': confidence or '',
                'updated_from': updated_from or '',
                'updated_to': updated_to or '',
            },
            'items': items,
        }

    @router.get(route_paths.ACTOR_EXPORT_ANALYST_PACK, response_class=JSONResponse)
    def export_analyst_pack(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 1000,
        history_limit: int = 1000,
    ) -> dict[str, object]:
        return _build_analyst_pack_payload(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            observations_limit=observations_limit,
            history_limit=history_limit,
        )

    @router.get(route_paths.ACTOR_EXPORT_ANALYST_PACK_PDF)
    def export_analyst_pack_pdf(
        actor_id: str,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        observations_limit: int = 500,
        history_limit: int = 500,
    ) -> Response:
        pack = _build_analyst_pack_payload(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            observations_limit=observations_limit,
            history_limit=history_limit,
        )
        actor_meta = pack.get('actor', {}) if isinstance(pack.get('actor', {}), dict) else {}
        actor_name = str(actor_meta.get('display_name') or actor_id).strip() or actor_id
        summary = pack.get('recent_change_summary', {})
        summary_dict = summary if isinstance(summary, dict) else {}
        lines: list[str] = [
            f'Actor ID: {actor_id}',
            f'Actor: {actor_name}',
            f'Exported At (UTC): {str(pack.get("exported_at") or "")}',
            '',
            'Recent Change Summary',
        ]
        for key in ('new_reports', 'new_items', 'targets', 'damage', 'ransomware'):
            lines.append(f'- {key}: {summary_dict.get(key, 0)}')

        lines.append('')
        lines.append('Priority Questions')
        for card in (pack.get('priority_questions', []) if isinstance(pack.get('priority_questions', []), list) else [])[:10]:
            if not isinstance(card, dict):
                continue
            lines.append(f"- {str(card.get('question_text') or '').strip()[:180]}")
            lines.append(f"  First Step: {str(card.get('first_step') or '').strip()[:180]}")
            lines.append(f"  Watch: {str(card.get('what_to_look_for') or '').strip()[:180]}")

        lines.append('')
        lines.append('Recent IOCs')
        for ioc in (pack.get('ioc_items', []) if isinstance(pack.get('ioc_items', []), list) else [])[:25]:
            if not isinstance(ioc, dict):
                continue
            ioc_type = str(ioc.get('ioc_type') or '').strip()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            source_ref = str(ioc.get('source_ref') or '').strip()
            lines.append(f'- [{ioc_type}] {ioc_value} {f"({source_ref})" if source_ref else ""}'.strip())

        lines.append('')
        lines.append('Observations')
        for obs in (pack.get('observations', []) if isinstance(pack.get('observations', []), list) else [])[:40]:
            if not isinstance(obs, dict):
                continue
            updated_at = str(obs.get('updated_at') or '').strip()[:19]
            updated_by = str(obs.get('updated_by') or '').strip()
            note = str(obs.get('note') or '').strip().replace('\n', ' ')[:180]
            lines.append(f'- {updated_at} {updated_by}: {note}')

        pdf_bytes = _render_simple_text_pdf(title=f'Analyst Pack - {actor_name}', lines=lines)
        safe_actor = ''.join(ch if ch.isalnum() or ch in {'-', '_'} else '-' for ch in actor_id).strip('-') or 'actor'
        return Response(
            content=pdf_bytes,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{safe_actor}-analyst-pack.pdf"'},
        )

    @router.get(route_paths.ACTOR_OBSERVATIONS_EXPORT_CSV)
    def export_observations_csv(
        actor_id: str,
        analyst: str | None = None,
        confidence: str | None = None,
        updated_from: str | None = None,
        updated_to: str | None = None,
    ) -> Response:
        items = _fetch_analyst_observations(
            actor_id,
            analyst=analyst,
            confidence=confidence,
            updated_from=updated_from,
            updated_to=updated_to,
            limit=None,
            offset=0,
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                'actor_id',
                'item_type',
                'item_key',
                'note',
                'source_ref',
                'confidence',
                'source_reliability',
                'information_credibility',
                'updated_by',
                'updated_at',
                'source_name',
                'source_title',
                'source_url',
                'source_date',
            ]
        )
        for item in items:
            writer.writerow(
                [
                    actor_id,
                    item.get('item_type', ''),
                    item.get('item_key', ''),
                    item.get('note', ''),
                    item.get('source_ref', ''),
                    item.get('confidence', ''),
                    item.get('source_reliability', ''),
                    item.get('information_credibility', ''),
                    item.get('updated_by', ''),
                    item.get('updated_at', ''),
                    item.get('source_name', ''),
                    item.get('source_title', ''),
                    item.get('source_url', ''),
                    item.get('source_date', ''),
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename="{actor_id}-observations.csv"',
            },
        )

    @router.post(route_paths.ACTOR_OBSERVATION_UPSERT, response_class=JSONResponse)
    async def upsert_observation(actor_id: str, item_type: str, item_key: str, request: Request) -> dict[str, object]:
        await _enforce_request_size(request, _default_body_limit_bytes)
        payload = await request.json()

        note = str(payload.get('note') or '').strip()[:4000]
        source_ref = str(payload.get('source_ref') or '').strip()[:500]
        confidence = str(payload.get('confidence') or 'moderate').strip().lower()
        if confidence not in {'low', 'moderate', 'high'}:
            confidence = 'moderate'
        source_reliability = str(payload.get('source_reliability') or '').strip().upper()[:1]
        if source_reliability and source_reliability not in {'A', 'B', 'C', 'D', 'E', 'F'}:
            source_reliability = ''
        information_credibility = str(payload.get('information_credibility') or '').strip()[:1]
        if information_credibility and information_credibility not in {'1', '2', '3', '4', '5', '6'}:
            information_credibility = ''
        updated_by = str(payload.get('updated_by') or '').strip()[:120]
        updated_at = _utc_now_iso()
        quality_guidance = observation_service.observation_quality_guidance_core(
            note=note,
            source_ref=source_ref,
            confidence=confidence,
            source_reliability=source_reliability,
            information_credibility=information_credibility,
        )

        safe_item_type = item_type.strip().lower()[:40]
        safe_item_key = item_key.strip()[:200]
        if not safe_item_type or not safe_item_key:
            raise HTTPException(status_code=400, detail='invalid observation key')

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            _upsert_observation_with_history(
                connection,
                actor_id=actor_id,
                item_type=safe_item_type,
                item_key=safe_item_key,
                note=note,
                source_ref=source_ref,
                confidence=confidence,
                source_reliability=source_reliability,
                information_credibility=information_credibility,
                updated_by=updated_by,
                updated_at=updated_at,
            )
            connection.commit()

        return {
            'ok': True,
            'item_type': safe_item_type,
            'item_key': safe_item_key,
            'note': note,
            'source_ref': source_ref,
            'confidence': confidence,
            'source_reliability': source_reliability,
            'information_credibility': information_credibility,
            'updated_by': updated_by,
            'updated_at': updated_at,
            'quality_guidance': quality_guidance,
        }

    @router.post(route_paths.ACTOR_OBSERVATIONS_AUTO_SNAPSHOT)
    async def auto_snapshot_observations(actor_id: str, request: Request) -> RedirectResponse:
        await _enforce_request_size(request, _default_body_limit_bytes)
        notebook = _fetch_actor_notebook(actor_id)
        highlights = notebook.get('recent_activity_highlights', [])
        entries = highlights if isinstance(highlights, list) else []
        saved = 0
        updated_at = _utc_now_iso()
        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            for item in entries[:5]:
                if not isinstance(item, dict):
                    continue
                item_key = str(item.get('source_id') or item.get('timeline_event_id') or '').strip()[:200]
                if not item_key:
                    continue
                title = str(item.get('evidence_title') or item.get('source_name') or 'source').strip()
                date = str(item.get('date') or '').strip()
                summary = str(item.get('text') or '').strip()
                note_parts = [part for part in [f'{date} {title}'.strip(), summary] if part]
                note = 'Auto: ' + ' | '.join(note_parts)
                _upsert_observation_with_history(
                    connection,
                    actor_id=actor_id,
                    item_type='source',
                    item_key=item_key,
                    note=note[:4000],
                    source_ref=f'auto-snapshot:{updated_at[:10]}',
                    confidence='moderate',
                    source_reliability='',
                    information_credibility='',
                    updated_by='auto',
                    updated_at=updated_at,
                )
                saved += 1
            if saved == 0:
                source_rows = connection.execute(
                    '''
                    SELECT id, title, source_name, published_at
                    FROM sources
                    WHERE actor_id = ?
                    ORDER BY COALESCE(published_at, retrieved_at) DESC
                    LIMIT 5
                    ''',
                    (actor_id,),
                ).fetchall()
                for row in source_rows:
                    source_id = str(row[0] or '').strip()[:200]
                    if not source_id:
                        continue
                    title = str(row[1] or row[2] or 'source').strip()
                    published_at = str(row[3] or '').strip()
                    note = f'Auto: {published_at} {title}'.strip()
                    _upsert_observation_with_history(
                        connection,
                        actor_id=actor_id,
                        item_type='source',
                        item_key=source_id,
                        note=note[:4000],
                        source_ref=f'auto-snapshot:{updated_at[:10]}',
                        confidence='moderate',
                        source_reliability='',
                        information_credibility='',
                        updated_by='auto',
                        updated_at=updated_at,
                    )
                    saved += 1
            connection.commit()
        return RedirectResponse(
            url=f'/?actor_id={actor_id}&notice=Auto-noted+{saved}+recent+changes',
            status_code=303,
        )

    @router.get(route_paths.ACTOR_OBSERVATION_HISTORY, response_class=JSONResponse)
    def observation_history(actor_id: str, item_type: str, item_key: str, limit: int = 25) -> dict[str, object]:
        safe_item_type = item_type.strip().lower()[:40]
        safe_item_key = item_key.strip()[:200]
        if not safe_item_type or not safe_item_key:
            raise HTTPException(status_code=400, detail='invalid observation key')
        safe_limit = max(1, min(100, int(limit)))

        with sqlite3.connect(_db_path()) as connection:
            if not _actor_exists(connection, actor_id):
                raise HTTPException(status_code=404, detail='actor not found')
            rows = connection.execute(
                '''
                SELECT note, source_ref, confidence, source_reliability,
                       information_credibility, updated_by, updated_at
                FROM analyst_observation_history
                WHERE actor_id = ? AND item_type = ? AND item_key = ?
                ORDER BY updated_at DESC
                LIMIT ?
                ''',
                (actor_id, safe_item_type, safe_item_key, safe_limit),
            ).fetchall()
            if not rows:
                latest_row = connection.execute(
                    '''
                    SELECT note, source_ref, confidence, source_reliability,
                           information_credibility, updated_by, updated_at
                    FROM analyst_observations
                    WHERE actor_id = ? AND item_type = ? AND item_key = ?
                    ''',
                    (actor_id, safe_item_type, safe_item_key),
                ).fetchone()
                if latest_row is not None:
                    rows = [latest_row]

        items = [
            {
                'note': str(row[0] or ''),
                'source_ref': str(row[1] or ''),
                'confidence': str(row[2] or 'moderate'),
                'source_reliability': str(row[3] or ''),
                'information_credibility': str(row[4] or ''),
                'updated_by': str(row[5] or ''),
                'updated_at': str(row[6] or ''),
            }
            for row in rows
        ]
        return {
            'actor_id': actor_id,
            'item_type': safe_item_type,
            'item_key': safe_item_key,
            'count': len(items),
            'items': items,
        }

    return router
