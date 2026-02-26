import sqlite3
import uuid
from pathlib import Path

from services import db_schema_service, notebook_service


def _noop(*_args, **_kwargs):
    return None


def _deps_for_cache_test(db_path: str, pipeline_fetch):
    return {
        'pipeline_fetch_actor_notebook_core': pipeline_fetch,
        'db_path': lambda: db_path,
        'source_tier': None,
        'min_confidence_weight': None,
        'source_days': None,
        'parse_published_datetime': lambda _value: None,
        'safe_json_string_list': lambda _value: [],
        'actor_signal_categories': _noop,
        'question_actor_relevance': _noop,
        'priority_update_evidence_dt': _noop,
        'question_org_alignment': _noop,
        'priority_rank_score': _noop,
        'phase_label_for_question': _noop,
        'priority_where_to_check': _noop,
        'priority_confidence_label': _noop,
        'quick_check_title': _noop,
        'short_decision_trigger': _noop,
        'telemetry_anchor_line': _noop,
        'priority_next_best_action': _noop,
        'guidance_line': _noop,
        'guidance_query_hint': _noop,
        'priority_disconfirming_signal': _noop,
        'confidence_change_threshold_line': _noop,
        'escalation_threshold_line': _noop,
        'expected_output_line': _noop,
        'priority_update_recency_label': _noop,
        'org_alignment_label': _noop,
        'fallback_priority_questions': _noop,
        'token_overlap': _noop,
        'build_actor_profile_from_mitre': _noop,
        'group_top_techniques': _noop,
        'favorite_attack_vectors': _noop,
        'known_technique_ids_for_entity': _noop,
        'emerging_techniques_from_timeline': _noop,
        'build_timeline_graph': _noop,
        'compact_timeline_rows': _noop,
        'actor_terms': _noop,
        'build_recent_activity_highlights': _noop,
        'build_top_change_signals': _noop,
        'build_recent_activity_synthesis': _noop,
        'recent_change_summary': _noop,
        'build_environment_checks': _noop,
        'build_notebook_kpis': _noop,
        'format_date_or_unknown': _noop,
    }


def _init_db(path: Path):
    with sqlite3.connect(path) as connection:
        db_schema_service.ensure_schema(connection)
        connection.execute(
            '''
            INSERT INTO actor_profiles (id, display_name, created_at, is_tracked)
            VALUES (?, ?, ?, 1)
            ''',
            ('actor-1', 'Actor One', '2026-02-26T00:00:00+00:00'),
        )
        connection.commit()


def test_notebook_cache_hit_reuses_pipeline_result(tmp_path):
    db_path = tmp_path / 'app.db'
    _init_db(db_path)
    calls = {'count': 0}

    def fake_pipeline_fetch(actor_id, **_kwargs):
        calls['count'] += 1
        return {'actor': {'id': actor_id}, 'generated': calls['count']}

    deps = _deps_for_cache_test(str(db_path), fake_pipeline_fetch)
    first = notebook_service.fetch_actor_notebook_wrapper_core(actor_id='actor-1', deps=deps)
    second = notebook_service.fetch_actor_notebook_wrapper_core(actor_id='actor-1', deps=deps)

    assert first['generated'] == 1
    assert second['generated'] == 1
    assert calls['count'] == 1


def test_notebook_cache_invalidates_when_sources_change(tmp_path):
    db_path = tmp_path / 'app.db'
    _init_db(db_path)
    calls = {'count': 0}

    def fake_pipeline_fetch(actor_id, **_kwargs):
        calls['count'] += 1
        return {'actor': {'id': actor_id}, 'generated': calls['count']}

    deps = _deps_for_cache_test(str(db_path), fake_pipeline_fetch)
    first = notebook_service.fetch_actor_notebook_wrapper_core(actor_id='actor-1', deps=deps)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            '''
            INSERT INTO sources (
                id, actor_id, source_name, url, published_at, ingested_at, source_date_type, retrieved_at, pasted_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(uuid.uuid4()),
                'actor-1',
                'Unit Test Source',
                'https://example.com/test',
                '',
                '2026-02-26T01:00:00+00:00',
                'ingested',
                '2026-02-26T01:00:00+00:00',
                'sample text',
            ),
        )
        connection.commit()
    second = notebook_service.fetch_actor_notebook_wrapper_core(actor_id='actor-1', deps=deps)

    assert first['generated'] == 1
    assert second['generated'] == 2
    assert calls['count'] == 2
