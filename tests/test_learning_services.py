import sqlite3

from services import db_schema_service
from services import environment_profile_service
from services import feedback_service
from services import source_reliability_service


def test_feedback_store_and_summary():
    connection = sqlite3.connect(':memory:')
    db_schema_service.ensure_schema(connection)
    feedback_service.store_feedback_event_core(
        connection,
        actor_id='actor-1',
        item_type='priority_question',
        item_id='q-1',
        feedback_label='useful',
        reason='helped triage',
        source_id=None,
        metadata={},
        now_iso='2026-01-01T00:00:00Z',
    )
    feedback_service.store_feedback_event_core(
        connection,
        actor_id='actor-1',
        item_type='priority_question',
        item_id='q-1',
        feedback_label='not_useful',
        reason='too broad',
        source_id=None,
        metadata={},
        now_iso='2026-01-01T01:00:00Z',
    )
    connection.commit()
    summary = feedback_service.feedback_summary_for_actor_core(connection, actor_id='actor-1', item_type='priority_question')
    row = summary['items']['priority_question'][0]
    assert row['votes'] == 2
    assert row['score'] == 0


def test_environment_profile_normalize_and_personalize():
    profile = environment_profile_service.normalize_environment_profile(
        {
            'query_dialect': 'kql',
            'field_mapping': {'domain': 'DnsQuery'},
            'default_time_window_hours': 48,
        }
    )
    assert profile['query_dialect'] == 'kql'
    query = environment_profile_service.personalize_query_core(
        'domain:bad.example',
        ioc_value='bad.example',
        profile=profile,
    )
    assert 'DnsQuery' in query
    assert 'ago(48h)' in query


def test_source_reliability_updates_and_adjustment():
    connection = sqlite3.connect(':memory:')
    db_schema_service.ensure_schema(connection)
    updated = source_reliability_service.apply_feedback_to_source_domains_core(
        connection,
        actor_id='actor-1',
        source_urls=['https://intel.example/report', 'https://intel.example/other'],
        rating_score=1,
        now_iso='2026-01-01T00:00:00Z',
    )
    assert updated == 1
    reliability = source_reliability_service.load_reliability_map_core(connection, actor_id='actor-1')
    assert float(reliability['intel.example']['reliability_score']) > 0.5
    assert source_reliability_service.confidence_weight_adjustment_core(0.9) == 1
    assert source_reliability_service.confidence_weight_adjustment_core(0.1) == -1
