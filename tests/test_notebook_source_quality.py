import sqlite3
import time
import asyncio
from datetime import datetime, timezone
from fastapi import BackgroundTasks

import pytest

import app as app_module
import route_paths
from tests.notebook_test_helpers import JsonRequest as _JsonRequest
from tests.notebook_test_helpers import app_endpoint as _app_endpoint
from tests.notebook_test_helpers import http_request as _http_request
from tests.notebook_test_helpers import setup_db as _setup_db


def test_source_quality_filters_scope_recent_change_inputs(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Filter', 'Source quality filter test')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        src_high = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'CISA',
            'https://www.cisa.gov/news-events/cybersecurity-advisories/example-1',
            '2026-02-20T00:00:00+00:00',
            'APT-Filter exploited CVE-2026-0001 against healthcare organizations.',
            'APT-Filter exploited CVE-2026-0001 against healthcare organizations.',
        )
        src_unrated = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Unknown Blog',
            'https://unknown-security.example/reports/apt-filter-update',
            '2026-02-19T00:00:00+00:00',
            'APT-Filter used PowerShell execution and targeted finance entities.',
            'APT-Filter used PowerShell execution and targeted finance entities.',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-high',
                actor['id'],
                '2026-02-20T00:00:00+00:00',
                'initial_access',
                'Initial access move',
                'APT-Filter exploited CVE-2026-0001 to gain access.',
                src_high,
                'Healthcare',
                '["T1190"]',
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-unrated',
                actor['id'],
                '2026-02-19T00:00:00+00:00',
                'execution',
                'Execution move',
                'APT-Filter used PowerShell for follow-on execution.',
                src_unrated,
                'Finance',
                '["T1190"]',
            ),
        )
        connection.commit()

    notebook_all = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        min_confidence_weight=0,
        source_days=3650,
    )
    all_urls = {
        str(item.get('source_url') or '')
        for item in notebook_all.get('recent_activity_highlights', [])
    }
    assert any('cisa.gov' in value for value in all_urls)
    assert any('unknown-security.example' in value for value in all_urls)
    assert any(
        str(item.get('technique_id') or '').strip().upper() == 'T1190'
        for item in notebook_all.get('emerging_techniques', [])
    )

    notebook_high_only = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        source_tier='high',
    )
    high_urls = [
        str(item.get('source_url') or '')
        for item in notebook_high_only.get('recent_activity_highlights', [])
    ]
    assert high_urls
    assert all('cisa.gov' in value for value in high_urls)
    filters = notebook_high_only.get('source_quality_filters', {})
    assert str(filters.get('source_tier') or '') == 'high'
    assert str(filters.get('total_sources') or '') == '2'
    assert str(filters.get('applied_sources') or '') == '1'
    assert str(filters.get('filtered_out_sources') or '') == '1'
    filtered_source_urls = {
        str(item.get('url') or '')
        for item in notebook_high_only.get('sources', [])
        if str(item.get('source_tier') or '').strip().lower() == 'high'
    }
    top_signal_urls = {
        str(evidence.get('source_url') or '')
        for signal in notebook_high_only.get('top_change_signals', [])
        if isinstance(signal, dict)
        for evidence in (signal.get('validated_sources') or [])
        if isinstance(evidence, dict) and str(evidence.get('source_url') or '').strip()
    }
    assert top_signal_urls
    assert top_signal_urls.issubset(filtered_source_urls)
    assert not any(
        str(item.get('technique_id') or '').strip().upper() == 'T1190'
        for item in notebook_high_only.get('emerging_techniques', [])
    )


def test_source_quality_filters_apply_weight_and_days(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Filter-Days', 'Source quality day/weight test')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        src_recent_high = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'CISA',
            'https://www.cisa.gov/news-events/cybersecurity-advisories/example-2',
            '2026-02-21T00:00:00+00:00',
            'APT-Filter-Days activity and exploitation details.',
            'APT-Filter-Days activity and exploitation details.',
        )
        src_old_medium = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Mandiant',
            'https://www.mandiant.com/resources/blog/legacy-activity-report',
            '2025-01-10T00:00:00+00:00',
            'APT-Filter-Days legacy campaign activity.',
            'APT-Filter-Days legacy campaign activity.',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-recent-high',
                actor['id'],
                '2026-02-21T00:00:00+00:00',
                'initial_access',
                'Recent high source',
                'APT-Filter-Days exploited internet-facing services.',
                src_recent_high,
                'Government',
                '["T1190"]',
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-old-medium',
                actor['id'],
                '2025-01-10T00:00:00+00:00',
                'execution',
                'Old medium source',
                'APT-Filter-Days executed staged tooling.',
                src_old_medium,
                'Technology',
                '["T1059"]',
            ),
        )
        connection.commit()

    notebook_filtered = app_module._fetch_actor_notebook(  # noqa: SLF001
        actor['id'],
        min_confidence_weight=3,
        source_days=60,
    )
    filtered_urls = [
        str(item.get('source_url') or '')
        for item in notebook_filtered.get('recent_activity_highlights', [])
    ]
    assert filtered_urls
    assert any('cisa.gov' in value for value in filtered_urls)
    assert all('mandiant.com' not in value for value in filtered_urls)
    filters = notebook_filtered.get('source_quality_filters', {})
    assert str(filters.get('min_confidence_weight') or '') == '3'
    assert str(filters.get('source_days') or '') == '60'
    assert str(filters.get('total_sources') or '') == '2'
    assert str(filters.get('applied_sources') or '') == '1'
    assert str(filters.get('filtered_out_sources') or '') == '1'


def test_generate_actor_requirements_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_generate_core(actor_id, org_context, priority_mode, **kwargs):
        captured['actor_id'] = actor_id
        captured['org_context'] = org_context
        captured['priority_mode'] = priority_mode
        captured.update(kwargs)
        return 2

    monkeypatch.setattr(app_module, 'pipeline_generate_actor_requirements_core', _fake_generate_core)

    inserted = app_module.generate_actor_requirements('actor-1', 'finance org', 'Operational')

    assert inserted == 2
    assert captured['actor_id'] == 'actor-1'
    assert captured['org_context'] == 'finance org'
    assert captured['priority_mode'] == 'Operational'


def test_strict_default_filters_use_rich_defaults_and_keep_soft_and_partial_sources(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Strict-Default', 'Strict default source filter scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        src_soft = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Context Feed',
            'https://example.com/soft',
            '2026-02-21T00:00:00+00:00',
            'General cyber activity with weak actor attribution.',
            'Weak actor attribution context',
            source_tier='context',
            confidence_weight=1,
            source_type='feed_soft_match',
        )
        src_partial = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Trusted Feed',
            'https://example.com/partial',
            '2026-02-22T00:00:00+00:00',
            'Partial actor-linked reporting with moderate confidence.',
            'Moderate actor attribution',
            source_tier='trusted',
            confidence_weight=2,
            source_type='feed_partial_match',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-soft',
                actor['id'],
                '2026-02-21T00:00:00+00:00',
                'execution',
                'Soft matched event',
                'Weakly-attributed event',
                src_soft,
                'Enterprise',
                '[]',
            ),
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-partial',
                actor['id'],
                '2026-02-22T00:00:00+00:00',
                'initial_access',
                'Partial matched event',
                'Moderately-attributed event',
                src_partial,
                'Enterprise',
                '[]',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    filters = notebook.get('source_quality_filters', {})
    assert str(filters.get('strict_default_mode') or '') == '1'
    assert str(filters.get('min_confidence_weight') or '') == '1'
    assert str(filters.get('total_sources') or '') == '2'
    assert str(filters.get('applied_sources') or '') == '2'
    assert int(filters.get('filtered_out_sources') or 0) == 0


def test_partial_match_source_can_surface_recent_activity_highlight(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Partial-Highlight', 'Partial highlight scope')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        source_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection,
            actor['id'],
            'Trusted Feed',
            'https://example.com/partial-highlight',
            '2026-02-22T00:00:00+00:00',
            'Operators used phishing lures and command-and-control infrastructure in recent campaigns.',
            'Phishing and C2 patterns observed',
            source_tier='trusted',
            confidence_weight=2,
            source_type='feed_partial_match',
        )
        connection.execute(
            '''
            INSERT INTO timeline_events (
                id, actor_id, occurred_at, category, title, summary, source_id, target_text, ttp_ids_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'evt-partial-highlight',
                actor['id'],
                '2026-02-22T00:00:00+00:00',
                'initial_access',
                'Partial matched event',
                'Phishing and exploit activity observed in current reporting.',
                source_id,
                'Enterprise',
                '[]',
            ),
        )
        connection.commit()

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001
    highlights = notebook.get('recent_activity_highlights', [])
    assert isinstance(highlights, list)
    assert len(highlights) >= 1
