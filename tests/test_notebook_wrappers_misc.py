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


def test_import_default_feeds_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_import_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)
        return 5

    monkeypatch.setattr(app_module, 'pipeline_import_default_feeds_for_actor_core', _fake_import_core)

    imported = app_module.import_default_feeds_for_actor('actor-feed-wrapper')

    assert imported == 5
    assert captured['actor_id'] == 'actor-feed-wrapper'
    assert captured['db_path'] == app_module.DB_PATH
    assert captured['default_cti_feeds'] == app_module.DEFAULT_CTI_FEEDS
    assert captured['actor_feed_lookback_days'] == app_module.ACTOR_FEED_LOOKBACK_DAYS
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert 'derive_source_from_url' in deps
    assert 'upsert_source_for_actor' in deps
    assert 'duckduckgo_actor_search_urls' in deps


def test_run_actor_generation_wrapper_delegates_to_pipeline_core(monkeypatch):
    monkeypatch.setattr(app_module, '_mark_actor_generation_started', lambda _actor_id: True)  # noqa: SLF001
    monkeypatch.setattr(app_module, '_mark_actor_generation_finished', lambda _actor_id: None)  # noqa: SLF001
    monkeypatch.setattr(app_module, '_create_generation_job', lambda **_kwargs: 'job-1')  # noqa: SLF001
    monkeypatch.setattr(app_module, '_mark_generation_job_started', lambda **_kwargs: None)  # noqa: SLF001
    monkeypatch.setattr(app_module, '_finalize_generation_job', lambda **_kwargs: None)  # noqa: SLF001

    captured: dict[str, object] = {}

    def _fake_run_core(actor_id, **kwargs):
        captured['actor_id'] = actor_id
        captured.update(kwargs)

    monkeypatch.setattr(app_module, 'pipeline_run_actor_generation_core', _fake_run_core)

    app_module.run_actor_generation('actor-runner-wrapper')

    assert captured['actor_id'] == 'actor-runner-wrapper'
    assert captured['db_path'] == app_module.DB_PATH
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert deps['set_actor_notebook_status'] is app_module.set_actor_notebook_status
    assert deps['import_default_feeds_for_actor'] is app_module.import_default_feeds_for_actor
    assert deps['build_notebook'] is app_module.build_notebook


def test_derive_source_from_url_wrapper_delegates_to_pipeline_core(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_derive_core(source_url, **kwargs):
        captured['source_url'] = source_url
        captured.update(kwargs)
        return {'source_name': 'example.com', 'source_url': source_url, 'pasted_text': 'ok'}

    monkeypatch.setattr(app_module, 'pipeline_derive_source_from_url_core', _fake_derive_core)

    result = app_module.derive_source_from_url('https://example.com/post', fallback_source_name='Example')

    assert result['source_name'] == 'example.com'
    assert captured['source_url'] == 'https://example.com/post'
    assert captured['fallback_source_name'] == 'Example'
    deps = captured['deps']
    assert isinstance(deps, dict)
    assert deps['safe_http_get'] is app_module._safe_http_get  # noqa: SLF001
    assert deps['extract_question_sentences'] is app_module._extract_question_sentences  # noqa: SLF001
    assert deps['first_sentences'] is app_module._first_sentences  # noqa: SLF001


def test_evidence_title_prefers_structured_title_over_pasted_text():
    source = {
        'title': 'Executive Threat Update',
        'headline': 'Should not be used',
        'pasted_text': 'First pasted sentence that would otherwise be chosen.',
        'url': 'https://example.com/article',
    }

    title = app_module._evidence_title_from_source(source)  # noqa: SLF001

    assert title == 'Executive Threat Update'


def test_priority_where_to_check_wrapper_delegates_to_priority_module(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_priority_where_to_check(guidance_items, question_text, **kwargs):
        captured['guidance_items'] = guidance_items
        captured['question_text'] = question_text
        captured.update(kwargs)
        return 'Firewall/VPN'

    monkeypatch.setattr(app_module.priority_questions, 'priority_where_to_check', _fake_priority_where_to_check)

    result = app_module._priority_where_to_check([{'platform': 'EDR'}], 'Is edge access compromised?')  # noqa: SLF001

    assert result == 'Firewall/VPN'
    assert captured['question_text'] == 'Is edge access compromised?'
    assert callable(captured['platforms_for_question'])


def test_question_org_alignment_preserves_overlap_scoring():
    score = app_module._question_org_alignment(  # noqa: SLF001
        'How should we protect finance payment systems from ransomware?',
        'Priority assets include finance payment systems and payroll operations.',
    )
    assert score >= 2


def test_platforms_for_question_wrapper_delegates_to_guidance_catalog(monkeypatch):
    monkeypatch.setattr(app_module.guidance_catalog, 'platforms_for_question', lambda _q: ['DNS/Proxy'])

    platforms = app_module._platforms_for_question('Any question')  # noqa: SLF001

    assert platforms == ['DNS/Proxy']


def test_platforms_for_question_dedupes_and_prioritizes_expected_domains():
    platforms = app_module._platforms_for_question(  # noqa: SLF001
        'Phish email with VPN exploit and DNS beacon plus process command line'
    )

    assert platforms[0] == 'M365'
    assert platforms.count('M365') == 1
    assert 'Email Gateway' in platforms
    assert 'Firewall/VPN' in platforms
    assert 'DNS/Proxy' in platforms
    assert 'EDR' in platforms


def test_extract_major_move_events_wrapper_delegates_to_timeline_module(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_extract_major_move_events(source_name, source_id, occurred_at, text, actor_terms, **kwargs):
        captured['source_name'] = source_name
        captured['source_id'] = source_id
        captured['occurred_at'] = occurred_at
        captured['text'] = text
        captured['actor_terms'] = actor_terms
        captured.update(kwargs)
        return [{'id': 'evt-1'}]

    monkeypatch.setattr(app_module.timeline_extraction, 'extract_major_move_events', _fake_extract_major_move_events)

    events = app_module._extract_major_move_events(  # noqa: SLF001
        'CISA',
        'src-1',
        '2026-02-20T00:00:00+00:00',
        'APT-Flow exploited edge devices.',
        ['apt-flow'],
    )

    assert events == [{'id': 'evt-1'}]
    assert captured['source_name'] == 'CISA'
    assert callable(captured['deps']['split_sentences'])
    assert callable(captured['deps']['extract_ttp_ids'])
    assert callable(captured['deps']['new_id'])


def test_extract_major_move_events_behavior_classifies_and_targets():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'CISA',
        'src-1',
        '2026-02-20T00:00:00+00:00',
        'APT-Flow targeted Acme Hospital and used PowerShell execution for access.',
        ['apt-flow'],
    )

    assert events
    assert events[0]['category'] == 'execution'
    assert 'Acme Hospital' in str(events[0]['target_text'])


def test_extract_major_move_events_ransomware_live_keeps_full_structured_synthesis():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-1',
        '2026-02-23T11:57:55.045635+00:00',
        (
            'Who: Qilin ransomware operators.\n'
            'What: 15 public victim disclosures in the last 90 days.\n'
            'When: Latest listed disclosure date is 2026-02-22.\n'
            'Where: US (6), FR (1), NZ (1).\n'
            'How/Targets: Manufacturing (3), Healthcare (2).'
        ),
        ['qilin'],
    )

    assert len(events) == 1
    assert events[0]['category'] == 'impact'
    assert '90d disclosures: 15' in str(events[0]['summary'])
    assert 'Top geographies: US (6), FR (1), NZ (1)' in str(events[0]['summary'])
    assert 'Top sectors: Manufacturing (3), Healthcare (2)' in str(events[0]['summary'])
    assert '\n' in str(events[0]['summary'])
    assert 'Who:' not in str(events[0]['summary'])
    assert 'What:' not in str(events[0]['summary'])
    assert 'When:' not in str(events[0]['summary'])


def test_extract_major_move_events_ransomware_live_normalizes_legacy_trend_blob():
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-legacy',
        '2026-02-17T09:37:10+00:00',
        (
            'qilin ransomware activity synthesis (tempo, geography, and target examples) from ransomware.live. '
            'Ransomware.live trend for qilin: 1469 total public victim disclosures, 15 in the last 90 days. '
            'Latest listed activity: 2026-02-16. '
            'Most frequent victim geographies in the current sample: US (8), CL (2), IT (1). '
            'Recently observed targets include: 2026-02-16 - Casartigiani (IT).'
        ),
        ['qilin'],
    )

    assert len(events) == 1
    assert events[0]['title'] == 'Qilin ransomware disclosure and targeting update'
    summary = str(events[0]['summary'])
    assert '90d disclosures: 15' in summary
    assert 'Total listed: 1469' in summary
    assert 'Top geographies: US (8), CL (2), IT (1)' in summary
    assert 'Ransomware.live trend for' not in summary
    assert 'Latest listed activity date:' not in summary


def test_extract_major_move_events_ransomware_live_uses_full_prose_summary():
    text = (
        'Qilin ransomware operators have 15 public victim disclosures in the last 90 days '
        '(1498 total listed disclosures in this ransomware.live sample). '
        'Latest listed disclosure date is 2026-02-22. '
        'Most frequently listed victim geographies in this sample are US (6), FR (1), NZ (1). '
        'Most frequently listed victim sectors are Not Found (5), Manufacturing (3), Healthcare (2). '
        'Recent listed victim examples: Example One; Example Two; Example Three. '
        'Analyst use: Treat this as trend context.'
    )
    events = app_module._extract_major_move_events(  # noqa: SLF001
        'Ransomware.live',
        'src-prose',
        '2026-02-23T12:23:57+00:00',
        text,
        ['qilin'],
    )

    assert len(events) == 1
    summary = str(events[0]['summary'])
    assert '90d disclosures: 15' in summary
    assert 'Total listed: 1498' in summary
    assert 'Top geographies: US (6), FR (1), NZ (1)' in summary
    assert 'Top sectors: Not Found (5), Manufacturing (3), Healthcare (2)' in summary
    assert 'Analyst use:' not in summary


