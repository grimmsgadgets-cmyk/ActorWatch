from datetime import datetime, timedelta, timezone

from pipelines.notebook_pipeline import (
    _extract_behavior_observables,
    _extract_ioc_candidates_from_text,
    _ioc_seen_within_days,
    _quick_check_is_evidence_backed_core,
    _relevant_iocs_for_quick_check,
    _select_event_ids_for_where_to_start_core,
)


def test_extract_ioc_candidates_skips_software_like_domain_tokens():
    text = (
        "The malware operator dashboard uses Next.js for the web UI framework. "
        "This section is implementation detail and not an IOC.\n\n"
        "Observed indicators include domain c2.bad-example.net and callback IP 185.88.1.45."
    )

    extracted = _extract_ioc_candidates_from_text(text)
    pairs = {(ioc_type, ioc_value) for ioc_type, ioc_value in extracted}

    assert ('domain', 'next.js') not in pairs
    assert ('domain', 'c2.bad-example.net') in pairs
    assert ('ip', '185.88.1.45') in pairs


def test_extract_ioc_candidates_requires_domain_ioc_context():
    spacer = " ".join(["release-note"] * 60)
    text = (
        "Product note: migrate frontend from legacy.bundle to modern.stack soon.\n"
        f"{spacer}\n"
        "Indicators: suspicious DNS domain bad-control.example and malware hash "
        "9f86d081884c7d659a2feaa0c55ad015."
    )

    extracted = _extract_ioc_candidates_from_text(text)
    pairs = {(ioc_type, ioc_value) for ioc_type, ioc_value in extracted}

    assert ('domain', 'legacy.bundle') not in pairs
    assert ('domain', 'modern.stack') not in pairs
    assert ('domain', 'bad-control.example') in pairs


def test_related_iocs_filter_software_tokens_and_use_confidence_fallback():
    card = {
        'question_text': 'Check for ransomware impact signs',
        'first_step': 'Start with DNS and proxy logs for suspicious domains in last 24h',
        'what_to_look_for': 'Repeated beaconing to suspicious domains by host and user',
        'query_hint': '',
        'telemetry_anchor': '',
    }
    ioc_items = [
        {'ioc_type': 'domain', 'ioc_value': 'next.js', 'source_ref': 'Cisco Talos Blog', 'confidence_score': 5},
        {'ioc_type': 'domain', 'ioc_value': 'bad-control.example', 'source_ref': 'IR note', 'confidence_score': 4},
        {'ioc_type': 'ip', 'ioc_value': '185.88.1.45', 'source_ref': 'IR note', 'confidence_score': 4},
    ]

    related = _relevant_iocs_for_quick_check(card, ioc_items, limit=4)
    values = [str(item.get('ioc_value') or '').lower() for item in related]

    assert 'next.js' not in values
    assert 'bad-control.example' in values


def test_related_iocs_do_not_fallback_for_event_id_first_quick_checks():
    card = {
        'question_text': 'Check for ransomware impact signs',
        'first_step': 'Start with Event IDs 4104, 4688, 4624, and 4698 for the last 24h',
        'what_to_look_for': 'Repeated suspicious process execution by host and user',
        'query_hint': '',
        'telemetry_anchor': '',
    }
    ioc_items = [
        {'ioc_type': 'url', 'ioc_value': 'https://bad.example/payload', 'source_ref': 'HQ feed', 'confidence_score': 5},
        {'ioc_type': 'domain', 'ioc_value': 'bad.example', 'source_ref': 'HQ feed', 'confidence_score': 5},
    ]

    related = _relevant_iocs_for_quick_check(card, ioc_items, limit=4)
    assert related == []


def test_related_iocs_require_semantic_link_not_type_only():
    card = {
        'question_text': 'Investigate suspicious DNS beaconing associated with Qilin infrastructure',
        'first_step': 'Use DNS logs for suspicious domains and callbacks',
        'what_to_look_for': 'Domains associated with Qilin campaigns',
        'query_hint': '',
        'telemetry_anchor': '',
    }
    ioc_items = [
        {'ioc_type': 'domain', 'ioc_value': 'bad-control.example', 'source_ref': 'Qilin intelligence update', 'confidence_score': 4},
        {'ioc_type': 'domain', 'ioc_value': 'random-domain.example', 'source_ref': 'Unrelated campaign report', 'confidence_score': 5},
    ]

    related = _relevant_iocs_for_quick_check(card, ioc_items, limit=4)
    values = [str(item.get('ioc_value') or '').lower() for item in related]

    assert 'bad-control.example' in values
    assert 'random-domain.example' not in values


def test_ioc_seen_within_days_enforces_recency_window():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(days=10)).isoformat()
    stale = (now - timedelta(days=220)).isoformat()

    parse = lambda value: datetime.fromisoformat(value) if value else None

    assert _ioc_seen_within_days({'last_seen_at': recent, 'created_at': recent}, days=180, parse_published_datetime=parse)
    assert not _ioc_seen_within_days({'last_seen_at': stale, 'created_at': stale}, days=180, parse_published_datetime=parse)


def test_evidence_backed_requires_ref_and_observable():
    assert _quick_check_is_evidence_backed_core(
        evidence_refs=[{'title': 'Report', 'date': '2026-02-20', 'url': 'https://example.test/r'}],
        observables={'event_ids': ['4688'], 'commands': [], 'markers': []},
    )
    assert not _quick_check_is_evidence_backed_core(
        evidence_refs=[],
        observables={'event_ids': ['4688'], 'commands': [], 'markers': []},
    )
    assert not _quick_check_is_evidence_backed_core(
        evidence_refs=[{'title': 'Report', 'date': '2026-02-20', 'url': 'https://example.test/r'}],
        observables={'event_ids': [], 'commands': [], 'markers': []},
    )


def test_event_id_selection_prefers_evidence_then_baseline_then_data_gap():
    evidence_mode = _select_event_ids_for_where_to_start_core(
        evidence_event_ids=['4688', '4104'],
        template_hint_event_ids=['4624'],
    )
    baseline_mode = _select_event_ids_for_where_to_start_core(
        evidence_event_ids=[],
        template_hint_event_ids=['4624', '4672'],
    )
    gap_mode = _select_event_ids_for_where_to_start_core(
        evidence_event_ids=[],
        template_hint_event_ids=[],
    )

    assert evidence_mode['mode'] == 'evidence'
    assert evidence_mode['event_ids'] == ['4688', '4104']
    assert baseline_mode['mode'] == 'baseline'
    assert str(baseline_mode['line']).startswith('Baseline suggestion:')
    assert gap_mode['mode'] == 'data_gap'
    assert 'Data gap:' in str(gap_mode['line'])


def test_no_self_ingestion_regression_event_ids_not_extracted_from_generated_quick_check_text():
    generated_quick_check_body = (
        'Behavior to hunt: suspicious host behavior. '
        'Where to start: Event IDs 4104, 4688, 4624, 4698 for last 24h.'
    )
    source_evidence_text = 'Observed suspicious PowerShell with encoded execution and vssadmin usage.'
    observables = _extract_behavior_observables(source_evidence_text)

    assert all(event_id not in observables.get('event_ids', []) for event_id in ['4104', '4688', '4624', '4698'])
    assert generated_quick_check_body  # explicit fixture to document non-input source
