from datetime import datetime, timedelta, timezone

from pipelines.notebook_pipeline import _extract_ioc_candidates_from_text, _ioc_seen_within_days, _relevant_iocs_for_quick_check


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
