from datetime import datetime, timezone

import services.quick_checks_view_service as quick_checks_view_service


def test_is_in_window_core_filters_30_day_window():
    start = datetime(2026, 1, 25, tzinfo=timezone.utc)
    end = datetime(2026, 2, 24, tzinfo=timezone.utc)

    assert quick_checks_view_service.is_in_window_core(
        '2026-02-10T00:00:00+00:00',
        window_start=start,
        window_end=end,
    )
    assert not quick_checks_view_service.is_in_window_core(
        '2025-12-20T00:00:00+00:00',
        window_start=start,
        window_end=end,
    )


def test_rank_quick_checks_core_uses_severity_then_evidence_then_recency():
    ranked = quick_checks_view_service.rank_quick_checks_core(
        [
            {
                'id': 'low-evidence',
                'priority': 'Low',
                'has_evidence': True,
                'last_seen_evidence_at': '2026-02-23T00:00:00+00:00',
            },
            {
                'id': 'high-no-evidence',
                'priority': 'High',
                'has_evidence': False,
                'last_seen_evidence_at': '',
            },
            {
                'id': 'high-with-evidence-older',
                'priority': 'High',
                'has_evidence': True,
                'last_seen_evidence_at': '2026-02-21T00:00:00+00:00',
            },
            {
                'id': 'high-with-evidence-newer',
                'priority': 'High',
                'has_evidence': True,
                'last_seen_evidence_at': '2026-02-24T00:00:00+00:00',
            },
        ]
    )
    assert [item['id'] for item in ranked] == [
        'high-with-evidence-newer',
        'high-with-evidence-older',
        'high-no-evidence',
        'low-evidence',
    ]


def test_rank_quick_checks_core_uses_evidence_tier_before_has_evidence():
    ranked = quick_checks_view_service.rank_quick_checks_core(
        [
            {'id': 'tier-c', 'priority': 'High', 'evidence_tier': 'C', 'has_evidence': True, 'last_seen_evidence_at': '2026-02-24T00:00:00+00:00'},
            {'id': 'tier-b', 'priority': 'High', 'evidence_tier': 'B', 'has_evidence': True, 'last_seen_evidence_at': '2026-02-20T00:00:00+00:00'},
            {'id': 'tier-d', 'priority': 'High', 'evidence_tier': 'D', 'has_evidence': False, 'last_seen_evidence_at': ''},
        ]
    )
    assert [item['id'] for item in ranked] == ['tier-b', 'tier-c', 'tier-d']


def test_apply_no_evidence_rule_core_sets_required_rendering_text():
    card = {
        'id': 'check-1',
        'has_evidence': False,
        'first_step': 'Run baseline telemetry review.',
        'evidence_used': ['some prior value'],
    }
    updated = quick_checks_view_service.apply_no_evidence_rule_core(card)
    assert str(updated.get('first_step') or '').startswith('Data gap:')
    assert updated.get('evidence_used') == ['No thread-linked evidence in last 30 days.']


def test_filter_iocs_for_check_core_prefilters_by_type_and_value():
    iocs = [
        {'ioc_type': 'domain', 'ioc_value': 'one.example'},
        {'ioc_type': 'domain', 'ioc_value': 'two.example'},
        {'ioc_type': 'ip', 'ioc_value': '198.51.100.10'},
    ]
    filtered = quick_checks_view_service.filter_iocs_for_check_core(
        iocs,
        relevant_types={'domain'},
        relevant_values={'two.example'},
    )
    assert len(filtered) == 1
    assert filtered[0]['ioc_type'] == 'domain'
    assert filtered[0]['ioc_value'] == 'two.example'
