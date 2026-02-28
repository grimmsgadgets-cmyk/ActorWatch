from services import notebook_service


def test_finalize_notebook_contract_populates_required_sections():
    payload = {
        'actor': {'id': 'actor-1', 'display_name': 'APT Demo'},
        'counts': {'sources': 0},
        'priority_questions': [],
        'top_change_signals': [],
        'recent_activity_synthesis': [],
        'recent_activity_highlights': [],
        'environment_checks': [],
    }

    finalized = notebook_service.finalize_notebook_contract_core(payload)

    cards = finalized.get('priority_questions', [])
    assert isinstance(cards, list)
    assert len(cards) >= 1
    first = cards[0]
    assert str(first.get('title') or '').strip()
    assert str(first.get('where_to_start') or '').strip()
    assert str(first.get('what_to_watch') or '').strip()
    assert str(first.get('decision_rule') or '').strip()
    assert str(first.get('analyst_output') or '').strip()

    assert isinstance(finalized.get('priority_phase_groups'), list)
    assert isinstance(finalized.get('top_change_signals'), list)
    assert isinstance(finalized.get('recent_activity_synthesis'), list)
    assert isinstance(finalized.get('recent_activity_highlights'), list)
    assert isinstance(finalized.get('environment_checks'), list)
    assert finalized.get('contract_status', {}).get('cards_complete') is True


def test_finalize_notebook_contract_simplifies_card_language():
    payload = {
        'actor': {'id': 'actor-2', 'display_name': 'Test Group'},
        'priority_questions': [
            {
                'id': 'q1',
                'title': 'Hunt C2 and lateral movement activity',
                'where_to_start': 'Start with telemetry tied to C2 and lateral movement.',
                'what_to_watch': 'Monitor TTP clusters for exfiltration.',
                'decision_rule': 'Escalate when command and control repeats.',
                'analyst_output': 'Provide telemetry summary.',
                'evidence_used': ['No actor-linked evidence in last 30 days.'],
            }
        ],
    }
    finalized = notebook_service.finalize_notebook_contract_core(payload)
    card = finalized['priority_questions'][0]
    combined = ' '.join(
        [
            str(card.get('title') or ''),
            str(card.get('where_to_start') or ''),
            str(card.get('what_to_watch') or ''),
            str(card.get('decision_rule') or ''),
        ]
    ).lower()
    assert 'telemetry' not in combined
    assert 'command and control' not in combined
