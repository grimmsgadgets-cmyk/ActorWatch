import priority_questions


def test_short_decision_trigger_uses_change_language_for_edge_activity():
    text = priority_questions.short_decision_trigger(
        'Are there signs of CVE exploitation against exposed VPN services?'
    )
    assert text.startswith('What changed since last review')
    assert 'external intrusion signals' in text


def test_short_decision_trigger_uses_change_language_for_default_case():
    text = priority_questions.short_decision_trigger('Is this actor currently active in our environment?')
    assert text == 'What changed since last review in this activity across current telemetry?'


def test_expected_output_line_for_lateral_movement():
    text = priority_questions.expected_output_line(
        'Are RDP and SMB pivots increasing between internal hosts?'
    )
    assert 'Record lateral movement delta' in text
    assert 'confidence shift' in text


def test_expected_output_line_default_shape():
    text = priority_questions.expected_output_line('General anomaly check question')
    assert text.startswith('Record the observed delta versus prior review')
    assert 'source links' in text


def test_priority_next_best_action_windows_execution_is_specific():
    text = priority_questions.priority_next_best_action(
        'Check for suspicious PowerShell execution activity',
        'Windows Event Logs, EDR',
    )
    assert 'Event IDs 4104, 4688, and 4698' in text
    assert 'scheduled tasks' in text


def test_priority_next_best_action_m365_is_specific():
    text = priority_questions.priority_next_best_action(
        'Check for phishing sender behavior',
        'M365, Email Gateway',
    )
    assert 'Defender Advanced Hunting' in text
    assert 'EmailEvents' in text


def test_priority_next_best_action_firewall_vpn_is_specific():
    text = priority_questions.priority_next_best_action(
        'Check for active edge intrusion signals',
        'Firewall/VPN, EDR',
    )
    assert 'firewall/VPN query' in text
    assert 'group by source IP and destination asset' in text
