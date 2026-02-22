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
