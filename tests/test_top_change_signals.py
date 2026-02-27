from pipelines.notebook_pipeline import build_top_change_signals


def test_top_change_signals_filters_out_generic_activity_synthesis():
    items = [
        {
            'evidence_title': 'Qilin ransomware: 15 victim disclosures in the last 90 days',
            'category': 'activity synthesis',
            'ttp_ids': '',
            'target_text': '',
            'corroboration_sources': '1',
            'source_id': 's1',
            'timeline_event_id': '',
            'evidence_group_domain': 'ransomware.live',
        },
        {
            'evidence_title': 'Campaign shifted to VPN exploit chain',
            'category': 'initial access',
            'ttp_ids': 'T1190',
            'target_text': 'Retail',
            'corroboration_sources': '2',
            'source_id': 's2',
            'timeline_event_id': 't2',
            'evidence_group_domain': 'cisa.gov',
        },
    ]

    top = build_top_change_signals(items, limit=3)

    assert len(top) == 1
    assert top[0]['evidence_title'] == 'Campaign shifted to VPN exploit chain'


def test_top_change_signals_prefers_domain_diversity():
    items = [
        {
            'evidence_title': 'Signal 1',
            'category': 'initial access',
            'ttp_ids': 'T1190',
            'target_text': 'Finance',
            'corroboration_sources': '2',
            'source_id': 's1',
            'timeline_event_id': 't1',
            'evidence_group_domain': 'a.example',
        },
        {
            'evidence_title': 'Signal 2',
            'category': 'execution',
            'ttp_ids': 'T1059',
            'target_text': '',
            'corroboration_sources': '2',
            'source_id': 's2',
            'timeline_event_id': 't2',
            'evidence_group_domain': 'a.example',
        },
        {
            'evidence_title': 'Signal 3',
            'category': 'impact',
            'ttp_ids': 'T1486',
            'target_text': 'Healthcare',
            'corroboration_sources': '1',
            'source_id': 's3',
            'timeline_event_id': 't3',
            'evidence_group_domain': 'b.example',
        },
    ]

    top = build_top_change_signals(items, limit=2)

    assert len(top) == 2
    domains = {str(item['evidence_group_domain']) for item in top}
    assert domains == {'a.example', 'b.example'}


def test_top_change_signals_excludes_ransomware_live_trend_noise():
    items = [
        {
            'evidence_title': 'qilin ransomware: 15 victim disclosures in the last 90 days',
            'text': 'qilin ransomware activity synthesis (tempo, geography, and target examples) from ransomware.live.',
            'category': 'impact',
            'ttp_ids': '',
            'target_text': '',
            'corroboration_sources': '1',
            'source_id': 's1',
            'timeline_event_id': 't1',
            'evidence_group_domain': 'ransomware.live',
            'evidence_source_label': 'ransomware.live',
            'source_url': 'https://api.ransomware.live/v2/groupvictims/qilin',
        },
        {
            'evidence_title': 'Qilin campaign added ESXi encryption path',
            'text': 'First observed this month; not seen in prior reports.',
            'category': 'impact',
            'ttp_ids': 'T1486',
            'target_text': '',
            'corroboration_sources': '1',
            'source_id': 's2',
            'timeline_event_id': 't2',
            'evidence_group_domain': 'dexpose.io',
            'evidence_source_label': 'DeXpose',
            'source_url': 'https://dexpose.io/report-1',
        },
    ]

    top = build_top_change_signals(items, limit=3)

    assert len(top) == 1
    assert 'ESXi encryption path' in str(top[0]['evidence_title'])


def test_top_change_signals_penalizes_vendor_boilerplate_without_actor_match():
    items = [
        {
            'evidence_title': 'Check Point Harmony Endpoint provides protection against this threat',
            'text': 'Check Point IPS provides protection against this threat and command injection attempts.',
            'category': 'impact',
            'ttp_ids': 'T1190',
            'target_text': '',
            'corroboration_sources': '3',
            'source_id': 's1',
            'timeline_event_id': 't1',
            'evidence_group_domain': 'vendor.example',
        },
        {
            'evidence_title': 'Qilin shifted to VPN exploit chain in telecom sector',
            'text': 'New campaign observed this month with actor-linked payload staging.',
            'category': 'initial access',
            'ttp_ids': 'T1190',
            'target_text': 'Telecommunications',
            'corroboration_sources': '2',
            'source_id': 's2',
            'timeline_event_id': 't2',
            'evidence_group_domain': 'cisa.gov',
        },
    ]

    top = build_top_change_signals(items, actor_terms=['Qilin'], limit=3)
    assert len(top) == 1
    assert 'Qilin shifted to VPN exploit chain' in str(top[0]['evidence_title'])
