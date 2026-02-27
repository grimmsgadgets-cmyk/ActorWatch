from services.analyst_text_service import ollama_review_change_signals_core, ollama_synthesize_recent_activity_core
from services.source_ingest_service import parse_published_datetime_core


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_ollama_review_change_signals_uses_rolling_baseline_when_older_missing():
    source_items = [
        {
            'published_at': '2026-01-20T00:00:00+00:00',
            'retrieved_at': '',
            'title': 'Older in-window baseline report',
            'source_name': 'Source A',
            'url': 'https://example.com/a',
            'pasted_text': 'Historical behavior.',
        },
        {
            'published_at': '2026-02-18T00:00:00+00:00',
            'retrieved_at': '',
            'title': 'Recent report',
            'source_name': 'Source B',
            'url': 'https://example.com/b',
            'pasted_text': 'New exploit chain observed.',
        },
    ]
    highlights = [
        {
            'source_published_at': '2026-02-18T00:00:00+00:00',
            'date': '2026-02-18',
            'evidence_title': 'New exploit chain observed',
            'evidence_source_label': 'Source B',
            'source_url': 'https://example.com/b',
            'category': 'initial_access',
            'ttp_ids': 'T1190',
            'target_text': 'Retail',
            'text': 'Actor shifted to edge exploit activity.',
        }
    ]

    posted_payloads: list[dict[str, object]] = []

    def fake_http_post(url, json, timeout):
        _ = url
        _ = timeout
        posted_payloads.append(json)
        return _FakeResponse(
            {
                'response': (
                    '{"changes":[{"summary":"Shifted to VPN exploit chain","why_new":"Not present in prior baseline reports.",'
                    '"window_days":30,"source_url":"https://example.com/b","source_label":"Source B",'
                    '"source_date":"2026-02-18","category":"initial_access","ttp_ids":["T1190"],'
                    '"target":"Retail","confidence":"high"}]}'
                )
            }
        )

    results = ollama_review_change_signals_core(
        'Qilin',
        source_items,
        highlights,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda key, default=None: default,
            'http_post': fake_http_post,
            'parse_published_datetime': parse_published_datetime_core,
        },
    )

    assert results
    assert results[0]['change_summary'] == 'Shifted to VPN exploit chain'
    assert results[0]['change_window_days'] == '30'
    assert isinstance(results[0]['validated_sources'], list)
    assert results[0]['validated_sources']
    assert posted_payloads


def test_ollama_review_change_signals_retries_after_first_failure():
    source_items = [
        {
            'published_at': '2026-02-18T00:00:00+00:00',
            'retrieved_at': '',
            'title': 'Recent report',
            'source_name': 'Source B',
            'url': 'https://example.com/b',
            'pasted_text': 'New exploit chain observed.',
        },
        {
            'published_at': '2026-01-20T00:00:00+00:00',
            'retrieved_at': '',
            'title': 'Baseline report',
            'source_name': 'Source A',
            'url': 'https://example.com/a',
            'pasted_text': 'Historical behavior.',
        },
    ]
    highlights = [
        {
            'source_published_at': '2026-02-18T00:00:00+00:00',
            'date': '2026-02-18',
            'evidence_title': 'New exploit chain observed',
            'evidence_source_label': 'Source B',
            'source_url': 'https://example.com/b',
            'category': 'initial_access',
            'ttp_ids': 'T1190',
            'target_text': 'Retail',
            'text': 'Actor shifted to edge exploit activity.',
        }
    ]
    calls = {'count': 0}

    def fake_http_post(url, json, timeout):
        _ = url
        _ = json
        _ = timeout
        calls['count'] += 1
        if calls['count'] == 1:
            raise TimeoutError('simulated first-attempt timeout')
        return _FakeResponse(
            {
                'response': (
                    '{"changes":[{"summary":"Shifted to VPN exploit chain","why_new":"Not present in prior baseline reports.",'
                    '"window_days":30,"source_url":"https://example.com/b","source_label":"Source B",'
                    '"source_date":"2026-02-18","category":"initial_access","ttp_ids":["T1190"],'
                    '"target":"Retail","confidence":"high"}]}'
                )
            }
        )

    results = ollama_review_change_signals_core(
        'Qilin',
        source_items,
        highlights,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda _key, default=None: default,
            'http_post': fake_http_post,
            'parse_published_datetime': parse_published_datetime_core,
        },
    )

    assert calls['count'] == 2
    assert results
    assert results[0]['change_summary'] == 'Shifted to VPN exploit chain'


def test_ollama_synthesize_recent_activity_returns_ordered_cards():
    highlights = [
        {
            'date': '2026-02-18',
            'category': 'initial_access',
            'target_text': 'Healthcare',
            'ttp_ids': 'T1190',
            'evidence_source_label': 'Source B',
            'source_url': 'https://example.com/b',
            'text': 'Edge exploit chain observed.',
        }
    ]

    def fake_http_post(url, json, timeout):
        _ = url
        _ = json
        _ = timeout
        return _FakeResponse(
            {
                'response': (
                    '{"items":['
                    '{"label":"Who is affected","text":"Healthcare entities were repeatedly targeted.","confidence":"Medium"},'
                    '{"label":"What changed","text":"Edge exploit activity increased in recent reporting.","confidence":"High"},'
                    '{"label":"What to do next","text":"Prioritize VPN edge telemetry and exploit detections.","confidence":"High"}'
                    ']}'
                )
            }
        )

    rows = ollama_synthesize_recent_activity_core(
        'Qilin',
        highlights,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda _key, default=None: default,
            'http_post': fake_http_post,
        },
    )

    assert [row['label'] for row in rows] == ['What changed', 'Who is affected', 'What to do next']
    assert rows[0]['confidence'] == 'High'
    assert rows[1]['lineage'] == '1 sources'


def test_ollama_synthesize_recent_activity_returns_empty_when_unavailable():
    rows = ollama_synthesize_recent_activity_core(
        'Qilin',
        [{'text': 'sample', 'source_url': 'https://example.com'}],
        deps={
            'ollama_available': lambda: False,
            'get_env': lambda _key, default=None: default,
            'http_post': lambda *_args, **_kwargs: _FakeResponse({'response': '{}'}),
        },
    )
    assert rows == []


def test_ollama_synthesize_recent_activity_retries_with_compact_payload_after_failure():
    highlights = [
        {
            'date': '2026-02-18',
            'category': 'initial_access',
            'target_text': 'Healthcare',
            'ttp_ids': 'T1190',
            'evidence_source_label': 'Source B',
            'source_url': 'https://example.com/b',
            'text': 'Edge exploit chain observed.',
        }
    ]
    calls: list[tuple[float, str]] = []

    def fake_http_post(url, json, timeout):
        _ = url
        calls.append((float(timeout), str(json.get('prompt') or '')))
        if len(calls) == 1:
            raise TimeoutError('simulated timeout')
        return _FakeResponse(
            {
                'response': (
                    '{"items":['
                    '{"label":"What changed","text":"Edge exploit activity increased.","confidence":"High"},'
                    '{"label":"Who is affected","text":"Healthcare entities are impacted.","confidence":"Medium"},'
                    '{"label":"What to do next","text":"Prioritize edge exploit detections.","confidence":"High"}'
                    ']}'
                )
            }
        )

    rows = ollama_synthesize_recent_activity_core(
        'Qilin',
        highlights,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda key, default=None: '20' if key == 'RECENT_ACTIVITY_OLLAMA_RETRY_TIMEOUT_SECONDS' else default,
            'http_post': fake_http_post,
        },
    )

    assert len(calls) == 2
    assert calls[0][0] == 15.0
    assert calls[1][0] == 20.0
    assert [row['label'] for row in rows] == ['What changed', 'Who is affected', 'What to do next']
