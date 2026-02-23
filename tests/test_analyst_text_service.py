from services.analyst_text_service import ollama_review_change_signals_core
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
