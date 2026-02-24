import json

import services.ioc_hunt_service as ioc_hunt_service


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_generate_ioc_hunt_queries_accepts_only_valid_evidence_backed_items():
    cards = [
        {
            'id': 'thread-1',
            'quick_check_title': 'DNS beaconing check',
            'related_iocs': [
                {'ioc_type': 'domain', 'ioc_value': 'bad.example'},
                {'ioc_type': 'ip', 'ioc_value': '203.0.113.7'},
            ],
            'evidence': [
                {
                    'id': 'src-1',
                    'source_url': 'https://example.com/report',
                    'source_title': 'Report',
                    'source_date': '2026-02-23',
                    'excerpt': 'Beaconing to bad.example observed.',
                }
            ],
        }
    ]

    model_items = {
        'items': [
            {
                'card_id': 'thread-1',
                'platform': 'DNS/Proxy',
                'ioc_value': 'bad.example',
                'query': 'dns.query:"bad.example" AND action:allowed',
                'why_this_query': 'Checks active DNS beaconing.',
                'evidence_source_ids': ['src-1'],
            },
            {
                'card_id': 'thread-1',
                'platform': 'DNS/Proxy',
                'ioc_value': 'fake.example',
                'query': 'dns.query:"fake.example"',
                'why_this_query': 'Invalid IOC',
                'evidence_source_ids': ['src-1'],
            },
            {
                'card_id': 'thread-1',
                'platform': 'DNS/Proxy',
                'ioc_value': '203.0.113.7',
                'query': 'destination.ip:198.51.100.9',
                'why_this_query': 'Missing IOC in query',
                'evidence_source_ids': ['src-1'],
            },
            {
                'card_id': 'thread-1',
                'platform': 'DNS/Proxy',
                'ioc_value': 'bad.example',
                'query': 'dns.query:"bad.example"',
                'why_this_query': 'Bad evidence source',
                'evidence_source_ids': ['src-404'],
            },
        ]
    }

    def _http_post(_url, json_payload=None, timeout=None, **_kwargs):
        _ = json_payload
        _ = timeout
        return _FakeResponse({'response': json.dumps(model_items)})
    result = ioc_hunt_service.generate_ioc_hunt_queries_core(
        'Qilin',
        cards,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda key, default='': default,
            'http_post': _http_post,
        },
    )

    assert result['available'] is True
    valid_items = result['items_by_card'].get('thread-1', [])
    assert len(valid_items) == 1
    assert valid_items[0]['ioc_value'] == 'bad.example'


def test_generate_ioc_hunt_queries_returns_unavailable_when_no_ollama():
    result = ioc_hunt_service.generate_ioc_hunt_queries_core(
        'Qilin',
        cards=[],
        deps={
            'ollama_available': lambda: False,
            'get_env': lambda key, default='': default,
            'http_post': lambda *_args, **_kwargs: None,
        },
    )

    assert result['available'] is False
    assert isinstance(result['reason'], str)


def test_generate_ioc_hunt_queries_applies_environment_personalization():
    cards = [
        {
            'id': 'thread-1',
            'quick_check_title': 'DNS beaconing check',
            'related_iocs': [{'ioc_type': 'domain', 'ioc_value': 'bad.example'}],
            'evidence': [{'id': 'src-1', 'source_url': 'https://example.com/r'}],
        }
    ]
    model_items = {
        'items': [
            {
                'card_id': 'thread-1',
                'platform': 'DNS/Proxy',
                'ioc_value': 'bad.example',
                'query': 'domain:bad.example',
                'why_this_query': '',
                'evidence_source_ids': ['src-1'],
            }
        ]
    }

    def _http_post(_url, json_payload=None, timeout=None, **_kwargs):
        _ = json_payload
        _ = timeout
        return _FakeResponse({'response': json.dumps(model_items)})

    result = ioc_hunt_service.generate_ioc_hunt_queries_core(
        'Qilin',
        cards,
        environment_profile={'query_dialect': 'splunk', 'default_time_window_hours': 24, 'field_mapping': {}},
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda key, default='': default,
            'http_post': _http_post,
            'personalize_query': lambda query, **_kwargs: f'{query} earliest=-24h',
        },
    )

    query = result['items_by_card']['thread-1'][0]['query']
    assert 'earliest=-24h' in str(query)


def test_generate_ioc_hunt_queries_ignores_non_actionable_domain_tokens():
    cards = [
        {
            'id': 'thread-1',
            'quick_check_title': 'PowerShell check',
            'related_iocs': [{'ioc_type': 'domain', 'ioc_value': 'next.js'}],
            'evidence': [{'id': 'src-1', 'source_url': 'https://example.com/r'}],
        }
    ]
    result = ioc_hunt_service.generate_ioc_hunt_queries_core(
        'Qilin',
        cards,
        deps={
            'ollama_available': lambda: True,
            'get_env': lambda key, default='': default,
            'http_post': lambda *_args, **_kwargs: None,
        },
    )

    assert result['available'] is False
    assert 'IOC context' in str(result['reason'])
