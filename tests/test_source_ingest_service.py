from services.source_ingest_service import import_ransomware_live_actor_activity_core


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_ransomware_live_ingest_builds_actionable_summary():
    saved: list[dict[str, object]] = []

    def fake_http_get(url, timeout=20.0, follow_redirects=True):
        assert 'groupvictims/qilin' in url
        return _FakeResponse(
            200,
            [
                {
                    'victim': 'Acme Health',
                    'attackdate': '2026-02-10T00:00:00Z',
                    'country': 'US',
                    'sector': 'healthcare',
                },
                {
                    'victim': 'Beta Bank',
                    'attackdate': '2026-01-16T00:00:00Z',
                    'country': 'DE',
                    'sector': 'financial services',
                },
            ],
        )

    def fake_upsert(
        connection,
        actor_id,
        source_name,
        source_url,
        published_at,
        pasted_text,
        trigger_excerpt=None,
        title=None,
        headline=None,
        og_title=None,
        html_title=None,
        publisher=None,
        site_name=None,
    ):
        _ = connection
        saved.append(
            {
                'actor_id': actor_id,
                'source_name': source_name,
                'source_url': source_url,
                'published_at': published_at,
                'pasted_text': pasted_text,
                'trigger_excerpt': trigger_excerpt,
                'title': title,
                'headline': headline,
                'publisher': publisher,
                'site_name': site_name,
            }
        )
        return 'src-1'

    imported = import_ransomware_live_actor_activity_core(
        connection=object(),
        actor_id='actor-1',
        actor_terms=['Qilin'],
        deps={
            'http_get': fake_http_get,
            'now_iso': lambda: '2026-02-22T00:00:00+00:00',
            'upsert_source_for_actor': fake_upsert,
        },
    )

    assert imported == 1
    assert len(saved) == 1
    record = saved[0]
    assert record['source_name'] == 'Ransomware.live'
    assert 'victim disclosures in the last 90 days' in str(record['title'])
    assert 'recent victims include' in str(record['trigger_excerpt']).lower()
    assert 'Acme Health' in str(record['pasted_text'])
    assert 'Most frequent victim geographies' in str(record['pasted_text'])
