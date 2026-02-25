import sqlite3
import json

import app as app_module
import services.web_backfill_service as web_backfill_service


def _setup_db(tmp_path):
    app_module.DB_PATH = str(tmp_path / 'test.db')
    app_module.initialize_sqlite()


def test_backfill_service_dedupes_urls_and_does_not_reinsert_existing(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Dedupe', None)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=actor['id'],
            source_name='CISA',
            source_url='https://www.cisa.gov/news-events/alerts/example-article',
            published_at='2025-01-01T00:00:00+00:00',
            pasted_text='Older article to make actor cold and test dedupe.',
        )
        connection.commit()

    def _search_candidates(_terms, _domains):
        return [
            'https://www.cisa.gov/news-events/alerts/example-article?utm_source=test',
            'https://www.cisa.gov/news-events/alerts/example-article',
            'https://attack.mitre.org/software/S1234/',
            'https://attack.mitre.org/software/S1234/',
        ]

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'mitre',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': 'APT-Dedupe reporting with sufficient analyst text and behaviors.' * 4,
            'title': 'MITRE S1234 profile',
            'headline': 'MITRE S1234 profile',
            'og_title': 'MITRE S1234 profile',
            'html_title': 'MITRE S1234 profile',
            'publisher': 'MITRE',
            'site_name': 'ATT&CK',
            'source_tier': 'high',
            'confidence_weight': 4,
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Dedupe',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    with sqlite3.connect(app_module.DB_PATH) as connection:
        rows = connection.execute(
            'SELECT url, source_type FROM sources WHERE actor_id = ? ORDER BY url ASC',
            (actor['id'],),
        ).fetchall()
    urls = [str(row[0]) for row in rows]
    assert len(urls) == 2
    assert any('attack.mitre.org/software/S1234' in url for url in urls)
    assert any(str(row[1] or '') == 'web_backfill' for row in rows)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        run_row = connection.execute(
            '''
            SELECT candidates_found, pages_fetched, pages_parsed_ok, sources_inserted, error_summary_json
            FROM backfill_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    assert run_row is not None
    assert int(run_row[3] or 0) >= 1
    error_summary = json.loads(str(run_row[4] or '{}'))
    assert isinstance(error_summary, dict)


def test_backfill_service_records_failure_reasons(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Errors', None)

    class _Resp403:
        status_code = 403
        text = ''

    def _http_get(*_args, **_kwargs):
        return _Resp403()

    def _derive_source_from_url(*_args, **_kwargs):
        raise TimeoutError('timed out')

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Errors',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': _http_get,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
            'backfill_max_seconds': 6.0,
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) == 0
    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT queries_attempted, candidates_found, pages_fetched, sources_inserted, error_summary_json
            FROM backfill_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    assert row is not None
    errors = json.loads(str(row[4] or '{}'))
    assert isinstance(errors, dict)
    assert int(errors.get('403') or 0) >= 1
    assert isinstance(errors.get('not_in_allowlist_domains', []), list)
    assert isinstance(errors.get('not_in_allowlist_registrable_domains', []), list)


def test_backfill_rejects_final_redirect_domain_and_tracks_rejected_domains(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Redirect', None)

    def _search_candidates(_terms, _domains):
        return ['https://unit42.paloaltonetworks.com/redirecting-writeup']

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = url
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': 'https://sub.evil-example.org/post',
            'source_name': 'unknown',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': 'APT-Redirect sample text with enough length.' * 5,
            'title': 'redirected article',
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Redirect',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) == 0

    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT sources_inserted, error_summary_json
            FROM backfill_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    assert row is not None
    assert int(row[0] or 0) == 0
    errors = json.loads(str(row[1] or '{}'))
    rejected_domains = errors.get('not_in_allowlist_domains', [])
    assert isinstance(rejected_domains, list)
    assert any(str(item[1]) == 'evil-example.org' for item in rejected_domains if isinstance(item, list) and len(item) >= 2)
    rejected_registrables = errors.get('not_in_allowlist_registrable_domains', [])
    assert isinstance(rejected_registrables, list)
    assert any(str(item[0]) == 'evil-example.org' for item in rejected_registrables if isinstance(item, list) and len(item) >= 1)


def test_backfill_primary_allowlist_includes_batch1_registrable_domains():
    expected = {
        'thedfirreport.com',
        'recordedfuture.com',
        'intel471.com',
        'sygnia.co',
        'checkpoint.com',
    }
    assert expected.issubset(set(web_backfill_service.PRIMARY_ALLOWLIST_REGISTRABLE))


def test_cluster_label_plus_mitre_structured_ingests_without_actor_name(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Cluster', None)

    def _search_candidates(_terms, _domains):
        return ['https://attack.mitre.org/software/S1234/']

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'mitre',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': ('Investigation notes map activity to UNC3944 tradecraft. ' * 12).strip(),
            'title': 'ATT&CK Software S1234',
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Cluster',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) >= 1
    with sqlite3.connect(app_module.DB_PATH) as connection:
        link_row = connection.execute(
            '''
            SELECT match_score, match_reasons_json, matched_terms_json, matcher_version
            FROM backfill_source_linkage
            WHERE actor_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    assert link_row is not None
    assert int(link_row[0] or 0) >= 3
    assert str(link_row[3] or '') == 'v2_scored_linking'
    reasons = json.loads(str(link_row[1] or '[]'))
    assert 'cluster_label' in reasons
    assert 'mitre_structured' in reasons


def test_mitre_structured_url_ingests_without_actor_term_in_text(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Structured', None)

    def _search_candidates(_terms, _domains):
        return ['https://attack.mitre.org/groups/G1234/']

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'mitre',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': ('Structured ATT&CK group profile with software and technique mappings. ' * 10).strip(),
            'title': 'ATT&CK Group G1234',
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Structured',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) >= 1


def test_generic_article_rejected_with_score_below_threshold(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Generic', None)

    def _search_candidates(_terms, _domains):
        return ['https://www.cisa.gov/news-events/alerts/generic-threat-update']

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'cisa',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': ('This article discusses general cyber hygiene and ransomware trends for all organizations. ' * 10).strip(),
            'title': 'General Threat Advisory',
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Generic',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) == 0
    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT error_summary_json
            FROM backfill_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    errors = json.loads(str(row[0] or '{}')) if row else {}
    assert int(errors.get('score_below_threshold') or 0) >= 1


def test_quick_check_style_text_does_not_raise_linkage_score(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Regression', None)

    def _search_candidates(_terms, _domains):
        return ['https://www.cisa.gov/news-events/alerts/telemetry-guidance']

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'cisa',
            'published_at': '2026-02-24T00:00:00+00:00',
            'pasted_text': (
                'Quick check guidance: monitor Event ID 4104, 4688, 4624, 4698 and validate logging coverage. '
                'Baseline suggestion only, no actor-linked evidence.'
            ) * 6,
            'title': 'Generic telemetry guidance',
        }

    result = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-Regression',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': lambda *_args, **_kwargs: None,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(result.get('ran'))
    assert int(result.get('inserted') or 0) == 0
    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT error_summary_json
            FROM backfill_runs
            WHERE actor_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    errors = json.loads(str(row[0] or '{}')) if row else {}
    assert int(errors.get('score_below_threshold') or 0) >= 1


def test_blank_published_at_sets_ingested_date_type_and_recency_fallback(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-IngestedDate', None)

    def _search_candidates(_terms, _domains):
        return ['https://attack.mitre.org/groups/G1234/']

    class _Resp404:
        status_code = 404
        text = ''

    def _http_get(*_args, **_kwargs):
        return _Resp404()

    def _derive_source_from_url(url, fallback_source_name=None, published_hint=None, fetch_timeout_seconds=18.0):
        _ = fallback_source_name
        _ = published_hint
        _ = fetch_timeout_seconds
        return {
            'source_url': url,
            'source_name': 'mitre',
            'published_at': '',
            'pasted_text': ('ATT&CK group profile details and procedure examples. ' * 12).strip(),
            'title': 'ATT&CK Group G1234',
        }

    first = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-IngestedDate',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': _http_get,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(first.get('ran'))
    assert int(first.get('inserted') or 0) >= 1

    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT published_at, ingested_at, source_date_type
            FROM sources
            WHERE actor_id = ?
            ORDER BY COALESCE(published_at, ingested_at, retrieved_at) DESC
            LIMIT 1
            ''',
            (actor['id'],),
        ).fetchone()
    assert row is not None
    assert str(row[0] or '') == ''
    assert str(row[1] or '').strip() != ''
    assert str(row[2] or '') == 'ingested'

    second = web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor['id'],
        actor_name='APT-IngestedDate',
        actor_aliases=[],
        deps={
            'db_path': lambda: app_module.DB_PATH,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': lambda: '2026-02-25T00:00:00+00:00',
            'http_get': _http_get,
            'search_candidates': _search_candidates,
            'derive_source_from_url': _derive_source_from_url,
            'upsert_source_for_actor': app_module._upsert_source_for_actor,  # noqa: SLF001
        },
    )
    assert bool(second.get('ran')) is False


def test_existing_published_records_keep_published_date_type(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-ExistingPublished', None)
    with sqlite3.connect(app_module.DB_PATH) as connection:
        source_id = app_module._upsert_source_for_actor(  # noqa: SLF001
            connection=connection,
            actor_id=actor['id'],
            source_name='Unit42',
            source_url='https://unit42.paloaltonetworks.com/existing-published/',
            published_at='2026-02-20T00:00:00+00:00',
            pasted_text='Existing source with explicit published date.' * 6,
        )
        connection.commit()

    with sqlite3.connect(app_module.DB_PATH) as connection:
        row = connection.execute(
            '''
            SELECT id, published_at, ingested_at, source_date_type
            FROM sources
            WHERE actor_id = ? AND id = ?
            ''',
            (actor['id'], source_id),
        ).fetchone()
    assert row is not None
    assert str(row[1] or '') == '2026-02-20T00:00:00+00:00'
    assert str(row[2] or '').strip() != ''
    assert str(row[3] or '') == 'published'
