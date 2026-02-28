import sqlite3
import time
import asyncio
from datetime import datetime, timezone
from fastapi import BackgroundTasks

import pytest

import app as app_module
import route_paths
from tests.notebook_test_helpers import JsonRequest as _JsonRequest
from tests.notebook_test_helpers import app_endpoint as _app_endpoint
from tests.notebook_test_helpers import http_request as _http_request
from tests.notebook_test_helpers import setup_db as _setup_db


def test_fetch_actor_notebook_payload_shape_regression(tmp_path):
    _setup_db(tmp_path)
    actor = app_module.create_actor_profile('APT-Payload', 'Payload shape scope')

    notebook = app_module._fetch_actor_notebook(actor['id'])  # noqa: SLF001

    required_keys = {
        'actor',
        'recent_activity_highlights',
        'priority_questions',
        'kpis',
    }
    assert required_keys.issubset(notebook.keys())
    assert isinstance(notebook['actor'], dict)
    assert isinstance(notebook['recent_activity_highlights'], list)
    assert isinstance(notebook['priority_questions'], list)
    assert isinstance(notebook['kpis'], dict)


def test_ioc_hunts_data_enforces_actor_scoping_end_to_end(tmp_path):
    _setup_db(tmp_path)
    actor_a = app_module.create_actor_profile('APT-A', 'Scope A')
    actor_b = app_module.create_actor_profile('APT-B', 'Scope B')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-a-1',
                actor_a['id'],
                'domain',
                'actor-a-only.example',
                'feed-a',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                2,
                4,
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-b-1',
                actor_b['id'],
                'domain',
                'actor-b-only.example',
                'feed-b',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                2,
                4,
            ),
        )
        connection.commit()

    notebook_a = app_module._fetch_actor_notebook(actor_a['id'])  # noqa: SLF001
    serialized_a = str(notebook_a)
    assert 'actor-a-only.example' in serialized_a
    assert 'actor-b-only.example' not in serialized_a


def test_actor_live_data_enforces_actor_scoping(tmp_path):
    _setup_db(tmp_path)
    actor_a = app_module.create_actor_profile('APT-Live-A', 'Live scope A')
    actor_b = app_module.create_actor_profile('APT-Live-B', 'Live scope B')

    with sqlite3.connect(app_module.DB_PATH) as connection:
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-live-a',
                actor_a['id'],
                'domain',
                'actor-a-live.example',
                'feed-a',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                1,
                4,
            ),
        )
        connection.execute(
            '''
            INSERT INTO ioc_items (
                id, actor_id, ioc_type, ioc_value, source_ref, validation_status,
                lifecycle_status, revoked, is_active, created_at, last_seen_at, seen_count, confidence_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                'ioc-live-b',
                actor_b['id'],
                'domain',
                'actor-b-live.example',
                'feed-b',
                'valid',
                'active',
                0,
                1,
                '2026-02-23T00:00:00+00:00',
                '2026-02-23T00:00:00+00:00',
                1,
                4,
            ),
        )
        connection.commit()

    notebook_a = app_module._fetch_actor_notebook(actor_a['id'])  # noqa: SLF001
    serialized_a = str(notebook_a)
    assert 'actor-a-live.example' in serialized_a
    assert 'actor-b-live.example' not in serialized_a


