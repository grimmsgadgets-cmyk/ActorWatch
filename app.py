import json
import hashlib
import logging
import os
import re
import socket
import sqlite3
import string
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from urllib.parse import urlparse

import httpx
import services.actor_state_service as actor_state_service
import services.actor_profile_service as actor_profile_service
import guidance_catalog
import services.generation_service as generation_service
import services.generation_journal_service as generation_journal_service
import services.feed_import_service as feed_import_service
import legacy_ui
import mitre_store
import services.db_schema_service as db_schema_service
import services.activity_highlight_service as activity_highlight_service
import services.analyst_text_service as analyst_text_service
import services.actor_search_service as actor_search_service
import services.app_wiring_service as app_wiring_service
import priority_questions
import services.priority_service as priority_service
import services.quick_check_service as quick_check_service
import services.rate_limit_service as rate_limit_service
import services.recent_activity_service as recent_activity_service
import services.refresh_ops_service as refresh_ops_service
import routes.routes_api as routes_api
import routes.routes_actor_ops as routes_actor_ops
import routes.routes_dashboard as routes_dashboard
import routes.routes_evolution as routes_evolution
import routes.routes_notebook as routes_notebook
import routes.routes_ui as routes_ui
import services.network_service as network_service
import services.notebook_service as notebook_service
import services.ioc_hunt_service as ioc_hunt_service
import services.ioc_store_service as ioc_store_service
import services.ioc_validation_service as ioc_validation_service
import services.source_ingest_service as source_ingest_service
import services.source_derivation_service as source_derivation_service
import services.source_store_service as source_store_service
import services.stix_service as stix_service
import services.web_backfill_service as web_backfill_service
import services.feedback_service as feedback_service
import services.environment_profile_service as environment_profile_service
import services.source_reliability_service as source_reliability_service
import services.requirements_service as requirements_service
import services.status_service as status_service
import services.metrics_service as metrics_service
import services.llm_cache_service as llm_cache_service
import services.alert_delivery_service as alert_delivery_service
import pipelines.timeline_extraction as timeline_extraction
import services.timeline_analytics_service as timeline_analytics_service
import services.timeline_view_service as timeline_view_service
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pipelines.feed_ingest import import_default_feeds_for_actor_core as pipeline_import_default_feeds_for_actor_core
from pipelines.generation_runner import run_actor_generation_core as pipeline_run_actor_generation_core
from pipelines.notebook_builder import build_notebook_core
from pipelines.notebook_pipeline import build_environment_checks as pipeline_build_environment_checks
from pipelines.notebook_pipeline import fetch_actor_notebook_core as pipeline_fetch_actor_notebook_core
from pipelines.notebook_pipeline import build_recent_activity_highlights as pipeline_build_recent_activity_highlights
from pipelines.notebook_pipeline import latest_reporting_recency_label as pipeline_latest_reporting_recency_label
from pipelines.notebook_pipeline import recent_change_summary as pipeline_recent_change_summary
from pipelines.notebook_pipeline import build_top_change_signals as pipeline_build_top_change_signals
from pipelines.requirements_pipeline import generate_actor_requirements_core as pipeline_generate_actor_requirements_core
from pipelines.source_derivation import canonical_group_domain as pipeline_canonical_group_domain
from pipelines.source_derivation import derive_source_from_url_core as pipeline_derive_source_from_url_core
from pipelines.source_derivation import evidence_source_label_from_source as pipeline_evidence_source_label_from_source
from pipelines.source_derivation import evidence_title_from_source as pipeline_evidence_title_from_source
from pipelines.source_derivation import extract_meta as pipeline_extract_meta
from pipelines.source_derivation import fallback_title_from_url as pipeline_fallback_title_from_url
from pipelines.source_derivation import strip_html as pipeline_strip_html


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    global AUTO_REFRESH_STOP_EVENT, AUTO_REFRESH_THREAD, GENERATION_WORKER_STOP_EVENT
    initialize_sqlite()
    GENERATION_WORKER_STOP_EVENT = Event()
    generation_service.start_generation_workers_core(
        deps={
            'run_actor_generation': run_actor_generation,
            'run_actor_llm_enrichment': run_actor_llm_enrichment,
            'stop_event': GENERATION_WORKER_STOP_EVENT,
        }
    )
    if AUTO_REFRESH_ENABLED:
        AUTO_REFRESH_STOP_EVENT = Event()
        AUTO_REFRESH_THREAD = Thread(
            target=_auto_refresh_loop,
            args=(AUTO_REFRESH_STOP_EVENT,),
            daemon=True,
            name='actor-auto-refresh',
        )
        AUTO_REFRESH_THREAD.start()
    try:
        yield
    finally:
        if AUTO_REFRESH_STOP_EVENT is not None:
            AUTO_REFRESH_STOP_EVENT.set()
        if AUTO_REFRESH_THREAD is not None:
            AUTO_REFRESH_THREAD.join(timeout=2.0)
        if GENERATION_WORKER_STOP_EVENT is not None:
            GENERATION_WORKER_STOP_EVENT.set()
            generation_service.stop_generation_workers_core()
        AUTO_REFRESH_STOP_EVENT = None
        AUTO_REFRESH_THREAD = None
        GENERATION_WORKER_STOP_EVENT = None


app = FastAPI(lifespan=app_lifespan)
DB_PATH = '/data/app.db'
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / 'templates'))
app.mount('/static', StaticFiles(directory=str(BASE_DIR / 'static')), name='static')
ATTACK_ENTERPRISE_STIX_URL = (
    'https://raw.githubusercontent.com/mitre/cti/master/enterprise-attack/enterprise-attack.json'
)
MITRE_GROUP_CACHE: list[dict[str, object]] | None = None
MITRE_DATASET_CACHE: dict[str, object] | None = None
MITRE_TECHNIQUE_PHASE_CACHE: dict[str, list[str]] | None = None
MITRE_CAMPAIGN_LINK_CACHE: dict[str, dict[str, set[str]]] | None = None
MITRE_TECHNIQUE_INDEX_CACHE: dict[str, dict[str, str]] | None = None
MITRE_SOFTWARE_CACHE: list[dict[str, object]] | None = None
AUTO_REFRESH_ENABLED = os.environ.get('AUTO_REFRESH_ENABLED', '1').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
AUTO_REFRESH_MIN_INTERVAL_HOURS = max(1, int(os.environ.get('AUTO_REFRESH_MIN_INTERVAL_HOURS', '24')))
AUTO_REFRESH_LOOP_SECONDS = max(30, int(os.environ.get('AUTO_REFRESH_LOOP_SECONDS', '300')))
AUTO_REFRESH_BATCH_SIZE = max(1, int(os.environ.get('AUTO_REFRESH_BATCH_SIZE', '3')))
PAGE_REFRESH_AUTO_TRIGGER_MINUTES = max(0, int(os.environ.get('PAGE_REFRESH_AUTO_TRIGGER_MINUTES', '30')))
RUNNING_STALE_RECOVERY_MINUTES = max(5, int(os.environ.get('RUNNING_STALE_RECOVERY_MINUTES', '10')))
AUTO_MERGE_DUPLICATE_ACTORS = os.environ.get('AUTO_MERGE_DUPLICATE_ACTORS', '1').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
ACTOR_FEED_LOOKBACK_DAYS = int(os.environ.get('ACTOR_FEED_LOOKBACK_DAYS', '180'))
FEED_IMPORT_MAX_SECONDS = max(20, int(os.environ.get('FEED_IMPORT_MAX_SECONDS', '90')))
FEED_FETCH_TIMEOUT_SECONDS = max(3.0, float(os.environ.get('FEED_FETCH_TIMEOUT_SECONDS', '10')))
FEED_ENTRY_SCAN_LIMIT = max(5, int(os.environ.get('FEED_ENTRY_SCAN_LIMIT', '12')))
FEED_IMPORTED_LIMIT = max(10, int(os.environ.get('FEED_IMPORTED_LIMIT', '30')))
ACTOR_SEARCH_LINK_LIMIT = max(1, int(os.environ.get('ACTOR_SEARCH_LINK_LIMIT', '6')))
FEED_REQUIRE_PUBLISHED_AT = os.environ.get('FEED_REQUIRE_PUBLISHED_AT', '1').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
ENFORCE_OLLAMA_SYNTHESIS = os.environ.get('ENFORCE_OLLAMA_SYNTHESIS', '0').strip().lower() in {
    '1', 'true', 'yes', 'on',
}

CAPABILITY_GRID_KEYS = [
    'initial_access',
    'persistence',
    'execution',
    'privilege_escalation',
    'defense_evasion',
    'command_and_control',
    'lateral_movement',
    'exfiltration',
    'impact',
    'tooling',
    'infrastructure',
    'targeting',
    'tempo',
]
BEHAVIORAL_MODEL_KEYS = [
    'access_strategy',
    'tool_acquisition',
    'persistence_philosophy',
    'targeting_logic',
    'adaptation_pattern',
    'operational_tempo',
]
ATTACK_TACTIC_TO_CAPABILITY_MAP = {
    'reconnaissance': 'targeting',
    'resource_development': 'infrastructure',
    'initial_access': 'initial_access',
    'execution': 'execution',
    'persistence': 'persistence',
    'privilege_escalation': 'privilege_escalation',
    'defense_evasion': 'defense_evasion',
    'credential_access': 'privilege_escalation',
    'discovery': 'lateral_movement',
    'lateral_movement': 'lateral_movement',
    'collection': 'exfiltration',
    'command_and_control': 'command_and_control',
    'exfiltration': 'exfiltration',
    'impact': 'impact',
}
# Feed catalog organized by use-case to simplify targeted pull strategies.
FEED_CATALOG: dict[str, list[tuple[str, str]]] = {
    'ioc': [
        ('CISA Alerts', 'https://www.cisa.gov/cybersecurity-advisories/all.xml'),
        ('NCSC UK', 'https://www.ncsc.gov.uk/api/1/services/v1/report-rss-feed.xml'),
        ('Cisco Talos', 'https://blog.talosintelligence.com/rss/'),
        ('Palo Alto Unit 42', 'https://unit42.paloaltonetworks.com/feed/'),
        ('Mandiant Blog', 'https://www.mandiant.com/resources/blog/rss.xml'),
        ('SentinelOne Labs', 'https://www.sentinelone.com/labs/feed/'),
        ('CrowdStrike Blog', 'https://www.crowdstrike.com/en-us/blog/feed/'),
        ('Securelist', 'https://securelist.com/feed/'),
    ],
    'research': [
        ('Microsoft Security', 'https://www.microsoft.com/en-us/security/blog/feed/'),
        ('Google Cloud Threat Intelligence', 'https://cloud.google.com/blog/topics/threat-intelligence/rss/'),
        ('Proofpoint Blog', 'https://www.proofpoint.com/us/blog/rss.xml'),
        ('Red Canary Blog', 'https://redcanary.com/blog/feed/'),
        ('Huntress Blog', 'https://www.huntress.com/blog/rss.xml'),
        ('Arctic Wolf Labs', 'https://arcticwolf.com/resources/blog/feed/'),
        ('Rapid7 Blog', 'https://www.rapid7.com/blog/rss/'),
        ('Sophos News', 'https://news.sophos.com/en-us/feed/'),
        ('Trend Micro Research', 'https://www.trendmicro.com/en_us/research.html/rss.xml'),
        ('ESET WeLiveSecurity', 'https://www.welivesecurity.com/en/rss/feed'),
        ('CISA News', 'https://www.cisa.gov/news.xml'),
        ('The DFIR Report', 'https://thedfirreport.com/feed/'),
        ('Recorded Future Blog', 'https://www.recordedfuture.com/feed'),
        ('Intel 471 Blog', 'https://www.intel471.com/blog/feed'),
        ('Sygnia Blog', 'https://www.sygnia.co/blog/feed/'),
        ('Check Point Research', 'https://research.checkpoint.com/feed/'),
        ('Malwarebytes Labs', 'https://www.malwarebytes.com/blog/feed/index.xml'),
        ('Zero Day Initiative Blog', 'https://www.zerodayinitiative.com/blog?format=rss'),
        ('NetWitness Blog', 'https://www.netwitness.com/en-us/blog/feed/'),
        ('Corelight Labs', 'https://corelight.com/blog/rss.xml'),
        ('EclecticIQ Blog', 'https://blog.eclecticiq.com/rss.xml'),
        ('LevelBlue SpiderLabs Blog', 'https://www.levelblue.com/en-us/resources/blogs/spiderlabs-blog/rss.xml'),
        ('CERT-FR', 'https://www.cert.ssi.gouv.fr/feed/'),
    ],
    'advisory': [
        ('Cisco PSIRT', 'https://sec.cloudapps.cisco.com/security/center/psirtrss20/CiscoSecurityAdvisory.xml'),
        ('Fortinet PSIRT', 'https://filestore.fortinet.com/fortiguard/rss/ir.xml'),
        ('Ivanti Security Advisory', 'https://www.ivanti.com/blog/topics/security-advisory/rss'),
        ('JPCERT Alerts', 'https://www.jpcert.or.jp/rss/jpcert.rdf'),
        ('JVN Vulnerability', 'https://jvn.jp/rss/jvn.rdf'),
    ],
    'context': [
        ('BleepingComputer', 'https://www.bleepingcomputer.com/feed/'),
        ('The Hacker News', 'https://feeds.feedburner.com/TheHackersNews'),
        ('Krebs on Security', 'https://krebsonsecurity.com/feed/'),
        ('The Record', 'https://therecord.media/feed'),
        ('Dark Reading', 'https://www.darkreading.com/rss.xml'),
        ('SANS Internet Storm Center', 'https://isc.sans.edu/rssfeed_full.xml'),
    ],
}

IOC_INTELLIGENCE_FEEDS = FEED_CATALOG['ioc']
PRIMARY_CTI_FEEDS = FEED_CATALOG['ioc'] + FEED_CATALOG['research']
EXPANDED_PRIMARY_ADVISORY_FEEDS = FEED_CATALOG['advisory']
SECONDARY_CONTEXT_FEEDS = FEED_CATALOG['context']
DEFAULT_CTI_FEEDS = PRIMARY_CTI_FEEDS + EXPANDED_PRIMARY_ADVISORY_FEEDS + SECONDARY_CONTEXT_FEEDS
SOURCE_CANDIDATES_BATCH1 = [
    {
        'name': 'The DFIR Report',
        'type': 'rss',
        'value': 'https://thedfirreport.com/feed/',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'Recorded Future Blog',
        'type': 'rss',
        'value': 'https://www.recordedfuture.com/feed',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Intel 471 Blog',
        'type': 'rss',
        'value': 'https://www.intel471.com/blog/feed',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Sygnia Blog',
        'type': 'rss',
        'value': 'https://www.sygnia.co/blog/feed/',
        'tier': 'medium',
        'category': 'ir_firm',
    },
    {
        'name': 'Check Point Research',
        'type': 'rss',
        'value': 'https://research.checkpoint.com/feed/',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'The DFIR Report',
        'type': 'domain',
        'value': 'thedfirreport.com',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'Recorded Future',
        'type': 'domain',
        'value': 'recordedfuture.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Intel 471',
        'type': 'domain',
        'value': 'intel471.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Sygnia',
        'type': 'domain',
        'value': 'sygnia.co',
        'tier': 'medium',
        'category': 'ir_firm',
    },
    {
        'name': 'Check Point',
        'type': 'domain',
        'value': 'checkpoint.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
]
SOURCE_CANDIDATES_BATCH2 = [
    {
        'name': 'Malwarebytes Labs',
        'type': 'rss',
        'value': 'https://www.malwarebytes.com/blog/feed/index.xml',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Zero Day Initiative Blog',
        'type': 'rss',
        'value': 'https://www.zerodayinitiative.com/blog?format=rss',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'NetWitness Blog',
        'type': 'rss',
        'value': 'https://www.netwitness.com/en-us/blog/feed/',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'Corelight Labs',
        'type': 'rss',
        'value': 'https://corelight.com/blog/rss.xml',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'EclecticIQ Blog',
        'type': 'rss',
        'value': 'https://blog.eclecticiq.com/rss.xml',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'LevelBlue SpiderLabs Blog',
        'type': 'rss',
        'value': 'https://www.levelblue.com/en-us/resources/blogs/spiderlabs-blog/rss.xml',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'CERT-FR',
        'type': 'rss',
        'value': 'https://www.cert.ssi.gouv.fr/feed/',
        'tier': 'high',
        'category': 'cert',
    },
    {
        'name': 'Malwarebytes',
        'type': 'domain',
        'value': 'malwarebytes.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'Zero Day Initiative',
        'type': 'domain',
        'value': 'zerodayinitiative.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'NetWitness',
        'type': 'domain',
        'value': 'netwitness.com',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'Corelight',
        'type': 'domain',
        'value': 'corelight.com',
        'tier': 'medium',
        'category': 'dfir',
    },
    {
        'name': 'EclecticIQ',
        'type': 'domain',
        'value': 'eclecticiq.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'LevelBlue',
        'type': 'domain',
        'value': 'levelblue.com',
        'tier': 'medium',
        'category': 'vendor_research',
    },
    {
        'name': 'CERT-FR',
        'type': 'domain',
        'value': 'cert.ssi.gouv.fr',
        'tier': 'high',
        'category': 'cert',
    },
]
ACTOR_SEARCH_DOMAINS = [
    'cisa.gov',
    'fbi.gov',
    'bleepingcomputer.com',
    'thehackernews.com',
    'therecord.media',
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'talosintelligence.com',
    'unit42.paloaltonetworks.com',
    'microsoft.com',
    'securelist.com',
    'ransomware.live',
]
TRUSTED_ACTIVITY_DOMAINS = set(ACTOR_SEARCH_DOMAINS + ['attack.mitre.org'])
HIGH_CONFIDENCE_SOURCE_DOMAINS = {
    'cisa.gov',
    'fbi.gov',
    'ncsc.gov.uk',
    'attack.mitre.org',
}
MEDIUM_CONFIDENCE_SOURCE_DOMAINS = {
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'talosintelligence.com',
    'unit42.paloaltonetworks.com',
    'microsoft.com',
    'securelist.com',
    'cloud.google.com',
    'proofpoint.com',
    'redcanary.com',
    'huntress.com',
    'arcticwolf.com',
    'rapid7.com',
    'sophos.com',
    'trendmicro.com',
    'welivesecurity.com',
    'eset.com',
}
SECONDARY_CONTEXT_DOMAINS = {
    'bleepingcomputer.com',
    'thehackernews.com',
    'therecord.media',
    'darkreading.com',
    'krebsonsecurity.com',
    'isc.sans.edu',
}
HIGH_CONFIDENCE_SOURCE_DOMAINS.update(
    {
        'jpcert.or.jp',
        'jvn.jp',
        'cert.ssi.gouv.fr',
    }
)
MEDIUM_CONFIDENCE_SOURCE_DOMAINS.update(
    {
        'cisco.com',
        'fortinet.com',
        'ivanti.com',
        'thedfirreport.com',
        'recordedfuture.com',
        'intel471.com',
        'sygnia.co',
        'checkpoint.com',
        'malwarebytes.com',
        'zerodayinitiative.com',
        'netwitness.com',
        'corelight.com',
        'eclecticiq.com',
        'levelblue.com',
    }
)
ACTOR_SEARCH_DOMAINS.extend(
    [
        'jpcert.or.jp',
        'jvn.jp',
        'cisco.com',
        'fortinet.com',
        'ivanti.com',
        'cloud.google.com',
        'proofpoint.com',
        'redcanary.com',
        'huntress.com',
        'arcticwolf.com',
        'rapid7.com',
        'sophos.com',
        'trendmicro.com',
        'welivesecurity.com',
        'eset.com',
        'thedfirreport.com',
        'recordedfuture.com',
        'intel471.com',
        'sygnia.co',
        'checkpoint.com',
        'malwarebytes.com',
        'zerodayinitiative.com',
        'netwitness.com',
        'corelight.com',
        'eclecticiq.com',
        'levelblue.com',
        'cert.ssi.gouv.fr',
    ]
)
TRUSTED_ACTIVITY_DOMAINS = set(ACTOR_SEARCH_DOMAINS + ['attack.mitre.org'])
QUESTION_SEED_KEYWORDS = [
    'should review',
    'should detect',
    'organizations should',
    'mitigate',
    'look for',
    'monitor',
    'search for',
    'hunt for',
    'indicator',
    'ioc',
    'cve-',
    'ttp',
    'phish',
    'powershell',
    'wmi',
    'dns',
    'beacon',
    'exploit',
]
OUTBOUND_ALLOWED_DOMAINS = {
    domain.strip().lower()
    for domain in os.environ.get('OUTBOUND_ALLOWED_DOMAINS', '').split(',')
    if domain.strip()
}
_DEFAULT_OUTBOUND_ALLOWED_DOMAINS = set(ACTOR_SEARCH_DOMAINS)
for _, _feed_url in DEFAULT_CTI_FEEDS:
    _host = urlparse(_feed_url).hostname
    if _host:
        _DEFAULT_OUTBOUND_ALLOWED_DOMAINS.add(_host.strip('.').lower())
_DEFAULT_OUTBOUND_ALLOWED_DOMAINS.update(
    {
        'attack.mitre.org',
        'raw.githubusercontent.com',
    }
)
if not OUTBOUND_ALLOWED_DOMAINS:
    OUTBOUND_ALLOWED_DOMAINS = _DEFAULT_OUTBOUND_ALLOWED_DOMAINS
ALLOW_HTTP_OUTBOUND = os.environ.get('ALLOW_HTTP_OUTBOUND', '0').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
DEFAULT_BODY_LIMIT_BYTES = 256 * 1024
SOURCE_UPLOAD_BODY_LIMIT_BYTES = 2 * 1024 * 1024
OBSERVATION_BODY_LIMIT_BYTES = 512 * 1024
TRUST_PROXY_HEADERS = os.environ.get('TRUST_PROXY_HEADERS', '0').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
RATE_LIMIT_ENABLED = os.environ.get('RATE_LIMIT_ENABLED', '1').strip().lower() not in {
    '0', 'false', 'no', 'off',
}
SOURCE_QUALITY_OVERWRITE_ON_UPSERT = os.environ.get('SOURCE_QUALITY_OVERWRITE_ON_UPSERT', '0').strip().lower() in {
    '1', 'true', 'yes', 'on',
}
RATE_LIMIT_WINDOW_SECONDS = max(1, int(os.environ.get('RATE_LIMIT_WINDOW_SECONDS', '60')))
RATE_LIMIT_DEFAULT_PER_MINUTE = max(1, int(os.environ.get('RATE_LIMIT_DEFAULT_PER_MINUTE', '60')))
RATE_LIMIT_HEAVY_PER_MINUTE = max(1, int(os.environ.get('RATE_LIMIT_HEAVY_PER_MINUTE', '15')))
BACKFILL_DEBUG_UI = os.environ.get('BACKFILL_DEBUG_UI', os.environ.get('UVICORN_RELOAD', '0')).strip().lower() in {
    '1', 'true', 'yes', 'on',
}
_RATE_LIMIT_STATE: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMIT_REQUEST_COUNTER = 0
_RATE_LIMIT_CLEANUP_EVERY = 512
AUTO_REFRESH_STOP_EVENT: Event | None = None
AUTO_REFRESH_THREAD: Thread | None = None
GENERATION_WORKER_STOP_EVENT: Event | None = None
LOGGER = logging.getLogger('actorwatch')
if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter('%(message)s'))
    LOGGER.addHandler(_handler)
LOGGER.setLevel(logging.INFO)


def _log_event(event: str, **fields: object) -> None:
    payload = {'event': event, **fields, 'ts': utc_now_iso()}
    try:
        LOGGER.info(json.dumps(payload, separators=(',', ':'), default=str))
    except Exception:
        LOGGER.info(str(payload))


def _run_tracked_actor_auto_refresh_once(*, limit: int = 3) -> int:
    try:
        queued = refresh_ops_service.run_tracked_actor_auto_refresh_once_core(
            db_path=DB_PATH,
            min_interval_hours=AUTO_REFRESH_MIN_INTERVAL_HOURS,
            limit=limit,
            deps={
                'parse_published_datetime': _parse_published_datetime,
                'enqueue_actor_generation': enqueue_actor_generation,
                'submit_actor_refresh_job': submit_actor_refresh_job,
                'on_actor_queued': lambda actor_id: _log_event('auto_refresh_actor_queued', actor_id=actor_id),
            },
        )
        metrics_service.record_refresh_queue_core(queued_count=queued)
        _log_event('auto_refresh_run', queued_count=queued, limit=limit)
        return queued
    except Exception as exc:
        metrics_service.record_refresh_queue_core(queued_count=0)
        _log_event('auto_refresh_failed', error=str(exc), limit=limit)
        raise


def _auto_refresh_loop(stop_event: Event) -> None:
    refresh_ops_service.auto_refresh_loop_core(
        stop_event=stop_event,
        loop_seconds=AUTO_REFRESH_LOOP_SECONDS,
        run_once=lambda: (
            _recover_stale_running_states(),
            _run_tracked_actor_auto_refresh_once(limit=AUTO_REFRESH_BATCH_SIZE),
        ),
    )


def _recover_stale_running_states() -> int:
    running_ids = generation_service.running_actor_ids_snapshot_core()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(5, RUNNING_STALE_RECOVERY_MINUTES))
    recovered = 0
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            '''
            SELECT id, notebook_updated_at
            FROM actor_profiles
            WHERE notebook_status = 'running'
            '''
        ).fetchall()
        for row in rows:
            actor_id = str(row[0] or '')
            if not actor_id or actor_id in running_ids:
                continue
            updated_raw = str(row[1] or '').strip()
            updated_dt = _parse_published_datetime(updated_raw) if updated_raw else None
            if updated_dt is not None and updated_dt > cutoff:
                continue
            connection.execute(
                '''
                UPDATE actor_profiles
                SET notebook_status = 'error',
                    notebook_message = 'Previous refresh stalled and was recovered. Refresh again.',
                    auto_refresh_last_status = 'error'
                WHERE id = ?
                ''',
                (actor_id,),
            )
            recovered += 1
        connection.commit()
    return recovered


def _sync_mitre_cache_to_store() -> None:
    mitre_store.MITRE_GROUP_CACHE = MITRE_GROUP_CACHE
    mitre_store.MITRE_DATASET_CACHE = MITRE_DATASET_CACHE
    mitre_store.MITRE_TECHNIQUE_PHASE_CACHE = MITRE_TECHNIQUE_PHASE_CACHE
    mitre_store.MITRE_CAMPAIGN_LINK_CACHE = MITRE_CAMPAIGN_LINK_CACHE
    mitre_store.MITRE_TECHNIQUE_INDEX_CACHE = MITRE_TECHNIQUE_INDEX_CACHE
    mitre_store.MITRE_SOFTWARE_CACHE = MITRE_SOFTWARE_CACHE


def _sync_mitre_cache_from_store() -> None:
    global MITRE_GROUP_CACHE, MITRE_DATASET_CACHE, MITRE_TECHNIQUE_PHASE_CACHE
    global MITRE_SOFTWARE_CACHE, MITRE_CAMPAIGN_LINK_CACHE, MITRE_TECHNIQUE_INDEX_CACHE
    MITRE_GROUP_CACHE = mitre_store.MITRE_GROUP_CACHE
    MITRE_DATASET_CACHE = mitre_store.MITRE_DATASET_CACHE
    MITRE_TECHNIQUE_PHASE_CACHE = mitre_store.MITRE_TECHNIQUE_PHASE_CACHE
    MITRE_SOFTWARE_CACHE = mitre_store.MITRE_SOFTWARE_CACHE
    MITRE_CAMPAIGN_LINK_CACHE = mitre_store.MITRE_CAMPAIGN_LINK_CACHE
    MITRE_TECHNIQUE_INDEX_CACHE = mitre_store.MITRE_TECHNIQUE_INDEX_CACHE


def _reset_mitre_caches() -> None:
    global MITRE_GROUP_CACHE, MITRE_DATASET_CACHE, MITRE_TECHNIQUE_PHASE_CACHE
    global MITRE_SOFTWARE_CACHE, MITRE_CAMPAIGN_LINK_CACHE, MITRE_TECHNIQUE_INDEX_CACHE
    MITRE_GROUP_CACHE = None
    MITRE_DATASET_CACHE = None
    MITRE_TECHNIQUE_PHASE_CACHE = None
    MITRE_SOFTWARE_CACHE = None
    MITRE_CAMPAIGN_LINK_CACHE = None
    MITRE_TECHNIQUE_INDEX_CACHE = None


def _configure_mitre_store() -> None:
    mitre_store.configure(db_path=DB_PATH, attack_url=ATTACK_ENTERPRISE_STIX_URL)


def _with_mitre_store_sync(callback):
    _configure_mitre_store()
    _sync_mitre_cache_to_store()
    try:
        return callback()
    finally:
        _sync_mitre_cache_from_store()


def _request_body_limit_bytes(method: str, path: str) -> int:
    return rate_limit_service.request_body_limit_bytes_core(
        method,
        path,
        SOURCE_UPLOAD_BODY_LIMIT_BYTES,
        OBSERVATION_BODY_LIMIT_BYTES,
        DEFAULT_BODY_LIMIT_BYTES,
    )


async def _enforce_request_size(request: Request, limit: int) -> None:
    if limit <= 0:
        return
    content_length = request.headers.get('content-length', '').strip()
    if content_length.isdigit() and int(content_length) > limit:
        raise HTTPException(
            status_code=413,
            detail=f'Request body too large. Limit for this endpoint is {limit} bytes.',
        )
    body = await request.body()
    if len(body) > limit:
        raise HTTPException(
            status_code=413,
            detail=f'Request body too large. Limit for this endpoint is {limit} bytes.',
        )


def _rate_limit_bucket(method: str, path: str) -> tuple[str, int] | None:
    return rate_limit_service.rate_limit_bucket_core(
        method,
        path,
        RATE_LIMIT_HEAVY_PER_MINUTE,
        RATE_LIMIT_DEFAULT_PER_MINUTE,
    )


def _request_client_id(request: Request) -> str:
    return rate_limit_service.request_client_id_core(
        request,
        trust_proxy_headers=TRUST_PROXY_HEADERS,
    )


def _prune_rate_limit_state(now: float) -> None:
    rate_limit_service.prune_rate_limit_state_core(
        now=now,
        rate_limit_state=_RATE_LIMIT_STATE,
        rate_limit_window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )


def _check_rate_limit(request: Request) -> tuple[bool, int, int]:
    global _RATE_LIMIT_REQUEST_COUNTER
    counter_ref = {'value': _RATE_LIMIT_REQUEST_COUNTER}
    limited, retry_after, limit = rate_limit_service.check_rate_limit_core(
        request,
        rate_limit_enabled=RATE_LIMIT_ENABLED,
        rate_limit_window_seconds=RATE_LIMIT_WINDOW_SECONDS,
        rate_limit_state=_RATE_LIMIT_STATE,
        rate_limit_lock=_RATE_LIMIT_LOCK,
        rate_limit_cleanup_every=_RATE_LIMIT_CLEANUP_EVERY,
        rate_limit_request_counter_ref=counter_ref,
        rate_limit_bucket=_rate_limit_bucket,
        request_client_id=_request_client_id,
        prune_rate_limit_state=_prune_rate_limit_state,
    )
    _RATE_LIMIT_REQUEST_COUNTER = int(counter_ref['value'])
    return (limited, retry_after, limit)


def _csrf_request_allowed(request: Request) -> bool:
    if request.method.upper() not in {'POST', 'PUT', 'PATCH', 'DELETE'}:
        return True
    host = request.headers.get('host', '').strip().lower()
    if not host:
        return True

    sec_fetch_site = request.headers.get('sec-fetch-site', '').strip().lower()
    if sec_fetch_site in {'cross-site'}:
        return False

    origin = request.headers.get('origin', '').strip()
    if origin:
        parsed_origin = urlparse(origin)
        if (parsed_origin.netloc or '').strip().lower() != host:
            return False

    referer = request.headers.get('referer', '').strip()
    if referer:
        parsed_referer = urlparse(referer)
        if (parsed_referer.netloc or '').strip().lower() != host:
            return False

    return True


@app.middleware('http')
async def add_security_headers(request: Request, call_next):
    started = time.perf_counter()

    def _finalize(response: JSONResponse | HTMLResponse) -> JSONResponse | HTMLResponse:
        duration_ms = int((time.perf_counter() - started) * 1000)
        route = request.scope.get('route')
        route_path = str(getattr(route, 'path', '') or request.url.path)
        metrics_service.record_request_core(
            method=request.method,
            path=route_path,
            status_code=int(response.status_code),
        )
        _log_event(
            'request_complete',
            method=request.method.upper(),
            path=route_path,
            status_code=int(response.status_code),
            duration_ms=duration_ms,
        )
        return response

    if not _csrf_request_allowed(request):
        return _finalize(JSONResponse(
            status_code=403,
            content={'detail': 'cross-site request blocked'},
        ))

    limit = _request_body_limit_bytes(request.method, request.url.path)
    if limit > 0:
        content_length = request.headers.get('content-length', '').strip()
        if content_length.isdigit() and int(content_length) > limit:
            return _finalize(JSONResponse(
                status_code=413,
                content={
                    'detail': (
                        f'Request body too large. Limit for this endpoint is {limit} bytes.'
                    )
                },
            ))

    limited, retry_after, limit = _check_rate_limit(request)
    if limited:
        return _finalize(JSONResponse(
            status_code=429,
            content={
                'detail': (
                    f'Rate limit exceeded for write requests. Try again in {retry_after} seconds.'
                )
            },
            headers={
                'Retry-After': str(retry_after),
                'X-RateLimit-Limit': str(limit),
            },
        ))

    try:
        response = await call_next(request)
    except Exception as exc:
        error_response = JSONResponse(status_code=500, content={'detail': 'internal server error'})
        _log_event('request_exception', method=request.method.upper(), path=request.url.path, error=str(exc))
        _finalize(error_response)
        raise
    csp_policy = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://unpkg.com; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com; "
        "img-src 'self' data: https:; "
        "font-src 'self' data:; "
        "connect-src 'self' https://nominatim.openstreetmap.org; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    )
    response.headers.setdefault('Content-Security-Policy', csp_policy)
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
    return _finalize(response)


def _prepare_db_path(path_value: str) -> str:
    db_parent = str(Path(path_value).resolve().parent)
    os.makedirs(db_parent, exist_ok=True)
    return path_value


def _resolve_startup_db_path() -> str:
    try:
        return _prepare_db_path(DB_PATH)
    except PermissionError:
        fallback = str(BASE_DIR / 'app.db')
        return _prepare_db_path(fallback)


def _db_path() -> str:
    return DB_PATH


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def baseline_entry() -> dict[str, str | float | list[str]]:
    return {
        'observed': '',
        'assessed': '',
        'confidence': 0.0,
        'evidence_refs': [],
    }


def baseline_capability_grid() -> dict[str, dict[str, str | float | list[str]]]:
    return {key: baseline_entry() for key in CAPABILITY_GRID_KEYS}


def baseline_behavioral_model() -> dict[str, dict[str, str | float | list[str]]]:
    return {key: baseline_entry() for key in BEHAVIORAL_MODEL_KEYS}


def generate_validation_template(delta_type: str, affected_category: str) -> dict[str, list[str]]:
    if delta_type == 'expansion':
        return {
            'tier1_basic': [
                'Confirm the report explicitly describes technique use (not speculation).',
                f'Confirm {affected_category} is not already present in the baseline.',
                'Identify the strongest evidence snippet/source for this claim.',
            ],
            'tier2_analytic': [
                (
                    f'Does this expand the actor options within {affected_category} '
                    'or just repeat known behavior?'
                ),
                'Does it contradict prior baseline assumptions? If yes, which?',
                'What additional evidence would increase confidence?',
            ],
            'tier3_strategic': [
                (
                    f'Does this {affected_category} shift suggest adaptation to defenses '
                    'or a new operational phase?'
                ),
                'Does this change the tracking priority for this actor?',
            ],
        }
    return {'tier1_basic': [], 'tier2_analytic': [], 'tier3_strategic': []}


def normalize_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail='list fields must be arrays')
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise HTTPException(status_code=400, detail='list values must be strings')
        normalized.append(item)
    return normalized


def _normalize_text(value: str) -> str:
    lowered = value.lower()
    translator = str.maketrans('', '', string.punctuation)
    return lowered.translate(translator)


def _token_set(value: str) -> set[str]:
    return {token for token in _normalize_text(value).split() if len(token) > 2}


def _token_overlap(a: str, b: str) -> float:
    a_tokens = _token_set(a)
    b_tokens = _token_set(b)
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens.intersection(b_tokens)) / len(a_tokens.union(b_tokens))


def _split_sentences(text: str) -> list[str]:
    sentences = [segment.strip() for segment in re.split(r'(?<=[.!?])\s+', text) if segment.strip()]
    return [sentence for sentence in sentences if len(sentence) >= 25]


def _extract_question_sentences(text: str) -> list[str]:
    matches: list[str] = []
    for sentence in _split_sentences(text):
        lowered = sentence.lower()
        if any(keyword in lowered for keyword in QUESTION_SEED_KEYWORDS):
            matches.append(sentence)
    return matches


def _question_from_sentence(sentence: str) -> str:
    lowered = sentence.lower()
    if any(token in lowered for token in ('phish', 'email')):
        return 'What evidence shows this actor is using email or phishing delivery right now?'
    if any(token in lowered for token in ('cve', 'vpn', 'edge', 'exploit')):
        return 'Which exposed systems are most at risk from this actor’s current exploit activity?'
    if any(token in lowered for token in ('powershell', 'wmi', 'scheduled task')):
        return 'Which endpoint execution patterns should we validate for this actor immediately?'
    if any(token in lowered for token in ('dns', 'domain', 'c2', 'beacon')):
        return 'What network indicators suggest active command-and-control behavior by this actor?'
    if any(token in lowered for token in ('hash', 'file', 'process', 'command line')):
        return 'Which endpoint artifacts best confirm this actor’s latest operational behavior?'
    compact = ' '.join(sentence.split())
    if len(compact) > 170:
        compact = compact[:170].rsplit(' ', 1)[0] + '...'
    return f'What should analysts verify next based on this report: {compact}'


def _sanitize_question_text(question: str) -> str:
    cleaned = ' '.join(question.strip().split())
    if not cleaned:
        return ''
    for pattern in (
        r'\bPIRs?\b',
        r'\bpriority intelligence requirements?\b',
        r'\bintelligence requirements?\b',
        r'\bcollection requirements?\b',
        r'\bkill chain\b',
    ):
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    if not cleaned:
        return ''
    if not cleaned.endswith('?'):
        cleaned = cleaned.rstrip('.!') + '?'
    if len(cleaned) > 220:
        cleaned = cleaned[:220].rsplit(' ', 1)[0] + '?'
    if not cleaned.lower().startswith(('what ', 'how ', 'which ', 'where ', 'when ', 'who ')):
        cleaned = f'What should we ask next: {cleaned}'
    return cleaned


def _first_sentences(text: str, count: int = 2) -> str:
    sentences = _split_sentences(text)
    if not sentences:
        compact = ' '.join(text.split())
        return compact[:240]
    return ' '.join(sentences[:count])


def _normalize_actor_key(value: str) -> str:
    return ' '.join(re.findall(r'[a-z0-9]+', value.lower()))


def _dedupe_actor_terms(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        key = _normalize_actor_key(text)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _mitre_alias_values(obj: dict[str, object]) -> list[str]:
    alias_candidates: list[str] = []
    for field in ('aliases', 'x_mitre_aliases'):
        raw = obj.get(field)
        if isinstance(raw, list):
            alias_candidates.extend(str(item).strip() for item in raw if str(item).strip())
    return _dedupe_actor_terms(alias_candidates)


def _candidate_overlap_score(actor_tokens: set[str], search_keys: set[str]) -> float:
    best_score = 0.0
    for search_key in search_keys:
        key_tokens = set(search_key.split())
        if not key_tokens:
            continue
        overlap = len(actor_tokens.intersection(key_tokens)) / len(actor_tokens.union(key_tokens))
        if overlap > best_score:
            best_score = overlap
    return best_score


def _mitre_dataset_path() -> Path:
    _configure_mitre_store()
    path = Path(os.environ.get('MITRE_ATTACK_PATH', '').strip()) if os.environ.get('MITRE_ATTACK_PATH', '').strip() else None
    if path is not None:
        return path
    return Path(DB_PATH).resolve().parent / 'mitre_enterprise_attack.json'


def _ensure_mitre_attack_dataset() -> bool:
    return _with_mitre_store_sync(lambda: mitre_store.ensure_mitre_attack_dataset())


def _load_mitre_groups() -> list[dict[str, object]]:
    return _with_mitre_store_sync(lambda: mitre_store.load_mitre_groups())


def _load_mitre_dataset() -> dict[str, object]:
    return _with_mitre_store_sync(lambda: mitre_store.load_mitre_dataset())


def _mitre_campaign_link_index() -> dict[str, dict[str, set[str]]]:
    return _with_mitre_store_sync(lambda: mitre_store.mitre_campaign_link_index())


def _normalize_technique_id(value: str) -> str:
    return mitre_store.normalize_technique_id(value)


def _mitre_technique_index() -> dict[str, dict[str, str]]:
    return _with_mitre_store_sync(lambda: mitre_store.mitre_technique_index())


def _mitre_valid_technique_ids() -> set[str]:
    return _with_mitre_store_sync(lambda: mitre_store.mitre_valid_technique_ids())


def _mitre_technique_phase_index() -> dict[str, list[str]]:
    return _with_mitre_store_sync(lambda: mitre_store.mitre_technique_phase_index())


def _capability_category_from_technique_id(ttp_id: str) -> str | None:
    return _with_mitre_store_sync(
        lambda: mitre_store.capability_category_from_technique_id(
            ttp_id,
            attack_tactic_to_capability_map=ATTACK_TACTIC_TO_CAPABILITY_MAP,
            capability_grid_keys=CAPABILITY_GRID_KEYS,
        )
    )


def _match_mitre_group(actor_name: str) -> dict[str, object] | None:
    return _with_mitre_store_sync(lambda: mitre_store.match_mitre_group(actor_name))


def _load_mitre_software() -> list[dict[str, object]]:
    return _with_mitre_store_sync(lambda: mitre_store.load_mitre_software())


def _match_mitre_software(name: str) -> dict[str, object] | None:
    return _with_mitre_store_sync(lambda: mitre_store.match_mitre_software(name))

def _build_actor_profile_from_mitre(actor_name: str) -> dict[str, str]:
    return _with_mitre_store_sync(
        lambda: mitre_store.build_actor_profile_from_mitre(
            actor_name,
            first_sentences=lambda text, count: _first_sentences(text, count=count),
        )
    )


def _group_top_techniques(group_stix_id: str, limit: int = 6) -> list[dict[str, str]]:
    return _with_mitre_store_sync(lambda: mitre_store.group_top_techniques(group_stix_id, limit=limit))


def _known_technique_ids_for_entity(entity_stix_id: str) -> set[str]:
    return _with_mitre_store_sync(lambda: mitre_store.known_technique_ids_for_entity(entity_stix_id))


def _favorite_attack_vectors(techniques: list[dict[str, str]], limit: int = 3) -> list[str]:
    _configure_mitre_store()
    return mitre_store.favorite_attack_vectors(techniques, limit=limit)


def _emerging_techniques_from_timeline(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
    limit: int = 5,
    min_distinct_sources: int = 2,
    min_event_count: int = 2,
) -> list[dict[str, object]]:
    return timeline_analytics_service.emerging_techniques_from_timeline_core(
        timeline_items,
        known_technique_ids,
        limit=limit,
        min_distinct_sources=min_distinct_sources,
        min_event_count=min_event_count,
        deps={
            'mitre_technique_index': _mitre_technique_index,
            'parse_published_datetime': _parse_published_datetime,
            'normalize_technique_id': _normalize_technique_id,
        },
    )


def _emerging_technique_ids_from_timeline(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
    limit: int = 5,
    min_distinct_sources: int = 2,
    min_event_count: int = 2,
) -> list[str]:
    return [
        str(item.get('technique_id') or '')
        for item in _emerging_techniques_from_timeline(
            timeline_items,
            known_technique_ids,
            limit=limit,
            min_distinct_sources=min_distinct_sources,
            min_event_count=min_event_count,
        )
        if str(item.get('technique_id') or '')
    ]


def _extract_ttp_ids(text: str) -> list[str]:
    matches = re.findall(r'\bT\d{4}(?:\.\d{3})?\b', text, flags=re.IGNORECASE)
    valid_ids = _mitre_valid_technique_ids()
    deduped: list[str] = []
    for value in matches:
        norm = value.upper()
        if valid_ids and norm not in valid_ids:
            continue
        if norm not in deduped:
            deduped.append(norm)
    return deduped


def _safe_json_string_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            return []
        result: list[str] = []
        for item in parsed:
            if isinstance(item, str):
                result.append(item)
        return result
    except Exception:
        return []


def _parse_iso_for_sort(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _short_date(value: str) -> str:
    return timeline_view_service.short_date_core(
        value,
        deps={
            'parse_published_datetime': _parse_published_datetime,
        },
    )


def _format_date_or_unknown(value: str) -> str:
    return timeline_view_service.format_date_or_unknown_core(
        value,
        deps={
            'parse_published_datetime': _parse_published_datetime,
        },
    )


def _freshness_badge(value: str | None) -> tuple[str, str]:
    return timeline_view_service.freshness_badge_core(
        value,
        deps={
            'parse_published_datetime': _parse_published_datetime,
        },
    )


def _bucket_label(value: str) -> str:
    return timeline_view_service.bucket_label_core(
        value,
        deps={
            'parse_iso_for_sort': _parse_iso_for_sort,
        },
    )


def _timeline_category_color(category: str) -> str:
    return timeline_view_service.timeline_category_color_core(category)


def _build_notebook_kpis(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
    open_questions_count: int,
    sources: list[dict[str, object]],
) -> dict[str, str]:
    return timeline_analytics_service.build_notebook_kpis_core(
        timeline_items,
        known_technique_ids,
        open_questions_count,
        sources,
        deps={
            'parse_published_datetime': _parse_published_datetime,
            'mitre_valid_technique_ids': _mitre_valid_technique_ids,
        },
    )


def _build_timeline_graph(timeline_items: list[dict[str, object]]) -> list[dict[str, object]]:
    return timeline_analytics_service.build_timeline_graph_core(
        timeline_items,
        deps={
            'bucket_label': _bucket_label,
            'timeline_category_color': _timeline_category_color,
        },
    )


def _first_seen_for_techniques(
    timeline_items: list[dict[str, object]],
    technique_ids: list[str],
) -> list[dict[str, str]]:
    return timeline_analytics_service.first_seen_for_techniques_core(
        timeline_items,
        technique_ids,
        deps={
            'parse_published_datetime': _parse_published_datetime,
            'short_date': _short_date,
        },
    )


def _severity_label(category: str, target_text: str, novelty: bool) -> str:
    return timeline_analytics_service.severity_label_core(category, target_text, novelty)


def _action_text(category: str) -> str:
    return timeline_analytics_service.action_text_core(category)


def _compact_timeline_rows(
    timeline_items: list[dict[str, object]],
    known_technique_ids: set[str],
) -> list[dict[str, object]]:
    return timeline_analytics_service.compact_timeline_rows_core(
        timeline_items,
        known_technique_ids,
        parse_iso_for_sort=_parse_iso_for_sort,
        short_date=_short_date,
        action_text=_action_text,
        severity_label=_severity_label,
    )


_question_priority_score = priority_questions.question_priority_score
_question_category_hints = priority_questions.question_category_hints
_actor_signal_categories = priority_questions.actor_signal_categories
_question_actor_relevance = priority_questions.question_actor_relevance
_fallback_priority_questions = priority_questions.fallback_priority_questions
_priority_know_focus = priority_questions.priority_know_focus
_priority_hunt_focus = priority_questions.priority_hunt_focus
_priority_decision_to_inform = priority_questions.priority_decision_to_inform
_priority_time_horizon = priority_questions.priority_time_horizon
_priority_disconfirming_signal = priority_questions.priority_disconfirming_signal
_priority_confidence_label = priority_questions.priority_confidence_label
_priority_strongest_evidence = priority_questions.priority_strongest_evidence
_priority_confidence_why = priority_questions.priority_confidence_why
_priority_assumptions = priority_questions.priority_assumptions
_priority_alternative_hypothesis = priority_questions.priority_alternative_hypothesis
_priority_next_best_action = priority_questions.priority_next_best_action
_priority_action_ladder = priority_questions.priority_action_ladder
_phase_label_for_question = priority_questions.phase_label_for_question
_short_decision_trigger = priority_questions.short_decision_trigger
_guidance_line = priority_questions.guidance_line
_priority_update_recency_label = priority_questions.priority_update_recency_label
_priority_recency_points = priority_questions.priority_recency_points
_priority_rank_score = priority_questions.priority_rank_score
_org_context_tokens = priority_questions.org_context_tokens
_org_alignment_label = priority_questions.org_alignment_label
_confidence_change_threshold_line = priority_questions.confidence_change_threshold_line
_escalation_threshold_line = priority_questions.escalation_threshold_line
_expected_output_line = priority_questions.expected_output_line
_quick_check_title = priority_questions.quick_check_title


def _priority_where_to_check(guidance_items: list[dict[str, object]], question_text: str) -> str:
    return priority_service.priority_where_to_check_core(
        guidance_items,
        question_text,
        deps={
            'priority_where_to_check': priority_questions.priority_where_to_check,
            'platforms_for_question': _platforms_for_question,
        },
    )


def _telemetry_anchor_line(guidance_items: list[dict[str, object]], question_text: str) -> str:
    return priority_service.telemetry_anchor_line_core(
        guidance_items,
        question_text,
        deps={
            'telemetry_anchor_line': priority_questions.telemetry_anchor_line,
            'platforms_for_question': _platforms_for_question,
        },
    )


def _guidance_query_hint(guidance_items: list[dict[str, object]], question_text: str) -> str:
    return priority_service.guidance_query_hint_core(
        guidance_items,
        question_text,
        deps={
            'guidance_query_hint': priority_questions.guidance_query_hint,
            'platforms_for_question': _platforms_for_question,
            'guidance_for_platform': _guidance_for_platform,
        },
    )


def _priority_update_evidence_dt(update: dict[str, object]) -> datetime | None:
    return priority_service.priority_update_evidence_dt_core(
        update,
        deps={
            'priority_update_evidence_dt': priority_questions.priority_update_evidence_dt,
            'parse_published_datetime': _parse_published_datetime,
        },
    )


def _question_org_alignment(question_text: str, org_context: str) -> int:
    return priority_service.question_org_alignment_core(
        question_text,
        org_context,
        deps={
            'question_org_alignment': priority_questions.question_org_alignment,
            'token_set': _token_set,
        },
    )


def _latest_reporting_recency_label(timeline_recent_items: list[dict[str, object]]) -> str:
    return pipeline_latest_reporting_recency_label(
        timeline_recent_items,
        parse_published_datetime=lambda value: _parse_published_datetime(value),
    )


def _build_environment_checks(
    timeline_recent_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    top_techniques: list[dict[str, str]],
) -> list[dict[str, str]]:
    recency_label = _latest_reporting_recency_label(timeline_recent_items)
    return pipeline_build_environment_checks(
        timeline_recent_items,
        recent_activity_highlights,
        top_techniques,
        recency_label=recency_label,
    )


def _recent_change_summary(
    timeline_recent_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
    source_items: list[dict[str, object]],
) -> dict[str, str]:
    return pipeline_recent_change_summary(
        timeline_recent_items,
        recent_activity_highlights,
        source_items,
    )


def _extract_target_hint(sentence: str) -> str:
    return timeline_extraction.extract_target_hint(sentence)


def _sentence_mentions_actor_terms(sentence: str, actor_terms: list[str]) -> bool:
    return timeline_extraction.sentence_mentions_actor_terms(sentence, actor_terms)


def _looks_like_activity_sentence(sentence: str) -> bool:
    return timeline_extraction.looks_like_activity_sentence(sentence)


def _actor_terms(actor_name: str, mitre_group_name: str, aliases_csv: str) -> list[str]:
    return actor_search_service.actor_terms_core(
        actor_name,
        mitre_group_name,
        aliases_csv,
        deps={
            'dedupe_actor_terms': _dedupe_actor_terms,
        },
    )


def _text_contains_actor_term(text: str, actor_terms: list[str]) -> bool:
    return actor_search_service.text_contains_actor_term_core(
        text,
        actor_terms,
        deps={
            'sentence_mentions_actor_terms': _sentence_mentions_actor_terms,
        },
    )


def _actor_query_feeds(actor_terms: list[str]) -> list[tuple[str, str]]:
    return actor_search_service.actor_query_feeds_core(actor_terms)


def _actor_search_queries(actor_terms: list[str]) -> list[str]:
    return actor_search_service.actor_search_queries_core(actor_terms)


def _domain_allowed_for_actor_search(url: str) -> bool:
    return actor_search_service.domain_allowed_for_actor_search_core(
        url,
        domains=ACTOR_SEARCH_DOMAINS,
    )


def _duckduckgo_actor_search_urls(actor_terms: list[str], limit: int = 20) -> list[str]:
    return actor_search_service.duckduckgo_actor_search_urls_core(
        actor_terms,
        limit=limit,
        deps={
            'actor_search_queries': _actor_search_queries,
            'http_get': httpx.get,
            'domain_allowed_for_actor_search': _domain_allowed_for_actor_search,
            're_finditer': re.finditer,
        },
    )


def _sentence_mentions_actor(sentence: str, actor_name: str) -> bool:
    return analyst_text_service.sentence_mentions_actor_core(
        sentence,
        actor_name,
        deps={
            're_findall': re.findall,
        },
    )


def _looks_like_navigation_noise(sentence: str) -> bool:
    return analyst_text_service.looks_like_navigation_noise_core(sentence)


def _build_actor_profile_summary(actor_name: str, source_texts: list[str]) -> str:
    return analyst_text_service.build_actor_profile_summary_core(
        actor_name,
        source_texts,
        deps={
            'split_sentences': _split_sentences,
            'looks_like_navigation_noise': _looks_like_navigation_noise,
            'sentence_mentions_actor': _sentence_mentions_actor,
            'normalize_text': _normalize_text,
            'token_overlap': _token_overlap,
        },
    )


def _build_recent_activity_highlights(
    timeline_items: list[dict[str, object]],
    sources: list[dict[str, object]],
    actor_terms: list[str],
) -> list[dict[str, str | None]]:
    def _source_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return ''

    return activity_highlight_service.build_recent_activity_highlights_core(
        timeline_items,
        sources,
        actor_terms,
        deps={
            'pipeline_build_recent_activity_highlights': pipeline_build_recent_activity_highlights,
            'trusted_activity_domains': TRUSTED_ACTIVITY_DOMAINS,
            'source_domain': _source_domain,
            'canonical_group_domain': _canonical_group_domain,
            'looks_like_activity_sentence': _looks_like_activity_sentence,
            'sentence_mentions_actor_terms': _sentence_mentions_actor_terms,
            'text_contains_actor_term': _text_contains_actor_term,
            'normalize_text': _normalize_text,
            'parse_published_datetime': _parse_published_datetime,
            'freshness_badge': _freshness_badge,
            'evidence_title_from_source': _evidence_title_from_source,
            'fallback_title_from_url': _fallback_title_from_url,
            'evidence_source_label_from_source': _evidence_source_label_from_source,
            'extract_ttp_ids': _extract_ttp_ids,
            'split_sentences': _split_sentences,
            'looks_like_navigation_noise': _looks_like_navigation_noise,
            'format_date_or_unknown': _format_date_or_unknown,
            'source_trust_score': _source_trust_score,
        },
    )


def _source_trust_score(url: str) -> int:
    try:
        host = (urlparse(url).hostname or '').strip('.').lower()
    except Exception:
        return 0
    if not host:
        return 0
    if any(host == domain or host.endswith(f'.{domain}') for domain in HIGH_CONFIDENCE_SOURCE_DOMAINS):
        return 4
    if any(host == domain or host.endswith(f'.{domain}') for domain in MEDIUM_CONFIDENCE_SOURCE_DOMAINS):
        return 3
    if any(host == domain or host.endswith(f'.{domain}') for domain in SECONDARY_CONTEXT_DOMAINS):
        return 1
    if any(host == domain or host.endswith(f'.{domain}') for domain in TRUSTED_ACTIVITY_DOMAINS):
        return 2
    return 0


def _source_tier_label(url: str) -> str:
    score = _source_trust_score(url)
    if score >= 4:
        return 'high'
    if score == 3:
        return 'medium'
    if score == 2:
        return 'trusted'
    if score == 1:
        return 'context'
    return 'unrated'


def _extract_target_from_activity_text(text: str) -> str:
    return timeline_extraction.extract_target_from_activity_text(text)


def _build_recent_activity_synthesis(
    highlights: list[dict[str, str | None]],
) -> list[dict[str, str]]:
    return recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            'extract_target_from_activity_text': _extract_target_from_activity_text,
            'parse_published_datetime': _parse_published_datetime,
        },
    )


def _timeline_category_from_sentence(sentence: str) -> str | None:
    return timeline_extraction.timeline_category_from_sentence(sentence)


def _extract_major_move_events(
    source_name: str,
    source_id: str,
    occurred_at: str,
    text: str,
    actor_terms: list[str],
    source_title: str | None = None,
) -> list[dict[str, object]]:
    return timeline_extraction.extract_major_move_events(
        source_name,
        source_id,
        occurred_at,
        text,
        actor_terms,
        source_title=source_title,
        deps={
            'split_sentences': _split_sentences,
            'extract_ttp_ids': _extract_ttp_ids,
            'new_id': lambda: str(uuid.uuid4()),
        },
    )


def _guidance_for_platform(platform: str, question_text: str) -> dict[str, str | None]:
    return guidance_catalog.guidance_for_platform(platform, question_text)


def _platforms_for_question(question_text: str) -> list[str]:
    return guidance_catalog.platforms_for_question(question_text)


def _strip_html(value: str) -> str:
    return pipeline_strip_html(value)


def _extract_meta(content: str, key_patterns: list[str]) -> str | None:
    return pipeline_extract_meta(content, key_patterns)


def _fallback_title_from_url(source_url: str) -> str:
    return pipeline_fallback_title_from_url(source_url)


def _evidence_title_from_source(source: dict[str, object]) -> str:
    return pipeline_evidence_title_from_source(
        source,
        split_sentences=lambda text: _split_sentences(text),
        fallback_title=lambda url: _fallback_title_from_url(url),
    )


def _evidence_source_label_from_source(source: dict[str, object]) -> str:
    return pipeline_evidence_source_label_from_source(
        source,
        evidence_title=lambda item: _evidence_title_from_source(item),
    )


def _canonical_group_domain(source: dict[str, object]) -> str:
    return pipeline_canonical_group_domain(
        source,
        evidence_source_label=lambda item: _evidence_source_label_from_source(item),
    )


def _validate_outbound_url(source_url: str, allowed_domains: set[str] | None = None) -> str:
    return network_service.validate_outbound_url_core(
        source_url,
        allowed_domains=allowed_domains,
        deps={
            'outbound_allowed_domains': OUTBOUND_ALLOWED_DOMAINS,
            'resolve_host': socket.getaddrinfo,
            'ipproto_tcp': socket.IPPROTO_TCP,
            'allow_http': ALLOW_HTTP_OUTBOUND,
        },
    )


def _safe_http_get(
    source_url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None = None,
    allowed_domains: set[str] | None = None,
    max_redirects: int = 3,
) -> httpx.Response:
    return network_service.safe_http_get_core(
        source_url,
        timeout=timeout,
        headers=headers,
        allowed_domains=allowed_domains,
        max_redirects=max_redirects,
        deps={
            'validate_url': lambda url, domains: _validate_outbound_url(url, allowed_domains=domains),
        },
    )


def derive_source_from_url(
    source_url: str,
    fallback_source_name: str | None = None,
    published_hint: str | None = None,
    fetch_timeout_seconds: float = 20.0,
) -> dict[str, str | None]:
    return source_derivation_service.derive_source_from_url_core(
        source_url,
        fallback_source_name=fallback_source_name,
        published_hint=published_hint,
        fetch_timeout_seconds=fetch_timeout_seconds,
        deps={
            'pipeline_derive_source_from_url_core': pipeline_derive_source_from_url_core,
            'safe_http_get': _safe_http_get,
            'extract_question_sentences': _extract_question_sentences,
            'first_sentences': _first_sentences,
        },
    )


def _parse_feed_entries(xml_text: str) -> list[dict[str, str | None]]:
    return source_ingest_service.parse_feed_entries_core(xml_text)


def _parse_published_datetime(value: str | None) -> datetime | None:
    return source_ingest_service.parse_published_datetime_core(value)


def _within_lookback(published_at: str | None, lookback_days: int) -> bool:
    return source_ingest_service.within_lookback_core(
        published_at=published_at,
        lookback_days=lookback_days,
    )


def _import_ransomware_live_actor_activity(
    connection: sqlite3.Connection,
    actor_id: str,
    actor_terms: list[str],
) -> int:
    return source_ingest_service.import_ransomware_live_actor_activity_core(
        connection=connection,
        actor_id=actor_id,
        actor_terms=actor_terms,
        deps={
            'http_get': httpx.get,
            'now_iso': utc_now_iso,
            'upsert_source_for_actor': _upsert_source_for_actor,
        },
    )


def _ollama_available() -> bool:
    return status_service.ollama_available_core(
        deps={
            'get_env': os.environ.get,
            'http_get': httpx.get,
        }
    )


def get_ollama_status() -> dict[str, str | bool]:
    return status_service.get_ollama_status_core(
        deps={
            'get_env': os.environ.get,
            'http_get': httpx.get,
        }
    )


def _ollama_generate_questions(actor_name: str, scope_statement: str | None, excerpts: list[str]) -> list[str]:
    return analyst_text_service.ollama_generate_questions_core(
        actor_name,
        scope_statement,
        excerpts,
        deps={
            'ollama_available': _ollama_available,
            'get_env': os.environ.get,
            'http_post': httpx.post,
            'sanitize_question_text': _sanitize_question_text,
        },
    )


def _ollama_review_change_signals(
    actor_name: str,
    source_items: list[dict[str, object]],
    recent_activity_highlights: list[dict[str, object]],
) -> list[dict[str, object]]:
    actor_key = llm_cache_service.actor_key_core(actor_name)
    cache_payload = {
        'actor_key': actor_key,
        'source_items': [
            {
                'id': str(item.get('id') or ''),
                'url': str(item.get('url') or ''),
                'published_at': str(item.get('published_at') or ''),
                'retrieved_at': str(item.get('retrieved_at') or ''),
                'fingerprint': str(item.get('source_fingerprint') or ''),
            }
            for item in source_items[:120]
            if isinstance(item, dict)
        ],
        'recent_activity_highlights': [
            {
                'source_url': str(item.get('source_url') or ''),
                'date': str(item.get('date') or ''),
                'text': str(item.get('text') or '')[:260],
            }
            for item in recent_activity_highlights[:60]
            if isinstance(item, dict)
        ],
    }
    cache_fp = llm_cache_service.input_fingerprint_core(
        cache_payload,
        deps={'sha256': hashlib.sha256},
    )
    if os.environ.get('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}:
        cached = llm_cache_service.load_cached_payload_core(
            actor_key=actor_key,
            cache_kind='review_change_signals',
            input_fingerprint=cache_fp,
            deps={
                'db_path': lambda: DB_PATH,
                'utc_now_iso': utc_now_iso,
            },
        )
        if isinstance(cached, list):
            return [item for item in cached if isinstance(item, dict)]
    started = time.perf_counter()
    result = analyst_text_service.ollama_review_change_signals_core(
        actor_name,
        source_items,
        recent_activity_highlights,
        deps={
            'ollama_available': _ollama_available,
            'get_env': os.environ.get,
            'http_post': httpx.post,
            'parse_published_datetime': _parse_published_datetime,
        },
    )
    if (
        os.environ.get('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}
        and isinstance(result, list)
        and len(result) > 0
    ):
        llm_cache_service.save_cached_payload_core(
            actor_key=actor_key,
            cache_kind='review_change_signals',
            input_fingerprint=cache_fp,
            payload=[item for item in result if isinstance(item, dict)],
            estimated_cost_ms=int((time.perf_counter() - started) * 1000),
            deps={
                'db_path': lambda: DB_PATH,
                'utc_now_iso': utc_now_iso,
            },
        )
    return result


def _ollama_synthesize_recent_activity(
    actor_name: str,
    highlights: list[dict[str, object]],
) -> list[dict[str, str]]:
    actor_key = llm_cache_service.actor_key_core(actor_name)
    cache_payload = {
        'actor_key': actor_key,
        'highlights': [
            {
                'source_url': str(item.get('source_url') or ''),
                'date': str(item.get('date') or ''),
                'text': str(item.get('text') or '')[:320],
                'category': str(item.get('category') or ''),
            }
            for item in highlights[:80]
            if isinstance(item, dict)
        ],
    }
    cache_fp = llm_cache_service.input_fingerprint_core(
        cache_payload,
        deps={'sha256': hashlib.sha256},
    )
    if os.environ.get('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}:
        cached = llm_cache_service.load_cached_payload_core(
            actor_key=actor_key,
            cache_kind='recent_activity_synthesis',
            input_fingerprint=cache_fp,
            deps={
                'db_path': lambda: DB_PATH,
                'utc_now_iso': utc_now_iso,
            },
        )
        if isinstance(cached, list):
            return [item for item in cached if isinstance(item, dict)]
    started = time.perf_counter()
    result = analyst_text_service.ollama_synthesize_recent_activity_core(
        actor_name,
        highlights,
        deps={
            'ollama_available': _ollama_available,
            'get_env': os.environ.get,
            'http_post': httpx.post,
        },
    )
    if (
        os.environ.get('LLM_CACHE_ENABLED', '1').strip().lower() not in {'0', 'false', 'off', 'no'}
        and isinstance(result, list)
        and len(result) > 0
    ):
        llm_cache_service.save_cached_payload_core(
            actor_key=actor_key,
            cache_kind='recent_activity_synthesis',
            input_fingerprint=cache_fp,
            payload=[item for item in result if isinstance(item, dict)],
            estimated_cost_ms=int((time.perf_counter() - started) * 1000),
            deps={
                'db_path': lambda: DB_PATH,
                'utc_now_iso': utc_now_iso,
            },
        )
    return result


def _ollama_enrich_quick_checks(
    actor_name: str,
    cards: list[dict[str, object]],
) -> dict[str, dict[str, str]]:
    return quick_check_service.generate_quick_check_overrides_core(
        actor_name,
        cards,
        deps={
            'ollama_available': _ollama_available,
            'get_env': os.environ.get,
            'http_post': httpx.post,
        },
    )


def _store_quick_check_overrides(
    connection: sqlite3.Connection,
    actor_id: str,
    overrides: dict[str, dict[str, str]],
    generated_at: str,
) -> None:
    quick_check_service.replace_quick_check_overrides_core(
        connection,
        actor_id=actor_id,
        overrides=overrides,
        generated_at=generated_at,
    )


def _ollama_generate_ioc_hunt_queries(
    actor_name: str,
    cards: list[dict[str, object]],
    environment_profile: dict[str, object] | None = None,
) -> dict[str, object]:
    return ioc_hunt_service.generate_ioc_hunt_queries_core(
        actor_name,
        cards,
        environment_profile=environment_profile,
        deps={
            'ollama_available': _ollama_available,
            'get_env': os.environ.get,
            'http_post': httpx.post,
            'personalize_query': _personalize_query,
        },
    )


def actor_exists(connection: sqlite3.Connection, actor_id: str) -> bool:
    return actor_profile_service.actor_exists_core(connection, actor_id)


def set_actor_notebook_status(actor_id: str, status: str, message: str) -> None:
    actor_profile_service.set_actor_notebook_status_core(
        actor_id=actor_id,
        status=status,
        message=message,
        deps={
            'db_path': lambda: DB_PATH,
            'utc_now_iso': utc_now_iso,
        },
    )


def _format_duration_ms(milliseconds: int | None) -> str:
    return status_service.format_duration_ms_core(milliseconds)


def _mark_actor_generation_started(actor_id: str) -> bool:
    return generation_service.mark_actor_generation_started_core(actor_id)


def _mark_actor_generation_finished(actor_id: str) -> None:
    generation_service.mark_actor_generation_finished_core(actor_id)


def _generation_journal_deps() -> dict[str, object]:
    return {
        'db_path': lambda: DB_PATH,
        'new_id': lambda: str(uuid.uuid4()),
        'utc_now_iso': utc_now_iso,
    }


def _create_generation_job(*, actor_id: str, trigger_type: str, initial_status: str = 'running') -> str:
    return generation_journal_service.create_generation_job_core(
        actor_id=actor_id,
        trigger_type=trigger_type,
        initial_status=initial_status,
        deps=_generation_journal_deps(),
    )


def _mark_generation_job_started(*, job_id: str) -> None:
    generation_journal_service.mark_generation_job_started_core(
        job_id=job_id,
        deps=_generation_journal_deps(),
    )


def _finalize_generation_job(
    *,
    job_id: str,
    status: str,
    imported_sources: int,
    duration_ms: int,
    final_message: str = '',
    error_message: str = '',
) -> None:
    generation_journal_service.finalize_generation_job_core(
        job_id=job_id,
        status=status,
        imported_sources=imported_sources,
        duration_ms=duration_ms,
        final_message=final_message,
        error_message=error_message,
        deps=_generation_journal_deps(),
    )


def _start_generation_phase(
    *,
    actor_id: str,
    job_id: str,
    phase_key: str,
    phase_label: str,
    attempt: int,
    message: str,
) -> str:
    return generation_journal_service.start_generation_phase_core(
        job_id=job_id,
        actor_id=actor_id,
        phase_key=phase_key,
        phase_label=phase_label,
        attempt=attempt,
        message=message,
        deps=_generation_journal_deps(),
    )


def _finish_generation_phase(
    *,
    phase_id: str,
    status: str,
    message: str = '',
    error_detail: str = '',
    duration_ms: int | None = None,
) -> None:
    generation_journal_service.finish_generation_phase_core(
        phase_id=phase_id,
        status=status,
        message=message,
        error_detail=error_detail,
        duration_ms=duration_ms,
        deps=_generation_journal_deps(),
    )


def run_actor_generation(actor_id: str, *, trigger_type: str = 'manual_refresh', job_id: str = '') -> None:
    started = time.perf_counter()
    success = False
    _log_event('generation_started', actor_id=actor_id)
    try:
        generation_service.run_actor_generation_core(
            actor_id=actor_id,
            deps={
                'mark_started': _mark_actor_generation_started,
                'mark_finished': _mark_actor_generation_finished,
                'pipeline_run_actor_generation_core': pipeline_run_actor_generation_core,
                'db_path': lambda: DB_PATH,
                'set_actor_notebook_status': set_actor_notebook_status,
                'import_default_feeds_for_actor': import_default_feeds_for_actor,
                'build_notebook': build_notebook,
                'enqueue_actor_llm_enrichment': enqueue_actor_llm_enrichment,
                'create_generation_job': _create_generation_job,
                'mark_generation_job_started': _mark_generation_job_started,
                'start_generation_phase': _start_generation_phase,
                'finish_generation_phase': _finish_generation_phase,
                'finalize_generation_job': _finalize_generation_job,
                'trigger_type': trigger_type,
                'job_id': job_id,
            },
        )
        success = True
    except Exception as exc:
        _log_event('generation_failed', actor_id=actor_id, error=str(exc))
        raise
    finally:
        metrics_service.record_generation_core(success=success)
        _log_event(
            'generation_completed',
            actor_id=actor_id,
            success=success,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )


def enqueue_actor_generation(
    actor_id: str,
    *,
    trigger_type: str = 'manual_refresh',
    job_id: str = '',
    priority: int | None = None,
) -> bool:
    _log_event('generation_enqueued', actor_id=actor_id)
    deps: dict[str, object] = {
        'run_actor_generation': run_actor_generation,
        'trigger_type': trigger_type,
        'job_id': job_id,
    }
    if priority is not None:
        deps['priority'] = int(priority)
    return generation_service.enqueue_actor_generation_core(
        actor_id=actor_id,
        deps=deps,
    )


def run_actor_llm_enrichment(actor_id: str, *, job_id: str = '') -> None:
    generation_service.run_actor_llm_enrichment_core(
        actor_id=actor_id,
        deps={
            'mark_started': generation_service.mark_actor_llm_enrichment_started_core,
            'mark_finished': generation_service.mark_actor_llm_enrichment_finished_core,
            'set_actor_notebook_status': set_actor_notebook_status,
            'refresh_actor_notebook_uncached': _refresh_actor_notebook_uncached,
            'start_phase': _start_generation_phase,
            'finish_phase': _finish_generation_phase,
            'job_id': job_id,
            'max_attempts': int(os.environ.get('LLM_ENRICHMENT_MAX_ATTEMPTS', '2')),
            'retry_sleep_seconds': float(os.environ.get('LLM_ENRICHMENT_RETRY_SECONDS', '2')),
        },
    )


def enqueue_actor_llm_enrichment(actor_id: str, *, job_id: str = '') -> None:
    _log_event('llm_enrichment_enqueued', actor_id=actor_id)
    generation_service.enqueue_actor_llm_enrichment_core(
        actor_id=actor_id,
        deps={
            'run_actor_llm_enrichment': run_actor_llm_enrichment,
            'job_id': job_id,
        },
    )


def list_actor_profiles() -> list[dict[str, object]]:
    return actor_profile_service.list_actor_profiles_core(
        deps={
            'db_path': lambda: DB_PATH,
        }
    )


def create_actor_profile(
    display_name: str,
    scope_statement: str | None,
    is_tracked: bool = True,
) -> dict[str, str | None]:
    return actor_profile_service.create_actor_profile_core(
        display_name=display_name,
        scope_statement=scope_statement,
        is_tracked=is_tracked,
        deps={
            'db_path': lambda: DB_PATH,
            'new_id': lambda: str(uuid.uuid4()),
            'utc_now_iso': utc_now_iso,
            'normalize_actor_name': actor_profile_service.normalize_actor_name_core,
        },
    )


def merge_actor_profiles(target_actor_id: str, source_actor_id: str) -> dict[str, object]:
    return actor_profile_service.merge_actor_profiles_core(
        target_actor_id=target_actor_id,
        source_actor_id=source_actor_id,
        deps={
            'db_path': lambda: DB_PATH,
            'utc_now_iso': utc_now_iso,
            'new_id': lambda: str(uuid.uuid4()),
        },
    )


def get_tracking_intent(actor_id: str) -> dict[str, object]:
    with sqlite3.connect(DB_PATH) as connection:
        if not actor_exists(connection, actor_id):
            raise HTTPException(status_code=404, detail='actor not found')
        return actor_profile_service.load_tracking_intent_core(connection, actor_id)


def upsert_tracking_intent(
    *,
    actor_id: str,
    why_track: str,
    mission_impact: str,
    intelligence_focus: str,
    key_questions: list[str],
    priority: str,
    impact: str,
    review_cadence_days: int,
    confirmation_min_sources: int,
    confirmation_max_age_days: int,
    confirmation_criteria: str,
    updated_by: str,
) -> dict[str, object]:
    return actor_profile_service.upsert_tracking_intent_core(
        actor_id=actor_id,
        why_track=why_track,
        mission_impact=mission_impact,
        intelligence_focus=intelligence_focus,
        key_questions=key_questions,
        priority=priority,
        impact=impact,
        review_cadence_days=review_cadence_days,
        confirmation_min_sources=confirmation_min_sources,
        confirmation_max_age_days=confirmation_max_age_days,
        confirmation_criteria=confirmation_criteria,
        updated_by=updated_by,
        deps={
            'db_path': lambda: DB_PATH,
            'utc_now_iso': utc_now_iso,
            'actor_exists': actor_exists,
        },
    )


def confirm_actor_assessment(actor_id: str, analyst: str, note: str) -> dict[str, object]:
    return actor_profile_service.confirm_actor_assessment_core(
        actor_id=actor_id,
        analyst=analyst,
        note=note,
        deps={
            'db_path': lambda: DB_PATH,
            'utc_now_iso': utc_now_iso,
            'actor_exists': actor_exists,
        },
    )


def dispatch_alert_deliveries(
    *,
    actor_id: str,
    alert_id: str,
    title: str,
    detail: str,
    severity: str,
    subscriptions: list[str],
) -> dict[str, int]:
    return alert_delivery_service.dispatch_alert_deliveries_core(
        actor_id=actor_id,
        alert_id=alert_id,
        title=title,
        detail=detail,
        severity=severity,
        subscriptions=subscriptions,
        db_path=DB_PATH,
        http_post=httpx.post,
    )


def seed_actor_profiles_from_mitre_groups() -> dict[str, int]:
    return actor_profile_service.seed_actor_profiles_from_mitre_groups_core(
        deps={
            'db_path': lambda: DB_PATH,
            'utc_now_iso': utc_now_iso,
            'new_id': lambda: str(uuid.uuid4()),
            'normalize_actor_name': actor_profile_service.normalize_actor_name_core,
            'load_mitre_groups': _load_mitre_groups,
        }
    )


def get_actor_refresh_stats(actor_id: str) -> dict[str, object]:
    try:
        return refresh_ops_service.actor_refresh_stats_core(
            actor_id=actor_id,
            db_path=DB_PATH,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail='actor not found')


def get_actor_refresh_timeline(actor_id: str) -> dict[str, object]:
    stats = get_actor_refresh_stats(actor_id)
    return {
        'actor_id': actor_id,
        'recent_generation_runs': stats.get('recent_generation_runs', []),
        'eta_seconds': stats.get('eta_seconds'),
        'avg_duration_ms': stats.get('avg_duration_ms'),
        'llm_cache_state': stats.get('llm_cache_state', {}),
        'queue_state': generation_service.queue_snapshot_core(),
    }


def submit_actor_refresh_job(actor_id: str, *, trigger_type: str = 'manual_refresh') -> dict[str, object]:
    active = generation_journal_service.active_generation_job_for_actor_core(
        actor_id=actor_id,
        deps=_generation_journal_deps(),
    )
    if isinstance(active, dict) and str(active.get('job_id') or '').strip():
        set_actor_notebook_status(
            actor_id,
            'running',
            'Refresh is already in progress for this actor.',
        )
        return {
            'actor_id': actor_id,
            'job_id': str(active.get('job_id') or ''),
            'status': str(active.get('status') or 'running'),
            'queued': False,
            'message': 'A refresh job is already in progress for this actor.',
        }
    job_id = _create_generation_job(actor_id=actor_id, trigger_type=trigger_type, initial_status='queued')
    set_actor_notebook_status(
        actor_id,
        'running',
        'Refresh queued. Waiting for worker slot...',
    )
    queue_priority = 2 if str(trigger_type or '').strip().lower() == 'auto_refresh' else 0
    enqueued = enqueue_actor_generation(
        actor_id,
        trigger_type=trigger_type,
        job_id=job_id,
        priority=queue_priority,
    )
    if not enqueued:
        _finalize_generation_job(
            job_id=job_id,
            status='skipped',
            imported_sources=0,
            duration_ms=0,
            final_message='Skipped because another refresh was already queued.',
            error_message='',
        )
        active = generation_journal_service.active_generation_job_for_actor_core(
            actor_id=actor_id,
            deps=_generation_journal_deps(),
        )
        return {
            'actor_id': actor_id,
            'job_id': str((active or {}).get('job_id') or job_id),
            'status': str((active or {}).get('status') or 'queued'),
            'queued': False,
            'message': 'Refresh already queued for this actor.',
        }
    return {
        'actor_id': actor_id,
        'job_id': str(job_id),
        'status': 'queued',
        'queued': True,
        'message': 'Refresh job queued.',
    }


def get_actor_refresh_job(actor_id: str, job_id: str) -> dict[str, object]:
    item = generation_journal_service.generation_job_detail_core(
        actor_id=actor_id,
        job_id=job_id,
        deps=_generation_journal_deps(),
    )
    if item is None:
        raise HTTPException(status_code=404, detail='refresh job not found')
    return item


def _upsert_source_for_actor(
    connection: sqlite3.Connection,
    actor_id: str,
    source_name: str,
    source_url: str,
    published_at: str | None,
    pasted_text: str,
    trigger_excerpt: str | None = None,
    title: str | None = None,
    headline: str | None = None,
    og_title: str | None = None,
    html_title: str | None = None,
    publisher: str | None = None,
    site_name: str | None = None,
    source_tier: str | None = None,
    confidence_weight: int | None = None,
    source_type: str | None = None,
    refresh_existing_content: bool = False,
) -> str:
    resolved_source_tier = str(source_tier or '').strip() or _source_tier_label(source_url)
    resolved_confidence_weight = (
        int(confidence_weight)
        if confidence_weight is not None
        else int(_source_trust_score(source_url))
    )
    return source_store_service.upsert_source_for_actor_core(
        connection=connection,
        actor_id=actor_id,
        source_name=source_name,
        source_url=source_url,
        published_at=published_at,
        pasted_text=pasted_text,
        trigger_excerpt=trigger_excerpt,
        title=title,
        headline=headline,
        og_title=og_title,
        html_title=html_title,
        publisher=publisher,
        site_name=site_name,
        source_type=source_type,
        source_tier=resolved_source_tier,
        confidence_weight=resolved_confidence_weight,
        overwrite_source_quality=SOURCE_QUALITY_OVERWRITE_ON_UPSERT,
        refresh_existing_content=refresh_existing_content,
        deps={
            'source_fingerprint': _source_fingerprint,
            'new_id': lambda: str(uuid.uuid4()),
            'now_iso': utc_now_iso,
        },
    )


def run_cold_actor_backfill(
    actor_id: str,
    actor_name: str,
    actor_aliases: list[str] | None = None,
) -> dict[str, object]:
    return web_backfill_service.run_cold_actor_backfill_core(
        actor_id=actor_id,
        actor_name=actor_name,
        actor_aliases=actor_aliases or [],
        deps={
            'db_path': _db_path,
            'sqlite_connect': sqlite3.connect,
            'utc_now_iso': utc_now_iso,
            'http_get': httpx.get,
            'derive_source_from_url': derive_source_from_url,
            'upsert_source_for_actor': _upsert_source_for_actor,
            'build_actor_profile_from_mitre': _build_actor_profile_from_mitre,
        },
    )


def _parse_ioc_values(raw: str) -> list[str]:
    return source_ingest_service.parse_ioc_values_core(raw)


def _validate_ioc_candidate(
    *,
    raw_value: str,
    raw_type: str | None,
    source_tier: str | None = None,
    extraction_method: str = 'manual',
) -> dict[str, object]:
    return ioc_validation_service.validate_ioc_candidate_core(
        raw_value=raw_value,
        raw_type=raw_type,
        source_tier=source_tier,
        extraction_method=extraction_method,
    )


def _upsert_ioc_item(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    raw_ioc_type: str,
    raw_ioc_value: str,
    source_ref: str | None,
    source_id: str | None,
    source_tier: str | None,
    extraction_method: str,
    now_iso: str,
    lifecycle_status: str = 'active',
    handling_tlp: str = 'TLP:CLEAR',
    confidence_score_override: int | None = None,
    observed_at: str | None = None,
    valid_from: str | None = None,
    valid_until: str | None = None,
    revoked: bool = False,
) -> dict[str, object]:
    return ioc_store_service.upsert_ioc_item_core(
        connection,
        actor_id=actor_id,
        raw_ioc_type=raw_ioc_type,
        raw_ioc_value=raw_ioc_value,
        source_ref=source_ref,
        source_id=source_id,
        source_tier=source_tier,
        extraction_method=extraction_method,
        now_iso=now_iso,
        lifecycle_status=lifecycle_status,
        handling_tlp=handling_tlp,
        confidence_score_override=confidence_score_override,
        observed_at=observed_at,
        valid_from=valid_from,
        valid_until=valid_until,
        revoked=revoked,
        deps={
            'validate_ioc_candidate': _validate_ioc_candidate,
        },
    )


def _export_actor_stix_bundle(connection: sqlite3.Connection, *, actor_id: str, actor_name: str) -> dict[str, object]:
    return stix_service.export_actor_bundle_core(
        connection,
        actor_id=actor_id,
        actor_name=actor_name,
    )


def _import_actor_stix_bundle(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    bundle: dict[str, object],
) -> dict[str, int]:
    return stix_service.import_actor_bundle_core(
        connection,
        actor_id=actor_id,
        bundle=bundle,
        now_iso=utc_now_iso(),
        upsert_ioc_item=_upsert_ioc_item,
    )


def _store_feedback_event(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    item_type: str,
    item_id: str,
    feedback_label: str,
    reason: str,
    source_id: str | None,
    metadata: dict[str, object] | None,
) -> dict[str, object]:
    return feedback_service.store_feedback_event_core(
        connection,
        actor_id=actor_id,
        item_type=item_type,
        item_id=item_id,
        feedback_label=feedback_label,
        reason=reason,
        source_id=source_id,
        metadata=metadata,
        now_iso=utc_now_iso(),
    )


def _feedback_summary_for_actor(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    item_type: str | None = None,
) -> dict[str, object]:
    return feedback_service.feedback_summary_for_actor_core(
        connection,
        actor_id=actor_id,
        item_type=item_type,
    )


def _normalize_environment_profile(payload: dict[str, object]) -> dict[str, object]:
    return environment_profile_service.normalize_environment_profile(payload)


def _upsert_environment_profile(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    profile: dict[str, object],
) -> dict[str, object]:
    return environment_profile_service.upsert_environment_profile_core(
        connection,
        actor_id=actor_id,
        profile=profile,
        now_iso=utc_now_iso(),
    )


def _load_environment_profile(connection: sqlite3.Connection, *, actor_id: str) -> dict[str, object]:
    return environment_profile_service.load_environment_profile_core(
        connection,
        actor_id=actor_id,
    )


def _apply_feedback_to_source_domains(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
    source_urls: list[str],
    rating_score: int,
) -> int:
    return source_reliability_service.apply_feedback_to_source_domains_core(
        connection,
        actor_id=actor_id,
        source_urls=source_urls,
        rating_score=rating_score,
        now_iso=utc_now_iso(),
    )


def _load_source_reliability_map(
    connection: sqlite3.Connection,
    *,
    actor_id: str,
) -> dict[str, dict[str, object]]:
    return source_reliability_service.load_reliability_map_core(
        connection,
        actor_id=actor_id,
    )


def _personalize_query(query: str, *, ioc_value: str, profile: dict[str, object]) -> str:
    return environment_profile_service.personalize_query_core(
        query,
        ioc_value=ioc_value,
        profile=profile,
    )


def _domain_from_url(url: str) -> str:
    return environment_profile_service.domain_from_url_core(url)


def _confidence_weight_adjustment(reliability_score: float) -> int:
    return source_reliability_service.confidence_weight_adjustment_core(reliability_score)


def _source_fingerprint(
    title: str | None,
    headline: str | None,
    og_title: str | None,
    html_title: str | None,
    pasted_text: str,
) -> str:
    return source_store_service.source_fingerprint_core(
        title=title,
        headline=headline,
        og_title=og_title,
        html_title=html_title,
        pasted_text=pasted_text,
        deps={
            'normalize_text': _normalize_text,
            'first_sentences': lambda text, count: _first_sentences(text, count=count),
        },
    )


def import_default_feeds_for_actor(actor_id: str) -> int:
    _log_event('feed_import_started', actor_id=actor_id)
    imported = 0
    success = False
    try:
        imported = feed_import_service.import_default_feeds_for_actor_core(
            actor_id=actor_id,
            deps={
                'pipeline_import_default_feeds_for_actor_core': pipeline_import_default_feeds_for_actor_core,
                'db_path': lambda: DB_PATH,
                'default_cti_feeds': DEFAULT_CTI_FEEDS,
                'primary_cti_feeds': PRIMARY_CTI_FEEDS + EXPANDED_PRIMARY_ADVISORY_FEEDS,
                'secondary_context_feeds': SECONDARY_CONTEXT_FEEDS,
                'actor_feed_lookback_days': ACTOR_FEED_LOOKBACK_DAYS,
                'feed_import_max_seconds': FEED_IMPORT_MAX_SECONDS,
                'feed_fetch_timeout_seconds': FEED_FETCH_TIMEOUT_SECONDS,
                'feed_entry_scan_limit': FEED_ENTRY_SCAN_LIMIT,
                'feed_imported_limit': FEED_IMPORTED_LIMIT,
                'actor_search_link_limit': ACTOR_SEARCH_LINK_LIMIT,
                'feed_require_published_at': FEED_REQUIRE_PUBLISHED_AT,
                'actor_exists': actor_exists,
                'build_actor_profile_from_mitre': _build_actor_profile_from_mitre,
                'actor_terms': _actor_terms,
                'actor_query_feeds': _actor_query_feeds,
                'import_ransomware_live_actor_activity': _import_ransomware_live_actor_activity,
                'safe_http_get': _safe_http_get,
                'parse_feed_entries': _parse_feed_entries,
                'text_contains_actor_term': _text_contains_actor_term,
                'within_lookback': _within_lookback,
                'parse_published_datetime': _parse_published_datetime,
                'derive_source_from_url': derive_source_from_url,
                'upsert_source_for_actor': _upsert_source_for_actor,
                'duckduckgo_actor_search_urls': _duckduckgo_actor_search_urls,
                'utc_now_iso': utc_now_iso,
            },
        )
        success = True
        return imported
    except Exception as exc:
        _log_event('feed_import_failed', actor_id=actor_id, error=str(exc))
        raise
    finally:
        metrics_service.record_feed_import_core(imported_count=imported, success=success)
        _log_event('feed_import_completed', actor_id=actor_id, success=success, imported=imported)


def generate_actor_requirements(actor_id: str, org_context: str, priority_mode: str) -> int:
    return requirements_service.generate_actor_requirements_core(
        actor_id=actor_id,
        org_context=org_context,
        priority_mode=priority_mode,
        deps={
            'pipeline_generate_actor_requirements_core': pipeline_generate_actor_requirements_core,
            'db_path': lambda: DB_PATH,
            'now_iso': utc_now_iso,
            'actor_exists': actor_exists,
            'build_actor_profile_from_mitre': _build_actor_profile_from_mitre,
            'actor_terms': _actor_terms,
            'split_sentences': _split_sentences,
            'sentence_mentions_actor_terms': _sentence_mentions_actor_terms,
            'looks_like_activity_sentence': _looks_like_activity_sentence,
            'ollama_available': _ollama_available,
            'sanitize_question_text': _sanitize_question_text,
            'question_from_sentence': _question_from_sentence,
            'token_overlap': _token_overlap,
            'normalize_text': _normalize_text,
            'new_id': lambda: str(uuid.uuid4()),
        },
    )


def build_notebook(
    actor_id: str,
    *,
    generate_questions: bool = True,
    rebuild_timeline: bool = True,
) -> None:
    notebook_service.build_notebook_wrapper_core(
        actor_id=actor_id,
        generate_questions=generate_questions,
        rebuild_timeline=rebuild_timeline,
        deps={
            'build_notebook_core': build_notebook_core,
            'db_path': lambda: DB_PATH,
            'now_iso': utc_now_iso,
            'actor_exists': actor_exists,
            'build_actor_profile_from_mitre': _build_actor_profile_from_mitre,
            'actor_terms_fn': _actor_terms,
            'extract_major_move_events': _extract_major_move_events,
            'normalize_text': _normalize_text,
            'token_overlap': _token_overlap,
            'extract_question_sentences': _extract_question_sentences,
            'sentence_mentions_actor_terms': _sentence_mentions_actor_terms,
            'sanitize_question_text': _sanitize_question_text,
            'question_from_sentence': _question_from_sentence,
            'ollama_generate_questions': _ollama_generate_questions,
            'platforms_for_question': _platforms_for_question,
            'guidance_for_platform': _guidance_for_platform,
            'ollama_enrich_quick_checks': _ollama_enrich_quick_checks,
            'store_quick_check_overrides': _store_quick_check_overrides,
        },
    )


def _fetch_actor_notebook(
    actor_id: str,
    *,
    source_tier: str | None = None,
    min_confidence_weight: int | None = None,
    source_days: int | None = None,
    prefer_cached: bool = True,
    build_on_cache_miss: bool = True,
) -> dict[str, object]:
    return notebook_service.fetch_actor_notebook_wrapper_core(
        actor_id=actor_id,
        deps=_fetch_actor_notebook_deps(
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days,
            prefer_cached=prefer_cached,
            build_on_cache_miss=build_on_cache_miss,
        ),
    )


def _fetch_actor_notebook_deps(
    *,
    source_tier: str | None = None,
    min_confidence_weight: int | None = None,
    source_days: int | None = None,
    prefer_cached: bool = True,
    build_on_cache_miss: bool = True,
) -> dict[str, object]:
    return {
        'pipeline_fetch_actor_notebook_core': pipeline_fetch_actor_notebook_core,
        'db_path': _db_path,
        'source_tier': source_tier,
        'min_confidence_weight': min_confidence_weight,
        'source_days': source_days,
        'prefer_cached': prefer_cached,
        'build_on_cache_miss': build_on_cache_miss,
        'parse_published_datetime': _parse_published_datetime,
        'safe_json_string_list': _safe_json_string_list,
        'actor_signal_categories': _actor_signal_categories,
        'question_actor_relevance': _question_actor_relevance,
        'priority_update_evidence_dt': _priority_update_evidence_dt,
        'question_org_alignment': _question_org_alignment,
        'priority_rank_score': _priority_rank_score,
        'phase_label_for_question': _phase_label_for_question,
        'priority_where_to_check': _priority_where_to_check,
        'priority_confidence_label': _priority_confidence_label,
        'quick_check_title': _quick_check_title,
        'short_decision_trigger': _short_decision_trigger,
        'telemetry_anchor_line': _telemetry_anchor_line,
        'priority_next_best_action': _priority_next_best_action,
        'guidance_line': _guidance_line,
        'guidance_query_hint': _guidance_query_hint,
        'priority_disconfirming_signal': _priority_disconfirming_signal,
        'confidence_change_threshold_line': _confidence_change_threshold_line,
        'escalation_threshold_line': _escalation_threshold_line,
        'expected_output_line': _expected_output_line,
        'priority_update_recency_label': _priority_update_recency_label,
        'org_alignment_label': _org_alignment_label,
        'fallback_priority_questions': _fallback_priority_questions,
        'token_overlap': _token_overlap,
        'build_actor_profile_from_mitre': _build_actor_profile_from_mitre,
        'group_top_techniques': _group_top_techniques,
        'favorite_attack_vectors': _favorite_attack_vectors,
        'known_technique_ids_for_entity': _known_technique_ids_for_entity,
        'emerging_techniques_from_timeline': _emerging_techniques_from_timeline,
        'build_timeline_graph': _build_timeline_graph,
        'compact_timeline_rows': _compact_timeline_rows,
        'actor_terms': _actor_terms,
        'build_recent_activity_highlights': _build_recent_activity_highlights,
        'build_top_change_signals': pipeline_build_top_change_signals,
        'ollama_review_change_signals': _ollama_review_change_signals,
        'ollama_synthesize_recent_activity': _ollama_synthesize_recent_activity,
        'enforce_ollama_synthesis': ENFORCE_OLLAMA_SYNTHESIS,
        'build_recent_activity_synthesis': _build_recent_activity_synthesis,
        'recent_change_summary': _recent_change_summary,
        'build_environment_checks': _build_environment_checks,
        'build_notebook_kpis': _build_notebook_kpis,
        'format_date_or_unknown': _format_date_or_unknown,
        'load_source_reliability_map': _load_source_reliability_map,
        'domain_from_url': _domain_from_url,
        'confidence_weight_adjustment': _confidence_weight_adjustment,
        'load_quick_check_overrides': lambda connection, actor: quick_check_service.load_quick_check_overrides_core(
            connection,
            actor_id=actor,
        ),
        'run_cold_actor_backfill': run_cold_actor_backfill,
        'rebuild_notebook': build_notebook,
        'backfill_debug_ui_enabled': BACKFILL_DEBUG_UI,
    }


def _refresh_actor_notebook_uncached(actor_id: str) -> dict[str, object]:
    return _fetch_actor_notebook(actor_id, prefer_cached=False)


def _initialize_sqlite_deps() -> dict[str, object]:
    return {
        'resolve_startup_db_path': _resolve_startup_db_path,
        'configure_mitre_store': lambda db_path: mitre_store.configure(
            db_path=db_path,
            attack_url=ATTACK_ENTERPRISE_STIX_URL,
        ),
        'clear_mitre_store_cache': mitre_store.clear_cache,
        'reset_app_mitre_caches': _reset_mitre_caches,
        'ensure_mitre_attack_dataset': _ensure_mitre_attack_dataset,
        'sqlite_connect': sqlite3.connect,
    }


def initialize_sqlite() -> None:
    global DB_PATH
    DB_PATH = db_schema_service.initialize_sqlite_core(deps=_initialize_sqlite_deps())
    if AUTO_MERGE_DUPLICATE_ACTORS:
        try:
            actor_profile_service.auto_merge_duplicate_actors_core(
                deps={
                    'db_path': lambda: DB_PATH,
                    'utc_now_iso': utc_now_iso,
                    'new_id': lambda: str(uuid.uuid4()),
                }
            )
        except Exception:
            pass
    if str(os.environ.get('MITRE_AUTO_SEED_ACTORS', '0')).strip().lower() in {'1', 'true', 'yes', 'on'}:
        try:
            seed_actor_profiles_from_mitre_groups()
        except Exception:
            pass
    _recover_stale_running_states()


def _register_routers() -> None:
    app_wiring_service.register_routers(
        app,
        deps={
            'routes_dashboard': routes_dashboard,
            'routes_api': routes_api,
            'routes_ui': routes_ui,
            'routes_actor_ops': routes_actor_ops,
            'routes_notebook': routes_notebook,
            'routes_evolution': routes_evolution,
            'list_actor_profiles': list_actor_profiles,
            'fetch_actor_notebook': _fetch_actor_notebook,
            'set_actor_notebook_status': set_actor_notebook_status,
            'run_actor_generation': run_actor_generation,
            'enqueue_actor_generation': enqueue_actor_generation,
            'metrics_snapshot': metrics_service.snapshot_metrics_core,
            'get_ollama_status': get_ollama_status,
            'page_refresh_auto_trigger_minutes': PAGE_REFRESH_AUTO_TRIGGER_MINUTES,
            'running_stale_recovery_minutes': RUNNING_STALE_RECOVERY_MINUTES,
            'recover_stale_running_states': _recover_stale_running_states,
            'format_duration_ms': _format_duration_ms,
            'templates': templates,
            'enforce_request_size': _enforce_request_size,
            'default_body_limit_bytes': DEFAULT_BODY_LIMIT_BYTES,
            'create_actor_profile': create_actor_profile,
            'merge_actor_profiles': merge_actor_profiles,
            'db_path': _db_path,
            'actor_exists': actor_exists,
            'source_upload_body_limit_bytes': SOURCE_UPLOAD_BODY_LIMIT_BYTES,
            'derive_source_from_url': derive_source_from_url,
            'upsert_source_for_actor': _upsert_source_for_actor,
            'import_default_feeds_for_actor': import_default_feeds_for_actor,
            'parse_ioc_values': _parse_ioc_values,
            'validate_ioc_candidate': _validate_ioc_candidate,
            'upsert_ioc_item': _upsert_ioc_item,
            'export_actor_stix_bundle': _export_actor_stix_bundle,
            'import_actor_stix_bundle': _import_actor_stix_bundle,
            'utc_now_iso': utc_now_iso,
            'get_actor_refresh_stats': get_actor_refresh_stats,
            'get_actor_refresh_timeline': get_actor_refresh_timeline,
            'submit_actor_refresh_job': submit_actor_refresh_job,
            'get_actor_refresh_job': get_actor_refresh_job,
            'generate_actor_requirements': generate_actor_requirements,
            'safe_json_string_list': _safe_json_string_list,
            'generate_ioc_hunt_queries': _ollama_generate_ioc_hunt_queries,
            'store_feedback_event': _store_feedback_event,
            'feedback_summary_for_actor': _feedback_summary_for_actor,
            'normalize_environment_profile': _normalize_environment_profile,
            'upsert_environment_profile': _upsert_environment_profile,
            'load_environment_profile': _load_environment_profile,
            'apply_feedback_to_source_domains': _apply_feedback_to_source_domains,
            'get_tracking_intent': get_tracking_intent,
            'upsert_tracking_intent': upsert_tracking_intent,
            'confirm_actor_assessment': confirm_actor_assessment,
            'dispatch_alert_deliveries': dispatch_alert_deliveries,
            'observation_body_limit_bytes': OBSERVATION_BODY_LIMIT_BYTES,
            'normalize_technique_id': _normalize_technique_id,
            'normalize_string_list': normalize_string_list,
            'capability_category_from_technique_id': _capability_category_from_technique_id,
            'generate_validation_template': generate_validation_template,
            'baseline_entry': baseline_entry,
            'resolve_delta_action': lambda actor_id, delta_id, requested_action: resolve_delta_action(
                actor_id,
                delta_id,
                requested_action,
            ),
        },
    )


_register_routers()


def actors_ui() -> str:
    return legacy_ui.render_actors_ui(
        actors=[
            {
                'id': actor['id'],
                'display_name': actor['display_name'],
            }
            for actor in list_actor_profiles()
        ]
    )


def root(
    request: Request,
    background_tasks: BackgroundTasks,
    actor_id: str | None = None,
    notice: str | None = None,
    source_tier: str | None = None,
    min_confidence_weight: str | None = None,
    source_days: str | None = None,
) -> HTMLResponse:
    return routes_dashboard.render_dashboard_root(
        request=request,
        background_tasks=background_tasks,
        actor_id=actor_id,
        notice=notice,
        source_tier=source_tier,
        min_confidence_weight=min_confidence_weight,
        source_days=source_days,
        deps={
            'list_actor_profiles': list_actor_profiles,
            'fetch_actor_notebook': _fetch_actor_notebook,
            'set_actor_notebook_status': set_actor_notebook_status,
            'run_actor_generation': run_actor_generation,
            'enqueue_actor_generation': enqueue_actor_generation,
            'get_ollama_status': get_ollama_status,
            'page_refresh_auto_trigger_minutes': PAGE_REFRESH_AUTO_TRIGGER_MINUTES,
            'running_stale_recovery_minutes': RUNNING_STALE_RECOVERY_MINUTES,
            'recover_stale_running_states': _recover_stale_running_states,
            'format_duration_ms': _format_duration_ms,
            'templates': templates,
        },
    )


@app.post('/actors/{actor_id}/initialize')
def initialize_actor_state(actor_id: str) -> dict[str, str]:
    return actor_state_service.initialize_actor_state_core(
        actor_id=actor_id,
        deps={
            'utc_now_iso': utc_now_iso,
            'baseline_capability_grid': baseline_capability_grid,
            'baseline_behavioral_model': baseline_behavioral_model,
            'db_path': lambda: DB_PATH,
            'actor_exists': actor_exists,
        },
    )


def resolve_delta_action(actor_id: str, delta_id: str, requested_action: str) -> dict[str, str]:
    return actor_state_service.resolve_delta_action_core(
        actor_id=actor_id,
        delta_id=delta_id,
        requested_action=requested_action,
        deps={
            'utc_now_iso': utc_now_iso,
            'db_path': lambda: DB_PATH,
            'actor_exists': actor_exists,
            'baseline_entry': baseline_entry,
        },
    )
