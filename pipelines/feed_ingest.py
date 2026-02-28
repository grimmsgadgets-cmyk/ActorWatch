"""Compatibility wrapper for feed ingestion helpers and core pipeline logic."""

from pipelines.feed_ingest_core import _apply_source_trust_boost
from pipelines.feed_ingest_core import _is_google_news_wrapper_url
from pipelines.feed_ingest_core import _promote_soft_sources_from_corroboration
from pipelines.feed_ingest_core import import_default_feeds_for_actor_core

__all__ = [
    '_apply_source_trust_boost',
    '_is_google_news_wrapper_url',
    '_promote_soft_sources_from_corroboration',
    'import_default_feeds_for_actor_core',
]
