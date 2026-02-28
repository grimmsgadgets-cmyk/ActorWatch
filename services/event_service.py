"""
Internal event system.

In standalone community-edition mode all emit() calls are no-ops.
When ActorWatch is embedded in a host platform (e.g. ThreatSpire), the
host registers handlers via on() at startup and receives events for
cross-module workflows, notifications, and audit logging.

Event names follow the pattern  <resource>.<action>:
    actor.created   actor.updated   actor.deleted
    ioc.added       ioc.bulk_imported
    timeline.event_added
    note.created
"""
from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

# { event_name: [handler, ...] }
_handlers: dict[str, list[Callable[[Any], None]]] = {}


def on(event: str, handler: Callable[[Any], None]) -> None:
    """Register a handler for an event.  Safe to call multiple times."""
    _handlers.setdefault(event, []).append(handler)


def off(event: str, handler: Callable[[Any], None]) -> None:
    """Deregister a previously registered handler (no-op if not found)."""
    if event in _handlers:
        try:
            _handlers[event].remove(handler)
        except ValueError:
            pass


def emit(event: str, payload: Any = None) -> None:
    """
    Fire all handlers registered for *event*.

    Handler exceptions are caught and logged so that a misbehaving
    subscriber never breaks the core application flow.
    """
    for handler in _handlers.get(event, []):
        try:
            handler(payload)
        except Exception:
            logger.exception('event_service: handler error for event %r', event)
