"""Compatibility wrapper for web backfill service exports."""

import services.web_backfill_service_core as _core

globals().update(
    {
        name: value
        for name, value in vars(_core).items()
        if not name.startswith('__')
    }
)

__all__ = [name for name in globals() if not name.startswith('__')]
