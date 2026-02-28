"""Compatibility wrapper for notebook pipeline exports."""

import pipelines.notebook_pipeline_core as _core

globals().update(
    {
        name: value
        for name, value in vars(_core).items()
        if not name.startswith('__')
    }
)

__all__ = [name for name in globals() if not name.startswith('__')]
