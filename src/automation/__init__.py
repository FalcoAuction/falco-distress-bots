from __future__ import annotations

from typing import Any


def write_run_summary(*args: Any, **kwargs: Any):
    from .run_summary import write_run_summary as _write_run_summary

    return _write_run_summary(*args, **kwargs)


def maybe_publish_to_vault(*args: Any, **kwargs: Any):
    from .site_publish import maybe_publish_to_vault as _maybe_publish_to_vault

    return _maybe_publish_to_vault(*args, **kwargs)


def write_agent_reports(*args: Any, **kwargs: Any):
    from .agents import write_agent_reports as _write_agent_reports

    return _write_agent_reports(*args, **kwargs)


__all__ = ["write_run_summary", "maybe_publish_to_vault", "write_agent_reports"]
