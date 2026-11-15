"""Compatibility alias for ``portfolio.paper``.

Prefer ``portfolio.paper`` for new code. Kept so existing
``from portfolio import trading`` / lazy package exports keep working.
"""

from __future__ import annotations

from portfolio.paper import *  # noqa: F403
