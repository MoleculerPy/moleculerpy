"""Local path bootstrap for running the demo directly from the repository."""

from __future__ import annotations

import sys
from pathlib import Path


def configure_local_paths() -> Path:
    """Add local package roots to ``sys.path`` and return repository root."""
    repo_root = Path(__file__).resolve().parents[3]
    candidate_paths = (
        repo_root / "moleculerpy",
        repo_root / "moleculerpy-channels",
        repo_root / "moleculerpy-repl" / "src",
    )

    for path in reversed(candidate_paths):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    return repo_root
