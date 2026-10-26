"""Create new validation run directories without reusing existing names."""

from pathlib import Path


def create_run_directory(run_root: Path, name: str) -> Path:
    """Claim a fresh validation run directory; an existing name is always a collision."""
    directory = run_root / name
    if directory.exists():
        raise ValueError(f"Validation run {name!r} already exists at {directory}")
    directory.mkdir(parents=True)
    return directory
