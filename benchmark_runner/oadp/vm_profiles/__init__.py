"""
VM profile loader with path-traversal prevention (SEC-001).

Profiles are YAML files stored alongside this module.  The loader rejects
profile names that contain path separators or resolve outside the
``vm_profiles/`` directory.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path

import yaml

from benchmark_runner.common.logger.logger_time_stamp import logger

_PROFILES_DIR = Path(__file__).resolve().parent

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")


def _validate_profile_name(name: str) -> None:
    """Raise ``ValueError`` if *name* could escape the profiles directory."""
    if not name or not _SAFE_NAME_RE.match(name):
        raise ValueError(f"Invalid VM profile name '{name}': must be alphanumeric with hyphens, dots, or underscores only")

    forbidden = {".", "..", "/", "\\"}
    if any(part in forbidden for part in Path(name).parts):
        raise ValueError(f"VM profile name '{name}' contains forbidden path components")


def load_vm_profile(profile_name: str) -> dict:
    """Load and return a VM profile dict, merging defaults.

    Raises ``FileNotFoundError`` if the profile YAML does not exist and
    ``ValueError`` if the name fails path-safety checks.
    """
    _validate_profile_name(profile_name)

    profile_path = (_PROFILES_DIR / f"{profile_name}.yaml").resolve()

    if not str(profile_path).startswith(str(_PROFILES_DIR)):
        raise ValueError(f"Resolved profile path '{profile_path}' is outside the profiles directory")

    if not profile_path.is_file():
        raise FileNotFoundError(f"VM profile '{profile_name}' not found at {profile_path}")

    raw = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"VM profile '{profile_name}' must be a YAML mapping, got {type(raw).__name__}")

    logger.info(f"Loaded VM profile '{profile_name}' from {profile_path}")
    return raw


def merge_disk_overrides(profile: dict, overrides: dict | None) -> dict:
    """Return a profile copy with per-disk fields overridden from the scenario.

    *overrides* maps disk names to field dicts, e.g.
    ``{"datadisk": {"expected_capacity": "20G"}}``.
    """
    if not overrides:
        return profile

    merged = copy.deepcopy(profile)
    disks = merged.get("disks", [])
    for disk in disks:
        disk_name = disk.get("name", "")
        if disk_name in overrides:
            disk.update(overrides[disk_name])
    return merged
