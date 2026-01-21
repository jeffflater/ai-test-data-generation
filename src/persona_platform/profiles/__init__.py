"""Profiles module - The Binding Layer of the platform.

Profiles are project-owned configurations that specify:
- Selected personas
- Dataset types
- Scale and volume
- Randomization seeds

They contain no generation logic or behavioral definitions.
"""

from persona_platform.profiles.base import Profile, DatasetConfig, OutputConfig
from persona_platform.profiles.loader import ProfileLoader, load_profile

__all__ = [
    "Profile",
    "DatasetConfig",
    "OutputConfig",
    "ProfileLoader",
    "load_profile",
]
