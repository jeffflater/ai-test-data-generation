"""
Persona Platform - A centralized, reusable dataset persona platform for test data generation.

This platform enables teams to generate many types of test datasets—API, streaming, files,
load testing, and negative cases—from shared behavioral personas.
"""

__version__ = "0.1.0"

from persona_platform.personas.base import Persona, Behavior, BehaviorType
from persona_platform.generators.base import Generator, DatasetType
from persona_platform.profiles.base import Profile
from persona_platform.engine.persona_engine import PersonaEngine
from persona_platform.engine.validation_engine import ValidationEngine

__all__ = [
    "Persona",
    "Behavior",
    "BehaviorType",
    "Generator",
    "DatasetType",
    "Profile",
    "PersonaEngine",
    "ValidationEngine",
]
