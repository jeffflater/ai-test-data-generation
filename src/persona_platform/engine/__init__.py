"""Engine module - Runtime & Tooling layer.

Contains:
- Persona Engine: Applies behavioral rules
- Validation Engine: Enforces schema correctness
"""

from persona_platform.engine.persona_engine import PersonaEngine
from persona_platform.engine.validation_engine import ValidationEngine, ValidationResult

__all__ = [
    "PersonaEngine",
    "ValidationEngine",
    "ValidationResult",
]
