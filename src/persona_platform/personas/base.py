"""Base classes for Personas - the Behavior Layer.

Personas are declarative definitions of real-world behavior that are:
- Dataset-type agnostic
- Composable and optionally inheritable
- Versioned and centrally owned

They define realism once to be reused everywhere.
"""

from abc import ABC
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class BehaviorType(str, Enum):
    """Types of behaviors that can be defined in a persona."""

    TIMING = "timing"
    ERROR_RATE = "error_rate"
    DATA_QUALITY = "data_quality"
    VOLUME = "volume"
    PATTERN = "pattern"
    CONSTRAINT = "constraint"
    MUTATION = "mutation"
    LATENCY = "latency"
    FREQUENCY = "frequency"


class Behavior(BaseModel):
    """A single behavioral characteristic of a persona.

    Behaviors describe how systems behave, not exact values.
    """

    name: str = Field(..., description="Unique name for this behavior")
    behavior_type: BehaviorType = Field(..., description="Category of behavior")
    description: str = Field(default="", description="Human-readable description")
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Behavior-specific parameters"
    )
    weight: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Probability weight for this behavior (0.0-1.0)"
    )

    def apply(self, data: dict[str, Any], seed: int | None = None) -> dict[str, Any]:
        """Apply this behavior to transform data.

        Args:
            data: The input data to transform
            seed: Optional random seed for deterministic behavior

        Returns:
            Transformed data with behavior applied
        """
        return data


class Persona(BaseModel):
    """A declarative definition of real-world behavior.

    Personas represent entities like:
    - Noisy devices
    - Legacy clients
    - Edge-case users
    - Normal users
    - High-volume systems

    They are dataset-type agnostic and focus on describing behavior.
    """

    name: str = Field(..., description="Unique identifier for this persona")
    version: str = Field(default="1.0.0", description="Semantic version")
    description: str = Field(default="", description="Human-readable description")
    tags: list[str] = Field(default_factory=list, description="Categorization tags")
    behaviors: list[Behavior] = Field(
        default_factory=list,
        description="List of behaviors that define this persona"
    )
    inherits_from: list[str] = Field(
        default_factory=list,
        description="Names of parent personas to inherit behaviors from"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )

    def get_behaviors_by_type(self, behavior_type: BehaviorType) -> list[Behavior]:
        """Get all behaviors of a specific type."""
        return [b for b in self.behaviors if b.behavior_type == behavior_type]

    def add_behavior(self, behavior: Behavior) -> None:
        """Add a behavior to this persona."""
        self.behaviors.append(behavior)

    def merge_with(self, other: "Persona") -> "Persona":
        """Create a new persona by merging with another.

        The current persona's behaviors take precedence over the other's.
        """
        merged_behaviors = list(other.behaviors)
        existing_names = {b.name for b in merged_behaviors}

        for behavior in self.behaviors:
            if behavior.name in existing_names:
                merged_behaviors = [
                    b if b.name != behavior.name else behavior
                    for b in merged_behaviors
                ]
            else:
                merged_behaviors.append(behavior)

        return Persona(
            name=f"{self.name}+{other.name}",
            version=self.version,
            description=f"Merged: {self.description} + {other.description}",
            tags=list(set(self.tags + other.tags)),
            behaviors=merged_behaviors,
            metadata={**other.metadata, **self.metadata},
        )


class PersonaBuilder:
    """Fluent builder for creating Personas."""

    def __init__(self, name: str):
        self._name = name
        self._version = "1.0.0"
        self._description = ""
        self._tags: list[str] = []
        self._behaviors: list[Behavior] = []
        self._inherits_from: list[str] = []
        self._metadata: dict[str, Any] = {}

    def version(self, version: str) -> "PersonaBuilder":
        self._version = version
        return self

    def description(self, description: str) -> "PersonaBuilder":
        self._description = description
        return self

    def tag(self, *tags: str) -> "PersonaBuilder":
        self._tags.extend(tags)
        return self

    def behavior(
        self,
        name: str,
        behavior_type: BehaviorType,
        parameters: dict[str, Any] | None = None,
        weight: float = 1.0,
        description: str = "",
    ) -> "PersonaBuilder":
        self._behaviors.append(
            Behavior(
                name=name,
                behavior_type=behavior_type,
                parameters=parameters or {},
                weight=weight,
                description=description,
            )
        )
        return self

    def inherits(self, *persona_names: str) -> "PersonaBuilder":
        self._inherits_from.extend(persona_names)
        return self

    def metadata(self, **kwargs: Any) -> "PersonaBuilder":
        self._metadata.update(kwargs)
        return self

    def build(self) -> Persona:
        return Persona(
            name=self._name,
            version=self._version,
            description=self._description,
            tags=self._tags,
            behaviors=self._behaviors,
            inherits_from=self._inherits_from,
            metadata=self._metadata,
        )
