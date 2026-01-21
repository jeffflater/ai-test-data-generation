"""Base classes for Generators - the Shape Layer.

Generators are responsible for:
- Converting persona behavior into concrete datasets
- Supporting dataset-specific formats
- Never redefining behavior (strict separation from personas)

They translate behavior into data shapes.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Iterator
import random

from pydantic import BaseModel, Field

from persona_platform.personas.base import Persona, Behavior, BehaviorType


class DatasetType(str, Enum):
    """Types of datasets that can be generated."""

    API = "api"
    STREAMING = "streaming"
    FILE = "file"
    LOAD = "load"
    FAULT = "fault"


class GeneratedRecord(BaseModel):
    """A single generated data record."""

    data: dict[str, Any] = Field(..., description="The generated data")
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Generation metadata (persona, behaviors applied, etc.)"
    )
    sequence_number: int = Field(default=0, description="Record sequence number")


class GeneratedDataset(BaseModel):
    """A complete generated dataset."""

    dataset_type: DatasetType = Field(..., description="Type of dataset")
    records: list[GeneratedRecord] = Field(default_factory=list, description="Generated records")
    persona_name: str = Field(..., description="Source persona name")
    seed: int | None = Field(default=None, description="Random seed used")
    total_count: int = Field(default=0, description="Total number of records")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Dataset metadata")

    def __iter__(self) -> Iterator[GeneratedRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)


class Generator(ABC):
    """Abstract base class for all dataset generators.

    Generators transform persona behaviors into concrete data shapes
    without redefining the behaviors themselves.
    """

    dataset_type: DatasetType

    def __init__(self, seed: int | None = None):
        """Initialize the generator.

        Args:
            seed: Optional random seed for deterministic generation
        """
        self._seed = seed
        self._rng = random.Random(seed)

    @property
    def seed(self) -> int | None:
        return self._seed

    @seed.setter
    def seed(self, value: int | None) -> None:
        self._seed = value
        self._rng = random.Random(value)

    @abstractmethod
    def generate(
        self,
        persona: Persona,
        count: int = 1,
        **options: Any,
    ) -> GeneratedDataset:
        """Generate a dataset from a persona.

        Args:
            persona: The persona defining behaviors
            count: Number of records to generate
            **options: Generator-specific options

        Returns:
            A GeneratedDataset containing the generated records
        """
        pass

    def generate_single(
        self,
        persona: Persona,
        sequence_number: int = 0,
        **options: Any,
    ) -> GeneratedRecord:
        """Generate a single record from a persona.

        Args:
            persona: The persona defining behaviors
            sequence_number: The sequence number for this record
            **options: Generator-specific options

        Returns:
            A single GeneratedRecord
        """
        dataset = self.generate(persona, count=1, **options)
        if dataset.records:
            record = dataset.records[0]
            record.sequence_number = sequence_number
            return record
        return GeneratedRecord(data={}, sequence_number=sequence_number)

    def stream(
        self,
        persona: Persona,
        count: int | None = None,
        **options: Any,
    ) -> Iterator[GeneratedRecord]:
        """Stream records one at a time.

        Args:
            persona: The persona defining behaviors
            count: Optional limit on records (None for infinite)
            **options: Generator-specific options

        Yields:
            Generated records one at a time
        """
        sequence = 0
        while count is None or sequence < count:
            yield self.generate_single(persona, sequence_number=sequence, **options)
            sequence += 1

    def _apply_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Apply a single behavior to data.

        Args:
            data: The input data
            behavior: The behavior to apply

        Returns:
            Modified data with behavior applied
        """
        if self._rng.random() > behavior.weight:
            return data

        return self._transform_for_behavior(data, behavior)

    def _transform_for_behavior(
        self,
        data: dict[str, Any],
        behavior: Behavior,
    ) -> dict[str, Any]:
        """Transform data according to a behavior.

        Override in subclasses for behavior-specific transformations.

        Args:
            data: The input data
            behavior: The behavior to apply

        Returns:
            Transformed data
        """
        return data

    def _apply_all_behaviors(
        self,
        data: dict[str, Any],
        persona: Persona,
    ) -> tuple[dict[str, Any], list[str]]:
        """Apply all persona behaviors to data.

        Args:
            data: The input data
            persona: The persona with behaviors to apply

        Returns:
            Tuple of (transformed data, list of applied behavior names)
        """
        applied_behaviors = []
        for behavior in persona.behaviors:
            original = data.copy()
            data = self._apply_behavior(data, behavior)
            if data != original:
                applied_behaviors.append(behavior.name)
        return data, applied_behaviors

    def _generate_base_data(self, **options: Any) -> dict[str, Any]:
        """Generate base data before applying behaviors.

        Override in subclasses to provide format-specific base data.

        Args:
            **options: Generator-specific options

        Returns:
            Base data dictionary
        """
        return {}
