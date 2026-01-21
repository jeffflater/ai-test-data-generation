"""Persona Engine - Runtime engine for applying behavioral rules.

The Persona Engine orchestrates the generation process by:
- Resolving persona inheritance
- Coordinating generators
- Applying behaviors consistently
"""

from typing import Any, Iterator
from datetime import datetime, timezone
from pathlib import Path
import json

from persona_platform.personas.base import Persona
from persona_platform.personas.registry import PersonaRegistry, get_global_registry
from persona_platform.generators.base import Generator, DatasetType, GeneratedDataset
from persona_platform.generators.registry import GeneratorRegistry, get_global_generator_registry
from persona_platform.profiles.base import Profile, DatasetConfig


class GenerationResult:
    """Result of a generation run."""

    def __init__(
        self,
        profile: Profile,
        datasets: dict[str, GeneratedDataset],
        start_time: datetime,
        end_time: datetime,
    ):
        self.profile = profile
        self.datasets = datasets
        self.start_time = start_time
        self.end_time = end_time

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    @property
    def total_records(self) -> int:
        return sum(len(ds) for ds in self.datasets.values())

    def get_dataset(self, key: str) -> GeneratedDataset | None:
        return self.datasets.get(key)

    def summary(self) -> dict[str, Any]:
        return {
            "profile": self.profile.name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "total_records": self.total_records,
            "datasets": {
                key: {
                    "type": ds.dataset_type.value,
                    "count": len(ds),
                    "persona": ds.persona_name,
                }
                for key, ds in self.datasets.items()
            },
        }


class PersonaEngine:
    """Engine for orchestrating dataset generation.

    The PersonaEngine:
    - Resolves persona inheritance
    - Coordinates generators with personas
    - Applies behaviors consistently
    - Manages the generation lifecycle
    """

    def __init__(
        self,
        persona_registry: PersonaRegistry | None = None,
        generator_registry: GeneratorRegistry | None = None,
    ):
        self.persona_registry = persona_registry or get_global_registry()
        self.generator_registry = generator_registry or get_global_generator_registry()

    def generate(self, profile: Profile) -> GenerationResult:
        """Generate all datasets defined in a profile.

        Args:
            profile: The profile defining what to generate

        Returns:
            GenerationResult containing all generated datasets
        """
        start_time = datetime.now(timezone.utc)
        datasets: dict[str, GeneratedDataset] = {}

        resolved_personas = self._resolve_personas(profile.personas)

        for dataset_config in profile.get_enabled_datasets():
            generator = self.generator_registry.create(
                dataset_config.dataset_type,
                seed=profile.seed,
            )

            if generator is None:
                continue

            for persona in resolved_personas:
                key = f"{persona.name}_{dataset_config.dataset_type.value}"
                dataset = generator.generate(
                    persona=persona,
                    count=dataset_config.count,
                    **dataset_config.options,
                )
                datasets[key] = dataset

        end_time = datetime.now(timezone.utc)

        return GenerationResult(
            profile=profile,
            datasets=datasets,
            start_time=start_time,
            end_time=end_time,
        )

    def generate_single(
        self,
        persona_name: str,
        dataset_type: DatasetType | str,
        count: int = 1,
        seed: int | None = None,
        **options: Any,
    ) -> GeneratedDataset | None:
        """Generate a single dataset for one persona.

        Args:
            persona_name: Name of the persona to use
            dataset_type: Type of dataset to generate
            count: Number of records
            seed: Optional random seed
            **options: Generator-specific options

        Returns:
            Generated dataset or None if persona/generator not found
        """
        persona = self.persona_registry.get_resolved(persona_name)
        if persona is None:
            return None

        if isinstance(dataset_type, str):
            dataset_type = DatasetType(dataset_type)

        generator = self.generator_registry.create(dataset_type, seed=seed)
        if generator is None:
            return None

        return generator.generate(persona, count=count, **options)

    def stream(
        self,
        persona_name: str,
        dataset_type: DatasetType | str,
        count: int | None = None,
        seed: int | None = None,
        **options: Any,
    ) -> Iterator[dict[str, Any]] | None:
        """Stream records one at a time.

        Args:
            persona_name: Name of the persona to use
            dataset_type: Type of dataset to generate
            count: Optional limit on records
            seed: Optional random seed
            **options: Generator-specific options

        Yields:
            Generated records as dictionaries
        """
        persona = self.persona_registry.get_resolved(persona_name)
        if persona is None:
            return None

        if isinstance(dataset_type, str):
            dataset_type = DatasetType(dataset_type)

        generator = self.generator_registry.create(dataset_type, seed=seed)
        if generator is None:
            return None

        for record in generator.stream(persona, count=count, **options):
            yield record.data

    def _resolve_personas(self, persona_names: list[str]) -> list[Persona]:
        """Resolve all personas with inheritance."""
        resolved = []
        for name in persona_names:
            persona = self.persona_registry.get_resolved(name)
            if persona:
                resolved.append(persona)
        return resolved

    def register_persona(self, persona: Persona) -> None:
        """Register a persona with the engine."""
        self.persona_registry.register(persona)

    def register_generator(
        self,
        dataset_type: DatasetType,
        generator_class: type[Generator],
    ) -> None:
        """Register a custom generator with the engine."""
        self.generator_registry.register(dataset_type, generator_class)

    def list_personas(self) -> list[str]:
        """List all available persona names."""
        return self.persona_registry.list_all()

    def list_generators(self) -> list[DatasetType]:
        """List all available generator types."""
        return self.generator_registry.list_types()

    def export_result(
        self,
        result: GenerationResult,
        output_dir: Path | str,
        format: str = "json",
    ) -> list[Path]:
        """Export generation results to files.

        Args:
            result: The generation result to export
            output_dir: Directory to write files to
            format: Output format (json, jsonl)

        Returns:
            List of created file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        created_files = []
        timestamp = result.start_time.strftime("%Y%m%d_%H%M%S")

        for key, dataset in result.datasets.items():
            filename = f"{key}_{timestamp}.{format}"
            filepath = output_dir / filename

            if format == "json":
                data = {
                    "metadata": {
                        "dataset_type": dataset.dataset_type.value,
                        "persona": dataset.persona_name,
                        "seed": dataset.seed,
                        "total_count": dataset.total_count,
                        "generated_at": timestamp,
                    },
                    "records": [r.data for r in dataset.records],
                }
                with open(filepath, "w") as f:
                    json.dump(data, f, indent=2, default=str)

            elif format == "jsonl":
                with open(filepath, "w") as f:
                    for record in dataset.records:
                        f.write(json.dumps(record.data, default=str) + "\n")

            created_files.append(filepath)

        summary_path = output_dir / f"summary_{timestamp}.json"
        with open(summary_path, "w") as f:
            json.dump(result.summary(), f, indent=2)
        created_files.append(summary_path)

        return created_files
