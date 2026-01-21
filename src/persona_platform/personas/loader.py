"""Persona Loader for loading personas from YAML files."""

from pathlib import Path
from typing import Any

import yaml

from persona_platform.personas.base import Persona, Behavior, BehaviorType
from persona_platform.personas.registry import PersonaRegistry


class PersonaLoader:
    """Loads personas from YAML files into a registry."""

    def __init__(self, registry: PersonaRegistry | None = None):
        from persona_platform.personas.registry import get_global_registry

        self.registry = registry if registry is not None else get_global_registry()

    def load_file(self, path: Path | str) -> list[Persona]:
        """Load personas from a single YAML file.

        Args:
            path: Path to the YAML file

        Returns:
            List of loaded personas
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Persona file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        return self._parse_personas(data)

    def load_directory(self, directory: Path | str, pattern: str = "*.yaml") -> list[Persona]:
        """Load all persona files from a directory.

        Args:
            directory: Path to the directory
            pattern: Glob pattern for files (default: *.yaml)

        Returns:
            List of all loaded personas
        """
        directory = Path(directory)
        if not directory.is_dir():
            raise NotADirectoryError(f"Not a directory: {directory}")

        personas = []
        for file_path in directory.glob(pattern):
            personas.extend(self.load_file(file_path))

        for file_path in directory.glob("*.yml"):
            if not any(p.name == file_path.stem for p in personas):
                personas.extend(self.load_file(file_path))

        return personas

    def _parse_personas(self, data: dict[str, Any] | list[dict[str, Any]]) -> list[Persona]:
        """Parse persona data from YAML structure."""
        personas = []

        if isinstance(data, dict):
            if "personas" in data:
                items = data["personas"]
            else:
                items = [data]
        else:
            items = data

        for item in items:
            persona = self._parse_single_persona(item)
            self.registry.register(persona)
            personas.append(persona)

        return personas

    def _parse_single_persona(self, data: dict[str, Any]) -> Persona:
        """Parse a single persona from a dictionary."""
        behaviors = []
        for b_data in data.get("behaviors", []):
            behavior_type_str = b_data.get("type", b_data.get("behavior_type", "pattern"))
            try:
                behavior_type = BehaviorType(behavior_type_str)
            except ValueError:
                behavior_type = BehaviorType.PATTERN

            behaviors.append(
                Behavior(
                    name=b_data["name"],
                    behavior_type=behavior_type,
                    description=b_data.get("description", ""),
                    parameters=b_data.get("parameters", {}),
                    weight=b_data.get("weight", 1.0),
                )
            )

        return Persona(
            name=data["name"],
            version=data.get("version", "1.0.0"),
            description=data.get("description", ""),
            tags=data.get("tags", []),
            behaviors=behaviors,
            inherits_from=data.get("inherits_from", data.get("inherits", [])),
            metadata=data.get("metadata", {}),
        )


def load_personas(path: Path | str, registry: PersonaRegistry | None = None) -> list[Persona]:
    """Convenience function to load personas from a file or directory.

    Args:
        path: Path to a YAML file or directory
        registry: Optional registry (uses global if not provided)

    Returns:
        List of loaded personas
    """
    loader = PersonaLoader(registry)
    path = Path(path)

    if path.is_dir():
        return loader.load_directory(path)
    return loader.load_file(path)
