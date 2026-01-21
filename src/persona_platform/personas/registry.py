"""Persona Registry for managing and accessing personas."""

from typing import Iterator
from persona_platform.personas.base import Persona


class PersonaRegistry:
    """Central registry for managing personas.

    The registry ensures:
    - No duplicate personas
    - Proper inheritance resolution
    - Version management
    """

    def __init__(self):
        self._personas: dict[str, dict[str, Persona]] = {}

    def register(self, persona: Persona) -> None:
        """Register a persona in the registry.

        Args:
            persona: The persona to register

        Raises:
            ValueError: If a persona with the same name and version already exists
        """
        if persona.name not in self._personas:
            self._personas[persona.name] = {}

        if persona.version in self._personas[persona.name]:
            raise ValueError(
                f"Persona '{persona.name}' version '{persona.version}' already exists"
            )

        self._personas[persona.name][persona.version] = persona

    def get(self, name: str, version: str | None = None) -> Persona | None:
        """Get a persona by name and optionally version.

        Args:
            name: The persona name
            version: Optional version (returns latest if not specified)

        Returns:
            The persona or None if not found
        """
        if name not in self._personas:
            return None

        versions = self._personas[name]
        if version:
            return versions.get(version)

        if not versions:
            return None
        latest_version = max(versions.keys())
        return versions[latest_version]

    def get_resolved(self, name: str, version: str | None = None) -> Persona | None:
        """Get a persona with all inherited behaviors resolved.

        Args:
            name: The persona name
            version: Optional version

        Returns:
            The fully resolved persona or None if not found
        """
        persona = self.get(name, version)
        if persona is None:
            return None

        if not persona.inherits_from:
            return persona

        resolved = persona.model_copy(deep=True)
        for parent_name in persona.inherits_from:
            parent = self.get_resolved(parent_name)
            if parent:
                resolved = resolved.merge_with(parent)

        resolved.inherits_from = []
        return resolved

    def list_all(self) -> list[str]:
        """List all registered persona names."""
        return list(self._personas.keys())

    def list_versions(self, name: str) -> list[str]:
        """List all versions of a persona."""
        if name not in self._personas:
            return []
        return list(self._personas[name].keys())

    def unregister(self, name: str, version: str | None = None) -> bool:
        """Remove a persona from the registry.

        Args:
            name: The persona name
            version: Optional version (removes all versions if not specified)

        Returns:
            True if something was removed, False otherwise
        """
        if name not in self._personas:
            return False

        if version:
            if version in self._personas[name]:
                del self._personas[name][version]
                if not self._personas[name]:
                    del self._personas[name]
                return True
            return False

        del self._personas[name]
        return True

    def clear(self) -> None:
        """Clear all personas from the registry."""
        self._personas.clear()

    def __len__(self) -> int:
        """Return the total number of persona versions."""
        return sum(len(versions) for versions in self._personas.values())

    def __iter__(self) -> Iterator[Persona]:
        """Iterate over all personas (latest versions only)."""
        for name in self._personas:
            persona = self.get(name)
            if persona:
                yield persona

    def __contains__(self, name: str) -> bool:
        """Check if a persona name exists in the registry."""
        return name in self._personas


_global_registry: PersonaRegistry | None = None


def get_global_registry() -> PersonaRegistry:
    """Get the global persona registry singleton."""
    global _global_registry
    if _global_registry is None:
        _global_registry = PersonaRegistry()
    return _global_registry
