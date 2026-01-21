"""Generator Registry for managing available generators."""

from typing import Type
from persona_platform.generators.base import Generator, DatasetType


class GeneratorRegistry:
    """Registry for dataset generators.

    Manages the available generators and provides factory methods
    for creating generator instances.
    """

    def __init__(self):
        self._generators: dict[DatasetType, Type[Generator]] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register the default generators."""
        from persona_platform.generators.api_generator import APIGenerator
        from persona_platform.generators.streaming_generator import StreamingGenerator
        from persona_platform.generators.file_generator import FileGenerator
        from persona_platform.generators.load_generator import LoadGenerator
        from persona_platform.generators.fault_generator import FaultGenerator

        self.register(DatasetType.API, APIGenerator)
        self.register(DatasetType.STREAMING, StreamingGenerator)
        self.register(DatasetType.FILE, FileGenerator)
        self.register(DatasetType.LOAD, LoadGenerator)
        self.register(DatasetType.FAULT, FaultGenerator)

    def register(self, dataset_type: DatasetType, generator_class: Type[Generator]) -> None:
        """Register a generator for a dataset type.

        Args:
            dataset_type: The type of dataset this generator produces
            generator_class: The generator class to register
        """
        self._generators[dataset_type] = generator_class

    def get(self, dataset_type: DatasetType | str) -> Type[Generator] | None:
        """Get a generator class by dataset type.

        Args:
            dataset_type: The dataset type (can be string or enum)

        Returns:
            The generator class or None if not found
        """
        if isinstance(dataset_type, str):
            try:
                dataset_type = DatasetType(dataset_type)
            except ValueError:
                return None

        return self._generators.get(dataset_type)

    def create(
        self,
        dataset_type: DatasetType | str,
        seed: int | None = None,
    ) -> Generator | None:
        """Create a generator instance.

        Args:
            dataset_type: The type of generator to create
            seed: Optional random seed

        Returns:
            A generator instance or None if type not found
        """
        generator_class = self.get(dataset_type)
        if generator_class is None:
            return None
        return generator_class(seed=seed)

    def list_types(self) -> list[DatasetType]:
        """List all registered dataset types."""
        return list(self._generators.keys())

    def unregister(self, dataset_type: DatasetType) -> bool:
        """Remove a generator from the registry.

        Args:
            dataset_type: The dataset type to remove

        Returns:
            True if removed, False if not found
        """
        if dataset_type in self._generators:
            del self._generators[dataset_type]
            return True
        return False

    def __contains__(self, dataset_type: DatasetType | str) -> bool:
        """Check if a dataset type is registered."""
        if isinstance(dataset_type, str):
            try:
                dataset_type = DatasetType(dataset_type)
            except ValueError:
                return False
        return dataset_type in self._generators


_global_registry: GeneratorRegistry | None = None


def get_global_generator_registry() -> GeneratorRegistry:
    """Get the global generator registry singleton."""
    global _global_registry
    if _global_registry is None:
        _global_registry = GeneratorRegistry()
    return _global_registry
