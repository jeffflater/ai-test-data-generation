"""Base classes for Profiles - the Binding Layer.

Profiles are project-specific configurations that declare intent without
containing generation logic or behavioral definitions.
"""

from typing import Any
from pathlib import Path
from enum import Enum

from pydantic import BaseModel, Field

from persona_platform.generators.base import DatasetType


class OutputFormat(str, Enum):
    """Supported output formats."""

    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"
    PARQUET = "parquet"
    XML = "xml"
    YAML = "yaml"


class OutputConfig(BaseModel):
    """Configuration for output generation."""

    format: OutputFormat = Field(default=OutputFormat.JSON, description="Output format")
    directory: str = Field(default="./output", description="Output directory")
    filename_pattern: str = Field(
        default="{persona}_{dataset_type}_{timestamp}",
        description="Pattern for output filenames"
    )
    compress: bool = Field(default=False, description="Whether to compress output")
    pretty_print: bool = Field(default=True, description="Pretty print JSON/XML output")
    include_metadata: bool = Field(
        default=True,
        description="Include generation metadata in output"
    )


class DatasetConfig(BaseModel):
    """Configuration for a specific dataset type."""

    dataset_type: DatasetType = Field(..., description="Type of dataset to generate")
    count: int = Field(default=100, ge=1, description="Number of records to generate")
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Generator-specific options"
    )
    enabled: bool = Field(default=True, description="Whether this dataset is enabled")


class Profile(BaseModel):
    """Project-specific configuration for dataset generation.

    A Profile declares:
    - Which personas to use
    - What dataset types to generate
    - Scale and volume settings
    - Randomization seeds

    It contains NO generation logic or behavioral definitions.
    """

    name: str = Field(..., description="Profile name")
    description: str = Field(default="", description="Profile description")
    version: str = Field(default="1.0.0", description="Profile version")

    personas: list[str] = Field(
        default_factory=list,
        description="List of persona names to use"
    )

    datasets: list[DatasetConfig] = Field(
        default_factory=list,
        description="Dataset configurations"
    )

    seed: int | None = Field(
        default=None,
        description="Global random seed for reproducibility"
    )

    output: OutputConfig = Field(
        default_factory=OutputConfig,
        description="Output configuration"
    )

    variables: dict[str, Any] = Field(
        default_factory=dict,
        description="Custom variables for template substitution"
    )

    tags: list[str] = Field(
        default_factory=list,
        description="Tags for categorization"
    )

    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )

    def get_enabled_datasets(self) -> list[DatasetConfig]:
        """Get only enabled dataset configurations."""
        return [d for d in self.datasets if d.enabled]

    def get_dataset_config(self, dataset_type: DatasetType) -> DatasetConfig | None:
        """Get configuration for a specific dataset type."""
        for config in self.datasets:
            if config.dataset_type == dataset_type:
                return config
        return None

    def add_persona(self, persona_name: str) -> None:
        """Add a persona to the profile."""
        if persona_name not in self.personas:
            self.personas.append(persona_name)

    def remove_persona(self, persona_name: str) -> bool:
        """Remove a persona from the profile."""
        if persona_name in self.personas:
            self.personas.remove(persona_name)
            return True
        return False

    def add_dataset(self, config: DatasetConfig) -> None:
        """Add a dataset configuration."""
        self.datasets.append(config)

    def resolve_variables(self, text: str) -> str:
        """Resolve variable placeholders in text.

        Variables are referenced as ${variable_name}.
        """
        result = text
        for key, value in self.variables.items():
            result = result.replace(f"${{{key}}}", str(value))
        return result


class ProfileBuilder:
    """Fluent builder for creating Profiles."""

    def __init__(self, name: str):
        self._name = name
        self._description = ""
        self._version = "1.0.0"
        self._personas: list[str] = []
        self._datasets: list[DatasetConfig] = []
        self._seed: int | None = None
        self._output = OutputConfig()
        self._variables: dict[str, Any] = {}
        self._tags: list[str] = []
        self._metadata: dict[str, Any] = {}

    def description(self, description: str) -> "ProfileBuilder":
        self._description = description
        return self

    def version(self, version: str) -> "ProfileBuilder":
        self._version = version
        return self

    def persona(self, *names: str) -> "ProfileBuilder":
        self._personas.extend(names)
        return self

    def dataset(
        self,
        dataset_type: DatasetType | str,
        count: int = 100,
        **options: Any,
    ) -> "ProfileBuilder":
        if isinstance(dataset_type, str):
            dataset_type = DatasetType(dataset_type)

        self._datasets.append(
            DatasetConfig(
                dataset_type=dataset_type,
                count=count,
                options=options,
            )
        )
        return self

    def seed(self, seed: int) -> "ProfileBuilder":
        self._seed = seed
        return self

    def output_format(self, format: OutputFormat | str) -> "ProfileBuilder":
        if isinstance(format, str):
            format = OutputFormat(format)
        self._output.format = format
        return self

    def output_directory(self, directory: str) -> "ProfileBuilder":
        self._output.directory = directory
        return self

    def variable(self, key: str, value: Any) -> "ProfileBuilder":
        self._variables[key] = value
        return self

    def tag(self, *tags: str) -> "ProfileBuilder":
        self._tags.extend(tags)
        return self

    def metadata(self, **kwargs: Any) -> "ProfileBuilder":
        self._metadata.update(kwargs)
        return self

    def build(self) -> Profile:
        return Profile(
            name=self._name,
            description=self._description,
            version=self._version,
            personas=self._personas,
            datasets=self._datasets,
            seed=self._seed,
            output=self._output,
            variables=self._variables,
            tags=self._tags,
            metadata=self._metadata,
        )
