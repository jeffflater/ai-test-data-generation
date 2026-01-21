"""Profile Loader for loading profiles from YAML files."""

from pathlib import Path
from typing import Any

import yaml

from persona_platform.profiles.base import (
    Profile,
    DatasetConfig,
    OutputConfig,
    OutputFormat,
)
from persona_platform.generators.base import DatasetType


class ProfileLoader:
    """Loads profiles from YAML files."""

    def load_file(self, path: Path | str) -> Profile:
        """Load a profile from a YAML file.

        Args:
            path: Path to the YAML file

        Returns:
            Loaded Profile instance
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Profile file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        return self._parse_profile(data)

    def load_from_string(self, content: str) -> Profile:
        """Load a profile from a YAML string.

        Args:
            content: YAML content as string

        Returns:
            Loaded Profile instance
        """
        data = yaml.safe_load(content)
        return self._parse_profile(data)

    def _parse_profile(self, data: dict[str, Any]) -> Profile:
        """Parse profile data from YAML structure."""
        datasets = []
        for d_data in data.get("datasets", []):
            dataset_type_str = d_data.get("type", d_data.get("dataset_type", "api"))
            try:
                dataset_type = DatasetType(dataset_type_str)
            except ValueError:
                dataset_type = DatasetType.API

            datasets.append(
                DatasetConfig(
                    dataset_type=dataset_type,
                    count=d_data.get("count", 100),
                    options=d_data.get("options", {}),
                    enabled=d_data.get("enabled", True),
                )
            )

        output_data = data.get("output", {})
        output_format_str = output_data.get("format", "json")
        try:
            output_format = OutputFormat(output_format_str)
        except ValueError:
            output_format = OutputFormat.JSON

        output = OutputConfig(
            format=output_format,
            directory=output_data.get("directory", "./output"),
            filename_pattern=output_data.get(
                "filename_pattern", "{persona}_{dataset_type}_{timestamp}"
            ),
            compress=output_data.get("compress", False),
            pretty_print=output_data.get("pretty_print", True),
            include_metadata=output_data.get("include_metadata", True),
        )

        return Profile(
            name=data["name"],
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            personas=data.get("personas", []),
            datasets=datasets,
            seed=data.get("seed"),
            output=output,
            variables=data.get("variables", {}),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )

    def save_file(self, profile: Profile, path: Path | str) -> None:
        """Save a profile to a YAML file.

        Args:
            profile: The profile to save
            path: Path for the output file
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = self._profile_to_dict(profile)

        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def _profile_to_dict(self, profile: Profile) -> dict[str, Any]:
        """Convert a Profile to a dictionary for YAML serialization."""
        datasets = []
        for d in profile.datasets:
            datasets.append({
                "type": d.dataset_type.value,
                "count": d.count,
                "options": d.options,
                "enabled": d.enabled,
            })

        return {
            "name": profile.name,
            "description": profile.description,
            "version": profile.version,
            "personas": profile.personas,
            "datasets": datasets,
            "seed": profile.seed,
            "output": {
                "format": profile.output.format.value,
                "directory": profile.output.directory,
                "filename_pattern": profile.output.filename_pattern,
                "compress": profile.output.compress,
                "pretty_print": profile.output.pretty_print,
                "include_metadata": profile.output.include_metadata,
            },
            "variables": profile.variables,
            "tags": profile.tags,
            "metadata": profile.metadata,
        }


def load_profile(path: Path | str) -> Profile:
    """Convenience function to load a profile from a file.

    Args:
        path: Path to the YAML file

    Returns:
        Loaded Profile instance
    """
    loader = ProfileLoader()
    return loader.load_file(path)
