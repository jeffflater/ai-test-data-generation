"""Utility helper functions."""

import hashlib
import random
from typing import Any


def generate_seed() -> int:
    """Generate a random seed value."""
    return random.randint(0, 2**31 - 1)


def deterministic_hash(value: str, max_value: int = 2**31 - 1) -> int:
    """Generate a deterministic hash from a string.

    Args:
        value: String to hash
        max_value: Maximum hash value

    Returns:
        Integer hash value
    """
    hash_bytes = hashlib.sha256(value.encode()).digest()
    hash_int = int.from_bytes(hash_bytes[:8], byteorder="big")
    return hash_int % max_value


def merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries.

    Values in override take precedence over base.

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value

    return result


def flatten_dict(
    d: dict[str, Any],
    parent_key: str = "",
    separator: str = ".",
) -> dict[str, Any]:
    """Flatten a nested dictionary.

    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix
        separator: Key separator

    Returns:
        Flattened dictionary
    """
    items: list[tuple[str, Any]] = []

    for key, value in d.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))

    return dict(items)


def unflatten_dict(
    d: dict[str, Any],
    separator: str = ".",
) -> dict[str, Any]:
    """Unflatten a flattened dictionary.

    Args:
        d: Flattened dictionary
        separator: Key separator

    Returns:
        Nested dictionary
    """
    result: dict[str, Any] = {}

    for key, value in d.items():
        parts = key.split(separator)
        current = result

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    return result


def chunk_list(lst: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split a list into chunks of specified size.

    Args:
        lst: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
