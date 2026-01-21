"""Generators module - The Shape Layer of the platform.

Generators convert persona behavior into concrete datasets supporting:
- API
- Streaming
- File-based
- Load/scale
- Mutation/fault injection
"""

from persona_platform.generators.base import Generator, DatasetType, GeneratedDataset
from persona_platform.generators.api_generator import APIGenerator
from persona_platform.generators.streaming_generator import StreamingGenerator
from persona_platform.generators.file_generator import FileGenerator
from persona_platform.generators.load_generator import LoadGenerator
from persona_platform.generators.fault_generator import FaultGenerator
from persona_platform.generators.registry import GeneratorRegistry

__all__ = [
    "Generator",
    "DatasetType",
    "GeneratedDataset",
    "APIGenerator",
    "StreamingGenerator",
    "FileGenerator",
    "LoadGenerator",
    "FaultGenerator",
    "GeneratorRegistry",
]
