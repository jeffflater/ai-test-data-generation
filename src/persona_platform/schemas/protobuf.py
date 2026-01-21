"""Protobuf schema parser.

Parses Protocol Buffer (.proto) files.
"""

import re
from pathlib import Path
from typing import Any

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class ProtobufParser:
    """Parser for Protocol Buffer schemas."""

    # Protobuf scalar types to internal types
    SCALAR_TYPES = {
        "double": "float",
        "float": "float",
        "int32": "integer",
        "int64": "integer",
        "uint32": "integer",
        "uint64": "integer",
        "sint32": "integer",
        "sint64": "integer",
        "fixed32": "integer",
        "fixed64": "integer",
        "sfixed32": "integer",
        "sfixed64": "integer",
        "bool": "boolean",
        "string": "string",
        "bytes": "string",
    }

    def __init__(self, proto_content: str):
        """Initialize parser with .proto content.

        Args:
            proto_content: Protocol buffer content as string
        """
        self.proto_content = proto_content
        self._package = self._parse_package()
        # Parse enums first since messages may reference them
        self._enums = self._parse_all_enums()
        # Collect message names first (for forward references)
        self._message_names = self._collect_message_names()
        # Now parse messages (can reference other message names)
        self._messages = self._parse_all_messages()

    @classmethod
    def from_file(cls, path: Path | str) -> "ProtobufParser":
        """Load parser from a file.

        Args:
            path: Path to .proto file

        Returns:
            Initialized parser
        """
        path = Path(path)
        content = path.read_text()
        return cls(content)

    @classmethod
    def from_string(cls, content: str) -> "ProtobufParser":
        """Load parser from a string.

        Args:
            content: Protobuf schema as string

        Returns:
            Initialized parser
        """
        return cls(content)

    def _parse_package(self) -> str | None:
        """Parse the package name from the proto file.

        Returns:
            Package name or None
        """
        match = re.search(r'package\s+([\w.]+)\s*;', self.proto_content)
        return match.group(1) if match else None

    def _parse_all_enums(self) -> dict[str, list[str]]:
        """Parse all enum definitions.

        Returns:
            Dict of enum name to list of values
        """
        enums = {}

        # Pattern to match enum definitions
        enum_pattern = re.compile(
            r'enum\s+(\w+)\s*\{([^}]+)\}',
            re.MULTILINE | re.DOTALL
        )

        for match in enum_pattern.finditer(self.proto_content):
            enum_name = match.group(1)
            enum_body = match.group(2)

            # Parse enum values
            values = []
            value_pattern = re.compile(r'(\w+)\s*=\s*\d+')
            for value_match in value_pattern.finditer(enum_body):
                value_name = value_match.group(1)
                # Skip the default UNSPECIFIED value if present
                if not value_name.endswith("_UNSPECIFIED"):
                    values.append(value_name)

            enums[enum_name] = values

        return enums

    def _collect_message_names(self) -> set[str]:
        """Collect all message names (for forward reference resolution).

        Returns:
            Set of message names
        """
        # Remove comments first
        content = re.sub(r'//.*$', '', self.proto_content, flags=re.MULTILINE)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        # Find all message declarations
        message_pattern = re.compile(r'message\s+(\w+)\s*\{')
        return set(match.group(1) for match in message_pattern.finditer(content))

    def _parse_all_messages(self) -> dict[str, dict[str, Any]]:
        """Parse all message definitions.

        Returns:
            Dict of message name to message info
        """
        messages = {}

        # Remove comments first
        content = re.sub(r'//.*$', '', self.proto_content, flags=re.MULTILINE)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        # Pattern to match message definitions (handles nested braces)
        message_starts = list(re.finditer(r'message\s+(\w+)\s*\{', content))

        for start_match in message_starts:
            message_name = start_match.group(1)
            start_pos = start_match.end()

            # Find matching closing brace
            depth = 1
            pos = start_pos
            while depth > 0 and pos < len(content):
                if content[pos] == '{':
                    depth += 1
                elif content[pos] == '}':
                    depth -= 1
                pos += 1

            message_body = content[start_pos:pos - 1]
            fields = self._parse_fields(message_body)

            messages[message_name] = {
                "fields": fields,
                "body": message_body,
            }

        return messages

    def _parse_fields(self, message_body: str) -> list[dict[str, Any]]:
        """Parse fields from a message body.

        Args:
            message_body: Content between message braces

        Returns:
            List of field definitions
        """
        fields = []

        # Pattern to match field definitions
        # Handles: [repeated] type name = number [options];
        field_pattern = re.compile(
            r'(repeated\s+)?'           # Optional repeated
            r'(map<[\w,\s]+>|\w+)\s+'   # Type (including map)
            r'(\w+)\s*=\s*(\d+)'         # Name and field number
            r'\s*(?:\[(.*?)\])?'         # Optional field options
            r'\s*;',
            re.MULTILINE
        )

        for match in field_pattern.finditer(message_body):
            is_repeated = bool(match.group(1))
            field_type = match.group(2).strip()
            field_name = match.group(3)
            field_number = int(match.group(4))
            options = match.group(5)

            # Skip nested message/enum definitions
            if field_type in ("message", "enum"):
                continue

            field_info = self._parse_field_type(field_type, is_repeated)
            field_info["name"] = field_name
            field_info["number"] = field_number
            field_info["repeated"] = is_repeated

            # Parse options for deprecated, etc.
            if options:
                field_info["deprecated"] = "deprecated = true" in options.lower()

            fields.append(field_info)

        return fields

    def _parse_field_type(self, type_str: str, is_repeated: bool) -> dict[str, Any]:
        """Parse a field type string.

        Args:
            type_str: Type string from proto file
            is_repeated: Whether field is repeated

        Returns:
            Field info dict
        """
        # Handle map types
        if type_str.startswith("map<"):
            map_match = re.match(r'map<(\w+),\s*(\w+)>', type_str)
            if map_match:
                value_type = map_match.group(2)
                return {
                    "type": "object",
                    "format": "map",
                }

        # Handle scalar types
        if type_str in self.SCALAR_TYPES:
            internal_type = self.SCALAR_TYPES[type_str]
            if is_repeated:
                return {
                    "type": "array",
                    "items_type": internal_type,
                }
            return {"type": internal_type}

        # Handle enum references
        if type_str in self._enums:
            return {
                "type": "string",
                "enum": self._enums[type_str],
            }

        # Handle message references (nested objects)
        if type_str in self._message_names:
            if is_repeated:
                return {
                    "type": "array",
                    "items_type": "object",
                    "items_ref": type_str,
                }
            return {
                "type": "object",
                "ref": type_str,
            }

        # Unknown type - treat as string
        if is_repeated:
            return {
                "type": "array",
                "items_type": "string",
            }
        return {"type": "string"}

    def list_messages(self) -> list[str]:
        """List all message names found in the proto file.

        Returns:
            List of message names
        """
        return list(self._messages.keys())

    def list_enums(self) -> list[str]:
        """List all enum names found in the proto file.

        Returns:
            List of enum names
        """
        return list(self._enums.keys())

    def parse_message(
        self,
        message_name: str,
        source_file: str | None = None,
    ) -> SchemaDefinition:
        """Parse a specific message by name.

        Args:
            message_name: Name of the message to parse
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if message_name not in self._messages:
            available = ", ".join(self._messages.keys())
            raise ValueError(f"Message '{message_name}' not found. Available: {available}")

        message_info = self._messages[message_name]
        proto_fields = message_info["fields"]

        fields = []
        for pf in proto_fields:
            # Handle array types
            if pf["type"] == "array":
                items_type = pf.get("items_type", "string")
                items_field = FieldSchema(
                    name="item",
                    type=items_type,
                )
                field = FieldSchema(
                    name=pf["name"],
                    type="array",
                    items=items_field,
                    required=False,
                    nullable=True,
                )
            else:
                # Handle nested objects
                properties = None
                if pf["type"] == "object" and "ref" in pf:
                    ref_name = pf["ref"]
                    if ref_name in self._messages:
                        nested_schema = self.parse_message(ref_name, source_file)
                        properties = {f.name: f for f in nested_schema.fields}

                field = FieldSchema(
                    name=pf["name"],
                    type=pf["type"],
                    enum=pf.get("enum"),
                    properties=properties,
                    required=False,  # Protobuf 3 fields are optional by default
                    nullable=True,
                )

            fields.append(field)

        full_name = f"{self._package}.{message_name}" if self._package else message_name

        return SchemaDefinition(
            name=message_name,
            description=None,
            fields=fields,
            source_format=SchemaFormat.PROTOBUF,
            source_file=source_file,
            source_entity=full_name,
            metadata={
                "package": self._package,
                "enums": list(self._enums.keys()),
            },
        )

    def parse(self, source_file: str | None = None) -> SchemaDefinition:
        """Parse the first message found in the proto file.

        Args:
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if not self._messages:
            raise ValueError("No messages found in proto file")

        first_message = next(iter(self._messages.keys()))
        return self.parse_message(first_message, source_file)
