"""SQL DDL schema parser.

Parses CREATE TABLE statements from SQL DDL files.
Supports common SQL dialects (PostgreSQL, MySQL, SQLite, SQL Server).
"""

import re
from pathlib import Path
from typing import Any

from persona_platform.schemas.base import FieldSchema, SchemaDefinition, SchemaFormat


class DDLParser:
    """Parser for SQL DDL CREATE TABLE statements."""

    # SQL type to internal type mapping
    TYPE_MAPPING = {
        # Integers
        "int": "integer",
        "integer": "integer",
        "smallint": "integer",
        "tinyint": "integer",
        "bigint": "integer",
        "serial": "integer",
        "bigserial": "integer",
        "smallserial": "integer",
        # Floats
        "float": "float",
        "real": "float",
        "double": "float",
        "double precision": "float",
        "decimal": "float",
        "numeric": "float",
        "money": "float",
        # Strings
        "char": "string",
        "character": "string",
        "varchar": "string",
        "character varying": "string",
        "text": "text",
        "clob": "text",
        "nchar": "string",
        "nvarchar": "string",
        "ntext": "text",
        # Boolean
        "boolean": "boolean",
        "bool": "boolean",
        "bit": "boolean",
        # Date/Time
        "date": "date",
        "time": "time",
        "datetime": "datetime",
        "datetime2": "datetime",
        "timestamp": "datetime",
        "timestamptz": "datetime",
        "timestamp with time zone": "datetime",
        "timestamp without time zone": "datetime",
        # Binary
        "binary": "string",
        "varbinary": "string",
        "blob": "string",
        "bytea": "string",
        # UUID
        "uuid": "uuid",
        "uniqueidentifier": "uuid",
        # JSON
        "json": "object",
        "jsonb": "object",
        # Arrays (PostgreSQL)
        "array": "array",
    }

    # Format hints based on column names
    NAME_FORMAT_HINTS = {
        "email": "email",
        "phone": "phone",
        "url": "url",
        "website": "url",
        "address": "address",
        "city": "city",
        "country": "country",
        "zip": "string",
        "postal": "string",
    }

    def __init__(self, ddl_content: str):
        """Initialize parser with DDL content.

        Args:
            ddl_content: SQL DDL content as string
        """
        self.ddl_content = ddl_content
        self._tables = self._parse_all_tables()

    @classmethod
    def from_file(cls, path: Path | str) -> "DDLParser":
        """Load parser from a file.

        Args:
            path: Path to SQL DDL file

        Returns:
            Initialized parser
        """
        path = Path(path)
        content = path.read_text()
        return cls(content)

    @classmethod
    def from_string(cls, content: str) -> "DDLParser":
        """Load parser from a string.

        Args:
            content: SQL DDL as string

        Returns:
            Initialized parser
        """
        return cls(content)

    def _parse_all_tables(self) -> dict[str, dict[str, Any]]:
        """Parse all CREATE TABLE statements from the DDL.

        Returns:
            Dict of table name to table info
        """
        tables = {}

        # Pattern to find CREATE TABLE start
        # Handles: CREATE TABLE [IF NOT EXISTS] [schema.]table_name
        start_pattern = re.compile(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
            r'(?:`?\"?(\w+)\"?`?\.)?'  # Optional schema
            r'`?\"?(\w+)\"?`?'          # Table name
            r'\s*\(',
            re.IGNORECASE
        )

        for match in start_pattern.finditer(self.ddl_content):
            schema_name = match.group(1)
            table_name = match.group(2)
            start_pos = match.end()

            # Find matching closing parenthesis (handling nested parens)
            columns_str = self._extract_balanced_parens(self.ddl_content, start_pos)

            if columns_str:
                columns = self._parse_columns(columns_str)

                tables[table_name] = {
                    "schema": schema_name,
                    "columns": columns,
                    "raw": self.ddl_content[match.start():start_pos + len(columns_str) + 1],
                }

        return tables

    def _extract_balanced_parens(self, content: str, start_pos: int) -> str | None:
        """Extract content up to the matching closing parenthesis.

        Args:
            content: Full DDL content
            start_pos: Position after the opening parenthesis

        Returns:
            Content between parens or None if not found
        """
        depth = 1
        pos = start_pos

        while depth > 0 and pos < len(content):
            char = content[pos]
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            pos += 1

        if depth == 0:
            return content[start_pos:pos - 1]
        return None

    def _parse_columns(self, columns_str: str) -> list[dict[str, Any]]:
        """Parse column definitions from the column string.

        Args:
            columns_str: Content between parentheses in CREATE TABLE

        Returns:
            List of column definitions
        """
        columns = []

        # Split by commas, but not commas inside parentheses
        parts = self._split_columns(columns_str)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Skip constraint definitions
            if self._is_constraint(part):
                continue

            column = self._parse_column(part)
            if column:
                columns.append(column)

        return columns

    def _split_columns(self, columns_str: str) -> list[str]:
        """Split column definitions, handling nested parentheses.

        Args:
            columns_str: Columns string to split

        Returns:
            List of column definition strings
        """
        parts = []
        current = []
        depth = 0

        for char in columns_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current))

        return parts

    def _is_constraint(self, part: str) -> bool:
        """Check if a part is a constraint definition (not a column).

        Args:
            part: Column definition string

        Returns:
            True if it's a constraint
        """
        constraint_keywords = [
            "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK",
            "CONSTRAINT", "INDEX", "KEY"
        ]
        upper_part = part.upper().strip()
        return any(upper_part.startswith(kw) for kw in constraint_keywords)

    def _parse_column(self, column_str: str) -> dict[str, Any] | None:
        """Parse a single column definition.

        Args:
            column_str: Column definition string

        Returns:
            Column info dict or None if not parseable
        """
        # Pattern to match column: name type[(size)] [constraints...]
        pattern = re.compile(
            r'`?\"?(\w+)\"?`?\s+'  # Column name
            r'(\w+(?:\s+\w+)?)'     # Type (may be two words like "double precision")
            r'(?:\s*\(([^)]+)\))?'  # Optional size/precision
            r'(.*)',                # Remaining constraints
            re.IGNORECASE
        )

        match = pattern.match(column_str.strip())
        if not match:
            return None

        name = match.group(1)
        sql_type = match.group(2).lower()
        size_str = match.group(3)
        constraints_str = match.group(4) or ""
        constraints_upper = constraints_str.upper()

        # Determine internal type
        internal_type = self._map_type(sql_type)

        # Check for array syntax (PostgreSQL)
        if "[]" in column_str or sql_type == "array":
            internal_type = "array"

        # Parse constraints
        nullable = "NOT NULL" not in constraints_upper
        required = "NOT NULL" in constraints_upper
        is_primary_key = "PRIMARY KEY" in constraints_upper
        has_default = "DEFAULT" in constraints_upper

        if is_primary_key:
            required = True
            nullable = False

        # Extract size constraints
        min_length = None
        max_length = None
        if size_str and internal_type == "string":
            try:
                max_length = int(size_str.split(",")[0].strip())
            except ValueError:
                pass

        # Try to detect format from column name
        format_hint = None
        name_lower = name.lower()
        for hint_name, hint_format in self.NAME_FORMAT_HINTS.items():
            if hint_name in name_lower:
                format_hint = hint_format
                break

        # UUID detection
        if internal_type == "string" and ("uuid" in name_lower or "guid" in name_lower):
            format_hint = "uuid"
        if sql_type == "uuid":
            format_hint = "uuid"

        # Email detection
        if internal_type == "string" and "email" in name_lower:
            format_hint = "email"

        # Extract default value
        default_value = None
        if has_default:
            default_match = re.search(r"DEFAULT\s+([^\s,]+)", constraints_str, re.IGNORECASE)
            if default_match:
                default_value = default_match.group(1).strip("'\"")

        # Extract enum values from CHECK constraint
        enum_values = None
        check_match = re.search(r"CHECK\s*\([^)]*IN\s*\(([^)]+)\)", constraints_str, re.IGNORECASE)
        if check_match:
            enum_str = check_match.group(1)
            enum_values = [v.strip().strip("'\"") for v in enum_str.split(",")]

        return {
            "name": name,
            "type": internal_type,
            "sql_type": sql_type,
            "format": format_hint,
            "required": required,
            "nullable": nullable,
            "primary_key": is_primary_key,
            "min_length": min_length,
            "max_length": max_length,
            "default": default_value,
            "enum": enum_values,
        }

    def _map_type(self, sql_type: str) -> str:
        """Map SQL type to internal type.

        Args:
            sql_type: SQL type name

        Returns:
            Internal type string
        """
        sql_type = sql_type.lower().strip()
        return self.TYPE_MAPPING.get(sql_type, "string")

    def list_tables(self) -> list[str]:
        """List all table names found in the DDL.

        Returns:
            List of table names
        """
        return list(self._tables.keys())

    def parse_table(
        self,
        table_name: str,
        source_file: str | None = None,
    ) -> SchemaDefinition:
        """Parse a specific table by name.

        Args:
            table_name: Name of the table to parse
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if table_name not in self._tables:
            available = ", ".join(self._tables.keys())
            raise ValueError(f"Table '{table_name}' not found. Available: {available}")

        table_info = self._tables[table_name]
        columns = table_info["columns"]

        fields = []
        for col in columns:
            field = FieldSchema(
                name=col["name"],
                type=col["type"],
                format=col.get("format"),
                required=col.get("required", False),
                nullable=col.get("nullable", True),
                enum=col.get("enum"),
                min_length=col.get("min_length"),
                max_length=col.get("max_length"),
                default=col.get("default"),
            )
            fields.append(field)

        return SchemaDefinition(
            name=table_name,
            description=None,
            fields=fields,
            source_format=SchemaFormat.DDL,
            source_file=source_file,
            source_entity=table_name,
            metadata={
                "schema": table_info.get("schema"),
                "primary_keys": [c["name"] for c in columns if c.get("primary_key")],
            },
        )

    def parse(self, source_file: str | None = None) -> SchemaDefinition:
        """Parse the first table found in the DDL.

        Args:
            source_file: Optional source file path

        Returns:
            Parsed SchemaDefinition
        """
        if not self._tables:
            raise ValueError("No tables found in DDL")

        first_table = next(iter(self._tables.keys()))
        return self.parse_table(first_table, source_file)
