# Schema Import Format Specification

This document defines the canonical JSON format for importing schemas into the test data generator. Instead of maintaining parsers for every possible schema format (SQL DDL, Avro, Protobuf, etc.), users can convert their schemas to this standard format using ChatGPT or any LLM.

## Why This Approach?

- **Universal compatibility**: Convert from ANY format (SQL, Avro, Protobuf, GraphQL, Terraform, YAML configs, etc.)
- **Zero maintenance burden**: No need to track evolving external format specifications
- **LLM-friendly**: Schema translation is an ideal task for ChatGPT/Claude
- **Simple validation**: One format to validate instead of many

---

## Canonical Format

### Basic Structure

```json
{
  "name": "entity_name",
  "description": "Optional description of this entity",
  "fields": [
    {
      "name": "field_name",
      "type": "string",
      "format": "email",
      "required": true
    }
  ]
}
```

### Field Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | **Yes** | Field name |
| `type` | string | **Yes** | Base type: `string`, `integer`, `float`, `boolean`, `array`, `object` |
| `format` | string | No | Format hint for generation (see Format Values below) |
| `description` | string | No | Human-readable description |
| `required` | boolean | No | Whether field is required (default: `false`) |
| `nullable` | boolean | No | Whether field can be null (default: `true`) |
| `enum` | array | No | List of allowed values |
| `min_length` | integer | No | Minimum string length |
| `max_length` | integer | No | Maximum string length |
| `pattern` | string | No | Regex pattern for validation |
| `minimum` | number | No | Minimum numeric value |
| `maximum` | number | No | Maximum numeric value |
| `exclusive_minimum` | boolean | No | Whether minimum is exclusive (default: `false`) |
| `exclusive_maximum` | boolean | No | Whether maximum is exclusive (default: `false`) |
| `default` | any | No | Default value |
| `items` | object | No | Schema for array items (when type is `array`) |
| `min_items` | integer | No | Minimum array length |
| `max_items` | integer | No | Maximum array length |
| `unique_items` | boolean | No | Whether array items must be unique |
| `properties` | object | No | Nested field schemas (when type is `object`) |

### Format Values

The `format` field hints at what kind of data to generate:

| Format | Description | Example Output |
|--------|-------------|----------------|
| `uuid` | UUID v4 | `"550e8400-e29b-41d4-a716-446655440000"` |
| `email` | Email address | `"user@example.com"` |
| `date` | ISO date | `"2024-01-15"` |
| `datetime` | ISO datetime | `"2024-01-15T10:30:00Z"` |
| `time` | ISO time | `"10:30:00"` |
| `url` | URL | `"https://example.com/page"` |
| `hostname` | Hostname | `"api.example.com"` |
| `ipv4` | IPv4 address | `"192.168.1.1"` |
| `ipv6` | IPv6 address | `"2001:0db8:85a3::8a2e:0370:7334"` |
| `phone` | Phone number | `"+1-555-123-4567"` |

---

## Complete Examples

### Simple User Schema

```json
{
  "name": "users",
  "description": "User account information",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "format": "uuid",
      "required": true,
      "nullable": false
    },
    {
      "name": "email",
      "type": "string",
      "format": "email",
      "required": true,
      "max_length": 255
    },
    {
      "name": "username",
      "type": "string",
      "required": true,
      "min_length": 3,
      "max_length": 50,
      "pattern": "^[a-zA-Z0-9_]+$"
    },
    {
      "name": "age",
      "type": "integer",
      "minimum": 0,
      "maximum": 150,
      "nullable": true
    },
    {
      "name": "role",
      "type": "string",
      "enum": ["admin", "moderator", "user", "guest"],
      "default": "user"
    },
    {
      "name": "is_active",
      "type": "boolean",
      "default": true
    },
    {
      "name": "created_at",
      "type": "string",
      "format": "datetime",
      "required": true
    }
  ]
}
```

### Schema with Nested Objects

```json
{
  "name": "orders",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "format": "uuid",
      "required": true
    },
    {
      "name": "customer",
      "type": "object",
      "required": true,
      "properties": {
        "name": {
          "name": "name",
          "type": "string",
          "required": true
        },
        "email": {
          "name": "email",
          "type": "string",
          "format": "email",
          "required": true
        },
        "address": {
          "name": "address",
          "type": "object",
          "properties": {
            "street": {
              "name": "street",
              "type": "string"
            },
            "city": {
              "name": "city",
              "type": "string"
            },
            "country": {
              "name": "country",
              "type": "string"
            }
          }
        }
      }
    },
    {
      "name": "items",
      "type": "array",
      "min_items": 1,
      "max_items": 100,
      "items": {
        "name": "item",
        "type": "object",
        "properties": {
          "product_id": {
            "name": "product_id",
            "type": "string",
            "format": "uuid"
          },
          "quantity": {
            "name": "quantity",
            "type": "integer",
            "minimum": 1
          },
          "price": {
            "name": "price",
            "type": "float",
            "minimum": 0
          }
        }
      }
    },
    {
      "name": "status",
      "type": "string",
      "enum": ["pending", "processing", "shipped", "delivered", "cancelled"]
    },
    {
      "name": "tags",
      "type": "array",
      "unique_items": true,
      "max_items": 10,
      "items": {
        "name": "tag",
        "type": "string"
      }
    }
  ]
}
```

---

## JSON Schema for Validation

Use this JSON Schema to validate that an import file matches the expected format:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Test Data Generator Import Schema",
  "type": "object",
  "required": ["name", "fields"],
  "properties": {
    "name": {
      "type": "string",
      "minLength": 1,
      "description": "Entity/table name"
    },
    "description": {
      "type": "string",
      "description": "Optional description"
    },
    "fields": {
      "type": "array",
      "minItems": 1,
      "items": { "$ref": "#/$defs/field" }
    }
  },
  "$defs": {
    "field": {
      "type": "object",
      "required": ["name", "type"],
      "properties": {
        "name": { "type": "string", "minLength": 1 },
        "type": {
          "type": "string",
          "enum": ["string", "integer", "float", "boolean", "array", "object"]
        },
        "format": {
          "type": "string",
          "enum": ["uuid", "email", "date", "datetime", "time", "url", "hostname", "ipv4", "ipv6", "phone"]
        },
        "description": { "type": "string" },
        "required": { "type": "boolean" },
        "nullable": { "type": "boolean" },
        "enum": { "type": "array" },
        "min_length": { "type": "integer", "minimum": 0 },
        "max_length": { "type": "integer", "minimum": 0 },
        "pattern": { "type": "string" },
        "minimum": { "type": "number" },
        "maximum": { "type": "number" },
        "exclusive_minimum": { "type": "boolean" },
        "exclusive_maximum": { "type": "boolean" },
        "default": {},
        "items": { "$ref": "#/$defs/field" },
        "min_items": { "type": "integer", "minimum": 0 },
        "max_items": { "type": "integer", "minimum": 0 },
        "unique_items": { "type": "boolean" },
        "properties": {
          "type": "object",
          "additionalProperties": { "$ref": "#/$defs/field" }
        }
      }
    }
  }
}
```

---

## Conversion Prompts

Copy these prompts into ChatGPT/Claude along with your source schema.

### Universal Prompt (Works with Any Format)

Use this prompt to convert from ANY schema format. Just paste your schema at the end.

```
Convert the following schema to the canonical JSON format for test data generation.

**Output Format:**
{
  "name": "<entity_name>",
  "description": "<optional description>",
  "fields": [
    {
      "name": "<field_name>",
      "type": "<string|integer|float|boolean|array|object>",
      "format": "<optional: uuid|email|date|datetime|time|url|hostname|ipv4|ipv6|phone>",
      "required": <true|false>,
      "nullable": <true|false>,
      "enum": ["<value1>", "<value2>"],
      "min_length": <number>,
      "max_length": <number>,
      "minimum": <number>,
      "maximum": <number>,
      "pattern": "<regex>",
      "default": <value>,
      "items": { <nested field schema for arrays> },
      "properties": { "<name>": <nested field schema for objects> }
    }
  ]
}

**Conversion Rules:**
1. Identify the entity/table/message name → use as "name"
2. Map each field/column/property to the fields array
3. Map types to: string, integer, float, boolean, array, object
4. Detect formats from type names or field names:
   - UUID/GUID → format: "uuid"
   - Fields named *email* → format: "email"
   - Fields named *phone* → format: "phone"
   - Fields named *url*/*uri* → format: "url"
   - DATE → format: "date"
   - DATETIME/TIMESTAMP → format: "datetime"
5. NOT NULL or required markers → required: true, nullable: false
6. PRIMARY KEY → required: true, nullable: false
7. String length limits → max_length
8. Numeric ranges or CHECK constraints → minimum/maximum
9. ENUM types or CHECK IN (...) → enum array
10. DEFAULT values → default property
11. Arrays/repeated fields → type: "array" with items schema
12. Nested objects/records → type: "object" with properties

**Important:**
- Output ONLY valid JSON, no markdown code blocks, no explanation
- Include only properties that have values (omit null/empty properties)
- For nested objects and arrays, recursively apply the same field schema format

**Schema to convert:**
[PASTE YOUR SCHEMA HERE]
```

---

### Format-Specific Prompts

For better results with specific formats, use these tailored prompts:

### SQL DDL → JSON

```
Convert the following SQL DDL to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (table name) and "fields" array
- Each field has: name, type, and optional properties
- Map SQL types: VARCHAR/TEXT → "string", INT/INTEGER/BIGINT → "integer", DECIMAL/FLOAT/DOUBLE → "float", BOOLEAN/BOOL → "boolean"
- Map special types: UUID → type "string" with format "uuid", TIMESTAMP/DATETIME → type "string" with format "datetime", DATE → type "string" with format "date"
- NOT NULL → required: true, nullable: false
- PRIMARY KEY → required: true, nullable: false
- VARCHAR(n) → max_length: n
- CHECK constraints with IN (...) → enum array
- CHECK constraints with ranges → minimum/maximum
- DEFAULT values → default property
- Column names containing "email" → format: "email", "phone" → format: "phone", "url" → format: "url"

**SQL DDL:**
```sql
[PASTE YOUR SQL HERE]
```

Output only valid JSON, no explanation.
```

### Apache Avro → JSON

```
Convert the following Avro schema (.avsc) to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (record name) and "fields" array
- Each field has: name, type, and optional properties
- Map Avro types: string → "string", int/long → "integer", float/double → "float", boolean → "boolean"
- Map logical types: uuid → format "uuid", timestamp-millis/timestamp-micros → format "datetime", date → format "date"
- Union types ["null", "X"] → nullable: true with the non-null type
- Fields not in a null union → required: true
- Avro enums → enum array (exclude _UNSPECIFIED values)
- Avro arrays → type "array" with items schema
- Avro maps → type "object"
- Nested records → type "object" with properties

**Avro Schema:**
```json
[PASTE YOUR AVRO SCHEMA HERE]
```

Output only valid JSON, no explanation.
```

### Protocol Buffers → JSON

```
Convert the following Protobuf (.proto) message to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (message name) and "fields" array
- Each field has: name, type, and optional properties
- Map protobuf types: string → "string", int32/int64/uint32/uint64/sint32/sint64 → "integer", float/double → "float", bool → "boolean", bytes → "string"
- repeated fields → type "array" with items schema
- map<K,V> → type "object"
- Nested messages → type "object" with properties
- Enums → enum array (exclude UNSPECIFIED values)
- All protobuf fields are optional by default → required: false

**Protobuf:**
```protobuf
[PASTE YOUR PROTO HERE]
```

Output only valid JSON, no explanation.
```

### OpenAPI/Swagger → JSON

```
Convert the following OpenAPI/Swagger schema to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (schema name) and "fields" array
- Each field has: name, type, and optional properties
- Map OpenAPI types directly: string → "string", integer → "integer", number → "float", boolean → "boolean", array → "array", object → "object"
- Preserve format values: uuid, email, date, date-time (convert to "datetime"), uri (convert to "url")
- Fields in "required" array → required: true
- x-nullable or nullable: true → nullable: true
- Copy over: enum, minLength, maxLength, pattern, minimum, maximum, minItems, maxItems, uniqueItems, default
- Resolve $ref references inline
- Nested objects → type "object" with properties
- Arrays → type "array" with items schema

**OpenAPI Schema:**
```yaml
[PASTE YOUR OPENAPI SCHEMA HERE]
```

Output only valid JSON, no explanation.
```

### GraphQL → JSON

```
Convert the following GraphQL type to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (type name) and "fields" array
- Each field has: name, type, and optional properties
- Map GraphQL scalars: String → "string", Int → "integer", Float → "float", Boolean → "boolean", ID → type "string" format "uuid"
- Non-null types (!) → required: true, nullable: false
- List types [X] → type "array" with items schema
- GraphQL enums → enum array
- Nested types → type "object" with properties
- Custom scalars: DateTime → type "string" format "datetime", Email → type "string" format "email", URL → type "string" format "url"

**GraphQL:**
```graphql
[PASTE YOUR GRAPHQL HERE]
```

Output only valid JSON, no explanation.
```

### TypeScript Interface → JSON

```
Convert the following TypeScript interface to the JSON schema format below.

**Target Format Rules:**
- Output a JSON object with "name" (interface name) and "fields" array
- Each field has: name, type, and optional properties
- Map TS types: string → "string", number → "integer" or "float" (use "integer" for IDs/counts, "float" for prices/decimals), boolean → "boolean"
- Optional properties (?) → required: false, nullable: true
- Required properties (no ?) → required: true
- Union types with null (X | null) → nullable: true
- String literal unions → enum array
- Arrays (X[] or Array<X>) → type "array" with items schema
- Nested interfaces/types → type "object" with properties
- Common patterns: id fields → format "uuid", email fields → format "email", createdAt/updatedAt → format "datetime"

**TypeScript:**
```typescript
[PASTE YOUR TYPESCRIPT HERE]
```

Output only valid JSON, no explanation.
```

---

## Usage

1. Copy the appropriate conversion prompt above
2. Paste your source schema where indicated
3. Run in ChatGPT or Claude
4. Save the output as `your_schema.json`
5. Import into the test data generator:
   ```bash
   persona-gen import-schema your_schema.json -o profiles/your_profile.yaml
   ```

---

## Tips for Best Results

1. **Be explicit about ambiguous types**: If ChatGPT isn't sure whether a number should be `integer` or `float`, clarify in your prompt.

2. **Handle large schemas in parts**: For schemas with many entities, convert one at a time.

3. **Validate the output**: Use the JSON Schema above to validate before importing.

4. **Review enum values**: LLMs sometimes miss or add enum values - double-check these.

5. **Check nullable vs required**: These semantics vary between formats - verify the conversion matches your intent.
