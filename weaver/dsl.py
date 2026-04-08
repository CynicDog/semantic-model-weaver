"""
weaver.dsl — Pydantic DSL for Cortex Analyst semantic models.

This module is the single source of truth for what a valid semantic model looks
like. Every pipeline stage (YAMLWriter, RefinementAgent, EvalLogger) works with
these types — never raw dicts or YAML strings.

Serialisation contract:
  - SemanticModel.to_yaml()   →  Cortex Analyst-compliant YAML (ready to POST)
  - SemanticModel.from_yaml() →  validated SemanticModel from an existing YAML
  - SemanticModel.to_dict()   →  plain dict, used for TruLens logging

Field names follow the Cortex Analyst YAML spec exactly.
The `schema` field on BaseTable is aliased (schema_ in Python, "schema" in YAML)
to avoid shadowing Pydantic's own .schema() method.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class DataType(StrEnum):
    VARCHAR = "VARCHAR"
    NUMBER = "NUMBER"
    FLOAT = "FLOAT"
    DATE = "DATE"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    BOOLEAN = "BOOLEAN"
    VARIANT = "VARIANT"
    OBJECT = "OBJECT"
    ARRAY = "ARRAY"


_TEMPORAL_TYPES = {
    DataType.DATE,
    DataType.TIMESTAMP_NTZ,
    DataType.TIMESTAMP_LTZ,
    DataType.TIMESTAMP_TZ,
}


class JoinType(StrEnum):
    LEFT_OUTER = "left_outer"
    INNER = "inner"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"
    CROSS = "cross"


class RelationshipType(StrEnum):
    MANY_TO_ONE = "many_to_one"
    ONE_TO_MANY = "one_to_many"
    ONE_TO_ONE = "one_to_one"
    MANY_TO_MANY = "many_to_many"


class FilterType(StrEnum):
    CATEGORICAL = "categorical"
    RANGE = "range"
    CUSTOM = "custom"


class BaseTable(BaseModel):
    """Points to the physical Snowflake table backing a semantic table."""

    model_config = ConfigDict(populate_by_name=True)

    database: str
    schema_: str = Field(..., alias="schema", serialization_alias="schema")
    table: str


class Dimension(BaseModel):
    """A categorical or ordinal column exposed for filtering and grouping."""

    name: str
    expr: str
    data_type: DataType
    description: str = ""
    synonyms: list[str] = Field(default_factory=list)
    sample_values: list[str] = Field(default_factory=list)


class TimeDimension(BaseModel):
    """A date/timestamp column used for time-based filtering and aggregation."""

    name: str
    expr: str
    data_type: DataType = DataType.DATE
    description: str = ""
    synonyms: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def must_be_temporal(self) -> TimeDimension:
        if self.data_type not in _TEMPORAL_TYPES:
            raise ValueError(
                f"TimeDimension '{self.name}' must have a date/timestamp data_type, "
                f"got {self.data_type!r}. Use Dimension for non-temporal columns."
            )
        return self


class Measure(BaseModel):
    """
    A numeric metric. The aggregation function is embedded in `expr`
    (e.g. SUM(trade_value), AVG(price)) following the Cortex Analyst spec.
    Do NOT add a default_aggregation field — Cortex Analyst does not use it.
    """

    name: str
    expr: str
    data_type: DataType
    description: str = ""
    synonyms: list[str] = Field(default_factory=list)


class Filter(BaseModel):
    """A pre-defined filter that Cortex Analyst can apply by name."""

    name: str
    synonyms: list[str] = Field(default_factory=list)
    filter_type: FilterType = FilterType.CATEGORICAL
    expr: str


class PrimaryKey(BaseModel):
    """Primary key declaration for a semantic table — required by Cortex Analyst for join tables."""

    columns: list[str]


class SemanticTable(BaseModel):
    """One logical table in the semantic model, backed by a physical Snowflake table."""

    name: str
    description: str = ""
    base_table: BaseTable
    primary_key: PrimaryKey | None = None
    dimensions: list[Dimension] = Field(default_factory=list)
    time_dimensions: list[TimeDimension] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)


class RelationshipColumn(BaseModel):
    left_column: str
    right_column: str


class Relationship(BaseModel):
    """
    Defines a join between two semantic tables.
    Use many_to_one for the most common FK → PK joins.
    """

    name: str
    left_table: str
    right_table: str
    relationship_type: RelationshipType = RelationshipType.MANY_TO_ONE
    join_type: JoinType = JoinType.LEFT_OUTER
    relationship_columns: list[RelationshipColumn]


class VerifiedQuery(BaseModel):
    """
    A human- or agent-verified NL question + correct SQL pair.
    These are injected into the semantic model to anchor Cortex Analyst's
    SQL generation for known-good questions.
    """

    name: str
    question: str
    sql: str
    verified_at: int = 0  # Unix timestamp (int64) — required by Cortex Analyst YAML spec
    verified_by: str = ""


def _strip_empty(obj: Any) -> Any:
    """Recursively remove None values and empty lists/strings from a dict tree."""
    if isinstance(obj, dict):
        return {k: _strip_empty(v) for k, v in obj.items() if v is not None and v != [] and v != ""}
    if isinstance(obj, list):
        return [_strip_empty(i) for i in obj]
    return obj


class SemanticModel(BaseModel):
    """
    The root of a Cortex Analyst semantic model.

    Usage
    -----
    Build programmatically:
        model = SemanticModel(name="my_model", tables=[...], relationships=[...])

    Round-trip through YAML:
        yaml_text = model.to_yaml()
        model2    = SemanticModel.from_yaml(yaml_text)

    Log to TruLens / Snowflake:
        record = model.to_dict()
    """

    name: str
    description: str = ""
    tables: list[SemanticTable]
    relationships: list[Relationship] = Field(default_factory=list)
    verified_queries: list[VerifiedQuery] = Field(default_factory=list)

    @model_validator(mode="after")
    def relationship_tables_exist(self) -> SemanticModel:
        table_names = {t.name for t in self.tables}
        for rel in self.relationships:
            for side, tname in (("left_table", rel.left_table), ("right_table", rel.right_table)):
                if tname not in table_names:
                    raise ValueError(
                        f"Relationship '{rel.name}': {side} '{tname}' not found in tables "
                        f"({sorted(table_names)})"
                    )
        return self

    def to_dict(self) -> dict:
        """
        Plain dict matching the Cortex Analyst YAML spec.
        Empty lists, empty strings, and None values are omitted.
        """
        # mode="json" coerces Enum members to their .value (plain strings/ints)
        raw = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        return _strip_empty(raw)

    def to_yaml(self) -> str:
        """Cortex Analyst-compliant YAML string, ready to POST to the REST API."""
        class _QuotedStr(str):
            pass

        def _quoted_representer(dumper, data):
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")

        dumper = yaml.Dumper
        dumper.add_representer(_QuotedStr, _quoted_representer)

        def _quote_synonyms(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: _quote_synonyms(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_QuotedStr(i) if isinstance(i, str) else _quote_synonyms(i) for i in obj]
            return obj

        data = self.to_dict()
        for table in data.get("tables", []):
            for section in ("dimensions", "time_dimensions", "measures"):
                for col in table.get(section, []):
                    if "synonyms" in col:
                        col["synonyms"] = [_QuotedStr(str(s)) for s in col["synonyms"]]

        return yaml.dump(
            data,
            Dumper=dumper,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            width=120,
        )

    @classmethod
    def from_yaml(cls, text: str) -> SemanticModel:
        """Parse and validate a Cortex Analyst YAML string."""
        data = yaml.safe_load(text)
        return cls.model_validate(data)

    @classmethod
    def from_yaml_file(cls, path: str) -> SemanticModel:
        with open(path, encoding="utf-8") as f:
            return cls.from_yaml(f.read())
