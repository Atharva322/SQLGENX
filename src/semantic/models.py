from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


IDENTIFIER_PATTERN = r"^[a-z][a-z0-9_]*$"
RELATIONSHIP = Literal["one_to_one", "many_to_one", "one_to_many"]


class SemanticEntity(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    table: str = Field(pattern=IDENTIFIER_PATTERN)
    primary_key: str = Field(pattern=IDENTIFIER_PATTERN)
    synonyms: list[str] = Field(default_factory=list)


class ApprovedJoin(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    left_table: str = Field(pattern=IDENTIFIER_PATTERN)
    left_column: str = Field(pattern=IDENTIFIER_PATTERN)
    right_table: str = Field(pattern=IDENTIFIER_PATTERN)
    right_column: str = Field(pattern=IDENTIFIER_PATTERN)
    relationship: RELATIONSHIP


class SemanticDimension(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    table: str = Field(pattern=IDENTIFIER_PATTERN)
    expression: str
    label: str
    synonyms: list[str] = Field(default_factory=list)
    sensitivity: str = "internal"


class CanonicalFilter(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    table: str = Field(pattern=IDENTIFIER_PATTERN)
    expression: str
    label: str
    synonyms: list[str] = Field(default_factory=list)


class SemanticMetricTest(BaseModel):
    question: str
    expected_sql: str


class SemanticMetric(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    version: str
    description: str
    source: str = Field(pattern=IDENTIFIER_PATTERN)
    expression: str
    allowed_dimensions: list[str] = Field(default_factory=list)
    allowed_filters: list[str] = Field(default_factory=list)
    synonyms: list[str] = Field(default_factory=list)
    owner: str
    sensitivity: str = "internal"
    tests: list[SemanticMetricTest] = Field(default_factory=list)

    @field_validator("allowed_dimensions", "allowed_filters")
    @classmethod
    def _dedupe_refs(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("duplicate references are not allowed")
        return values


class SemanticLayerDefinition(BaseModel):
    id: str = Field(pattern=IDENTIFIER_PATTERN)
    version: str
    owners: dict[str, str] = Field(default_factory=dict)
    entities: list[SemanticEntity]
    approved_joins: list[ApprovedJoin] = Field(default_factory=list)
    dimensions: list[SemanticDimension]
    filters: list[CanonicalFilter] = Field(default_factory=list)
    metrics: list[SemanticMetric]

    @model_validator(mode="after")
    def _strict_references(self) -> "SemanticLayerDefinition":
        _ensure_unique("entity", [item.id for item in self.entities])
        _ensure_unique("entity_table", [item.table for item in self.entities])
        _ensure_unique("join", [item.id for item in self.approved_joins])
        _ensure_unique("dimension", [item.id for item in self.dimensions])
        _ensure_unique("filter", [item.id for item in self.filters])
        _ensure_unique("metric", [item.id for item in self.metrics])

        tables = {item.table for item in self.entities}
        dimension_ids = {item.id for item in self.dimensions}
        filter_ids = {item.id for item in self.filters}
        for join in self.approved_joins:
            if join.left_table not in tables or join.right_table not in tables:
                raise ValueError(f"join {join.id} references unknown table")
        for dimension in self.dimensions:
            if dimension.table not in tables:
                raise ValueError(f"dimension {dimension.id} references unknown table")
        for filter_def in self.filters:
            if filter_def.table not in tables:
                raise ValueError(f"filter {filter_def.id} references unknown table")
        for metric in self.metrics:
            if metric.source not in tables:
                raise ValueError(f"metric {metric.id} references unknown source")
            for dimension_id in metric.allowed_dimensions:
                if dimension_id not in dimension_ids:
                    raise ValueError(f"metric {metric.id} references unknown dimension {dimension_id}")
            for filter_id in metric.allowed_filters:
                if filter_id not in filter_ids:
                    raise ValueError(f"metric {metric.id} references unknown filter {filter_id}")
        return self

    @classmethod
    def from_yaml_payload(cls, payload: dict[str, Any]) -> "SemanticLayerDefinition":
        raw = payload.get("semantic_layer", payload)
        if not isinstance(raw, dict):
            raise ValueError("semantic layer YAML must contain a mapping")
        return cls.model_validate(raw)


def _ensure_unique(kind: str, values: list[str]) -> None:
    seen: set[str] = set()
    duplicates = sorted({value for value in values if value in seen or seen.add(value)})
    if duplicates:
        raise ValueError(f"duplicate {kind} IDs: {', '.join(duplicates)}")
