from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from src.semantic.models import SemanticLayerDefinition
from src.semantic.validation import validate_semantic_layer


DEFAULT_SEMANTIC_LAYER_PATH = Path("semantic") / "metrics.yaml"


def load_semantic_layer(path: str | Path = DEFAULT_SEMANTIC_LAYER_PATH) -> SemanticLayerDefinition:
    payload = _load_yaml(path)
    definition = SemanticLayerDefinition.from_yaml_payload(payload)
    validate_semantic_layer(definition)
    return definition


@lru_cache(maxsize=8)
def load_semantic_layer_cached(path: str = str(DEFAULT_SEMANTIC_LAYER_PATH)) -> SemanticLayerDefinition:
    return load_semantic_layer(path)


def _load_yaml(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"Semantic layer YAML not found: {target}")
    payload = yaml.safe_load(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Semantic layer YAML must parse to a mapping")
    return payload
