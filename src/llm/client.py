from dataclasses import dataclass

from src.config.settings import get_settings


@dataclass
class GeneratedSQL:
    sql: str
    explanation: str
    accessed_tables: list[str]
    accessed_columns: list[str]
    model_confidence: float


class LLMClient:
    """Provider abstraction for OpenAI/Anthropic text-to-SQL generation."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def generate_structured_sql(self, question: str, prompt_context: str) -> GeneratedSQL:
        # Placeholder generation until provider wiring is implemented.
        sql = "SELECT 'Text-to-SQL scaffold ready' AS status"
        explanation = (
            'Placeholder SQL returned by scaffold. '
            'Next phase should wire real provider calls with structured outputs.'
        )
        return GeneratedSQL(
            sql=sql,
            explanation=explanation,
            accessed_tables=[],
            accessed_columns=[],
            model_confidence=0.45,
        )
