from dataclasses import dataclass, field
import re


STOPWORDS = {
    "the",
    "a",
    "an",
    "is",
    "are",
    "to",
    "for",
    "of",
    "in",
    "on",
    "by",
    "from",
    "and",
    "or",
    "with",
    "what",
    "show",
    "give",
    "list",
}


@dataclass
class AlignmentResult:
    score: float
    back_translated_question: str
    warnings: list[str] = field(default_factory=list)


def _tokenize(text: str) -> set[str]:
    tokens = re.findall(r"[a-zA-Z0-9_]+", text.lower())
    return {token for token in tokens if token not in STOPWORDS and len(token) > 2}


def verify_sql_alignment(
    original_question: str,
    back_translated_question: str,
    low_alignment_threshold: float = 0.55,
) -> AlignmentResult:
    source_tokens = _tokenize(original_question)
    translated_tokens = _tokenize(back_translated_question)
    warnings: list[str] = []

    if not source_tokens:
        score = 0.0
    else:
        intersection = source_tokens.intersection(translated_tokens)
        union = source_tokens.union(translated_tokens)
        score = len(intersection) / max(1, len(union))

    if score < low_alignment_threshold:
        warnings.append(
            "Low SQL-to-question alignment detected. Generated SQL may not answer the original question."
        )

    return AlignmentResult(
        score=round(min(1.0, max(0.0, score)), 3),
        back_translated_question=back_translated_question,
        warnings=warnings,
    )
