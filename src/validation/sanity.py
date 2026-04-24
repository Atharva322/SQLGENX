from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd


@dataclass
class SanityCheckResult:
    score: float
    warnings: list[str] = field(default_factory=list)


def _is_date_like_column(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in ["date", "time", "timestamp", "at"])


def analyze_result_sanity(rows: list[dict]) -> SanityCheckResult:
    if not rows:
        return SanityCheckResult(score=0.4, warnings=["No rows returned for sanity validation."])
    if any("error" in row for row in rows):
        return SanityCheckResult(
            score=0.1, warnings=["Execution returned error payload; sanity checks failed."]
        )

    frame = pd.DataFrame(rows)
    warnings: list[str] = []
    score = 1.0

    # NULL-heavy columns can indicate weak joins.
    for column in frame.columns:
        null_ratio = float(frame[column].isna().mean())
        if null_ratio >= 0.8:
            warnings.append(
                f"Column '{column}' is {null_ratio:.0%} NULL; possible bad JOIN or missing relationships."
            )
            score -= 0.15

    numeric_cols = frame.select_dtypes(include=["number"]).columns
    for column in numeric_cols:
        series = frame[column].dropna()
        if series.empty:
            continue

        abs_max = float(series.abs().max())
        if abs_max > 1e12:
            warnings.append(f"Column '{column}' has extreme magnitude values (>|1e12|).")
            score -= 0.15

        if "count" in column.lower():
            if (series < 0).any():
                warnings.append(f"Column '{column}' contains negative counts, which is implausible.")
                score -= 0.2
            if float(series.max()) > 1e9:
                warnings.append(f"Column '{column}' has unusually high counts (>1e9).")
                score -= 0.1

    now_utc = datetime.now(tz=timezone.utc)
    for column in frame.columns:
        if not _is_date_like_column(column):
            continue
        parsed = pd.to_datetime(frame[column], errors="coerce", utc=True)
        valid = parsed.dropna()
        if valid.empty:
            continue
        if valid.min() > valid.max():
            warnings.append(f"Column '{column}' has invalid date ordering.")
            score -= 0.1
        if valid.max().to_pydatetime() > now_utc:
            warnings.append(f"Column '{column}' contains future timestamps beyond current data horizon.")
            score -= 0.1

    score = round(max(0.0, min(1.0, score)), 3)
    return SanityCheckResult(score=score, warnings=warnings)
