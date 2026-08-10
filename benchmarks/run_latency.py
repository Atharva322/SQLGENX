from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "benchmarks" / "results"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run(
    profile: str,
    repetitions: int,
    limit: int | None,
    concurrency: int = 1,
    cold: bool = False,
) -> dict[str, Any]:
    normalized = "deterministic-report-smoke" if profile == "deterministic" else profile
    if normalized == "deterministic-report-smoke":
        from benchmarks.profiles.deterministic import run as run_profile

        return run_profile(repetitions=repetitions, limit=limit, concurrency=concurrency)
    if normalized == "integration-service":
        from benchmarks.profiles.integration_service import run as run_profile

        return run_profile(
            repetitions=repetitions, limit=limit, concurrency=concurrency, cold=cold
        )
    if normalized == "integration-http":
        from benchmarks.profiles.integration_http import run as run_profile

        return run_profile(repetitions=repetitions, limit=limit, concurrency=concurrency)
    if normalized == "live-provider":
        from benchmarks.profiles.live_provider import run as run_profile

        return run_profile(repetitions=repetitions, limit=limit, concurrency=concurrency)
    raise ValueError(f"Unknown benchmark profile: {profile}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="deterministic-report-smoke")
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--cold", action="store_true")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    report = run(
        profile=args.profile,
        repetitions=args.repetitions,
        limit=args.limit,
        concurrency=args.concurrency,
        cold=args.cold,
    )
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output = args.output or RESULTS_DIR / (
        f"{report['profile']}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(output),
                "profile": report["profile"],
                "sample_count": report["sample_count"] if "sample_count" in report else 0,
            }
        )
    )


if __name__ == "__main__":
    main()
