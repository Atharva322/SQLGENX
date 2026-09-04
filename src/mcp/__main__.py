"""stdio entry point: ``python -m src.mcp``.

The JSON-RPC stream runs over stdout, so nothing in the import path may print to stdout.
loguru defaults to stderr and the audit logger writes to ``logs/query_audit.log``.
"""

from src.mcp.server import mcp


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
