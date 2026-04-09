#!/usr/bin/env python3
from __future__ import annotations

import argparse

from crawler.config import DEFAULT_BOOTSTRAP_XLSX, DEFAULT_DB
from crawler.webapp import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Flask dashboard for the SQLite vehicle classifier.",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--bootstrap-xlsx",
        default=DEFAULT_BOOTSTRAP_XLSX,
        help=f"Source workbook used when initializing the DB (default: {DEFAULT_BOOTSTRAP_XLSX})",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=3340,
        help="Bind port (default: 3340)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable Flask debug mode.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    app = create_app(
        db_path=args.db,
        bootstrap_xlsx=args.bootstrap_xlsx,
    )
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
        threaded=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
