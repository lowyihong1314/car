from __future__ import annotations

from datetime import datetime

from .config import parse_args
from .runner import RunOptions, run_classification


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def main() -> int:
    args = parse_args()
    options = RunOptions(
        db=args.db,
        bootstrap_xlsx=args.bootstrap_xlsx,
        reimport_db=args.reimport_db,
        limit=args.limit,
        start_row=1,
        delay=args.delay,
        overwrite=args.overwrite,
        allow_unknown=args.allow_unknown,
        fix_hybrid=args.fix_hybrid,
        browser=args.browser,
    )

    def handle_event(event_type: str, payload: dict) -> None:
        if event_type == "db_ready" and payload.get("imported_rows") is not None:
            print(
                f"Initialized SQLite DB from {payload['bootstrap_xlsx']}: "
                f"{payload['db_path']} ({payload['imported_rows']} row(s))"
            )
        elif event_type == "client_ready":
            if payload.get("browser_enabled"):
                print("Browser mode enabled (Playwright headless, wait 1s per page).")
            elif options.browser:
                print("Browser mode requested, but Playwright is not available. Falling back to HTTP-only mode.")
            if options.fix_hybrid:
                print("Fix-hybrid mode: re-classifying suspect electric/petrol rows.")
        elif event_type == "run_started":
            print(f"Processing {payload['total_tasks']} row(s)...")
        elif event_type == "row_start":
            print(f"[{_ts()}] Row {payload['row_number']}: {payload['car_info']}")
        elif event_type == "row_done":
            print(
                f"[{_ts()}]   -> type={payload['fuel_type']}, "
                f"url={payload['url'] or '-'}, "
                f"url_prove={payload['url_prove']}"
            )
        elif event_type == "log":
            print(f"[{_ts()}]      {payload['message']}")
        elif event_type == "run_stopped":
            print("Run stopped by user.")
        elif event_type == "run_finished":
            if payload["total_tasks"] == 0:
                print("No eligible rows found.")
                print(f"SQLite DB unchanged: {payload['db_path']}")
            else:
                print(f"Saved: {payload['db_path']}")

    run_classification(options, event_callback=handle_event)
    return 0
