from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from .http_client import SimpleHttpClient
from .sqlite_ops import iter_target_rows, load_database_context_values, write_result_row
from .sources import classify_vehicle


EventCallback = Callable[[str, dict], None]


@dataclass(slots=True)
class RunOptions:
    db: str
    bootstrap_xlsx: str
    reimport_db: bool = False
    limit: int = 100
    start_row: int = 1
    delay: float = 2.0
    overwrite: bool = False
    allow_unknown: bool = False
    fix_hybrid: bool = False
    browser: bool = False


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def run_classification(
    options: RunOptions,
    *,
    event_callback: EventCallback | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> dict:
    def emit(event_type: str, **payload) -> None:
        if event_callback:
            event_callback(event_type, payload)

    def emit_log(message: str, *, row_number: int | None = None, car_info: str = "") -> None:
        emit(
            "log",
            message=message,
            row_number=row_number,
            car_info=car_info,
            timestamp=_now_iso(),
        )

    connection = None
    client = None
    processed = 0
    total_tasks = 0

    def finish_stopped(db_path: str) -> dict:
        emit(
            "run_stopped",
            processed=processed,
            total_tasks=total_tasks,
            db_path=db_path,
            message="Stopped after the current row.",
            timestamp=_now_iso(),
        )
        return {
            "processed": processed,
            "total_tasks": total_tasks,
            "db_path": db_path,
            "imported_rows": imported_rows,
            "stopped": True,
        }

    try:
        connection, db_path, imported_rows, bootstrap_xlsx = load_database_context_values(
            db=options.db,
            bootstrap_xlsx=options.bootstrap_xlsx,
            reimport_db=options.reimport_db,
        )
        emit(
            "db_ready",
            db_path=str(db_path),
            bootstrap_xlsx=str(bootstrap_xlsx),
            imported_rows=imported_rows,
            timestamp=_now_iso(),
        )

        tasks = list(
            iter_target_rows(
                connection=connection,
                limit=options.limit,
                overwrite=options.overwrite,
                allow_unknown=options.allow_unknown,
                fix_hybrid=options.fix_hybrid,
                start_row=options.start_row,
            )
        )
        total_tasks = len(tasks)

        emit(
            "run_started",
            total_tasks=total_tasks,
            db_path=str(db_path),
            browser_requested=options.browser,
            delay=options.delay,
            start_row=options.start_row,
            timestamp=_now_iso(),
        )

        if not tasks:
            emit(
                "run_finished",
                processed=0,
                total_tasks=0,
                db_path=str(db_path),
                message="No eligible rows found.",
                timestamp=_now_iso(),
            )
            return {
                "processed": 0,
                "total_tasks": 0,
                "db_path": str(db_path),
                "imported_rows": imported_rows,
            }

        client = SimpleHttpClient(delay_seconds=options.delay, use_browser=options.browser)
        emit(
            "client_ready",
            browser_requested=options.browser,
            browser_enabled=client.use_browser,
            timestamp=_now_iso(),
        )

        if should_stop and should_stop():
            return finish_stopped(str(db_path))

        for index, task in enumerate(tasks, start=1):
            if should_stop and should_stop():
                return finish_stopped(str(db_path))
            emit(
                "row_start",
                row_number=task.row_number,
                car_info=task.car_info,
                index=index,
                total_tasks=total_tasks,
                timestamp=_now_iso(),
            )
            result = classify_vehicle(
                client,
                task,
                log_callback=lambda message, row_number=task.row_number, car_info=task.car_info: emit_log(
                    message,
                    row_number=row_number,
                    car_info=car_info,
                ),
            )
            write_result_row(
                connection=connection,
                task=task,
                result=result,
            )
            processed = index
            emit(
                "row_done",
                row_number=task.row_number,
                car_info=task.car_info,
                fuel_type=result.fuel_type,
                url=result.url or "",
                url_prove=bool(result.url_prove),
                index=index,
                total_tasks=total_tasks,
                timestamp=_now_iso(),
            )
            if should_stop and should_stop():
                return finish_stopped(str(db_path))

        emit(
            "run_finished",
            processed=processed,
            total_tasks=total_tasks,
            db_path=str(db_path),
            timestamp=_now_iso(),
        )
        return {
            "processed": processed,
            "total_tasks": total_tasks,
            "db_path": str(db_path),
            "imported_rows": imported_rows,
        }
    except Exception as exc:
        emit(
            "run_error",
            message=str(exc),
            processed=processed,
            timestamp=_now_iso(),
        )
        raise
    finally:
        if client is not None:
            client.close()
        if connection is not None:
            connection.close()
