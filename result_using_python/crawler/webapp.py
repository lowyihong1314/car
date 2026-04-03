from __future__ import annotations

import json
import queue
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request

from .runner import RunOptions, run_classification
from .sqlite_ops import (
    connect_database,
    fetch_brand_type_breakdown,
    fetch_dashboard_summary,
    fetch_recent_updates,
    fetch_vehicle_rows,
    resolve_user_path,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _coerce_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class EventBroker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: set[queue.Queue] = set()

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    def publish(self, event_type: str, payload: dict) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put({"event": event_type, "payload": payload})


class JobManager:
    def __init__(self, *, db_path: Path, bootstrap_xlsx: Path) -> None:
        self.db_path = db_path
        self.bootstrap_xlsx = bootstrap_xlsx
        self._lock = threading.Lock()
        self._cancel_requested = threading.Event()
        self._broker = EventBroker()
        self._thread: threading.Thread | None = None
        self._logs: deque[dict] = deque(maxlen=200)
        self._state: dict = {
            "running": False,
            "stop_requested": False,
            "job_id": None,
            "processed": 0,
            "total_tasks": 0,
            "current_row": None,
            "current_car_info": "",
            "start_row": 1,
            "started_at": None,
            "finished_at": None,
            "error": "",
            "last_event": "",
            "message": "Idle",
        }

    def subscribe(self) -> queue.Queue:
        return self._broker.subscribe()

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        self._broker.unsubscribe(subscriber)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                **self._state,
                "logs": list(self._logs),
                "db_path": str(self.db_path),
                "bootstrap_xlsx": str(self.bootstrap_xlsx),
            }

    def _append_log(self, event_type: str, payload: dict) -> None:
        self._logs.append(
            {
                "event": event_type,
                "timestamp": payload.get("timestamp", _now_iso()),
                "message": payload.get("message", ""),
                "payload": payload,
            }
        )

    def _publish(self, event_type: str, payload: dict) -> None:
        self._broker.publish(event_type, payload)

    def _handle_event(self, job_id: str, event_type: str, payload: dict) -> None:
        message = payload.get("message", "")
        with self._lock:
            if self._state["job_id"] != job_id:
                return
            self._state["last_event"] = event_type

            if event_type == "log":
                payload = {**payload, "job_id": job_id}
                self._append_log(event_type, payload)
            elif event_type == "run_started":
                self._state["running"] = True
                self._state["stop_requested"] = False
                self._state["processed"] = 0
                self._state["total_tasks"] = int(payload.get("total_tasks", 0))
                self._state["start_row"] = int(payload.get("start_row", self._state["start_row"]))
                self._state["started_at"] = payload.get("timestamp", _now_iso())
                self._state["finished_at"] = None
                self._state["error"] = ""
                message = f"Processing {self._state['total_tasks']} row(s)..."
            elif event_type == "row_start":
                self._state["current_row"] = payload.get("row_number")
                self._state["current_car_info"] = payload.get("car_info", "")
                message = f"Row {payload.get('row_number')}: {payload.get('car_info', '')}"
            elif event_type == "row_done":
                self._state["processed"] = int(payload.get("index", self._state["processed"]))
                self._state["current_row"] = payload.get("row_number")
                self._state["current_car_info"] = payload.get("car_info", "")
                message = (
                    f"Row {payload.get('row_number')} -> {payload.get('fuel_type')} "
                    f"({payload.get('url') or '-'})"
                )
            elif event_type == "run_finished":
                self._state["running"] = False
                self._state["stop_requested"] = False
                self._cancel_requested.clear()
                self._state["processed"] = int(payload.get("processed", self._state["processed"]))
                self._state["total_tasks"] = int(payload.get("total_tasks", self._state["total_tasks"]))
                self._state["finished_at"] = payload.get("timestamp", _now_iso())
                self._state["current_row"] = None
                self._state["current_car_info"] = ""
                message = payload.get("message") or "Run finished."
            elif event_type == "run_stopped":
                self._state["running"] = False
                self._state["stop_requested"] = False
                self._cancel_requested.clear()
                self._state["processed"] = int(payload.get("processed", self._state["processed"]))
                self._state["total_tasks"] = int(payload.get("total_tasks", self._state["total_tasks"]))
                self._state["finished_at"] = payload.get("timestamp", _now_iso())
                self._state["current_row"] = None
                self._state["current_car_info"] = ""
                message = payload.get("message") or "Stopped."
            elif event_type == "run_error":
                self._state["running"] = False
                self._state["stop_requested"] = False
                self._cancel_requested.clear()
                self._state["finished_at"] = payload.get("timestamp", _now_iso())
                self._state["error"] = payload.get("message", "Unknown error")
                message = self._state["error"]
            elif event_type == "stop_requested":
                self._state["stop_requested"] = True
                message = payload.get("message") or "Stopping after the current row."
            elif event_type == "db_ready":
                imported_rows = payload.get("imported_rows")
                if imported_rows is not None:
                    message = f"Initialized DB with {imported_rows} row(s)."
                else:
                    message = "Database ready."
            elif event_type == "client_ready":
                if payload.get("browser_enabled"):
                    message = "Browser mode enabled."
                elif payload.get("browser_requested"):
                    message = "Browser requested, but Playwright is unavailable. Using HTTP-only mode."
                else:
                    message = "HTTP client ready."

            if event_type != "log":
                self._state["message"] = message or self._state["message"]
                payload = {**payload, "job_id": job_id, "message": self._state["message"]}
                self._append_log(event_type, payload)

        self._publish(event_type, payload)

    def start_run(self, *, form_data: dict) -> tuple[bool, dict]:
        with self._lock:
            if self._state["running"]:
                snapshot = {
                    **self._state,
                    "logs": list(self._logs),
                    "db_path": str(self.db_path),
                    "bootstrap_xlsx": str(self.bootstrap_xlsx),
                }
                return False, snapshot

            job_id = str(uuid.uuid4())
            self._cancel_requested.clear()
            self._logs.clear()
            self._state.update(
                {
                    "running": True,
                    "stop_requested": False,
                    "job_id": job_id,
                    "processed": 0,
                    "total_tasks": 0,
                    "current_row": None,
                    "current_car_info": "",
                    "start_row": max(1, _coerce_int(form_data.get("start_row"), 1)),
                    "started_at": _now_iso(),
                    "finished_at": None,
                    "error": "",
                    "last_event": "queued",
                    "message": "Queued",
                }
            )
            self._append_log(
                "queued",
                {
                    "job_id": job_id,
                    "timestamp": self._state["started_at"],
                    "message": "Queued",
                },
            )

            options = RunOptions(
                db=str(self.db_path),
                bootstrap_xlsx=str(self.bootstrap_xlsx),
                reimport_db=_coerce_bool(form_data.get("reimport_db")),
                limit=max(1, _coerce_int(form_data.get("limit"), 100)),
                start_row=max(1, _coerce_int(form_data.get("start_row"), 1)),
                delay=max(0.0, _coerce_float(form_data.get("delay"), 1.5)),
                overwrite=_coerce_bool(form_data.get("overwrite")),
                allow_unknown=_coerce_bool(form_data.get("allow_unknown", True)),
                fix_hybrid=_coerce_bool(form_data.get("fix_hybrid")),
                browser=_coerce_bool(form_data.get("browser")),
            )

            self._thread = threading.Thread(
                target=self._run_job,
                args=(job_id, options),
                daemon=True,
                name=f"classifier-{job_id[:8]}",
            )
            self._thread.start()
            snapshot = {
                **self._state,
                "logs": list(self._logs),
                "db_path": str(self.db_path),
                "bootstrap_xlsx": str(self.bootstrap_xlsx),
            }

        self._publish("queued", snapshot)
        return True, snapshot

    def stop_run(self) -> tuple[bool, dict]:
        with self._lock:
            if not self._state["running"]:
                snapshot = {
                    **self._state,
                    "logs": list(self._logs),
                    "db_path": str(self.db_path),
                    "bootstrap_xlsx": str(self.bootstrap_xlsx),
                }
                return False, snapshot
            if self._state["stop_requested"]:
                snapshot = {
                    **self._state,
                    "logs": list(self._logs),
                    "db_path": str(self.db_path),
                    "bootstrap_xlsx": str(self.bootstrap_xlsx),
                }
                return False, snapshot

            self._cancel_requested.set()
            self._state["stop_requested"] = True
            self._state["last_event"] = "stop_requested"
            self._state["message"] = "Stopping after the current row."
            payload = {
                "job_id": self._state["job_id"],
                "timestamp": _now_iso(),
                "message": self._state["message"],
            }
            self._append_log("stop_requested", payload)
            snapshot = {
                **self._state,
                "logs": list(self._logs),
                "db_path": str(self.db_path),
                "bootstrap_xlsx": str(self.bootstrap_xlsx),
            }

        self._publish("stop_requested", payload)
        return True, snapshot

    def _run_job(self, job_id: str, options: RunOptions) -> None:
        try:
            run_classification(
                options,
                event_callback=lambda event_type, payload: self._handle_event(job_id, event_type, payload),
                should_stop=self._cancel_requested.is_set,
            )
        except Exception as exc:
            self._handle_event(
                job_id,
                "run_error",
                {
                    "message": str(exc),
                    "timestamp": _now_iso(),
                },
            )


def _sse_frame(event_type: str, payload: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def create_app(*, db_path: str, bootstrap_xlsx: str) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).resolve().parent / "templates"),
    )
    resolved_db_path = resolve_user_path(db_path)
    resolved_bootstrap_xlsx = resolve_user_path(bootstrap_xlsx)
    manager = JobManager(db_path=resolved_db_path, bootstrap_xlsx=resolved_bootstrap_xlsx)
    app.config["JOB_MANAGER"] = manager
    app.config["DB_PATH"] = str(resolved_db_path)
    app.config["BOOTSTRAP_XLSX"] = str(resolved_bootstrap_xlsx)

    @app.get("/")
    def dashboard():
        return render_template(
            "dashboard.html",
            db_path=str(resolved_db_path),
            bootstrap_xlsx=str(resolved_bootstrap_xlsx),
            default_limit=100,
            default_delay=1.5,
        )

    @app.get("/api/vehicles")
    def vehicles_api():
        connection = connect_database(resolved_db_path)
        try:
            rows, total = fetch_vehicle_rows(
                connection,
                search=request.args.get("q", ""),
                fuel_type=request.args.get("type", ""),
                limit=_coerce_int(request.args.get("limit"), 30),
                offset=_coerce_int(request.args.get("offset"), 0),
            )
        finally:
            connection.close()
        return jsonify(
            {
                "rows": rows,
                "total": total,
                "limit": max(1, min(_coerce_int(request.args.get("limit"), 30), 30)),
                "offset": _coerce_int(request.args.get("offset"), 0),
            }
        )

    @app.get("/api/summary")
    def summary_api():
        connection = connect_database(resolved_db_path)
        try:
            summary = fetch_dashboard_summary(connection)
            brand_type_breakdown = fetch_brand_type_breakdown(connection)
            recent_updates = fetch_recent_updates(connection, limit=10)
        finally:
            connection.close()
        return jsonify(
            {
                "summary": summary,
                "brand_type_breakdown": brand_type_breakdown,
                "recent_updates": recent_updates,
            }
        )

    @app.get("/api/status")
    def status_api():
        return jsonify(manager.snapshot())

    @app.post("/api/run")
    def run_api():
        payload = request.get_json(silent=True) or request.form.to_dict()
        accepted, snapshot = manager.start_run(form_data=payload)
        response_payload = {
            **snapshot,
            "accepted": accepted,
        }
        if not accepted:
            response_payload["notice"] = "A run is already in progress."
        return jsonify(response_payload), (202 if accepted else 200)

    @app.post("/api/stop")
    def stop_api():
        accepted, snapshot = manager.stop_run()
        response_payload = {
            **snapshot,
            "accepted": accepted,
        }
        if not accepted:
            if snapshot.get("stop_requested"):
                response_payload["notice"] = "Stop has already been requested."
            else:
                response_payload["notice"] = "No active run to stop."
        return jsonify(response_payload), (202 if accepted else 200)

    @app.get("/events")
    def events():
        subscriber = manager.subscribe()

        def stream():
            try:
                yield _sse_frame("snapshot", manager.snapshot())
                while True:
                    try:
                        message = subscriber.get(timeout=15)
                    except queue.Empty:
                        yield ": keep-alive\n\n"
                        continue
                    yield _sse_frame(message["event"], message["payload"])
            finally:
                manager.unsubscribe(subscriber)

        response = Response(stream(), mimetype="text/event-stream")
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    return app
