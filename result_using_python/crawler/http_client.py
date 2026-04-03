from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser

from .config import HTTP_BACKOFF_SECONDS, RENDER_WAIT_MS, USER_AGENT

try:
    from playwright.sync_api import sync_playwright as _sync_playwright

    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


class SimpleHttpClient:
    def __init__(self, delay_seconds: float, use_browser: bool = False) -> None:
        self.delay_seconds = max(delay_seconds, 0.0)
        self.browser_requested = use_browser
        self.browser_available = _PLAYWRIGHT_AVAILABLE
        self.use_browser = use_browser and _PLAYWRIGHT_AVAILABLE
        self.last_request_ts = 0.0
        self.robot_parsers: dict[str, urllib.robotparser.RobotFileParser] = {}
        self.text_cache: dict[str, str] = {}
        self.json_cache: dict[str, dict] = {}
        self._playwright_ctx: object = None  # lazy-initialised browser context

    def _sleep_if_needed(self) -> None:
        elapsed = time.monotonic() - self.last_request_ts
        wait_for = self.delay_seconds - elapsed
        if wait_for > 0:
            time.sleep(wait_for)

    def get_json(self, url: str, *, timeout: int = 20) -> dict:
        if url in self.json_cache:
            return self.json_cache[url]
        for attempt, backoff in enumerate(HTTP_BACKOFF_SECONDS, start=1):
            if backoff:
                time.sleep(backoff)
            self._sleep_if_needed()
            request = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    body = response.read().decode("utf-8", errors="replace")
                self.last_request_ts = time.monotonic()
                payload = json.loads(body)
                self.json_cache[url] = payload
                return payload
            except urllib.error.HTTPError as exc:
                self.last_request_ts = time.monotonic()
                if exc.code == 429 and attempt < len(HTTP_BACKOFF_SECONDS):
                    continue
                raise RuntimeError(f"HTTP {exc.code} for {url}") from exc

    def get_text(self, url: str, *, timeout: int = 20) -> str:
        if url in self.text_cache:
            return self.text_cache[url]
        for attempt, backoff in enumerate(HTTP_BACKOFF_SECONDS, start=1):
            if backoff:
                time.sleep(backoff)
            self._sleep_if_needed()
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xml,text/xml;q=0.9,*/*;q=0.8",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    body = response.read().decode(charset, errors="replace")
                self.last_request_ts = time.monotonic()
                self.text_cache[url] = body
                return body
            except urllib.error.HTTPError as exc:
                self.last_request_ts = time.monotonic()
                if exc.code == 429 and attempt < len(HTTP_BACKOFF_SECONDS):
                    continue
                raise RuntimeError(f"HTTP {exc.code} for {url}") from exc

    def get_text_rendered(self, url: str, wait_ms: int = RENDER_WAIT_MS) -> str:
        """Fetch a page using a headless browser, wait for JS to render, return full text.

        Falls back to get_text() if Playwright is unavailable or use_browser is False.
        Results are cached the same as get_text().
        """
        if not self.use_browser:
            return self.get_text(url)
        if url in self.text_cache:
            return self.text_cache[url]

        self._sleep_if_needed()

        if self._playwright_ctx is None:
            pw = _sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            self._playwright_ctx = browser.new_context(
                user_agent=USER_AGENT,
                java_script_enabled=True,
            )

        page = self._playwright_ctx.new_page()
        try:
            page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)
            body = page.content()
        finally:
            page.close()

        self.last_request_ts = time.monotonic()
        self.text_cache[url] = body
        return body

    def close(self) -> None:
        """Release the browser if it was opened."""
        if self._playwright_ctx is not None:
            try:
                self._playwright_ctx.close()
            except Exception:
                pass
            self._playwright_ctx = None

    def allowed_by_robots(self, url: str, *, timeout: int = 8) -> bool:
        parsed = urllib.parse.urlsplit(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self.robot_parsers.get(base)
        if parser is None:
            parser = urllib.robotparser.RobotFileParser()
            robots_url = f"{base}/robots.txt"
            try:
                # Fetch with explicit timeout — RobotFileParser.read() has no timeout param
                req = urllib.request.Request(robots_url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                parser.parse(body.splitlines())
            except Exception:
                # Unreachable / timeout / 4xx → treat as allowed (fail open)
                parser.parse([])
            self.robot_parsers[base] = parser
        return parser.can_fetch(USER_AGENT, url)
