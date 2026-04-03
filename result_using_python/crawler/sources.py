from __future__ import annotations

from collections import deque
import html
import re
import urllib.parse

from .config import (
    BRAND_DEFAULTS,
    BRAND_URL_PATTERNS,
    CAR_INFO_PATTERNS,
    OFFICIAL_CANDIDATE_LIMIT,
    OFFICIAL_CRAWL_MAX_DEPTH,
    OFFICIAL_CRAWL_MAX_LINKS_PER_PAGE,
    OFFICIAL_CRAWL_MAX_PAGES,
    OFFICIAL_DOMAINS,
    SITEMAP_NESTED_LIMIT,
    SITEMAP_PRIORITY_KEYWORDS,
    WIKIPEDIA_SEARCH_API,
    WIKIPEDIA_SEARCH_LIMIT,
    WIKIPEDIA_SUMMARY_API,
)
from .http_client import SimpleHttpClient
from .models import CandidateResult, RowTask, unknown_result
from .text_utils import (
    appears_relevant,
    build_search_queries,
    canonicalize_http_url,
    dedupe_preserve_order,
    extract_discovery_tokens,
    extract_internal_links,
    extract_page_text,
    extract_robots_sitemaps,
    extract_sitemap_urls,
    is_sitemap_index,
    normalize_url_for_match,
    score_text,
    score_hub_url,
    score_url_for_model,
    sort_sitemaps_by_priority,
)

_COMMON_HUB_PATHS: tuple[str, ...] = (
    "/",
    "/models",
)

_COMMON_SITEMAP_PATHS: tuple[str, ...] = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
)


def build_candidate_result(
    fuel_type: str,
    url: str,
    url_prove: bool,
    score: int,
) -> CandidateResult:
    return CandidateResult(
        fuel_type=fuel_type,
        url=url,
        url_prove=bool(url and url_prove),
        score=score,
    )


def choose_official_candidate_urls(
    task: RowTask, sitemap_urls: list[str], max_urls: int = OFFICIAL_CANDIDATE_LIMIT
) -> list[str]:
    tokens = extract_discovery_tokens(task.car_info, task.brand)
    if not tokens:
        return []

    scored: list[tuple[int, str]] = []
    for url in sitemap_urls:
        score = score_url_for_model(url, tokens, task.brand)
        if score > 0:
            scored.append((score, url))

    scored.sort(key=lambda item: (-item[0], len(item[1])))
    ordered_urls = [url for _, url in scored]
    return dedupe_preserve_order(ordered_urls)[:max_urls]


def brand_pattern_candidates(task: RowTask) -> list[str]:
    patterns = BRAND_URL_PATTERNS.get(task.brand, [])
    tokens = extract_discovery_tokens(task.car_info, task.brand)
    if not tokens or not patterns:
        return []
    model_slug = "-".join(tokens[:3])
    model_compact = "".join(tokens[:3])
    model_base = tokens[0]  # just the base model name, e.g. "triton" or "xpander"
    candidates: list[str] = []
    for pattern in patterns:
        candidates.append(pattern.replace("{model}", model_slug))
        if model_compact != model_slug:
            candidates.append(pattern.replace("{model}", model_compact))
        if model_base != model_slug and model_base != model_compact:
            candidates.append(pattern.replace("{model}", model_base))
    return candidates


def _official_base_url(domain: str) -> str:
    return f"https://{domain}"


def _emit_log(log_callback, message: str) -> None:
    if log_callback:
        log_callback(message)


def _discover_from_sitemaps(
    client: SimpleHttpClient,
    task: RowTask,
    domain: str,
    *,
    log_callback=None,
) -> list[str]:
    base_url = _official_base_url(domain)
    sitemap_candidates: list[str] = []
    robots_sitemaps: list[str] = []

    robots_url = f"{base_url}/robots.txt"
    try:
        robots_text = client.get_text(robots_url, timeout=8)
        robots_sitemaps = extract_robots_sitemaps(robots_text)
    except Exception:
        pass

    sitemap_candidates.extend(robots_sitemaps or (f"{base_url}{path}" for path in _COMMON_SITEMAP_PATHS))

    discovered_urls: list[str] = []
    for sitemap_url in dedupe_preserve_order(sitemap_candidates)[:6]:
        try:
            if not client.allowed_by_robots(sitemap_url):
                _emit_log(log_callback, f"Skip sitemap by robots: {sitemap_url}")
                continue
            sitemap_text = client.get_text(sitemap_url, timeout=10)
        except Exception:
            _emit_log(log_callback, f"Sitemap fetch failed: {sitemap_url}")
            continue

        urls = extract_sitemap_urls(sitemap_text)
        if not urls:
            _emit_log(log_callback, f"Sitemap empty: {sitemap_url}")
            continue

        if is_sitemap_index(sitemap_text):
            nested_urls: list[str] = []
            sorted_nested = sort_sitemaps_by_priority(urls, SITEMAP_PRIORITY_KEYWORDS)
            for nested in sorted_nested[:SITEMAP_NESTED_LIMIT]:
                try:
                    if not client.allowed_by_robots(nested):
                        _emit_log(log_callback, f"Skip nested sitemap by robots: {nested}")
                        continue
                    nested_text = client.get_text(nested, timeout=10)
                except Exception:
                    _emit_log(log_callback, f"Nested sitemap fetch failed: {nested}")
                    continue
                nested_urls.extend(extract_sitemap_urls(nested_text))
                if len(choose_official_candidate_urls(task, nested_urls)) >= OFFICIAL_CANDIDATE_LIMIT:
                    break
            urls = nested_urls

        for url in choose_official_candidate_urls(task, urls):
            normalized = canonicalize_http_url(url)
            if client.allowed_by_robots(normalized):
                discovered_urls.append(normalized)

    unique_urls = dedupe_preserve_order(discovered_urls)[:OFFICIAL_CANDIDATE_LIMIT]
    if unique_urls:
        _emit_log(log_callback, f"Sitemap candidates on {domain}: {len(unique_urls)}")
    return unique_urls


def _discover_by_internal_crawl(
    client: SimpleHttpClient,
    task: RowTask,
    domain: str,
    *,
    log_callback=None,
) -> list[str]:
    base_url = _official_base_url(domain)
    allowed_domains = {domain.lower()}
    queue: deque[tuple[str, int]] = deque(
        (canonicalize_http_url(f"{base_url}{path}"), 0) for path in _COMMON_HUB_PATHS
    )
    visited: set[str] = set()
    scored_candidates: list[tuple[int, str]] = []

    tokens = extract_discovery_tokens(task.car_info, task.brand)
    if not tokens:
        return []

    while queue and len(visited) < OFFICIAL_CRAWL_MAX_PAGES:
        url, depth = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            if not client.allowed_by_robots(url):
                _emit_log(log_callback, f"Skip page by robots: {url}")
                continue
            html_text = client.get_text(url, timeout=10)
        except Exception:
            _emit_log(log_callback, f"Page fetch failed: {url}")
            continue

        self_score = score_url_for_model(url, tokens, task.brand)
        if self_score > 0:
            scored_candidates.append((self_score + 1, url))

        links = extract_internal_links(html_text, url, allowed_domains=allowed_domains)
        page_candidates: list[tuple[int, str]] = []
        hub_links: list[tuple[int, str]] = []
        for link in links[:OFFICIAL_CRAWL_MAX_LINKS_PER_PAGE]:
            candidate_score = score_url_for_model(link, tokens, task.brand)
            if candidate_score > 0:
                page_candidates.append((candidate_score, link))
                continue
            if depth >= OFFICIAL_CRAWL_MAX_DEPTH:
                continue
            hub_score = score_hub_url(link)
            if hub_score > 0:
                hub_links.append((hub_score, link))

        scored_candidates.extend(page_candidates)
        if page_candidates:
            continue

        def hub_sort_key(item: tuple[int, str]) -> tuple[int, int, int]:
            score, link = item
            path = urllib.parse.urlsplit(link).path.lower()
            locale_bonus = 0
            if path == "/en" or path.startswith("/en/"):
                locale_bonus = 3
            elif path.startswith("/en-"):
                locale_bonus = 2
            return (-score, -locale_bonus, len(link))

        hub_links.sort(key=hub_sort_key)
        for _, link in hub_links[:6]:
            if link not in visited:
                queue.append((link, depth + 1))

    scored_candidates.sort(key=lambda item: (-item[0], len(item[1])))
    unique_urls = dedupe_preserve_order(url for _, url in scored_candidates)[:OFFICIAL_CANDIDATE_LIMIT]
    if unique_urls:
        _emit_log(log_callback, f"Internal crawl candidates on {domain}: {len(unique_urls)}")
    return unique_urls


def official_domain_candidates(client: SimpleHttpClient, task: RowTask, *, log_callback=None) -> list[str]:
    candidates: list[str] = []
    for domain in OFFICIAL_DOMAINS.get(task.brand, []):
        _emit_log(log_callback, f"Scan official domain: {domain}")
        before_count = len(dedupe_preserve_order(candidates))
        candidates.extend(_discover_by_internal_crawl(client, task, domain, log_callback=log_callback))
        after_count = len(dedupe_preserve_order(candidates))
        if after_count == before_count:
            _emit_log(log_callback, f"No crawl hit on {domain}, try sitemaps")
            candidates.extend(_discover_from_sitemaps(client, task, domain, log_callback=log_callback))

    for url in brand_pattern_candidates(task):
        try:
            normalized = canonicalize_http_url(url)
            if normalized not in candidates and client.allowed_by_robots(normalized):
                candidates.append(normalized)
                _emit_log(log_callback, f"Brand pattern candidate: {normalized}")
        except Exception:
            continue

    return dedupe_preserve_order(candidates)[:OFFICIAL_CANDIDATE_LIMIT]


def wikipedia_search(
    client: SimpleHttpClient, query: str, limit: int = WIKIPEDIA_SEARCH_LIMIT
) -> list[str]:
    params = urllib.parse.urlencode(
        {
            "action": "query",
            "format": "json",
            "list": "search",
            "srsearch": query,
            "srlimit": str(limit),
        }
    )
    try:
        payload = client.get_json(f"{WIKIPEDIA_SEARCH_API}?{params}")
    except RuntimeError:
        return []

    titles = []
    for item in payload.get("query", {}).get("search", []):
        title = item.get("title")
        if title:
            titles.append(str(title))
    return titles


def wikipedia_summary(client: SimpleHttpClient, title: str) -> dict | None:
    encoded_title = urllib.parse.quote(title, safe="")
    try:
        return client.get_json(f"{WIKIPEDIA_SUMMARY_API}{encoded_title}")
    except Exception:
        return None


def summary_to_text(summary: dict) -> tuple[str, str]:
    text_parts = [
        str(summary.get("title", "")),
        str(summary.get("extract", "")),
        str(summary.get("description", "")),
    ]
    combined = html.unescape(" ".join(part for part in text_parts if part)).strip()

    url = ""
    content_urls = summary.get("content_urls", {})
    desktop = content_urls.get("desktop", {})
    if isinstance(desktop, dict):
        url = str(desktop.get("page", "")).strip()

    return combined, url


_URL_HYBRID_MARKERS = frozenset([
    "hybrid", "phev", "mhev", "plug-in", "electric", "-ev", "/ev", "e-tron",
    "ioniq", "bev", "plug_in",
])


def _url_implies_hybrid(url: str) -> bool:
    """Return True if the URL path itself signals a hybrid/EV variant page."""
    path = urllib.parse.urlsplit(url).path.lower()
    return any(marker in path for marker in _URL_HYBRID_MARKERS)


def _is_exact_model_candidate(url: str, task: RowTask) -> bool:
    tokens = extract_discovery_tokens(task.car_info, task.brand)
    if not tokens:
        return False

    path = urllib.parse.urlsplit(url).path.lower().rstrip("/")
    token = tokens[0]
    exact_patterns = (
        rf"/{re.escape(token)}$",
        rf"/{re.escape(token)}/overview$",
        rf"/{re.escape(token)}/{re.escape(token)}/\d{{4}}/overview$",
    )
    return any(re.search(pattern, path) for pattern in exact_patterns)


def classify_from_official_sites(client: SimpleHttpClient, task: RowTask, *, log_callback=None) -> CandidateResult:
    car_info_lower = task.car_info.lower()
    car_info_has_hybrid = any(kw in car_info_lower for kw in ["hybrid", "phev", "mhev",
                                                               "plug-in", "electric", "ev",
                                                               "ioniq", "e-tron", "bev"])
    best_unknown_url = ""
    candidate_urls = official_domain_candidates(client, task, log_callback=log_callback)
    if candidate_urls:
        _emit_log(log_callback, f"Official candidates found: {len(candidate_urls)}")
    else:
        _emit_log(log_callback, "Official candidates found: 0")
    for url in candidate_urls:
        # Skip hybrid/EV-specific pages when this vehicle isn't a hybrid
        if not car_info_has_hybrid and _url_implies_hybrid(url):
            _emit_log(log_callback, f"Skip hybrid-looking URL: {url}")
            continue
        try:
            _emit_log(log_callback, f"Check official URL: {url}")
            text = extract_page_text(client.get_text_rendered(url))
        except Exception:
            _emit_log(log_callback, f"Official URL failed: {url}")
            continue
        if not appears_relevant(text, task):
            _emit_log(log_callback, f"Official URL not relevant: {url}")
            continue
        if not best_unknown_url:
            best_unknown_url = url
        fuel_type, score, url_prove = score_text(text, task.car_info)
        if fuel_type == "unknown":
            if _is_exact_model_candidate(url, task):
                _emit_log(log_callback, f"Official exact model page found but fuel unknown: {url}")
                return build_candidate_result("unknown", url, False, 0)
            continue
        _emit_log(log_callback, f"Official match: {fuel_type} @ {url}")
        return build_candidate_result(fuel_type, url, url_prove, score)
    if best_unknown_url:
        _emit_log(log_callback, f"Keep official URL with unknown fuel: {best_unknown_url}")
        return build_candidate_result("unknown", best_unknown_url, False, 0)
    _emit_log(log_callback, "Official sources produced no usable match")
    return unknown_result()


def classify_from_wikipedia(client: SimpleHttpClient, task: RowTask, *, log_callback=None) -> CandidateResult:
    seen_titles: set[str] = set()
    car_info_lower = task.car_info.lower()
    car_info_has_hybrid = any(kw in car_info_lower for kw in ["hybrid", "phev", "mhev",
                                                               "plug-in", "electric", "ev",
                                                               "ioniq", "e-tron", "bev"])

    for query in build_search_queries(task):
        _emit_log(log_callback, f"Wikipedia query: {query}")
        for title in wikipedia_search(client, query):
            if title in seen_titles:
                continue
            seen_titles.add(title)

            # Skip Wikipedia articles about hybrid/EV variants when this vehicle isn't one
            title_lower = title.lower()
            if not car_info_has_hybrid and any(m in title_lower for m in
                    ["hybrid", "electric", "phev", "e-tron", "ioniq", "bev", "plug-in"]):
                _emit_log(log_callback, f"Skip Wikipedia hybrid page: {title}")
                continue

            summary = wikipedia_summary(client, title)
            if not summary:
                _emit_log(log_callback, f"Wikipedia summary missing: {title}")
                continue

            combined, url = summary_to_text(summary)
            if not combined:
                continue
            if not appears_relevant(combined, task):
                _emit_log(log_callback, f"Wikipedia page not relevant: {title}")
                continue

            fuel_type, score, url_prove = score_text(combined, task.car_info)
            if fuel_type == "unknown":
                _emit_log(log_callback, f"Wikipedia page inconclusive: {title}")
                continue

            _emit_log(log_callback, f"Wikipedia match: {fuel_type} @ {title}")
            return build_candidate_result(fuel_type, url, url_prove, score)

    _emit_log(log_callback, "Wikipedia fallback produced no usable match")
    return unknown_result()


def classify_from_car_info(task: RowTask, *, log_callback=None) -> CandidateResult:
    """Classify purely from the car_info string itself — no HTTP needed.

    Checks two sources in order:
    1. CAR_INFO_PATTERNS — regex rules for model-series patterns (e.g. Nissan CD/CW trucks)
    2. KEYWORDS — explicit fuel-type words in the string (DIESEL, PETROL, HYBRID, etc.)
    """
    from .config import KEYWORDS  # local import avoids circular dependency risk

    # 1. Regex pattern rules (brand+model series → known fuel type)
    for pattern, fuel_type, evidence_url in CAR_INFO_PATTERNS:
        if pattern.search(task.car_info):
            _emit_log(log_callback, f"car_info regex hit: {fuel_type}")
            return build_candidate_result(fuel_type, evidence_url, bool(evidence_url), 1)

    # 2. Keyword scan (priority: hybrid > electric > diesel > petrol)
    lowered = task.car_info.lower()
    for fuel_type in ("electric/petrol", "electric", "diesel", "petrol"):
        for kw in KEYWORDS[fuel_type]:
            if kw in lowered:
                _emit_log(log_callback, f"car_info keyword hit: {fuel_type} ({kw})")
                return build_candidate_result(fuel_type, "", False, 1)

    return unknown_result()


def classify_vehicle(client: SimpleHttpClient, task: RowTask, *, log_callback=None) -> CandidateResult:
    # 1. Brand-level default (e.g. TESLA → electric, HINO → diesel)
    brand_default = BRAND_DEFAULTS.get(task.brand.upper())
    if brand_default:
        fuel_type, evidence_url = brand_default
        _emit_log(log_callback, f"brand default hit: {fuel_type}")
        return build_candidate_result(fuel_type, evidence_url, bool(evidence_url), 1)

    # 2. Car_info contains an explicit fuel-type keyword — no HTTP needed
    hint = classify_from_car_info(task, log_callback=log_callback)
    if hint.fuel_type != "unknown":
        return hint

    # 3. Official brand website via sitemap
    _emit_log(log_callback, "No local rule hit, checking official sources")
    official_result = classify_from_official_sites(client, task, log_callback=log_callback)
    if official_result.fuel_type != "unknown" or official_result.url:
        return official_result

    # 4. Wikipedia fallback
    _emit_log(log_callback, "Falling back to Wikipedia")
    result = classify_from_wikipedia(client, task, log_callback=log_callback)
    if result.fuel_type == "unknown" and not result.url:
        _emit_log(log_callback, "Final result remains unknown")
    return result
