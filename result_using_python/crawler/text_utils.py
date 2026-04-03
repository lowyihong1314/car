from __future__ import annotations

import html
import re
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Iterable

from .config import KEYWORDS, NEGATIVE_HINTS
from .models import RowTask

# Tokens that carry no model-identity information and should be ignored when
# extracting model tokens or building search queries.
_NOISE_TOKENS: frozenset[str] = frozenset({
    # Registration / condition status
    "RECOND", "UNREG", "UNREGISTERED", "REGISTERED", "NEW", "USED",
    # Transmission
    "AT", "MT", "CVT", "DCT", "AMT", "DSG", "PDK", "SA", "SA",
    # Drive configuration
    "2WD", "4WD", "AWD", "FWD", "RWD", "4X2", "4X4", "6X2", "6X4", "8X4",
    # Body / trim noise
    "ABS", "HELE", "PI", "SR", "SRS", "QU", "AB", "GAS", "NR",
    "SWB", "LWB", "STD",
    # Common suffix tokens that appear after real model name
    "SEDAN", "HATCHBACK", "WAGON", "ESTATE", "COUPE", "CONVERTIBLE",
    "SUV", "MPV", "VAN", "TRUCK", "LORRY", "CARGO", "TIPPER", "RIGID",
    "BUS", "COACH", "FLATBED", "CURTAINSIDER", "REFRIGERATED",
    # Parenthesised transmission shorthand — stripped by regex already but keep as safety
    "A", "D", "L", "R", "V", "E", "T", "N", "S", "X",
})

_SKIP_LINK_PREFIXES: tuple[str, ...] = (
    "javascript:",
    "mailto:",
    "tel:",
    "data:",
    "about:",
    "#",
)

_SKIP_LINK_SUFFIXES: tuple[str, ...] = (
    ".css",
    ".js",
    ".json",
    ".svg",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
    ".pdf",
    ".zip",
    ".xml",
)

_OFFICIAL_HUB_KEYWORDS: tuple[str, ...] = (
    "/model",
    "/models",
    "/vehicle",
    "/vehicles",
    "/car",
    "/cars",
    "/range",
    "/lineup",
    "/product",
    "/products",
    "/truck",
    "/trucks",
    "/suv",
    "/sedan",
    "/pickup",
    "/commercial",
)

_OFFICIAL_SKIP_KEYWORDS: tuple[str, ...] = (
    "/legal",
    "/privacy",
    "/cookie",
    "/cookies",
    "/contact",
    "/dealer",
    "/dealers",
    "/retailer",
    "/retailers",
    "/owner",
    "/owners",
    "/career",
    "/careers",
    "/news",
    "/press",
    "/media",
    "/investor",
    "/sustainability",
    "/compliance",
    "/configurator",
    "/finance",
    "/accessories",
    "/brochure",
    "/warranty",
    "/service",
)

_MODEL_PAGE_SKIP_KEYWORDS: tuple[str, ...] = (
    "/support/",
    "/help/",
    "/video-tutorials/",
    "/inside-audi/",
    "/owners/",
    "/inventory/",
    "/search/",
    "/build/",
    "/configurator/",
    "/shopping-tools/",
    "/dealer/",
    "/retailer/",
    "/tools/",
    "/form/",
    "/legal/",
    "/layer/",
)

_ENGINE_SENTENCE_TERMS: tuple[str, ...] = (
    "powered by",
    "engine powers",
    "gasoline direct injection",
    "turbocharged",
    "twin-turbo",
    "engine",
    "horsepower",
    "torque",
    "cylinder",
    "v6",
    "v8",
    "v10",
    "v12",
)


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def extract_brand(car_info: str) -> str:
    """Return the brand token normalized to UPPER_SNAKE_CASE (hyphens → underscores)."""
    raw = car_info.split(" ", 1)[0].strip()
    return raw.upper().replace("-", "_")


def _clean_parts(car_info: str, brand: str) -> list[str]:
    """Split car_info into meaningful model tokens, removing brand repetitions and noise."""
    text = car_info.replace("_", " ")
    text = re.sub(r"\([^)]*\)", " ", text)   # strip parenthesised groups like (A)
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", text) if p]
    brand_core = brand.replace("_", "").upper()
    brand_parts = {part for part in brand.replace("_", " ").upper().split() if part}
    result = []
    for part in parts:
        up = part.upper()
        # skip brand repetitions (exact or compacted, e.g. MERCEDES == MERCEDESBENZ stripped)
        if up == brand.upper() or up == brand_core or up in brand_parts:
            continue
        # skip pure noise tokens
        if up in _NOISE_TOKENS:
            continue
        result.append(part.lower())
    return result


def extract_model_tokens(car_info: str, brand: str) -> list[str]:
    return _clean_parts(car_info, brand)


def extract_discovery_tokens(car_info: str, brand: str) -> list[str]:
    """Return tokens suitable for URL matching and search discovery."""
    result: list[str] = []
    for token in _clean_parts(car_info, brand):
        if token.isdigit() and len(token) < 3:
            continue
        if len(token) == 1:
            continue
        result.append(token)
    return result


def normalize_query_text(car_info: str, brand: str = "") -> str:
    """Return a cleaned query string with noise words removed."""
    parts = _clean_parts(car_info, brand) if brand else car_info.replace("_", " ").split()
    return " ".join(parts).strip()


def strip_html(html_text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_meta_text(html_text: str) -> str:
    values: list[str] = []

    title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
    if title_match:
        values.append(strip_html(title_match.group(1)))

    meta_patterns = (
        r'(?is)<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\'](.*?)["\']',
    )
    for pattern in meta_patterns:
        for value in re.findall(pattern, html_text):
            cleaned = html.unescape(str(value)).strip()
            if cleaned:
                values.append(cleaned)

    return " ".join(dedupe_preserve_order(values)).strip()


def extract_page_text(html_text: str) -> str:
    meta_text = extract_meta_text(html_text)
    body_text = strip_html(html_text)
    combined = " ".join(part for part in (meta_text, body_text) if part)
    return re.sub(r"\s+", " ", combined).strip()


def canonicalize_http_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        return url
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc.lower(), path or "/", "", ""))


def extract_internal_links(
    html_text: str,
    base_url: str,
    *,
    allowed_domains: set[str] | None = None,
) -> list[str]:
    links: list[str] = []
    for raw_href in re.findall(r'(?is)href=["\']([^"\']+)["\']', html_text):
        href = html.unescape(raw_href).strip()
        if not href:
            continue
        lowered_href = href.lower()
        if lowered_href.startswith(_SKIP_LINK_PREFIXES):
            continue

        absolute = canonicalize_http_url(urllib.parse.urljoin(base_url, href))
        parsed = urllib.parse.urlsplit(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if allowed_domains and parsed.netloc.lower() not in allowed_domains:
            continue
        lowered_path = parsed.path.lower()
        if lowered_path.endswith(_SKIP_LINK_SUFFIXES):
            continue
        links.append(absolute)
    return dedupe_preserve_order(links)


def extract_robots_sitemaps(robots_text: str) -> list[str]:
    urls = [html.unescape(match.strip()) for match in re.findall(r"(?im)^sitemap:\s*(\S+)", robots_text)]
    return dedupe_preserve_order(url for url in urls if url.startswith(("http://", "https://")))


def score_hub_url(url: str) -> int:
    lowered = canonicalize_http_url(url).lower()
    path = urllib.parse.urlsplit(lowered).path or "/"
    segments = [segment for segment in path.strip("/").split("/") if segment]

    if any(keyword in path for keyword in _OFFICIAL_SKIP_KEYWORDS):
        return 0

    if not segments:
        return 2

    if re.fullmatch(r"/[a-z]{2}(?:-[a-z]{2})?", path):
        return 3

    primary_hubs = {"models", "vehicles", "cars", "range", "lineup", "products"}
    secondary_hubs = {
        "suv-models",
        "electric-models",
        "commercial-vehicles",
        "all-cars",
        "our-range",
        "suv",
        "sports-cars",
        "saloon-cars",
        "trucks",
    }

    if len(segments) <= 2 and any(segment in primary_hubs for segment in segments):
        return 4
    if len(segments) <= 3 and any(segment in secondary_hubs for segment in segments):
        return 3
    return 0


def focus_text_on_model(text: str, car_info: str) -> str:
    brand = car_info.split(" ", 1)[0].strip().upper().replace("-", "_") if car_info else ""
    tokens = extract_discovery_tokens(car_info, brand)
    if not tokens:
        return text

    lowered = text.lower()
    windows: list[str] = []
    for token in tokens[:3]:
        start = 0
        hits = 0
        while hits < 6:
            index = lowered.find(token, start)
            if index == -1:
                break
            hits += 1
            start = index + len(token)
            left = max(0, index - 450)
            right = min(len(text), index + 700)
            window = text[left:right].strip()
            if window:
                windows.append(window)

    if not windows:
        return text

    combined = " ... ".join(dedupe_preserve_order(windows))
    return re.sub(r"\s+", " ", combined).strip()


def build_search_queries(task: RowTask) -> list[str]:
    brand = task.brand.replace("_", " ")
    tokens = extract_discovery_tokens(task.car_info, task.brand)
    short_model = " ".join(tokens[:3])
    queries = [f"{brand} {short_model}".strip(), f"{brand} {' '.join(tokens)}".strip()]
    return dedupe_preserve_order(q for q in queries if q.strip())


def appears_relevant(text: str, task: RowTask) -> bool:
    """Return True only if at least one meaningful model token appears in the page text.

    Brand name alone is not sufficient — every page on ford.com contains 'ford',
    so we require at least one non-brand, non-noise model token to be present.
    """
    lowered = text.lower()
    meaningful = extract_discovery_tokens(task.car_info, task.brand)
    if not meaningful:
        # Fallback: accept if brand appears (no model to match against)
        brand_tokens = [t.lower() for t in re.split(r"[^a-zA-Z0-9]+", task.brand) if t]
        return any(t in lowered for t in brand_tokens[:2])
    return any(t in lowered for t in meaningful[:4])


def _infer_petrol_from_engine_context(lowered: str) -> tuple[int, bool]:
    if any(keyword in lowered for keyword in KEYWORDS["electric"]):
        return 0, False
    if any(keyword in lowered for keyword in KEYWORDS["electric/petrol"]):
        return 0, False
    if any(keyword in lowered for keyword in KEYWORDS["diesel"]):
        return 0, False

    if "gasoline direct injection" in lowered or "gasoline engine" in lowered:
        return 4, True
    if "gasoline" in lowered:
        return 3, True

    petrol_patterns = (
        r"\bpowered by\b[^.]{0,160}\b(?:twin[- ]turbo|turbocharged)\b[^.]{0,160}\b(?:engine|three-cylinder|four-cylinder|five-cylinder|six-cylinder|v6|v8|v10|v12)\b",
        r"\b(?:twin[- ]turbo|turbocharged)\b[^.]{0,120}\b(?:three-cylinder|four-cylinder|five-cylinder|six-cylinder|v6|v8|v10|v12)\b",
        r"\b\d(?:\.\d)?-lit(?:er|re)\b[^.]{0,80}\b(?:engine|three-cylinder|four-cylinder|five-cylinder|six-cylinder|v6|v8|v10|v12)\b",
    )
    for pattern in petrol_patterns:
        if re.search(pattern, lowered):
            return 2, False

    return 0, False


def score_text(text: str, car_info: str = "") -> tuple[str, int, bool]:
    focused_text = focus_text_on_model(text, car_info) if car_info else text
    lowered = focused_text.lower()
    scores = Counter()

    for fuel_type, keywords in KEYWORDS.items():
        for keyword in keywords:
            count = lowered.count(keyword)
            if count:
                scores[fuel_type] += count

    hybrid_score = scores.get("electric/petrol", 0)
    electric_score = scores.get("electric", 0)
    diesel_score = scores.get("diesel", 0)
    petrol_score = scores.get("petrol", 0)

    # Hybrid wins only when it clearly dominates.
    # If the car_info string itself contains a hybrid keyword (e.g. "HYBRID", "PHEV"),
    # we trust the page at face value.  Otherwise require the hybrid page score to be
    # at least 2× every competing fuel type — this prevents a model-lineup page that
    # *also* has a hybrid variant from overriding the primary powertrain of the
    # specific non-hybrid vehicle we are looking up.
    car_info_lower = car_info.lower()
    car_info_has_hybrid = any(kw in car_info_lower for kw in KEYWORDS["electric/petrol"])
    rival = max(diesel_score, petrol_score)
    # car_info explicitly says hybrid → normal threshold
    # car_info has no hybrid hint → require hybrid to dominate 3× AND appear at least 3 times
    # (prevents a single sidebar/footer mention from overriding a petrol/diesel page)
    if car_info_has_hybrid:
        hybrid_ok = hybrid_score > 0 and hybrid_score >= rival
    else:
        hybrid_ok = hybrid_score >= 3 and hybrid_score >= rival * 3
    if hybrid_ok:
        return "electric/petrol", hybrid_score + electric_score + petrol_score, True

    strong_electric = any(
        phrase in lowered for phrase in ("battery electric", "fully electric", "all-electric", "electric vehicle", "electric motor")
    )
    if electric_score > max(diesel_score, petrol_score) and (strong_electric or electric_score >= 2):
        contradicted = any(term in lowered for term in NEGATIVE_HINTS["electric"])
        return "electric", electric_score, not contradicted

    explicit_car_hint = any(
        keyword in car_info_lower
        for keyword in (
            *KEYWORDS["electric/petrol"],
            *KEYWORDS["electric"],
            *KEYWORDS["diesel"],
            *KEYWORDS["petrol"],
        )
    )
    substantial_scores = [score for score in (hybrid_score, electric_score, diesel_score, petrol_score) if score >= 3]
    if len(substantial_scores) >= 2 and not explicit_car_hint:
        return "unknown", max(substantial_scores), False

    if diesel_score > 0 and petrol_score > 0 and abs(diesel_score - petrol_score) <= 1:
        return "unknown", max(diesel_score, petrol_score), False

    if diesel_score > petrol_score:
        contradicted = any(term in lowered for term in NEGATIVE_HINTS["diesel"])
        return "diesel", diesel_score, not contradicted
    if petrol_score > 0:
        contradicted = any(term in lowered for term in NEGATIVE_HINTS["petrol"])
        return "petrol", petrol_score, not contradicted

    inferred_score, inferred_prove = _infer_petrol_from_engine_context(lowered)
    if inferred_score > 0:
        return "petrol", inferred_score, inferred_prove

    return "unknown", 0, False


def extract_sitemap_urls(xml_text: str) -> list[str]:
    cleaned = xml_text.replace("<![CDATA[", "").replace("]]>", "")
    try:
        root = ET.fromstring(cleaned)
        urls = []
        for element in root.iter():
            if element.tag.split("}")[-1].lower() == "loc" and element.text:
                urls.append(html.unescape(element.text.strip()))
        if urls:
            return dedupe_preserve_order(urls)
    except ET.ParseError:
        pass

    urls = [
        html.unescape(match.strip())
        for match in re.findall(r"<loc>\s*(.*?)\s*</loc>", cleaned, flags=re.IGNORECASE | re.DOTALL)
    ]
    return dedupe_preserve_order(urls)


def normalize_url_for_match(url: str) -> str:
    return urllib.parse.unquote(url).lower()


def is_sitemap_index(xml_text: str) -> bool:
    return bool(re.search(r"<sitemapindex[\s>]", xml_text, flags=re.IGNORECASE))


def sort_sitemaps_by_priority(sitemap_urls: list[str], priority_keywords: list[str]) -> list[str]:
    def priority_score(url: str) -> int:
        lowered = url.lower()
        return sum(1 for kw in priority_keywords if kw in lowered)
    return sorted(sitemap_urls, key=priority_score, reverse=True)


def score_url_for_model(url: str, tokens: list[str], brand: str) -> int:
    lowered = normalize_url_for_match(url)
    parsed_path = urllib.parse.urlsplit(url).path.lower()
    if any(keyword in parsed_path for keyword in _MODEL_PAGE_SKIP_KEYWORDS):
        return 0
    path_segments = [segment for segment in parsed_path.split("/") if segment]
    score = 0
    matched_token = False
    for token in tokens[:4]:
        if not token:
            continue
        exact_segment_matches = sum(1 for segment in path_segments if segment == token)
        if exact_segment_matches:
            score += 4 * exact_segment_matches
            matched_token = True
        if token in parsed_path:
            score += 3
            matched_token = True
        elif token in lowered:
            score += 1
            matched_token = True
    if not matched_token:
        return 0
    brand_text = brand.replace("_", "").lower()
    if brand_text and brand_text in parsed_path.replace("-", "").replace("/", ""):
        score += 1
    if parsed_path.count("/") <= 3:
        score += 1
    return score
