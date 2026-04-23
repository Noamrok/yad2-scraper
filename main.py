"""
Yad2 Real Estate Scraper
Configure RentConfig / BuyConfig and SEARCH_MODE at the bottom, then run.
Runs daily and sends the top listing via WhatsApp (Green API).
"""

import asyncio
import json
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlencode
from playwright.async_api import async_playwright, Page


# ─── WHATSAPP / GREEN API ─────────────────────────────────────────────────────

GREENAPI_INSTANCE = os.environ.get("GREENAPI_INSTANCE", "7107595331")
GREENAPI_TOKEN    = os.environ.get("GREENAPI_TOKEN",    "8595fb6709ca44c69673d31784d40e50017e6cc9a78f41e9b0")
WHATSAPP_PHONES   = ["972527924155"] #+ ["972526938256"]  # add more numbers as needed
WHATSAPP_ENABLED  = True  # set to False to disable sending (debug mode)

# ─── SEARCH MODE ──────────────────────────────────────────────────────────────
# "rent"  → only rental search  (currently enabled)
# "buy"   → only purchase search
# "both"  → run both

SEARCH_MODE = "rent"


# ─── CONFIGURATION ────────────────────────────────────────────────────────────

@dataclass
class RentConfig:
    cities: list = field(default_factory=lambda: ["זכרון יעקב", "פרדס חנה כרכור", "בנימינה גבעת עדה", ])
    min_price: Optional[int] = None
    max_price: Optional[int] = 6000
    min_rooms: Optional[float] = 3
    max_rooms: Optional[float] = 5
    min_floor: Optional[int] = None
    max_floor: Optional[int] = None
    property_types: list = field(default_factory=lambda: [])  # 1=דירה, 2=בית פרטי — empty=הכל
    ac: bool = True
    elevator: bool = False
    parking: bool = False
    mamad: bool = False
    exclude_neighborhoods: list = field(default_factory=lambda: ["שיכון המסילה"])

@dataclass
class BuyConfig:
    cities: list = field(default_factory=lambda: ["פרדס חנה כרכור", "בנימינה גבעת עדה", "זכרון יעקב"])
    min_price: Optional[int] = None
    max_price: Optional[int] = 2_000_000
    min_rooms: Optional[float] = None
    max_rooms: Optional[float] = None
    min_floor: Optional[int] = None
    max_floor: Optional[int] = None
    property_types: list = field(default_factory=lambda: [])  # 1=דירה, 2=בית פרטי — empty=הכל
    price_dropped: bool = False
    pinuy_binuy: bool = True
    mamad: bool = False
    exclude_neighborhoods: list = field(default_factory=lambda: [])


# ─── CITY / AREA / REGION TABLE ───────────────────────────────────────────────
# Known cities for instant lookup; unknown cities are resolved via autocomplete API.

REGION_SLUG = {
    1: "center-and-sharon",
    2: "south",
    3: "tel-aviv-area",
    4: "north",
    5: "coastal-north",
    6: "jerusalem-area",
    7: "judea-and-samaria",
}

CITIES = {
    "פרדס חנה כרכור": dict(city=7800, area=15, region=5, slug="coastal-north"),
    "חדרה":            dict(city=6700, area=15, region=5, slug="coastal-north"),
    "זכרון יעקב":      dict(city=9300, area=67, region=5, slug="coastal-north"),
    "קיסריה":          dict(city=1260, area=15, region=5, slug="coastal-north"),
    "חיפה":            dict(city=4000, area=5,  region=5, slug="coastal-north"),
    "תל אביב יפו":     dict(city=5000, area=1,  region=3, slug="tel-aviv-area"),
    "רמת גן":          dict(city=8300, area=9,  region=1, slug="center-and-sharon"),
    "גבעתיים":         dict(city=6300, area=3,  region=3, slug="tel-aviv-area"),
    "חולון":           dict(city=6900, area=42, region=1, slug="center-and-sharon"),
    "בת ים":           dict(city=6200, area=11, region=3, slug="tel-aviv-area"),
    "נתניה":           dict(city=7400, area=17, region=1, slug="center-and-sharon"),
    "הרצליה":          dict(city=6400, area=18, region=1, slug="center-and-sharon"),
    "רעננה":           dict(city=8700, area=42, region=1, slug="center-and-sharon"),
    "כפר סבא":         dict(city=6900, area=42, region=1, slug="center-and-sharon"),
    "פתח תקווה":       dict(city=7900, area=4,  region=1, slug="center-and-sharon"),
    "ראשון לציון":     dict(city=8600, area=3,  region=3, slug="tel-aviv-area"),
    "רחובות":          dict(city=8400, area=12, region=1, slug="center-and-sharon"),
    "ירושלים":         dict(city=3000, area=7,  region=6, slug="jerusalem-area"),
    "באר שבע":         dict(city=7100, area=21, region=2, slug="south"),
    "אשדוד":           dict(city=70,   area=21, region=2, slug="south"),
    "אשקלון":          dict(city=7100, area=21, region=2, slug="south"),
}


# ─── URL BUILDER ──────────────────────────────────────────────────────────────

async def resolve_city(page: Page, city_name: str) -> dict:
    if city_name in CITIES:
        return CITIES[city_name]
    encoded = city_name.replace(" ", "%20")
    api = f"https://gw.yad2.co.il/address-autocomplete/realestate/v2?text={encoded}&lang=he"
    try:
        resp = await page.request.get(api)
        data = await resp.json()
        results = data.get("cities") or data.get("data", {}).get("results", [])
        if results:
            r = results[0]
            city_id  = r.get("cityId")  or r.get("id")
            area_id  = r.get("areaId")
            region_id = r.get("regionId")
            slug = REGION_SLUG.get(region_id, "")
            entry = dict(city=city_id, area=area_id, region=region_id, slug=slug)
            print(f"  [resolve_city] resolved '{city_name}' → {entry}")
            CITIES[city_name] = entry
            return entry
        else:
            print(f"  [resolve_city] no results in response")
    except Exception as e:
        print(f"  [resolve_city] exception: {e}")
    return {}


async def fetch_all_city_listings(page: Page, section: str, geo: dict, city_name: str) -> list[dict]:
    """Fetch all listings for a city with no user filters, for market baseline."""
    from urllib.parse import urlencode
    params: dict = {}
    if geo.get("city"):  params["city"]  = geo["city"]
    if geo.get("area"):  params["area"]  = geo["area"]
    params["bBox"] = "29.4,34.2,33.4,35.9"
    params["zoom"] = "10"
    api_url = f"https://gw.yad2.co.il/realestate-feed/{section}/map?{urlencode(params)}"

    try:
        resp = await page.request.get(api_url)
        data = await resp.json()
        markers = data.get("data", {}).get("markers", [])
    except Exception:
        markers = []

    # Deduplicate
    seen: set = set()
    unique = []
    for m in markers:
        t = m.get("token")
        if t and t not in seen:
            seen.add(t)
            unique.append(m)

    # If result looks truncated, scroll the list page to get more
    if len(unique) < 50 and geo.get("city"):
        slug = geo.get("slug", "")
        list_url = (
            f"https://www.yad2.co.il/realestate/{section}/{slug}"
            f"?city={geo['city']}&area={geo.get('area', '')}"
            if slug else
            f"https://www.yad2.co.il/realestate/{section}"
            f"?city={geo['city']}&area={geo.get('area', '')}"
        )

        scroll_markers: list = []

        async def _capture(response):
            if f"realestate-feed/{section}/map" in response.url:
                try:
                    d = await response.json()
                    scroll_markers.extend(d.get("data", {}).get("markers", []))
                except Exception:
                    pass

        page.on("response", _capture)
        await page.goto(list_url, wait_until="domcontentloaded", timeout=40000)

        prev_count = -1
        stale = 0
        while stale < 3:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1500)
            cur = len({m.get("token") for m in scroll_markers if m.get("token")})
            if cur == prev_count:
                stale += 1
            else:
                stale = 0
            prev_count = cur

        page.remove_listener("response", _capture)

        for m in scroll_markers:
            t = m.get("token")
            if t and t not in seen:
                seen.add(t)
                unique.append(m)

    # City post-filter (API may return whole region)
    if city_name:
        cf = city_name.lower()
        unique = [m for m in unique if cf in m.get("address", {}).get("city", {}).get("text", "").lower()]

    print(f"  [baseline] {len(unique)} city-wide listings fetched for analysis")
    return unique


def build_url(section: str, cfg, geo: dict) -> str:
    slug = geo.get("slug", "")
    base = f"https://www.yad2.co.il/realestate/{section}/{slug}" if slug else f"https://www.yad2.co.il/realestate/{section}"

    p: dict = {}
    if geo.get("city"):  p["city"] = geo["city"]
    if geo.get("area"):  p["area"] = geo["area"]

    if cfg.min_price is not None: p["minPrice"] = cfg.min_price
    if cfg.max_price is not None: p["maxPrice"] = cfg.max_price
    if cfg.min_rooms is not None: p["minRooms"] = cfg.min_rooms
    if cfg.max_rooms is not None: p["maxRooms"] = cfg.max_rooms
    if cfg.min_floor is not None: p["minFloor"] = cfg.min_floor
    if cfg.max_floor is not None: p["maxFloor"] = cfg.max_floor
    if cfg.property_types:        p["property"] = ",".join(str(x) for x in cfg.property_types)

    if hasattr(cfg, "parking")    and cfg.parking:       p["parking"]        = 1
    if hasattr(cfg, "elevator")   and cfg.elevator:      p["elevator"]       = 1
    if hasattr(cfg, "ac")         and cfg.ac:            p["airConditioner"] = 1
    if hasattr(cfg, "mamad")      and cfg.mamad:         p["safeRoom"]       = 1
    if hasattr(cfg, "price_dropped") and cfg.price_dropped: p["isPriceDropped"] = 1
    if hasattr(cfg, "pinuy_binuy") and cfg.pinuy_binuy:  p["pinuyBinuy"]     = 1

    return f"{base}?{urlencode(p)}"


# ─── MARKET ANALYSIS ──────────────────────────────────────────────────────────

def extract_features(marker: dict) -> dict | None:
    price = marker.get("price")
    if not price:
        return None
    det   = marker.get("additionalDetails", {})
    rooms = det.get("roomsCount")
    sqm   = det.get("squareMeter")
    floor = marker.get("address", {}).get("house", {}).get("floor", 0) or 0
    tags  = {(t.get("text") or t.get("key") or "").lower() for t in (marker.get("tags") or []) if isinstance(t, dict)}
    props = marker.get("propertyCondition", {}) or {}

    has_parking  = bool(det.get("parking")  or "parking"  in tags or props.get("parking"))
    has_elevator = bool(det.get("elevator") or "elevator" in tags or props.get("elevator"))
    has_mamad    = bool(det.get("safeRoom") or "ממ\"ד"    in str(marker).lower() or props.get("safeRoom"))

    return {
        "price":        float(price),
        "rooms":        float(rooms) if rooms else 0.0,
        "sqm":          float(sqm)   if sqm   else 0.0,
        "floor":        float(floor),
        "has_parking":  float(has_parking),
        "has_elevator": float(has_elevator),
        "has_mamad":    float(has_mamad),
        "price_per_sqm": float(price) / float(sqm) if sqm else 0.0,
    }


def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def build_price_model(all_features: list[dict | None]):
    """
    Returns (score_fn, market_stats).
    score_fn(feature_dict) -> (predicted_price, deal_label, reason_str)
    market_stats: dict for printing the market summary table.
    """
    import statistics

    feats = [f for f in all_features if f]

    # ── Market stats per room bucket ──────────────────────────────────────────
    from collections import defaultdict
    by_rooms: dict = defaultdict(list)
    for f in feats:
        r = round(f["rooms"] * 2) / 2  # round to nearest 0.5
        by_rooms[r].append(f["price"])

    market_stats = {}
    for r, prices in by_rooms.items():
        prices_s = sorted(prices)
        market_stats[r] = {
            "count":  len(prices_s),
            "median": statistics.median(prices_s),
            "p25":    _percentile(prices_s, 25),
            "p75":    _percentile(prices_s, 75),
        }

    # Amenity prevalence for bonus context
    n = len(feats) or 1
    pct_parking  = sum(f["has_parking"]  for f in feats) / n
    pct_elevator = sum(f["has_elevator"] for f in feats) / n
    pct_mamad    = sum(f["has_mamad"]    for f in feats) / n
    sqm_feats    = [f for f in feats if f["sqm"] > 0]
    avg_ppm      = sum(f["price_per_sqm"] for f in sqm_feats) / len(sqm_feats) if sqm_feats else 0
    market_stats["_meta"] = {
        "total": len(feats),
        "pct_parking": pct_parking,
        "pct_elevator": pct_elevator,
        "pct_mamad": pct_mamad,
        "avg_price_per_sqm": avg_ppm,
    }

    # ── Try sklearn models ────────────────────────────────────────────────────
    _use_ml = False
    _lr = None

    FEATURE_COLS = ["rooms", "sqm", "floor", "has_parking", "has_elevator", "has_mamad"]

    if len(feats) >= 10:
        try:
            from sklearn.linear_model import LinearRegression
            import numpy as np

            X = np.array([[f[c] for c in FEATURE_COLS] for f in feats])
            y = np.array([f["price"] for f in feats])

            _lr = LinearRegression().fit(X, y)
            _use_ml = True
        except ImportError:
            print("  [analysis] sklearn not installed — using percentile scoring")

    def _combined_score(discount: float, feat: dict) -> float:
        """
        Blend price discount with a feature-richness bonus.
        discount: % below predicted price (positive = cheap)
        feature bonus: amenities the listing has that are rare in the city
        """
        discount_norm = max(-100.0, min(discount, 100.0)) / 100.0

        bonus = 0.0
        if feat["has_parking"]  and pct_parking  < 0.5: bonus += 0.15
        if feat["has_elevator"] and pct_elevator < 0.5: bonus += 0.10
        if feat["has_mamad"]    and pct_mamad    < 0.5: bonus += 0.15
        if feat["sqm"] > 0 and avg_ppm > 0:
            ppm_discount = (avg_ppm - feat["price_per_sqm"]) / avg_ppm
            bonus += max(0.0, ppm_discount * 0.3)

        return 0.7 * discount_norm + 0.3 * bonus

    def score_fn(feat: dict | None):
        if feat is None:
            return None, "unknown", "no data", 0.0

        price  = feat["price"]
        rooms  = round(feat["rooms"] * 2) / 2
        stats  = market_stats.get(rooms, {})
        median = stats.get("median", 0)
        p25    = stats.get("p25", 0)
        p75    = stats.get("p75", 0)

        reasons = []

        if _use_ml and _lr is not None:
            import numpy as np
            x_vec = np.array([[feat[c] for c in FEATURE_COLS]])
            predicted = float(_lr.predict(x_vec)[0])
            predicted = max(predicted, 1)
            discount  = (predicted - price) / predicted * 100
        elif median > 0:
            predicted = median
            discount  = (median - price) / median * 100
        else:
            return None, "unknown", "insufficient data", 0.0

        combined = _combined_score(discount, feat)

        # Deal label from combined score
        if combined > 0.12:
            label = "GREAT DEAL"
        elif combined > 0.05:
            label = "Good deal"
        elif combined >= -0.15:
            label = "Fair"
        else:
            label = "Overpriced"

        # Build reason string
        if median > 0:
            vs_median = (price - median) / median * 100
            reasons.append(f"median ₪{median:,.0f}, listed ₪{price:,.0f} ({vs_median:+.0f}%)")
        if p25 and p75:
            reasons.append(f"range ₪{p25:,.0f}–₪{p75:,.0f}")
        if feat["has_parking"] and pct_parking < 0.4:
            reasons.append(f"has parking (only {pct_parking:.0%} do)")
        if feat["has_mamad"] and pct_mamad < 0.4:
            reasons.append(f"has ממ\"ד (only {pct_mamad:.0%} do)")
        if feat["sqm"] > 0 and avg_ppm > 0:
            ppm = feat["price_per_sqm"]
            diff = (ppm - avg_ppm) / avg_ppm * 100
            reasons.append(f"₪{ppm:.0f}/m² (city avg ₪{avg_ppm:.0f}, {diff:+.0f}%)")

        return predicted, label, " | ".join(reasons), combined

    return score_fn, market_stats


# ─── SCRAPER ──────────────────────────────────────────────────────────────────

_PINUY_TERMS = ("פינוי בינוי", "פינוי-בינוי", "התחדשות עירונית", 'תמ"א 38', "תמא 38")


async def _fetch_listing_text(page: Page, token: str) -> str:
    """Return the raw description text for a listing, empty string on failure."""
    api = f"https://gw.yad2.co.il/realestate-item/{token}"
    try:
        resp = await page.request.get(api, timeout=8000)
        if resp.ok:
            data = await resp.json()
            info = data.get("data") or {}
            search_text = info.get("searchText", "") or ""
            meta_desc   = (info.get("metaData") or {}).get("description", "") or ""
            return search_text + " " + meta_desc
    except Exception:
        pass
    return ""


async def _split_pinuy_binuy(page: Page, markers: list[dict]) -> tuple[list[dict], list[dict]]:
    """Fetch each listing's description and split into pinuy-binuy vs other."""
    print(f"  [pinuy_binuy] checking {len(markers)} listings for פינוי בינוי …")
    pinuy, other = [], []
    for m in markers:
        token = m.get("token", "")
        if not token:
            other.append(m)
            continue
        text = await _fetch_listing_text(page, token)
        if any(term in text for term in _PINUY_TERMS):
            pinuy.append(m)
        else:
            other.append(m)
    print(f"  [pinuy_binuy] found {len(pinuy)} פינוי בינוי, {len(other)} other")
    return pinuy, other


async def _scrape_one_city(page: Page, section: str, cfg, city_name: str, label: str) -> tuple[list[dict], any]:
    """Scrape, filter, score, and print listings for a single city. Returns (markers, score_fn)."""
    from urllib.parse import urlparse, parse_qs

    geo = await resolve_city(page, city_name)

    # Build price model from city-wide baseline (no user filters)
    baseline = await fetch_all_city_listings(page, section, geo, city_name)
    score_fn, market_stats = build_price_model([extract_features(m) for m in baseline])

    url = build_url(section, cfg, geo)
    print(f"\n{'='*60}")
    print(f"  {label} — {city_name}")
    print(f"  URL: {url}")
    print(f"{'='*60}")

    markers: list = []

    async def capture(response):
        if f"realestate-feed/{section}/map" in response.url:
            try:
                d = await response.json()
                markers.extend(d.get("data", {}).get("markers", []))
            except Exception:
                pass

    page.on("response", capture)
    await page.goto(url, wait_until="domcontentloaded", timeout=40000)
    await page.wait_for_timeout(6000)
    page.remove_listener("response", capture)

    qs = parse_qs(urlparse(url).query)
    api_params = {k: v[0] for k, v in qs.items() if k not in ("region",)}
    api_params["bBox"] = "29.4,34.2,33.4,35.9"
    api_params["zoom"] = "7"
    api_url = f"https://gw.yad2.co.il/realestate-feed/{section}/map?{urlencode(api_params)}"
    try:
        resp = await page.request.get(api_url)
        d = await resp.json()
        markers.extend(d.get("data", {}).get("markers", []))
    except Exception:
        pass

    print(f"  Markers from API: {len(markers)}")

    if not markers:
        print(f"  No markers for {city_name} — falling back to DOM.")
        items = await scrape_dom(page)
        _print_dom(items, f"{label} — {city_name}")
        return items, score_fn

    # Deduplicate
    seen: set = set()
    unique = []
    for m in markers:
        t = m.get("token")
        if t and t not in seen:
            seen.add(t)
            unique.append(m)

    # Post-filter by city name
    cf = city_name.lower()
    unique = [m for m in unique if cf in m.get("address", {}).get("city", {}).get("text", "").lower()]

    # Sanity filter
    min_sane = 50_000 if section == "forsale" else 1_500
    unique = [m for m in unique if (m.get("price") or 0) >= min_sane]

    # Exclude unwanted neighborhoods
    if hasattr(cfg, "exclude_neighborhoods") and cfg.exclude_neighborhoods:
        excluded = [n.lower() for n in cfg.exclude_neighborhoods]
        before = len(unique)
        unique = [
            m for m in unique
            if m.get("address", {}).get("neighborhood", {}).get("text", "").lower() not in excluded
        ]
        if len(unique) < before:
            print(f"  Excluded {before - len(unique)} listings in: {cfg.exclude_neighborhoods}")

    print(f"  After filters: {len(unique)}")

    if hasattr(cfg, "pinuy_binuy") and cfg.pinuy_binuy:
        pinuy, other = await _split_pinuy_binuy(page, unique)
        _print_markers(pinuy,  f"{label} — {city_name} – פינוי בינוי",        score_fn, market_stats)
        _print_markers(other,  f"{label} — {city_name} – Other opportunities", score_fn, market_stats)
    else:
        _print_markers(unique, f"{label} — {city_name}", score_fn, market_stats)

    return unique, score_fn


async def scrape_section(page: Page, section: str, cfg, label: str) -> dict[str, tuple[list[dict], any]]:
    """Run _scrape_one_city for each city in cfg.cities. Returns {city_name: (markers, score_fn)}."""
    results = {}
    for city_name in (cfg.cities or []):
        markers, score_fn = await _scrape_one_city(page, section, cfg, city_name, label)
        results[city_name] = (markers, score_fn)
    return results


async def scrape_dom(page: Page) -> list[dict]:
    try:
        await page.wait_for_selector("[data-testid='feed-list']", timeout=10000)
    except Exception:
        pass
    cards = await page.query_selector_all("[data-testid='feed-list'] a[href*='/item/']")
    seen: set = set()
    items = []
    for card in cards:
        href = await card.get_attribute("href") or ""
        bare = href.split("?")[0]
        if not bare or bare in seen:
            continue
        seen.add(bare)
        text = await card.inner_text()
        if text.strip():
            link = href if href.startswith("http") else f"https://www.yad2.co.il{href}"
            items.append({"_dom_text": text.strip(), "_url": link})
    return items


# ─── PRINT ────────────────────────────────────────────────────────────────────

def _print_markers(markers: list[dict], label: str, score_fn=None, market_stats: dict | None = None):
    if score_fn:
        markers = sorted(markers, key=lambda m: score_fn(extract_features(m))[3], reverse=True)
    print(f"\n  ── {label} ({len(markers)} listings) ──")
    if not markers:
        print("  No listings found.")
        _print_market_summary(market_stats)
        return
    for i, m in enumerate(markers[:25], 1):
        price     = m.get("price")
        _red      = "\033[91m"
        _reset    = "\033[0m"
        price_str = f"{_red}₪{price:,}{_reset}" if price else "לא צוין מחיר"
        addr      = m.get("address", {})
        city      = addr.get("city", {}).get("text", "")
        street    = addr.get("street", {}).get("text", "")
        hood      = addr.get("neighborhood", {}).get("text", "")
        house     = addr.get("house", {}).get("number", "")
        floor_val = addr.get("house", {}).get("floor", "")
        det       = m.get("additionalDetails", {})
        rooms     = det.get("roomsCount", "")
        size      = det.get("squareMeter", "")
        token     = m.get("token", "")
        link      = f"https://www.yad2.co.il/realestate/item/{token}" if token else ""

        addr_str = f"{street} {house}".strip()
        if hood:
            addr_str = f"{addr_str}, {hood}".strip(", ")
        location = f"{addr_str}, {city}".strip(", ")

        parts = [p for p in [
            f"{rooms} חדרים" if rooms else "",
            f"קומה {floor_val}" if floor_val != "" else "",
            f"{size}מ״ר" if size else "",
        ] if p]

        print(f"  {i:>2}. {price_str} | {location}")
        if parts:
            print(f"      {' | '.join(parts)}")
        if link:
            print(f"      {link}")

        if score_fn:
            feat = extract_features(m)
            _, deal_label, reason, combined = score_fn(feat)
            if deal_label and deal_label != "unknown":
                print(f"      Deal: {deal_label} (score {combined:.2f}) — {reason}")

    if len(markers) > 25:
        print(f"  ... and {len(markers) - 25} more listings.")

    _print_market_summary(market_stats)


def _print_market_summary(market_stats: dict | None):
    if not market_stats:
        return
    meta = market_stats.get("_meta", {})
    total = meta.get("total", 0)
    if not total:
        return
    print(f"\n  ── Market baseline ({total} city-wide listings) ──")
    print(f"  {'Rooms':<7} {'#':>5}  {'Median':>11}  {'25th pct':>11}  {'75th pct':>11}")
    for r in sorted(k for k in market_stats if k != "_meta"):
        s = market_stats[r]
        print(f"  {r:<7.1f} {s['count']:>5}  ₪{s['median']:>10,.0f}  ₪{s['p25']:>10,.0f}  ₪{s['p75']:>10,.0f}")
    pct_p  = meta.get("pct_parking",  0)
    pct_e  = meta.get("pct_elevator", 0)
    pct_m  = meta.get("pct_mamad",    0)
    avg_pm = meta.get("avg_price_per_sqm", 0)
    print(f"  Amenities: parking {pct_p:.0%} | elevator {pct_e:.0%} | ממ\"ד {pct_m:.0%}")
    if avg_pm:
        print(f"  Avg price/m²: ₪{avg_pm:,.0f}")


def _print_dom(items: list[dict], label: str):
    print(f"\n  ── {label} ({len(items)} listings) ──")
    if not items:
        print("  No listings found.")
        return
    for i, item in enumerate(items[:25], 1):
        lines = [l for l in item["_dom_text"].splitlines() if l.strip()][:3]
        print(f"  {i:>2}. {' | '.join(lines)}")
        if item.get("_url"):
            print(f"      {item['_url']}")
    if len(items) > 25:
        print(f"  ... and {len(items) - 25} more listings.")


# ─── WHATSAPP SENDER ──────────────────────────────────────────────────────────

def send_whatsapp(message: str):
    if not WHATSAPP_ENABLED:
        print(f"  [whatsapp] DISABLED — message not sent:\n{message}")
        return
    url = f"https://api.green-api.com/waInstance{GREENAPI_INSTANCE}/sendMessage/{GREENAPI_TOKEN}"
    for phone in WHATSAPP_PHONES:
        payload = json.dumps({"chatId": f"{phone}@c.us", "message": message}).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                print(f"  [whatsapp] Sent to {phone} — HTTP {resp.status}")
        except Exception as e:
            print(f"  [whatsapp] Failed for {phone}: {e}")


def _format_single_listing(listing: dict, rank: int, section: str, score_fn) -> str:
    price  = listing.get("price")
    addr   = listing.get("address", {})
    city   = addr.get("city", {}).get("text", "")
    street = addr.get("street", {}).get("text", "")
    house  = addr.get("house", {}).get("number", "")
    hood   = addr.get("neighborhood", {}).get("text", "")
    floor  = addr.get("house", {}).get("floor", "")
    det    = listing.get("additionalDetails", {})
    rooms  = det.get("roomsCount", "")
    sqm    = det.get("squareMeter", "")
    token  = listing.get("token", "")
    link   = f"https://www.yad2.co.il/realestate/item/{token}" if token else ""

    addr_str = f"{street} {house}".strip()
    if hood:
        addr_str = f"{addr_str}, {hood}".strip(", ")
    location = f"{addr_str}, {city}".strip(", ")

    parts = []
    if rooms:       parts.append(f"{rooms} rooms")
    if floor != "": parts.append(f"floor {floor}")
    if sqm:         parts.append(f"{sqm}m²")

    feat = extract_features(listing)
    _, label, reason, score = score_fn(feat)

    lines = [
        f"#{rank} 📍 {location}",
        f"   💰 ₪{price:,}" if price else "   💰 Price not listed",
    ]
    if parts:
        lines.append("   " + " | ".join(parts))
    if label and label != "unknown":
        lines.append(f"   ⭐ {label} (score: {score:.2f})")
    if reason:
        lines.append(f"   {reason}")
    if link:
        lines.append(f"   🔗 {link}")

    return "\n".join(lines)


def _build_whatsapp_message(top_listings: list[dict], section: str, score_fn, header: str = "") -> str:
    mode_label = "RENT" if section == "rent" else "BUY"
    title = header if header else f"🏠 Yad2 Top {len(top_listings)} {mode_label} Picks"
    parts = [title]
    for i, listing in enumerate(top_listings, 1):
        parts.append("")
        parts.append(_format_single_listing(listing, i, section, score_fn))
    return "\n".join(parts)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def _top_n_sorted(markers: list[dict], score_fn, n: int = 3) -> list[dict]:
    return sorted(markers, key=lambda m: score_fn(extract_features(m))[3], reverse=True)[:n]


async def _run_once(seen_tokens: set | None = None) -> set:
    """
    Scrape and send WhatsApp for top listings.
    If seen_tokens is None → first run, send top 3.
    If seen_tokens is a set → poll run, send only new listings.
    Returns the updated set of all seen tokens.
    """
    is_first_run = seen_tokens is None
    if seen_tokens is None:
        seen_tokens = set()

    rent_cfg = RentConfig()
    buy_cfg  = BuyConfig()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="he-IL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        import datetime
        date_str = datetime.datetime.now().strftime("%d/%m/%y")
        all_parts: list[str] = []

        if SEARCH_MODE in ("rent", "both"):
            city_results = await scrape_section(page, "rent", rent_cfg, "RENT – שכירות")
            for city_name, (markers, score_fn) in city_results.items():
                if not markers:
                    continue
                if is_first_run:
                    top3 = _top_n_sorted(markers, score_fn, 3)
                    all_parts.append(_build_whatsapp_message(top3, "rent", score_fn, header=f"🏠 Top 3 RENT — {city_name}"))
                    seen_tokens.update(m.get("token") for m in markers if m.get("token"))
                else:
                    new = [m for m in markers if m.get("token") and m.get("token") not in seen_tokens]
                    if new:
                        new_sorted = _top_n_sorted(new, score_fn, len(new))
                        all_parts.append(_build_whatsapp_message(new_sorted, "rent", score_fn, header=f"🆕 {len(new)} New RENT — {city_name}"))
                        seen_tokens.update(m.get("token") for m in new if m.get("token"))
                    else:
                        print(f"  [poll] No new rent listings for {city_name}.")

        if SEARCH_MODE in ("buy", "both"):
            city_results = await scrape_section(page, "forsale", buy_cfg, "BUY  – קנייה")
            for city_name, (markers, score_fn) in city_results.items():
                if not markers:
                    continue
                if is_first_run:
                    top3 = _top_n_sorted(markers, score_fn, 3)
                    all_parts.append(_build_whatsapp_message(top3, "forsale", score_fn, header=f"🏠 Top 3 BUY — {city_name}"))
                    seen_tokens.update(m.get("token") for m in markers if m.get("token"))
                else:
                    new = [m for m in markers if m.get("token") and m.get("token") not in seen_tokens]
                    if new:
                        new_sorted = _top_n_sorted(new, score_fn, len(new))
                        all_parts.append(_build_whatsapp_message(new_sorted, "forsale", score_fn, header=f"🆕 {len(new)} New BUY — {city_name}"))
                        seen_tokens.update(m.get("token") for m in new if m.get("token"))
                    else:
                        print(f"  [poll] No new buy listings for {city_name}.")

        if all_parts:
            full_msg = f"📅 {date_str}\n\n" + "\n\n─────────────────────\n\n".join(all_parts)
            print(f"\n  [whatsapp message]\n{full_msg}\n")
            send_whatsapp(full_msg)

        await browser.close()

    return seen_tokens


def run_polling():
    import datetime
    POLL_INTERVAL = 10 * 60  # 10 minutes in seconds

    print("[scheduler] Starting — first run now, then polling every 10 minutes. Press Ctrl+C to stop.")
    seen_tokens = None  # None signals first run
    while True:
        print(f"\n[scheduler] Run at {datetime.datetime.now().strftime('%H:%M:%S')}")
        seen_tokens = asyncio.run(_run_once(seen_tokens))
        print(f"[scheduler] Sleeping 10 minutes…")
        time.sleep(POLL_INTERVAL)


SEEN_TOKENS_FILE = "seen_tokens.json"

def load_seen_tokens() -> set | None:
    try:
        with open(SEEN_TOKENS_FILE) as f:
            tokens = set(json.load(f))
            print(f"[tokens] Loaded {len(tokens)} seen tokens from {SEEN_TOKENS_FILE}")
            return tokens
    except FileNotFoundError:
        print(f"[tokens] No {SEEN_TOKENS_FILE} found — treating as first run")
        return None

def save_seen_tokens(tokens: set):
    with open(SEEN_TOKENS_FILE, "w") as f:
        json.dump(list(tokens), f)
    print(f"[tokens] Saved {len(tokens)} tokens to {SEEN_TOKENS_FILE}")


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        seen = load_seen_tokens()
        seen = asyncio.run(_run_once(seen))
        save_seen_tokens(seen)
    else:
        run_polling()
