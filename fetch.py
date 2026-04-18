"""
San Antonio Local Business Lead Generation & Enrichment System
Targets businesses showing signs of needing digital marketing services.
"""

import asyncio
import json
import csv
import re
import time
import logging
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, quote_plus

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("lead_gen")

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
EXPORTS_DIR = BASE_DIR / "exports"

for d in [DATA_DIR, DASHBOARD_DIR, EXPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOCATION = "San Antonio, TX"
LOCATION_QUERY = "San Antonio Texas"

SEARCH_CATEGORIES = [
    "roofing contractor",
    "HVAC company",
    "plumbing service",
    "law firm",
    "med spa",
    "general contractor",
    "restaurant",
    "electrician",
    "landscaping",
    "auto repair",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw.strip()


def normalize_zip(raw: str) -> str:
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", raw or "")
    return m.group(1) if m else ""


def business_key(name: str, phone: str) -> str:
    slug = re.sub(r"\W+", "", (name + phone).lower())
    return hashlib.md5(slug.encode()).hexdigest()[:12]


def retry_get(url: str, **kwargs) -> Optional[requests.Response]:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"Attempt {attempt}/{RETRY_ATTEMPTS} failed for {url}: {e}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    return None

# ── Website Audit ─────────────────────────────────────────────────────────────

def audit_website(url: str) -> dict:
    audit = {
        "reachable": False,
        "load_time_ms": None,
        "mobile_friendly": False,
        "has_meta_description": False,
        "has_title": False,
        "has_h1": False,
        "has_google_analytics": False,
        "has_meta_pixel": False,
        "has_gtm": False,
        "cms": "unknown",
        "https": False,
        "canonical": False,
        "og_tags": False,
    }

    if not url:
        return audit

    # ensure scheme
    if not url.startswith("http"):
        url = "https://" + url

    audit["https"] = url.startswith("https://")

    try:
        start = time.time()
        r = requests.get(
            url,
            headers={**HEADERS, "Accept": "text/html"},
            timeout=12,
            allow_redirects=True,
        )
        elapsed = int((time.time() - start) * 1000)
        audit["load_time_ms"] = elapsed
        audit["reachable"] = r.status_code < 400

        html = r.text
        soup = BeautifulSoup(html, "lxml")

        # Meta checks
        title = soup.find("title")
        audit["has_title"] = bool(title and title.text.strip())

        meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        audit["has_meta_description"] = bool(meta_desc and meta_desc.get("content", "").strip())

        audit["has_h1"] = bool(soup.find("h1"))

        canonical = soup.find("link", rel="canonical")
        audit["canonical"] = bool(canonical)

        og = soup.find("meta", property="og:title")
        audit["og_tags"] = bool(og)

        # Tracking pixels
        html_lower = html.lower()
        audit["has_google_analytics"] = (
            "google-analytics.com" in html_lower
            or "gtag(" in html_lower
            or "ga(" in html_lower
        )
        audit["has_meta_pixel"] = (
            "connect.facebook.net" in html_lower
            or "fbq(" in html_lower
        )
        audit["has_gtm"] = "googletagmanager.com" in html_lower

        # Mobile viewport
        viewport = soup.find("meta", attrs={"name": "viewport"})
        audit["mobile_friendly"] = bool(viewport)

        # CMS detection
        if "wp-content" in html or "wp-includes" in html:
            audit["cms"] = "WordPress"
        elif "shopify" in html_lower:
            audit["cms"] = "Shopify"
        elif "squarespace" in html_lower:
            audit["cms"] = "Squarespace"
        elif "wix.com" in html_lower:
            audit["cms"] = "Wix"
        elif "webflow" in html_lower:
            audit["cms"] = "Webflow"
        elif "joomla" in html_lower:
            audit["cms"] = "Joomla"
        elif "drupal" in html_lower:
            audit["cms"] = "Drupal"

    except Exception as e:
        log.debug(f"Website audit failed for {url}: {e}")

    return audit

# ── Lead Scoring ──────────────────────────────────────────────────────────────

def score_lead(business: dict, audit: dict) -> tuple[int, list[str]]:
    score = 30
    flags = []

    # No website
    if not business.get("website"):
        score += 15
        flags.append("No website")
    else:
        # Poor website performance
        if audit.get("load_time_ms") and audit["load_time_ms"] > 3000:
            score += 5
            flags.append("Slow website")
        if not audit.get("mobile_friendly"):
            score += 5
            flags.append("Not mobile-friendly")
        if not audit.get("has_google_analytics") and not audit.get("has_meta_pixel"):
            score += 5
            flags.append("No tracking pixels")
        if not audit.get("has_meta_description") or not audit.get("has_title"):
            score += 5
            flags.append("Poor SEO on-page")

    # Reviews
    review_count = business.get("review_count", 0) or 0
    rating = business.get("rating", 5.0) or 5.0

    if review_count < 50:
        score += 10
        flags.append("Low reviews")

    if rating < 4.2:
        score += 10
        flags.append("Low rating")

    # Ads
    if not business.get("ads_presence"):
        score += 15
        flags.append("No ads detected")

    # SEO signals
    seo_issues = 0
    if not audit.get("has_h1"):
        seo_issues += 1
    if not audit.get("canonical"):
        seo_issues += 1
    if not audit.get("og_tags"):
        seo_issues += 1
    if seo_issues >= 2:
        score += 10
        flags.append("Weak SEO signals")

    # High competition niche
    high_comp = ["roofing", "hvac", "plumbing", "law"]
    if any(c in (business.get("category") or "").lower() for c in high_comp):
        flags.append("High competition niche")

    if not business.get("website"):
        flags.append("Outdated/Missing web presence")

    return min(score, 100), flags

# ── Auto-generated Notes ──────────────────────────────────────────────────────

def generate_notes(business: dict, flags: list[str]) -> str:
    notes = []
    name = business.get("business_name", "This business")

    if "No website" in flags:
        notes.append(f"{name} has no website — losing leads daily to competitors online.")
    if "Low reviews" in flags:
        notes.append("Fewer than 50 reviews signals low online visibility and trust.")
    if "Low rating" in flags:
        notes.append(f"Rating below 4.2 ({business.get('rating')}) hurts conversions — reputation mgmt needed.")
    if "No ads detected" in flags:
        notes.append("No paid ad presence means competitors are capturing all paid search traffic.")
    if "Poor SEO on-page" in flags:
        notes.append("Missing meta tags and SEO structure — invisible to Google organic results.")
    if "Not mobile-friendly" in flags:
        notes.append("Site not mobile-optimized — 60%+ of local searches are on mobile.")
    if "No tracking pixels" in flags:
        notes.append("No Analytics or Meta Pixel — can't retarget visitors or measure ROI.")

    return " | ".join(notes) if notes else "Identified as a digital marketing opportunity in San Antonio."

# ── Yelp Scraper ──────────────────────────────────────────────────────────────

def scrape_yelp(category: str, location: str = "San Antonio, TX") -> list[dict]:
    businesses = []
    query = quote_plus(category)
    loc = quote_plus(location)
    url = f"https://www.yelp.com/search?find_desc={query}&find_loc={loc}&sortby=rating"

    log.info(f"[Yelp] Searching: {category}")
    r = retry_get(url)
    if not r:
        return businesses

    soup = BeautifulSoup(r.text, "lxml")

    # Yelp embeds data in JSON script tags
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or "{}")
            if isinstance(data, list):
                items = data
            elif data.get("@type") == "ItemList":
                items = data.get("itemListElement", [])
            else:
                continue

            for item in items:
                biz = item.get("item", item)
                if not isinstance(biz, dict):
                    continue
                name = biz.get("name", "")
                if not name:
                    continue

                addr = biz.get("address", {})
                rating_data = biz.get("aggregateRating", {})

                phone_raw = biz.get("telephone", "")
                website = biz.get("url", "")
                # strip yelp redirect
                if "yelp.com/biz_redir" in website:
                    website = ""

                businesses.append({
                    "business_name": name.strip(),
                    "category": category,
                    "phone": normalize_phone(phone_raw),
                    "email": "",
                    "website": website,
                    "google_maps_url": "",
                    "yelp_url": biz.get("@id", ""),
                    "city": addr.get("addressLocality", "San Antonio"),
                    "state": addr.get("addressRegion", "TX"),
                    "zip": normalize_zip(addr.get("postalCode", "")),
                    "rating": float(rating_data.get("ratingValue", 0) or 0),
                    "review_count": int(rating_data.get("reviewCount", 0) or 0),
                    "last_review_date": "",
                    "social_links": {},
                    "ads_presence": False,
                    "source": "Yelp",
                })
        except Exception as e:
            log.debug(f"[Yelp] JSON parse error: {e}")

    # Also try card-based scraping as fallback
    cards = soup.select("[class*='businessName'], [data-testid='serp-ia-card']")
    log.info(f"[Yelp] Found {len(businesses)} via JSON-LD for '{category}'")
    return businesses

# ── Yellow Pages Scraper ──────────────────────────────────────────────────────

def scrape_yellowpages(category: str) -> list[dict]:
    businesses = []
    slug = re.sub(r"\s+", "-", category.lower())
    url = f"https://www.yellowpages.com/san-antonio-tx/{slug}"

    log.info(f"[YellowPages] Searching: {category}")
    r = retry_get(url)
    if not r:
        return businesses

    soup = BeautifulSoup(r.text, "lxml")
    cards = soup.select(".result .info")

    for card in cards[:20]:
        try:
            name_el = card.select_one(".business-name")
            name = name_el.text.strip() if name_el else ""
            if not name:
                continue

            phone_el = card.select_one(".phones")
            phone = normalize_phone(phone_el.text if phone_el else "")

            website_el = card.select_one("a.track-visit-website")
            website = website_el.get("href", "") if website_el else ""
            if website and "yellowpages.com" in website:
                website = ""

            addr_el = card.select_one(".adr")
            city_el = card.select_one(".locality")
            state_el = card.select_one(".region")
            zip_el = card.select_one(".postal-code")

            rating_el = card.select_one(".result-rating em")
            rating = float(rating_el.text.strip()) if rating_el else 0.0

            review_el = card.select_one(".count")
            count_text = review_el.text.strip() if review_el else "0"
            review_count = int(re.sub(r"\D", "", count_text) or 0)

            businesses.append({
                "business_name": name,
                "category": category,
                "phone": phone,
                "email": "",
                "website": website,
                "google_maps_url": "",
                "yelp_url": "",
                "city": city_el.text.strip() if city_el else "San Antonio",
                "state": state_el.text.strip() if state_el else "TX",
                "zip": zip_el.text.strip() if zip_el else "",
                "rating": rating,
                "review_count": review_count,
                "last_review_date": "",
                "social_links": {},
                "ads_presence": False,
                "source": "YellowPages",
            })
        except Exception as e:
            log.debug(f"[YellowPages] Card parse error: {e}")

    log.info(f"[YellowPages] Found {len(businesses)} for '{category}'")
    return businesses

# ── Google Maps Scraper (Playwright) ─────────────────────────────────────────

async def scrape_google_maps(category: str, playwright_instance) -> list[dict]:
    businesses = []
    query = f"{category} in San Antonio Texas"
    url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    log.info(f"[Google Maps] Searching: {category}")

    browser = await playwright_instance.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=HEADERS["User-Agent"],
        viewport={"width": 1280, "height": 900},
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # scroll results pane to load more
        for _ in range(4):
            try:
                pane = page.locator('[role="feed"]')
                await pane.evaluate("el => el.scrollTop += 1200")
                await asyncio.sleep(1.5)
            except Exception:
                break

        cards = await page.query_selector_all('div[role="feed"] > div > div[jsaction]')
        log.info(f"[Google Maps] {len(cards)} cards found for '{category}'")

        for card in cards[:25]:
            try:
                # Click to expand details
                await card.click()
                await asyncio.sleep(1.5)

                name_el = await page.query_selector('h1[class*="fontHeadlineLarge"]')
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    continue

                # Rating
                rating = 0.0
                rating_el = await page.query_selector('span[role="img"][aria-label*="stars"]')
                if rating_el:
                    aria = await rating_el.get_attribute("aria-label") or ""
                    m = re.search(r"([\d.]+)", aria)
                    if m:
                        rating = float(m.group(1))

                # Review count
                review_count = 0
                review_el = await page.query_selector('button[aria-label*="reviews"]')
                if review_el:
                    txt = await review_el.inner_text()
                    m = re.search(r"([\d,]+)", txt)
                    if m:
                        review_count = int(m.group(1).replace(",", ""))

                # Phone
                phone = ""
                phone_el = await page.query_selector('button[data-item-id*="phone"]')
                if phone_el:
                    phone = normalize_phone(await phone_el.get_attribute("aria-label") or "")

                # Website
                website = ""
                web_el = await page.query_selector('a[data-item-id*="authority"]')
                if web_el:
                    website = await web_el.get_attribute("href") or ""

                # Address
                addr = ""
                addr_el = await page.query_selector('button[data-item-id*="address"]')
                if addr_el:
                    addr = (await addr_el.get_attribute("aria-label") or "").replace("Address: ", "")

                zip_code = normalize_zip(addr)
                maps_url = page.url

                businesses.append({
                    "business_name": name,
                    "category": category,
                    "phone": phone,
                    "email": "",
                    "website": website,
                    "google_maps_url": maps_url,
                    "yelp_url": "",
                    "city": "San Antonio",
                    "state": "TX",
                    "zip": zip_code,
                    "rating": rating,
                    "review_count": review_count,
                    "last_review_date": "",
                    "social_links": {},
                    "ads_presence": False,
                    "source": "Google Maps",
                })
            except PlaywrightTimeout:
                log.debug(f"[Google Maps] Timeout on a card for '{category}'")
            except Exception as e:
                log.debug(f"[Google Maps] Card error: {e}")

    except Exception as e:
        log.error(f"[Google Maps] Page error for '{category}': {e}")
    finally:
        await browser.close()

    log.info(f"[Google Maps] Collected {len(businesses)} for '{category}'")
    return businesses

# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(leads: list[dict]) -> list[dict]:
    seen = {}
    for lead in leads:
        key = business_key(lead.get("business_name", ""), lead.get("phone", ""))
        if key not in seen:
            seen[key] = lead
        else:
            # Prefer record with more data
            existing = seen[key]
            if not existing.get("website") and lead.get("website"):
                seen[key] = lead
            if not existing.get("phone") and lead.get("phone"):
                existing["phone"] = lead["phone"]
    return list(seen.values())

# ── Filter + Qualify ──────────────────────────────────────────────────────────

def is_qualified(lead: dict) -> bool:
    """Only pass businesses that show at least one marketing weakness."""
    rating = lead.get("rating", 5.0) or 5.0
    reviews = lead.get("review_count", 100) or 100
    has_site = bool(lead.get("website", "").strip())

    return (
        rating < 4.2
        or reviews < 50
        or not has_site
    )

# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("San Antonio Lead Generation System — Starting")
    log.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    all_businesses = []

    # --- Static scrapers ---
    for cat in SEARCH_CATEGORIES:
        try:
            yp = scrape_yellowpages(cat)
            all_businesses.extend(yp)
            time.sleep(1)
        except Exception as e:
            log.error(f"YellowPages error for '{cat}': {e}")

        try:
            yelp = scrape_yelp(cat)
            all_businesses.extend(yelp)
            time.sleep(1.5)
        except Exception as e:
            log.error(f"Yelp error for '{cat}': {e}")

    # --- Dynamic scraper (Google Maps) ---
    log.info("[Google Maps] Launching Playwright...")
    try:
        async with async_playwright() as pw:
            for cat in SEARCH_CATEGORIES:
                try:
                    gm = await scrape_google_maps(cat, pw)
                    all_businesses.extend(gm)
                    await asyncio.sleep(2)
                except Exception as e:
                    log.error(f"Google Maps error for '{cat}': {e}")
    except Exception as e:
        log.error(f"Playwright fatal error: {e}")

    log.info(f"Total raw records: {len(all_businesses)}")

    # --- Dedup ---
    unique = deduplicate(all_businesses)
    log.info(f"After deduplication: {len(unique)}")

    # --- Enrich + Score ---
    enriched = []
    for i, biz in enumerate(unique):
        log.info(f"[{i+1}/{len(unique)}] Enriching: {biz.get('business_name')}")
        try:
            audit = audit_website(biz.get("website", ""))
            biz["website_audit"] = audit

            # Update SEO score (0–100 simple calculation)
            seo_points = sum([
                audit.get("has_title", False),
                audit.get("has_meta_description", False),
                audit.get("has_h1", False),
                audit.get("canonical", False),
                audit.get("og_tags", False),
                audit.get("mobile_friendly", False),
            ])
            biz["seo_score"] = int((seo_points / 6) * 100) if biz.get("website") else 0

            score, flags = score_lead(biz, audit)
            biz["score"] = score
            biz["flags"] = flags
            biz["notes"] = generate_notes(biz, flags)
            enriched.append(biz)
        except Exception as e:
            log.error(f"Enrichment error for {biz.get('business_name')}: {e}")

    # --- Filter qualified ---
    qualified = [b for b in enriched if is_qualified(b)]
    # Sort by score desc
    qualified.sort(key=lambda x: x.get("score", 0), reverse=True)
    log.info(f"Qualified leads: {len(qualified)}")

    # --- Build output payload ---
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "fetched_at": now,
        "source": "Google Maps, Yelp, YellowPages",
        "location": LOCATION,
        "total": len(enriched),
        "qualified": len(qualified),
        "leads": [
            {
                "business_name": b.get("business_name", ""),
                "category": b.get("category", ""),
                "phone": b.get("phone", ""),
                "email": b.get("email", ""),
                "website": b.get("website", ""),
                "google_maps_url": b.get("google_maps_url", ""),
                "city": b.get("city", "San Antonio"),
                "state": b.get("state", "TX"),
                "zip": b.get("zip", ""),
                "rating": b.get("rating", 0),
                "review_count": b.get("review_count", 0),
                "seo_score": b.get("seo_score", 0),
                "ads_presence": b.get("ads_presence", False),
                "flags": b.get("flags", []),
                "score": b.get("score", 30),
                "notes": b.get("notes", ""),
                "source": b.get("source", ""),
                "website_audit": b.get("website_audit", {}),
            }
            for b in qualified
        ],
    }

    # --- Save JSON ---
    for path in [DATA_DIR / "leads.json", DASHBOARD_DIR / "leads.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        log.info(f"Saved: {path}")

    # --- Export CSV for GoHighLevel ---
    csv_path = EXPORTS_DIR / "ghl.csv"
    ghl_fields = [
        "Business Name", "Contact Name", "Phone", "Email", "Website",
        "City", "State", "Zip", "Lead Score", "Flags", "Notes", "Source URL",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ghl_fields)
        writer.writeheader()
        for b in payload["leads"]:
            writer.writerow({
                "Business Name": b["business_name"],
                "Contact Name": "",
                "Phone": b["phone"],
                "Email": b["email"],
                "Website": b["website"],
                "City": b["city"],
                "State": b["state"],
                "Zip": b["zip"],
                "Lead Score": b["score"],
                "Flags": "; ".join(b["flags"]),
                "Notes": b["notes"],
                "Source URL": b.get("google_maps_url") or b.get("source", ""),
            })
    log.info(f"Saved GHL CSV: {csv_path}")

    log.info("=" * 60)
    log.info(f"DONE — {len(qualified)} qualified leads saved.")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
