"""
San Antonio Local Business Lead Generation & Enrichment System
Built on insights from "The Seven Figure Agency Roadmap" by Josh Nelson.

Key principles applied:
- Niche down to service businesses that NEED phone calls to survive
- Qualify based on digital weakness signals Josh identified
- Enrich with real emails found on business websites (no hallucinating)
- Score based on Josh's recurring revenue potential signals
- Generate outreach notes using his sales framework
"""

import asyncio
import json
import csv
import re
import time
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, quote_plus

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("lead_gen")

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
EXPORTS_DIR = BASE_DIR / "exports"

for d in [DATA_DIR, DASHBOARD_DIR, EXPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOCATION = "San Antonio, TX"

# ── Josh Nelson's proven niches ───────────────────────────────────────────────
# From the book: focus on service businesses where phone calls = revenue
# These are businesses that NEED digital marketing to survive
# Rule of Five Ones: pick niches with recurring revenue potential

SEARCH_CATEGORIES = {
    # HIGH VALUE — Emergency/urgent service businesses ($2k-5k/mo retainer potential)
    "plumbing service":         {"monthly_value": 3500, "urgency": "high", "label": "Plumbing"},
    "HVAC company":             {"monthly_value": 3500, "urgency": "high", "label": "HVAC"},
    "roofing contractor":       {"monthly_value": 4000, "urgency": "high", "label": "Roofing"},
    "electrician":              {"monthly_value": 3000, "urgency": "high", "label": "Electrician"},
    "water damage restoration": {"monthly_value": 4500, "urgency": "high", "label": "Restoration"},

    # HIGH VALUE — Professional services ($1.5k-3k/mo retainer potential)
    "personal injury lawyer":   {"monthly_value": 5000, "urgency": "high", "label": "Law Firm"},
    "family law attorney":      {"monthly_value": 3500, "urgency": "high", "label": "Law Firm"},
    "immigration lawyer":       {"monthly_value": 3000, "urgency": "med",  "label": "Law Firm"},
    "dental office":            {"monthly_value": 3000, "urgency": "med",  "label": "Dental"},
    "chiropractor":             {"monthly_value": 2500, "urgency": "med",  "label": "Chiro"},

    # HIGH VALUE — Aesthetics/Medical ($2k-4k/mo retainer potential)
    "med spa":                  {"monthly_value": 4000, "urgency": "med",  "label": "Med Spa"},
    "plastic surgeon":          {"monthly_value": 5000, "urgency": "med",  "label": "Med Spa"},
    "dermatologist":            {"monthly_value": 3000, "urgency": "med",  "label": "Medical"},

    # SOLID — Contractors ($1.5k-3k/mo retainer potential)
    "general contractor":       {"monthly_value": 2500, "urgency": "med",  "label": "Contractor"},
    "painting contractor":      {"monthly_value": 2000, "urgency": "low",  "label": "Contractor"},
    "landscaping":              {"monthly_value": 2000, "urgency": "low",  "label": "Landscaping"},
    "pest control":             {"monthly_value": 2500, "urgency": "med",  "label": "Pest Control"},
    "pool service":             {"monthly_value": 2000, "urgency": "low",  "label": "Pool"},

    # SOLID — Auto/Specialty ($1k-2k/mo retainer potential)
    "auto repair shop":         {"monthly_value": 1500, "urgency": "med",  "label": "Auto Repair"},
    "auto body shop":           {"monthly_value": 1500, "urgency": "med",  "label": "Auto Repair"},
    "towing service":           {"monthly_value": 2000, "urgency": "high", "label": "Towing"},

    # SOLID — Health/Fitness ($1k-2.5k/mo retainer potential)
    "gym fitness center":       {"monthly_value": 1500, "urgency": "low",  "label": "Fitness"},
    "physical therapy":         {"monthly_value": 2500, "urgency": "med",  "label": "Medical"},

    # SOLID — Beauty ($1k-2k/mo retainer potential)
    "hair salon":               {"monthly_value": 1200, "urgency": "low",  "label": "Salon"},
    "barbershop":               {"monthly_value": 1000, "urgency": "low",  "label": "Salon"},
    "nail salon":               {"monthly_value": 1000, "urgency": "low",  "label": "Salon"},
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

RETRY_ATTEMPTS = 3
RETRY_DELAY = 2

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

# ── Email Finder ──────────────────────────────────────────────────────────────
# IMPORTANT: Only finds REAL emails from the business website.
# Never generates or guesses emails. No hallucinating.

def find_email_from_website(website_url: str) -> str:
    """
    Scrapes the business website to find a real contact email.
    Checks: homepage, /contact, /about pages.
    Returns empty string if no real email found.
    """
    if not website_url:
        return ""

    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    # Email regex pattern
    email_pattern = re.compile(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
    )

    # Domains to exclude (not real business emails)
    excluded_domains = [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "sentry.io", "w3.org", "schema.org", "example.com",
        "wixpress.com", "squarespace.com", "wordpress.com",
        "png", "jpg", "gif", "svg", "webp"
    ]

    pages_to_check = ["", "/contact", "/contact-us", "/about", "/about-us"]
    found_emails = []

    for page in pages_to_check:
        try:
            url = website_url.rstrip("/") + page
            r = requests.get(url, headers=HEADERS, timeout=8, allow_redirects=True)
            if r.status_code >= 400:
                continue

            # Check for mailto links first (most reliable)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("mailto:"):
                    email = href.replace("mailto:", "").split("?")[0].strip().lower()
                    if email and not any(d in email for d in excluded_domains):
                        found_emails.append(email)

            # Also search raw HTML for email patterns
            emails = email_pattern.findall(r.text)
            for email in emails:
                email = email.lower()
                if not any(d in email for d in excluded_domains):
                    found_emails.append(email)

            if found_emails:
                break

        except Exception:
            continue

    # Return the first unique real email found
    seen = set()
    for email in found_emails:
        if email not in seen:
            seen.add(email)
            return email  # Return first good one

    return ""

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
        "has_contact_form": False,
        "has_phone_number": False,
        "has_reviews_widget": False,
        "cms": "unknown",
        "https": False,
        "canonical": False,
        "og_tags": False,
    }

    if not url:
        return audit

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
        html_lower = html.lower()

        # SEO basics
        title = soup.find("title")
        audit["has_title"] = bool(title and title.text.strip())

        meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        audit["has_meta_description"] = bool(
            meta_desc and meta_desc.get("content", "").strip()
        )

        audit["has_h1"] = bool(soup.find("h1"))

        canonical = soup.find("link", rel="canonical")
        audit["canonical"] = bool(canonical)

        og = soup.find("meta", property="og:title")
        audit["og_tags"] = bool(og)

        # Mobile
        viewport = soup.find("meta", attrs={"name": "viewport"})
        audit["mobile_friendly"] = bool(viewport)

        # Tracking pixels
        audit["has_google_analytics"] = (
            "google-analytics.com" in html_lower
            or "gtag(" in html_lower
        )
        audit["has_meta_pixel"] = (
            "connect.facebook.net" in html_lower
            or "fbq(" in html_lower
        )
        audit["has_gtm"] = "googletagmanager.com" in html_lower

        # Conversion signals
        audit["has_contact_form"] = bool(
            soup.find("form") or "contact-form" in html_lower
        )
        audit["has_phone_number"] = bool(
            re.search(r"\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", html)
        )
        audit["has_reviews_widget"] = (
            "birdeye" in html_lower
            or "podium" in html_lower
            or "grade.us" in html_lower
            or "reviews" in html_lower
        )

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
        elif "godaddy" in html_lower:
            audit["cms"] = "GoDaddy"

    except Exception as e:
        log.debug(f"Website audit failed for {url}: {e}")

    return audit

# ── Lead Scoring ──────────────────────────────────────────────────────────────
# Based on Josh Nelson's qualification criteria from Seven Figure Agency Roadmap:
# - Service businesses that need phone calls to survive = highest value
# - No online presence = biggest opportunity
# - Recurring revenue potential = priority signal

def score_lead(business: dict, audit: dict) -> tuple:
    score = 30
    flags = []
    cat_info = SEARCH_CATEGORIES.get(business.get("search_query", ""), {})

    # ── No website (biggest opportunity per Josh Nelson) ──
    if not business.get("website"):
        score += 20
        flags.append("No website")
    else:
        if not audit.get("reachable"):
            score += 15
            flags.append("Broken website")
        else:
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
            if not audit.get("has_contact_form"):
                score += 3
                flags.append("No contact form")

    # ── Reviews signals ──
    review_count = business.get("review_count", 0) or 0
    rating = business.get("rating", 5.0) or 5.0

    if review_count < 10:
        score += 15
        flags.append("Almost no reviews")
    elif review_count < 50:
        score += 10
        flags.append("Low reviews")

    if rating < 3.5:
        score += 15
        flags.append("Very low rating")
    elif rating < 4.2:
        score += 8
        flags.append("Low rating")

    # ── No ads presence ──
    if not business.get("ads_presence"):
        score += 10
        flags.append("No ads detected")

    # ── SEO signals ──
    seo_issues = sum([
        not audit.get("has_h1"),
        not audit.get("canonical"),
        not audit.get("og_tags"),
        not audit.get("has_meta_description"),
    ])
    if seo_issues >= 3:
        score += 10
        flags.append("Weak SEO signals")
    elif seo_issues >= 2:
        score += 5
        flags.append("Some SEO issues")

    # ── High value niche bonus (Josh's proven niches) ──
    urgency = cat_info.get("urgency", "low")
    if urgency == "high":
        score += 10
        flags.append("High urgency niche")
    elif urgency == "med":
        score += 5

    # ── Recurring revenue potential flag ──
    monthly_value = cat_info.get("monthly_value", 1000)
    if monthly_value >= 3000:
        flags.append("High retainer potential ($3k+/mo)")
    elif monthly_value >= 2000:
        flags.append("Solid retainer potential ($2k+/mo)")
    else:
        flags.append("Standard retainer potential")

    return min(score, 100), flags

# ── Outreach Notes Generator ──────────────────────────────────────────────────
# Based on Josh Nelson's sales framework:
# Step 1: Initial Call → Step 2: Pre-meeting Research → Step 3: Review Meeting

def generate_notes(business: dict, flags: list, audit: dict) -> str:
    """
    Generates a sales-ready outreach note based on Josh Nelson's
    sales framework from Seven Figure Agency Roadmap.
    Identifies the pain, shows the opportunity, suggests the pitch.
    """
    notes = []
    name = business.get("business_name", "This business")
    cat_info = SEARCH_CATEGORIES.get(business.get("search_query", ""), {})
    label = cat_info.get("label", "local business")
    monthly_value = cat_info.get("monthly_value", 1500)

    # Pain identification (Josh says: tap into their needs and desires)
    pain_points = []

    if "No website" in flags or "Broken website" in flags:
        pain_points.append(
            f"has no working website — losing every lead searching online"
        )
    if "Almost no reviews" in flags:
        pain_points.append("has almost no online reviews — invisible to new customers")
    elif "Low reviews" in flags:
        pain_points.append("has under 50 reviews — struggling to build trust online")
    if "Very low rating" in flags:
        pain_points.append(
            f"has a {business.get('rating')} star rating — reputation is hurting conversions"
        )
    elif "Low rating" in flags:
        pain_points.append(
            f"has a {business.get('rating')} star rating — below the 4.2 threshold customers trust"
        )
    if "No ads detected" in flags:
        pain_points.append("is running zero paid ads — competitors are capturing all the paid traffic")
    if "No tracking pixels" in flags:
        pain_points.append("has no Analytics or Meta Pixel — can't measure or retarget")
    if "Not mobile-friendly" in flags:
        pain_points.append("site isn't mobile-friendly — losing 60%+ of local searchers")

    if pain_points:
        notes.append(f"PAIN: {name} " + "; ".join(pain_points[:3]) + ".")

    # Opportunity (Josh says: make clear what you can bring to the table)
    notes.append(
        f"OPPORTUNITY: {label} in San Antonio with estimated retainer value of "
        f"${monthly_value:,}/mo. High competition niche where digital presence = phone calls."
    )

    # Pitch angle (Josh says: position yourself as the expert in their space)
    if "No website" in flags:
        notes.append(
            "PITCH: Lead with website build + SEO package. "
            "Show them competitors who rank #1 and ask how many calls they think that generates."
        )
    elif "Low rating" in flags or "Very low rating" in flags:
        notes.append(
            "PITCH: Lead with reputation management. "
            "Show them their rating vs top competitor. Offer review generation system."
        )
    elif "No ads detected" in flags:
        notes.append(
            "PITCH: Lead with paid ads audit. "
            "Show them competitors running ads and estimate what they're spending. "
            "Offer to run a pilot campaign."
        )
    else:
        notes.append(
            "PITCH: Full digital audit approach. "
            "Show gaps in SEO, tracking, and online presence vs top local competitors."
        )

    return " | ".join(notes)

# ── YellowPages Scraper ───────────────────────────────────────────────────────

def scrape_yellowpages(search_query: str) -> list:
    businesses = []
    slug = re.sub(r"\s+", "-", search_query.lower())
    url = f"https://www.yellowpages.com/san-antonio-tx/{slug}"

    log.info(f"[YellowPages] Searching: {search_query}")
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

            city_el = card.select_one(".locality")
            state_el = card.select_one(".region")
            zip_el = card.select_one(".postal-code")

            rating_el = card.select_one(".result-rating em")
            rating = float(rating_el.text.strip()) if rating_el else 0.0

            review_el = card.select_one(".count")
            count_text = review_el.text.strip() if review_el else "0"
            review_count = int(re.sub(r"\D", "", count_text) or 0)

            cat_info = SEARCH_CATEGORIES.get(search_query, {})

            businesses.append({
                "business_name": name,
                "category": cat_info.get("label", search_query),
                "search_query": search_query,
               for d in [DATA_DIR, DASHBOARD_DIR, EXPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOCATION = "San Antonio, TX"

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
RETRY_DELAY = 2

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

        title = soup.find("title")
        audit["has_title"] = bool(title and title.text.strip())

        meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        audit["has_meta_description"] = bool(meta_desc and meta_desc.get("content", "").strip())

        audit["has_h1"] = bool(soup.find("h1"))

        canonical = soup.find("link", rel="canonical")
        audit["canonical"] = bool(canonical)

        og = soup.find("meta", property="og:title")
        audit["og_tags"] = bool(og)

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

        viewport = soup.find("meta", attrs={"name": "viewport"})
        audit["mobile_friendly"] = bool(viewport)

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

    except Exception as e:
        log.debug(f"Website audit failed for {url}: {e}")

    return audit

# ── Lead Scoring ──────────────────────────────────────────────────────────────

def score_lead(business: dict, audit: dict) -> tuple:
    score = 30
    flags = []

    if not business.get("website"):
        score += 15
        flags.append("No website")
    else:
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

    review_count = business.get("review_count", 0) or 0
    rating = business.get("rating", 5.0) or 5.0

    if review_count < 50:
        score += 10
        flags.append("Low reviews")

    if rating < 4.2:
        score += 10
        flags.append("Low rating")

    if not business.get("ads_presence"):
        score += 15
        flags.append("No ads detected")

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

    high_comp = ["roofing", "hvac", "plumbing", "law"]
    if any(c in (business.get("category") or "").lower() for c in high_comp):
        flags.append("High competition niche")

    return min(score, 100), flags


def generate_notes(business: dict, flags: list) -> str:
    notes = []
    name = business.get("business_name", "This business")

    if "No website" in flags:
        notes.append(f"{name} has no website — losing leads daily to competitors online.")
    if "Low reviews" in flags:
        notes.append("Fewer than 50 reviews signals low online visibility and trust.")
    if "Low rating" in flags:
        notes.append(f"Rating below 4.2 ({business.get('rating')}) hurts conversions.")
    if "No ads detected" in flags:
        notes.append("No paid ad presence — competitors capture all paid search traffic.")
    if "Poor SEO on-page" in flags:
        notes.append("Missing meta tags — invisible to Google organic results.")
    if "Not mobile-friendly" in flags:
        notes.append("Site not mobile-optimized — 60%+ of local searches are on mobile.")
    if "No tracking pixels" in flags:
        notes.append("No Analytics or Meta Pixel — can't retarget visitors or measure ROI.")

    return " | ".join(notes) if notes else "Identified as a digital marketing opportunity in San Antonio."

# ── YellowPages Scraper ───────────────────────────────────────────────────────

def scrape_yellowpages(category: str) -> list:
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

# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(leads: list) -> list:
    seen = {}
    for lead in leads:
        key = business_key(lead.get("business_name", ""), lead.get("phone", ""))
        if key not in seen:
            seen[key] = lead
        else:
            existing = seen[key]
            if not existing.get("website") and lead.get("website"):
                seen[key] = lead
    return list(seen.values())


def is_qualified(lead: dict) -> bool:
    rating = lead.get("rating", 5.0) or 5.0
    reviews = lead.get("review_count", 100) or 100
    has_site = bool(lead.get("website", "").strip())
    return (rating < 4.2 or reviews < 50 or not has_site)

# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("San Antonio Lead Generation System — Starting")
    log.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    all_businesses = []

    # YellowPages scraping
    for cat in SEARCH_CATEGORIES:
        try:
            yp = scrape_yellowpages(cat)
            all_businesses.extend(yp)
            time.sleep(1)
        except Exception as e:
            log.error(f"YellowPages error for '{cat}': {e}")

    log.info(f"Total raw records: {len(all_businesses)}")

    unique = deduplicate(all_businesses)
    log.info(f"After deduplication: {len(unique)}")

    # Enrich + Score
    enriched = []
    for i, biz in enumerate(unique):
        log.info(f"[{i+1}/{len(unique)}] Enriching: {biz.get('business_name')}")
        try:
            audit = audit_website(biz.get("website", ""))
            biz["website_audit"] = audit

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

    qualified = [b for b in enriched if is_qualified(b)]
    qualified.sort(key=lambda x: x.get("score", 0), reverse=True)
    log.info(f"Qualified leads: {len(qualified)}")

    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "fetched_at": now,
        "source": "YellowPages",
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

    # Save to proper directories
    for path in [DATA_DIR / "leads.json", DASHBOARD_DIR / "leads.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        log.info(f"Saved: {path}")

    # GHL CSV Export
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
