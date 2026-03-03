import os
import json
import re
import time
import hashlib
import random
from urllib.parse import urlparse

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gbuild

# ============================================================
# FAST MODE CONFIG
# ============================================================
SHEET_NAME = "Pipeline"

# Keep these tight for speed
MAX_DTC_TO_ADD = 60
MAX_AGENCY_TO_ADD = 60

# Common Crawl (free) – keep small so we don't stall
CC_LIMIT_PER_QUERY = 120
CC_MAX_RETRIES = 2          # FAST MODE: fewer retries
CC_TIMEOUT_SEC = 15         # FAST MODE: shorter network timeout
CC_BACKOFF_BASE = 1.4
CC_JITTER = 0.4

SLEEP_SEC = 0.25

DEFAULT_STAGE = "Lead Identified"
DEFAULT_FOLLOW_UP_STATUS = "Not Started"
DEFAULT_PERSONA_DEPTH = "Low"

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com", "reddit.com",
    "google.com", "shop.app", "apps.apple.com", "play.google.com",
}

# ============================================================
# DTC FILTERS (reduce junk)
# ============================================================
SHOPIFY_RE = re.compile(r"(cdn\.shopify\.com|myshopify\.com|Shopify)", re.I)
PASSWORD_RE = re.compile(r"(enter store using password|opening soon|password)", re.I)
GENERIC_DEV_RE = re.compile(r"(example store|test store|coming soon)", re.I)

# "Legit store" hints
CART_RE = re.compile(r"(/cart|add to cart|checkout)", re.I)
PRODUCT_RE = re.compile(r"(/products/|product__|product-form|schema\.org/Product)", re.I)

SIGNALS = {
    "meta_pixel": re.compile(r"connect\.facebook\.net|fbq\(", re.I),
    "tiktok_pixel": re.compile(r"tiktok.*pixel|ttq\(", re.I),
    "google_tag": re.compile(r"googletagmanager\.com|gtag\(", re.I),
    "klaviyo": re.compile(r"klaviyo", re.I),
    "attentive": re.compile(r"attentive", re.I),
    "postscript": re.compile(r"postscript", re.I),
    "triplewhale": re.compile(r"triplewhale", re.I),
    "northbeam": re.compile(r"northbeam", re.I),
    "recharge": re.compile(r"recharge", re.I),
}

# ============================================================
# AGENCY FILTERS
# ============================================================
AGENCY_KEYWORDS = re.compile(
    r"(case studies|portfolio|clients|our work|work|results|testimonials|"
    r"paid social|meta ads|facebook ads|tiktok ads|performance marketing|"
    r"creative strategy|ugc|creative testing|ecommerce|shopify|dtc)",
    re.I
)

AGENCY_SEARCH_QUERIES = [
    "performance marketing agency case studies ecommerce",
    "paid social agency portfolio shopify",
    "meta ads creative agency case studies",
    "tiktok ads agency ugc creative testing",
]

# ============================================================
# Helpers
# ============================================================
def norm_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host
    except Exception:
        return url.strip().lower()


def stable_id(lead_type: str, domain: str) -> str:
    raw = f"{lead_type}:{domain}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:10]


def fetch_html(url: str, timeout=18) -> str:
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-LeadGen/FAST/1.0)"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return r.text[:250000]
    except Exception:
        return ""


def detect_signals(html: str) -> dict:
    return {k: bool(rx.search(html)) for k, rx in SIGNALS.items()}


def signal_count(sig: dict) -> int:
    return sum(1 for v in sig.values() if v)


def extract_canonical_domain(html: str) -> str | None:
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    return None


def is_legit_dtc(domain: str, html: str) -> tuple[bool, str]:
    """
    FAST MODE quality gates:
    - Shopify present
    - not password/coming soon/test store
    - not myshopify.com as final domain
    - has at least 1 marketing signal
    - has at least 1 commerce hint (cart or product schema/page)
    """
    if not html:
        return False, "no_html"
    if not SHOPIFY_RE.search(html):
        return False, "not_shopify"
    if PASSWORD_RE.search(html):
        return False, "password_or_coming_soon"
    if GENERIC_DEV_RE.search(html):
        return False, "dev_store_text"
    if domain.endswith(".myshopify.com"):
        return False, "myshopify_domain"

    sig = detect_signals(html)
    if signal_count(sig) < 1:
        return False, "no_marketing_signals"

    if not (CART_RE.search(html) or PRODUCT_RE.search(html)):
        return False, "no_storefront_hints"

    return True, f"signals={signal_count(sig)}"


# ============================================================
# Google Sheets
# ============================================================
def sheets_client(sa_json: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
    return gbuild("sheets", "v4", credentials=creds)


def get_existing_domains(svc, sheet_id: str) -> set:
    rng = f"{SHEET_NAME}!C2:C"
    resp = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=rng).execute()
    vals = resp.get("values", [])
    domains = set()
    for row in vals:
        if not row:
            continue
        d = norm_domain(row[0])
        if d:
            domains.add(d)
    return domains


def append_rows(svc, sheet_id: str, rows: list):
    body = {"values": rows}
    svc.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()


def row_for_pipeline(
    lead_type: str,
    company: str,
    website: str,
    current_creative_style: str,
    observed_weakness: str,
    budget_fit: str,
    notes: str,
):
    """
    Your headers A..Y, with Q..V left BLANK per your request.
    """
    return [
        lead_type,                    # A
        company[:120],                # B
        website,                      # C
        "", "", "", "",               # D-G
        "", "",                       # H-I
        current_creative_style[:250], # J
        observed_weakness[:250],      # K
        DEFAULT_PERSONA_DEPTH,        # L
        "",                           # M
        "",                           # N
        budget_fit,                   # O
        DEFAULT_STAGE,                # P
        "", "", "", "", "", "",       # Q-V blank
        "",                           # W
        notes[:700],                  # X
        DEFAULT_FOLLOW_UP_STATUS      # Y
    ]


# ============================================================
# Common Crawl (FAST MODE)
# ============================================================
def cc_latest_index_id() -> str:
    r = requests.get("https://index.commoncrawl.org/collinfo.json", timeout=20)
    r.raise_for_status()
    data = r.json()
    return data[0]["id"]


def cc_request(index_id: str, params: dict):
    endpoint = f"https://index.commoncrawl.org/{index_id}-index"
    transient = {429, 500, 502, 503, 504}

    for attempt in range(1, CC_MAX_RETRIES + 1):
        try:
            r = requests.get(endpoint, params=params, timeout=CC_TIMEOUT_SEC)
            if r.status_code == 404:
                return []  # no results
            if r.status_code in transient:
                backoff = (CC_BACKOFF_BASE ** attempt) + random.random() * CC_JITTER
                print(f"[CC] transient {r.status_code} attempt {attempt}/{CC_MAX_RETRIES} for {params.get('url')}; sleep {backoff:.2f}s")
                time.sleep(backoff)
                continue
            r.raise_for_status()

            urls = []
            for line in r.text.splitlines():
                try:
                    obj = json.loads(line)
                    u = obj.get("url")
                    if u:
                        urls.append(u)
                except Exception:
                    continue
            return urls

        except Exception as e:
            backoff = (CC_BACKOFF_BASE ** attempt) + random.random() * CC_JITTER
            print(f"[CC] error attempt {attempt}/{CC_MAX_RETRIES} for {params.get('url')}: {e}; sleep {backoff:.2f}s")
            time.sleep(backoff)

    # FAST MODE: give up quickly
    return []


def cc_search_wild(index_id: str, wild_pattern: str, limit: int) -> list[str]:
    params = {
        "url": wild_pattern,
        "output": "json",
        "limit": str(limit),
        "collapse": "urlkey",
    }
    return cc_request(index_id, params)


def homepages_from_urls(urls: list[str], max_domains: int) -> list[str]:
    out, seen = [], set()
    for u in urls:
        d = norm_domain(u)
        if not d or d in seen:
            continue
        if d in BAD_DOMAINS:
            continue
        out.append(f"https://{d}/")
        seen.add(d)
        if len(out) >= max_domains:
            break
    return out


# ============================================================
# Agency discovery fallback (DDG via ddgs if installed)
# ============================================================
def ddg_search_domains(queries: list[str], max_results_per_query: int = 15) -> set:
    """
    Free-ish fallback: DDG via ddgs package.
    If ddgs isn't installed or fails (rate-limit), returns empty set.
    """
    try:
        from ddgs import DDGS
    except Exception:
        print("[DDG] ddgs not installed; skipping DDG fallback.")
        return set()

    domains = set()
    try:
        with DDGS() as ddgs:
            for q in queries:
                try:
                    results = ddgs.text(q, max_results=max_results_per_query)
                    for r in results or []:
                        href = r.get("href") or r.get("url") or ""
                        d = norm_domain(href)
                        if d and d not in BAD_DOMAINS:
                            domains.add(d)
                    time.sleep(0.8)
                except Exception as e:
                    print(f"[DDG] query failed: {q} err={e}")
                    continue
    except Exception as e:
        print(f"[DDG] DDGS failed entirely: {e}")
        return set()

    return domains


# ============================================================
# MAIN
# ============================================================
def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    svc = sheets_client(sa_json)
    existing = get_existing_domains(svc, sheet_id)

    index_id = cc_latest_index_id()
    print(f"Using Common Crawl index: {index_id}")

    rows_to_add = []

    # ----------------------------
    # 1) DTC (Common Crawl seed -> canonical domain -> strict legit filter)
    # ----------------------------
    dtc_seed = cc_search_wild(index_id, "*.myshopify.com/*", limit=CC_LIMIT_PER_QUERY)
    dtc_candidates = homepages_from_urls(dtc_seed, max_domains=120)
    print(f"DTC candidate domains (seeded): {len(dtc_candidates)}")

    dtc_added = 0
    for homepage in dtc_candidates:
        if dtc_added >= MAX_DTC_TO_ADD:
            break

        d = norm_domain(homepage)
        if not d or d in existing or d in BAD_DOMAINS:
            continue

        html = fetch_html(homepage)
        if not html:
            continue

        # switch to canonical storefront domain if possible
        canon = extract_canonical_domain(html)
        if canon and canon != d and canon not in BAD_DOMAINS and not canon.endswith(".myshopify.com"):
            html2 = fetch_html(f"https://{canon}/")
            if html2:
                homepage = f"https://{canon}/"
                d = canon
                html = html2

        ok, reason = is_legit_dtc(d, html)
        if not ok:
            continue

        sig = detect_signals(html)
        sig_list = [k.replace("_", " ").title() for k, v in sig.items() if v]

        current_style = "Shopify + " + (", ".join(sig_list) if sig_list else "signals")
        weakness = "Needs scalable creative testing + fatigue prevention"
        budget_fit = "Med"
        lead_id = stable_id("DTC", d)
        notes = f"id={lead_id} | {reason}"

        rows_to_add.append(
            row_for_pipeline(
                lead_type="DTC",
                company=d,
                website=homepage,
                current_creative_style=current_style,
                observed_weakness=weakness,
                budget_fit=budget_fit,
                notes=notes,
            )
        )
        existing.add(d)
        dtc_added += 1
        time.sleep(SLEEP_SEC)

    print(f"DTC added this run: {dtc_added}")

    # ----------------------------
    # 2) Agencies (FAST): use DDG fallback + validate homepage keywords
    # ----------------------------
    agency_domains = ddg_search_domains(AGENCY_SEARCH_QUERIES, max_results_per_query=18)
    agency_homepages = [f"https://{d}/" for d in agency_domains if d and d not in BAD_DOMAINS]
    random.shuffle(agency_homepages)

    agency_added = 0
    for homepage in agency_homepages:
        if agency_added >= MAX_AGENCY_TO_ADD:
            break

        d = norm_domain(homepage)
        if not d or d in existing or d in BAD_DOMAINS:
            continue

        html = fetch_html(homepage)
        if not html:
            continue

        if not AGENCY_KEYWORDS.search(html):
            continue

        # Minimal label
        hits = []
        for kw in ["case studies", "portfolio", "clients", "paid social", "meta ads", "tiktok ads", "creative strategy", "ugc"]:
            if re.search(kw, html, re.I):
                hits.append(kw)

        current_style = "Agency: " + (", ".join(hits) if hits else "services")
        weakness = "Potential partner: creative overflow + testing system"
        budget_fit = "Med"
        lead_id = stable_id("Agency", d)
        notes = f"id={lead_id} | source=ddg"

        rows_to_add.append(
            row_for_pipeline(
                lead_type="Agency",
                company=d,
                website=homepage,
                current_creative_style=current_style,
                observed_weakness=weakness,
                budget_fit=budget_fit,
                notes=notes,
            )
        )
        existing.add(d)
        agency_added += 1
        time.sleep(SLEEP_SEC)

    print(f"Agency added this run: {agency_added}")

    # ----------------------------
    # Write
    # ----------------------------
    if rows_to_add:
        append_rows(svc, sheet_id, rows_to_add)
        print(f"Added {len(rows_to_add)} new leads total.")
    else:
        print("No new leads found this run.")


if __name__ == "__main__":
    main()
