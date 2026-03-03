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
# CONFIG
# ============================================================
SHEET_NAME = "Pipeline"

CC_LIMIT_PER_QUERY = 160
MAX_DTC_TO_ADD = 60
MAX_AGENCY_TO_ADD = 60

SLEEP_SEC = 0.25

CC_TIMEOUT_SEC = 15
CC_MAX_RETRIES = 2
CC_BACKOFF_BASE = 1.4
CC_JITTER = 0.4

DEFAULT_STAGE = "Lead Identified"
DEFAULT_FOLLOW_UP_STATUS = "Not Started"
DEFAULT_PERSONA_DEPTH = "Low"

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com", "reddit.com",
    "google.com", "shop.app", "apps.apple.com", "play.google.com",
}

# ============================================================
# DTC HEURISTICS
# ============================================================
SHOPIFY_RE = re.compile(r"(cdn\.shopify\.com|myshopify\.com|Shopify)", re.I)
PASSWORD_RE = re.compile(r"(enter store using password|opening soon|password)", re.I)
GENERIC_DEV_RE = re.compile(r"(example store|test store|coming soon)", re.I)

CART_RE = re.compile(r"(/cart|add to cart|checkout)", re.I)
PRODUCT_RE = re.compile(r"(/products/|schema\.org/Product|product-form|product__)", re.I)

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
# AGENCY HEURISTICS
# ============================================================
AGENCY_KEYWORDS = re.compile(
    r"(case studies|portfolio|clients|our work|results|testimonials|"
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
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-LeadGen/FAST/1.1)"},
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


def dtc_quality(domain: str, html: str) -> tuple[bool, str, int]:
    """
    Two-tier legit filter:
    - Must be Shopify + not password/dev + not myshopify domain
    - Must have >=1 marketing signal
    - Tier 1: also has storefront hints (cart or product)
    - Tier 2: allow missing storefront hints (some sites hide this in JS)
    Returns: (ok, reason, tier)
    """
    if not html:
        return False, "no_html", 0
    if not SHOPIFY_RE.search(html):
        return False, "not_shopify", 0
    if PASSWORD_RE.search(html):
        return False, "password_or_coming_soon", 0
    if GENERIC_DEV_RE.search(html):
        return False, "dev_store_text", 0
    if domain.endswith(".myshopify.com"):
        return False, "myshopify_domain", 0

    sig = detect_signals(html)
    sc = signal_count(sig)
    if sc < 1:
        return False, "no_marketing_signals", 0

    has_storefront = bool(CART_RE.search(html) or PRODUCT_RE.search(html))
    if has_storefront:
        return True, f"tier1 signals={sc}", 1

    # Tier 2 (looser, still legit enough)
    return True, f"tier2 signals={sc}", 2


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
    # Q..V blank per your request
    return [
        lead_type, company[:120], website,
        "", "", "", "",                      # D-G
        "", "",                              # H-I
        current_creative_style[:250],        # J
        observed_weakness[:250],             # K
        DEFAULT_PERSONA_DEPTH,               # L
        "", "",                              # M-N
        budget_fit,                          # O
        DEFAULT_STAGE,                       # P
        "", "", "", "", "", "",              # Q-V blank
        "",                                  # W
        notes[:700],                         # X
        DEFAULT_FOLLOW_UP_STATUS             # Y
    ]


# ============================================================
# Common Crawl (FAST)
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
                return []
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

    return []


def cc_search_wild(index_id: str, wild_pattern: str, limit: int) -> list[str]:
    params = {"url": wild_pattern, "output": "json", "limit": str(limit), "collapse": "urlkey"}
    return cc_request(index_id, params)


def homepages_from_urls(urls: list[str], max_domains: int) -> list[str]:
    out, seen = [], set()
    for u in urls:
        d = norm_domain(u)
        if not d or d in seen or d in BAD_DOMAINS:
            continue
        out.append(f"https://{d}/")
        seen.add(d)
        if len(out) >= max_domains:
            break
    return out


# ============================================================
# DDG fallback (robust import)
# ============================================================
def ddg_search_domains(queries: list[str], max_results_per_query: int = 18) -> set:
    try:
        from ddgs import DDGS
    except Exception as e:
        print(f"[DDG] ddgs not available; skipping. err={e}")
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

    rows = []

    # ----------------------------
    # DTC: seed via myshopify -> canonical -> quality tiers
    # ----------------------------
    dtc_seed = cc_search_wild(index_id, "*.myshopify.com/*", limit=CC_LIMIT_PER_QUERY)
    dtc_candidates = homepages_from_urls(dtc_seed, max_domains=120)
    print(f"DTC candidate domains (seeded): {len(dtc_candidates)}")

    added_dtc = 0
    for homepage in dtc_candidates:
        if added_dtc >= MAX_DTC_TO_ADD:
            break

        d = norm_domain(homepage)
        if not d or d in existing or d in BAD_DOMAINS:
            continue

        html = fetch_html(homepage)
        if not html:
            continue

        canon = extract_canonical_domain(html)
        if canon and canon != d and canon not in BAD_DOMAINS and not canon.endswith(".myshopify.com"):
            html2 = fetch_html(f"https://{canon}/")
            if html2:
                homepage = f"https://{canon}/"
                d = canon
                html = html2

        ok, reason, tier = dtc_quality(d, html)
        if not ok:
            continue

        sig = detect_signals(html)
        sig_list = [k.replace("_", " ").title() for k, v in sig.items() if v]

        current_style = "Shopify + " + (", ".join(sig_list) if sig_list else "signals")
        weakness = "Needs scalable creative testing + fatigue prevention"
        budget_fit = "Med"
        lead_id = stable_id("DTC", d)
        notes = f"id={lead_id} | {reason}"

        rows.append(row_for_pipeline("DTC", d, homepage, current_style, weakness, budget_fit, notes))
        existing.add(d)
        added_dtc += 1
        time.sleep(SLEEP_SEC)

    print(f"DTC added this run: {added_dtc}")

    # ----------------------------
    # Agencies: DDG fallback + validate homepage content
    # ----------------------------
    agency_domains = ddg_search_domains(AGENCY_SEARCH_QUERIES, max_results_per_query=18)
    agency_homepages = [f"https://{d}/" for d in agency_domains if d and d not in BAD_DOMAINS]
    random.shuffle(agency_homepages)

    added_agency = 0
    for homepage in agency_homepages:
        if added_agency >= MAX_AGENCY_TO_ADD:
            break

        d = norm_domain(homepage)
        if not d or d in existing or d in BAD_DOMAINS:
            continue

        html = fetch_html(homepage)
        if not html:
            continue

        if not AGENCY_KEYWORDS.search(html):
            continue

        hits = []
        for kw in ["case studies", "portfolio", "clients", "paid social", "meta ads", "tiktok ads", "creative strategy", "ugc"]:
            if re.search(kw, html, re.I):
                hits.append(kw)

        current_style = "Agency: " + (", ".join(hits) if hits else "services")
        weakness = "Potential partner: creative overflow + testing system"
        budget_fit = "Med"
        lead_id = stable_id("Agency", d)
        notes = f"id={lead_id} | source=ddg"

        rows.append(row_for_pipeline("Agency", d, homepage, current_style, weakness, budget_fit, notes))
        existing.add(d)
        added_agency += 1
        time.sleep(SLEEP_SEC)

    print(f"Agency added this run: {added_agency}")

    if rows:
        append_rows(svc, sheet_id, rows)
        print(f"Added {len(rows)} new leads total.")
    else:
        print("No new leads found this run.")


if __name__ == "__main__":
    main()
