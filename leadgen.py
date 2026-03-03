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

# Higher caps so you can collect more weekly
MAX_DTC_TO_ADD = 120
MAX_AGENCY_TO_ADD = 80

# DDG caps (more volume)
DDG_RESULTS_PER_QUERY_DTC = 50
DDG_RESULTS_PER_QUERY_AGENCY = 35

SLEEP_SEC = 0.20  # keep low but not zero to avoid rate limiting

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
SHOPIFY_RE = re.compile(r"(cdn\.shopify\.com|myshopify\.com|Shopify|Powered by Shopify)", re.I)
PASSWORD_RE = re.compile(r"(enter store using password|opening soon|password)", re.I)
GENERIC_DEV_RE = re.compile(r"(example store|test store|coming soon)", re.I)

CART_RE = re.compile(r"(/cart|add to cart|checkout|shop pay)", re.I)
PRODUCT_RE = re.compile(r"(/products/|schema\.org/Product|product-form|product__)", re.I)

POLICY_RE = re.compile(r"(privacy policy|refund policy|return policy|shipping policy|terms of service)", re.I)

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

# More DTC queries = more volume
DTC_SEARCH_QUERIES = [
    # Shopify footprints
    '"Powered by Shopify" "shipping policy"',
    '"Powered by Shopify" "refund policy"',
    '"Powered by Shopify" "privacy policy" "terms of service"',
    'inurl:/products/ "Powered by Shopify"',
    '"Powered by Shopify" "add to cart"',
    '"Powered by Shopify" "Shop Pay"',

    # Stack-based discovery (often higher-spend stores)
    '"Powered by Shopify" "Klaviyo"',
    '"Powered by Shopify" "Attentive"',
    '"Powered by Shopify" "Postscript"',
    '"Powered by Shopify" "Triple Whale"',
    '"Powered by Shopify" "Northbeam"',
    '"Powered by Shopify" "Recharge"',

    # Broader ecommerce footprints
    '"cdn.shopify.com" "privacy policy"',
    '"cdn.shopify.com" "refund policy"',
    '"Shop Pay" "Powered by Shopify"',
]

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
    "creative strategy agency paid social case studies",
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
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-LeadGen/FAST/UPGRADED/1.0)"},
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


def dtc_ok(domain: str, html: str) -> tuple[bool, str, str]:
    """
    Controlled gate + scoring:
    - Shopify present
    - not password/dev
    - not myshopify domain
    - must have storefront hint OR policy page hint OR marketing signals
    Budget Fit:
      High: meta_pixel + (klaviyo/attentive/postscript) OR triplewhale/northbeam
      Med: >=1 signal OR (policy+storefront)
      Low: storefront only
    Returns: (ok, reason, budget_fit)
    """
    if not html:
        return False, "no_html", "Low"
    if not SHOPIFY_RE.search(html):
        return False, "not_shopify", "Low"
    if PASSWORD_RE.search(html):
        return False, "password_or_coming_soon", "Low"
    if GENERIC_DEV_RE.search(html):
        return False, "dev_store_text", "Low"
    if domain.endswith(".myshopify.com"):
        return False, "myshopify_domain", "Low"

    sig = detect_signals(html)
    sc = signal_count(sig)
    has_storefront = bool(CART_RE.search(html) or PRODUCT_RE.search(html))
    has_policy = bool(POLICY_RE.search(html))

    if not (has_storefront or has_policy or sc >= 1):
        return False, "no_storefront_policy_or_signals", "Low"

    # Budget Fit scoring
    has_meta = sig.get("meta_pixel", False)
    has_sms_email = sig.get("klaviyo", False) or sig.get("attentive", False) or sig.get("postscript", False)
    has_attrib = sig.get("triplewhale", False) or sig.get("northbeam", False)

    if has_attrib or (has_meta and has_sms_email):
        budget = "High"
    elif sc >= 1 or (has_policy and has_storefront):
        budget = "Med"
    else:
        budget = "Low"

    reason = f"signals={sc} storefront={has_storefront} policy={has_policy}"
    return True, reason, budget


def agency_budget_fit(html: str) -> str:
    """
    Very rough heuristic:
    - High if they mention ecommerce + paid social + case studies/clients
    - Med if they mention 2+ key proof terms
    """
    text = html.lower()
    proof = 0
    for kw in ["case studies", "clients", "portfolio", "results", "testimonials"]:
        proof += 1 if kw in text else 0
    paid = 1 if ("paid social" in text or "meta ads" in text or "facebook ads" in text or "tiktok ads" in text) else 0
    ecommerce = 1 if ("ecommerce" in text or "shopify" in text or "dtc" in text) else 0

    if proof >= 2 and paid and ecommerce:
        return "High"
    if proof >= 1 and (paid or ecommerce):
        return "Med"
    return "Low"


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


def row_for_pipeline(lead_type, company, website, style, weakness, budget_fit, notes):
    # Q..V blank per your request
    return [
        lead_type, company[:120], website,
        "", "", "", "",             # D-G
        "", "",                     # H-I
        style[:250],                # J
        weakness[:250],             # K
        DEFAULT_PERSONA_DEPTH,      # L
        "", "",                     # M-N
        budget_fit,                 # O
        DEFAULT_STAGE,              # P
        "", "", "", "", "", "",     # Q-V
        "",                         # W
        notes[:700],                # X
        DEFAULT_FOLLOW_UP_STATUS    # Y
    ]


# ============================================================
# DDG discovery
# ============================================================
def ddg_search_domains(queries: list[str], max_results_per_query: int) -> set:
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

    rows = []

    # ----------------------------
    # DTC via DDG (expanded)
    # ----------------------------
    dtc_domains = ddg_search_domains(DTC_SEARCH_QUERIES, max_results_per_query=DDG_RESULTS_PER_QUERY_DTC)
    dtc_homepages = [f"https://{d}/" for d in dtc_domains if d and d not in BAD_DOMAINS]
    random.shuffle(dtc_homepages)

    dtc_added = 0
    for homepage in dtc_homepages:
        if dtc_added >= MAX_DTC_TO_ADD:
            break
        d = norm_domain(homepage)
        if not d or d in existing:
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

        ok, reason, budget = dtc_ok(d, html)
        if not ok:
            continue

        sig = detect_signals(html)
        sig_list = [k.replace("_", " ").title() for k, v in sig.items() if v]
        style = "Shopify + " + (", ".join(sig_list) if sig_list else "storefront/policy")
        weakness = "Needs scalable creative testing + fatigue prevention"
        lead_id = stable_id("DTC", d)
        notes = f"id={lead_id} | source=ddg | {reason} | stack={','.join(sig_list) if sig_list else 'none'}"

        rows.append(row_for_pipeline("DTC", d, homepage, style, weakness, budget, notes))
        existing.add(d)
        dtc_added += 1
        time.sleep(SLEEP_SEC)

    print(f"DTC added this run: {dtc_added}")

    # ----------------------------
    # Agencies via DDG (still working)
    # ----------------------------
    agency_domains = ddg_search_domains(AGENCY_SEARCH_QUERIES, max_results_per_query=DDG_RESULTS_PER_QUERY_AGENCY)
    agency_homepages = [f"https://{d}/" for d in agency_domains if d and d not in BAD_DOMAINS]
    random.shuffle(agency_homepages)

    agency_added = 0
    for homepage in agency_homepages:
        if agency_added >= MAX_AGENCY_TO_ADD:
            break
        d = norm_domain(homepage)
        if not d or d in existing:
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

        style = "Agency: " + (", ".join(hits) if hits else "services")
        weakness = "Potential partner: creative overflow + testing system"
        budget = agency_budget_fit(html)
        lead_id = stable_id("Agency", d)
        notes = f"id={lead_id} | source=ddg | proof={','.join(hits) if hits else 'none'}"

        rows.append(row_for_pipeline("Agency", d, homepage, style, weakness, budget, notes))
        existing.add(d)
        agency_added += 1
        time.sleep(SLEEP_SEC)

    print(f"Agency added this run: {agency_added}")

    if rows:
        append_rows(svc, sheet_id, rows)
        print(f"Added {len(rows)} new leads total.")
    else:
        print("No new leads found this run.")


if __name__ == "__main__":
    main()
