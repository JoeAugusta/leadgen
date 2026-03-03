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

# =========================================
# CONFIG
# =========================================
SHEET_NAME = "Pipeline"

CC_LIMIT_PER_QUERY = 350
MAX_DTC_DOMAINS = 120
MAX_AGENCY_DOMAINS = 140

SLEEP_SEC = 0.4

CC_MAX_RETRIES = 4
CC_BACKOFF_BASE_SEC = 1.35
CC_JITTER_SEC = 0.5

DEFAULT_STAGE = "Lead Identified"
DEFAULT_FOLLOW_UP_STATUS = "Not Started"
DEFAULT_PERSONA_DEPTH = "Low"

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com", "reddit.com",
    "google.com", "shop.app", "apps.apple.com", "play.google.com",
}

# =========================================
# DTC SIGNALS / FILTERS
# =========================================
SHOPIFY_RE = re.compile(r"cdn\.shopify\.com|myshopify\.com|Shopify", re.I)
PASSWORD_RE = re.compile(r"password|enter store using password|opening soon", re.I)

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

AGENCY_KEYWORDS = re.compile(
    r"(case studies|portfolio|clients|results|testimonials|paid social|meta ads|facebook ads|tiktok ads|"
    r"performance marketing|creative strategy|ugc|creative testing|ecommerce marketing|shopify agency|dtc)",
    re.I
)

# =========================================
# Helpers
# =========================================
def norm_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host
    except Exception:
        return url.strip().lower()


def stable_id(lead_type: str, domain: str) -> str:
    raw = f"{lead_type}:{domain}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:10]


def fetch_html(url: str, timeout=20) -> str:
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-LeadGen/1.2)"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return r.text[:250000]
    except Exception:
        return ""


def detect_signals(html: str) -> dict:
    return {k: bool(rx.search(html)) for k, rx in SIGNALS.items()}


def score_signal_count(sig: dict) -> int:
    return sum(1 for v in sig.values() if v)


def extract_canonical_domain(html: str) -> str | None:
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    return None


def is_legit_dtc_store(domain: str, html: str) -> tuple[bool, str]:
    if not html:
        return False, "no_html"
    if not SHOPIFY_RE.search(html):
        return False, "not_shopify"
    if PASSWORD_RE.search(html):
        return False, "password_or_coming_soon"
    if domain.endswith(".myshopify.com"):
        return False, "myshopify_subdomain"
    sig = detect_signals(html)
    if score_signal_count(sig) < 1:
        return False, "no_tracking_signals"
    return True, "ok"


# =========================================
# Google Sheets
# =========================================
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
    persona_depth: str,
    current_agency: str,
    media_buyer_inhouse: str,
    budget_fit: str,
    stage: str,
    notes: str,
    follow_up_status: str
):
    # Leaves Q..V blank per your request
    return [
        lead_type,                    # A
        company[:120],                # B
        website,                      # C
        "", "", "", "",               # D-G
        "", "",                       # H-I
        current_creative_style[:250], # J
        observed_weakness[:250],      # K
        persona_depth,                # L
        current_agency,               # M
        media_buyer_inhouse,          # N
        budget_fit,                   # O
        stage,                        # P
        "", "", "", "", "", "",       # Q-V blank
        "",                           # W
        notes[:700],                  # X
        follow_up_status              # Y
    ]


# =========================================
# Common Crawl
# =========================================
def cc_latest_index_id() -> str:
    r = requests.get("https://index.commoncrawl.org/collinfo.json", timeout=25)
    r.raise_for_status()
    data = r.json()
    return data[0]["id"]


def request_with_retries(url: str, params: dict, timeout: int):
    transient = {429, 500, 502, 503, 504}
    for attempt in range(1, CC_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)

            if r.status_code == 404:
                return r  # no results

            if r.status_code in transient:
                backoff = (CC_BACKOFF_BASE_SEC ** attempt) + random.random() * CC_JITTER_SEC
                print(f"[CC] transient {r.status_code} attempt {attempt}/{CC_MAX_RETRIES} for {params.get('url')}; sleep {backoff:.2f}s")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            backoff = (CC_BACKOFF_BASE_SEC ** attempt) + random.random() * CC_JITTER_SEC
            print(f"[CC] error attempt {attempt}/{CC_MAX_RETRIES} for {params.get('url')}: {e}; sleep {backoff:.2f}s")
            time.sleep(backoff)

    return None


def cc_search(index_id: str, url_value: str, match_type: str | None, limit: int) -> list[str]:
    endpoint = f"https://index.commoncrawl.org/{index_id}-index"
    params = {
        "url": url_value,
        "output": "json",
        "limit": str(limit),
        "collapse": "urlkey",
    }
    if match_type:
        params["matchType"] = match_type

    r = request_with_retries(endpoint, params=params, timeout=45)
    if r is None or r.status_code == 404:
        return []

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


# =========================================
# MAIN
# =========================================
def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    svc = sheets_client(sa_json)
    existing = get_existing_domains(svc, sheet_id)

    index_id = cc_latest_index_id()
    print(f"Using Common Crawl index: {index_id}")

    new_rows = []

    def add_dtc(homepage_url: str):
        nonlocal new_rows, existing

        domain = norm_domain(homepage_url)
        if not domain or domain in existing or domain in BAD_DOMAINS:
            return

        html = fetch_html(homepage_url)
        if not html:
            return

        # Try to switch from myshopify -> canonical storefront
        canon = extract_canonical_domain(html)
        if canon and canon != domain and canon not in BAD_DOMAINS and not canon.endswith(".myshopify.com"):
            html2 = fetch_html(f"https://{canon}/")
            if html2:
                homepage_url = f"https://{canon}/"
                domain = canon
                html = html2

        ok, reason = is_legit_dtc_store(domain, html)
        if not ok:
            return

        sig = detect_signals(html)
        sig_list = [k.replace("_", " ").title() for k, v in sig.items() if v]
        current_creative_style = "Shopify + " + (", ".join(sig_list) if sig_list else "signals")
        observed_weakness = "Needs scalable creative testing + fatigue prevention"
        budget_fit = "Med"
        lead_id = stable_id("DTC", domain)

        notes = f"id={lead_id} | domain={domain} | {reason} | signals={len(sig_list)}"

        row = row_for_pipeline(
            lead_type="DTC",
            company=domain,
            website=homepage_url,
            current_creative_style=current_creative_style,
            observed_weakness=observed_weakness,
            persona_depth=DEFAULT_PERSONA_DEPTH,
            current_agency="",
            media_buyer_inhouse="",
            budget_fit=budget_fit,
            stage=DEFAULT_STAGE,
            notes=notes,
            follow_up_status=DEFAULT_FOLLOW_UP_STATUS
        )

        new_rows.append(row)
        existing.add(domain)
        time.sleep(SLEEP_SEC)

    def add_agency(homepage_url: str, source: str):
        nonlocal new_rows, existing

        domain = norm_domain(homepage_url)
        if not domain or domain in existing or domain in BAD_DOMAINS:
            return

        html = fetch_html(homepage_url)
        if not html:
            return

        if not AGENCY_KEYWORDS.search(html):
            return

        hits = []
        for kw in ["case studies", "portfolio", "clients", "paid social", "meta ads", "tiktok ads", "creative strategy", "ugc"]:
            if re.search(kw, html, re.I):
                hits.append(kw)

        current_creative_style = "Agency: " + (", ".join(hits) if hits else "marketing services")
        observed_weakness = "Potential partner: creative overflow + testing system"
        budget_fit = "Med"
        lead_id = stable_id("Agency", domain)

        notes = f"id={lead_id} | domain={domain} | source={source}"

        row = row_for_pipeline(
            lead_type="Agency",
            company=domain,
            website=homepage_url,
            current_creative_style=current_creative_style,
            observed_weakness=observed_weakness,
            persona_depth=DEFAULT_PERSONA_DEPTH,
            current_agency="",
            media_buyer_inhouse="",
            budget_fit=budget_fit,
            stage=DEFAULT_STAGE,
            notes=notes,
            follow_up_status=DEFAULT_FOLLOW_UP_STATUS
        )

        new_rows.append(row)
        existing.add(domain)
        time.sleep(SLEEP_SEC)

    # -------------------------
    # DTC discovery (wildcard that actually works)
    # -------------------------
    dtc_seed_urls = cc_search(index_id, "*.myshopify.com/*", match_type=None, limit=CC_LIMIT_PER_QUERY)
    dtc_homepages = homepages_from_urls(dtc_seed_urls, max_domains=MAX_DTC_DOMAINS)
    print(f"DTC candidate domains (seeded): {len(dtc_homepages)}")

    for u in dtc_homepages:
        add_dtc(u)

    # -------------------------
    # Agency discovery (path mining that CC supports)
    # We search by prefix for common agency pages across many domains.
    # -------------------------
    agency_urls = []

    # These tend to exist on agencies and be indexed.
    # NOTE: We use prefix match on "https://" plus path pieces by TLD buckets.
    # This is a heuristic but works better than directory link extraction.
    for tld in ["com", "io", "co", "agency", "net", "us", "ca", "uk", "au"]:
        for path in ["/case-studies", "/portfolio", "/work", "/clients", "/our-work", "/services/paid-social"]:
            prefix = f"https://*.{tld}{path}"
            # CC doesn't support wildcard in prefix matchType, so we fallback to wild search style:
            # Use non-prefix wildcard query for domain+path combinations.
            agency_urls += cc_search(index_id, f"*.{tld}{path}*", match_type=None, limit=80)

    # Convert to homepages
    agency_homepages = homepages_from_urls(agency_urls, max_domains=MAX_AGENCY_DOMAINS)
    print(f"Agency candidate domains (path-mined): {len(agency_homepages)}")

    for u in agency_homepages:
        add_agency(u, source="cc_path_mining")

    # -------------------------
    # Write
    # -------------------------
    if new_rows:
        append_rows(svc, sheet_id, new_rows)
        print(f"Added {len(new_rows)} new leads.")
    else:
        print("No new leads found this run.")


if __name__ == "__main__":
    main()
