import os
import json
import re
import time
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gbuild


# =========================
# Config
# =========================
SHEET_NAME = "Pipeline"  # must match your Google Sheet tab name exactly

# How many candidates to pull per discovery bucket
CC_LIMIT_PER_QUERY = 250

# How many unique domains to attempt per run (keep modest to avoid timeouts)
MAX_DTC_DOMAINS = 120
MAX_AGENCY_DOMAINS = 120

# Sleep between HTTP calls to be polite + avoid throttling
SLEEP_SEC = 0.5

# Filter out social platforms that aren't direct leads
BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com",
    "snapchat.com", "reddit.com"
}

# Signals to detect on a site (used for scoring)
SIGNALS = {
    "shopify": re.compile(r"shopify|cdn\.shopify\.com|myshopify\.com", re.I),
    "woocommerce": re.compile(r"woocommerce", re.I),
    "bigcommerce": re.compile(r"bigcommerce", re.I),

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

AGENCY_KEYWORDS = [
    r"case studies", r"clients", r"portfolio", r"results", r"testimonials",
    r"paid social", r"meta ads", r"facebook ads", r"tiktok ads",
    r"ecommerce", r"shopify", r"dtc", r"performance marketing",
    r"creative strategy", r"ugc", r"creative testing"
]


# =========================
# Helpers
# =========================
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
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-LeadGen/1.0)"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return r.text[:250000]
    except Exception:
        return ""


def detect_signals(html: str) -> dict:
    return {k: bool(rx.search(html)) for k, rx in SIGNALS.items()}


def score_dtc(sig: dict) -> int:
    # Broad infra-based scoring (no niche)
    score = 0
    score += 3 if sig["shopify"] else 0
    score += 2 if sig["meta_pixel"] else 0
    score += 2 if sig["tiktok_pixel"] else 0
    score += 1 if sig["google_tag"] else 0
    score += 1 if sig["klaviyo"] else 0
    score += 1 if sig["attentive"] else 0
    score += 1 if sig["postscript"] else 0
    score += 1 if sig["triplewhale"] else 0
    score += 1 if sig["northbeam"] else 0
    score += 1 if sig["recharge"] else 0
    score += 1 if sig["woocommerce"] else 0
    score += 1 if sig["bigcommerce"] else 0
    return score


def score_agency(html: str) -> int:
    score = 0
    if re.search(r"case studies|portfolio|clients|results", html, re.I):
        score += 3
    if re.search(r"paid social|meta ads|facebook ads|tiktok ads", html, re.I):
        score += 3
    if re.search(r"shopify|ecommerce|dtc", html, re.I):
        score += 2
    if re.search(r"creative strategy|ugc|creative testing", html, re.I):
        score += 1
    if re.search(r"scale|scaling|growth", html, re.I):
        score += 1
    return score


# =========================
# Google Sheets
# =========================
def sheets_client(sa_json: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
    return gbuild("sheets", "v4", credentials=creds)


def get_existing_domains(svc, sheet_id: str) -> set:
    # Assumes Website is column C in Pipeline (per your screenshot)
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
    url: str,
    signals_str: str,
    weakness: str,
    score: int,
    lead_id: str,
    today: str
):
    """
    Matches your visible Pipeline headers:
    A Lead Type
    B Company Name
    C Website
    D Primary Contact
    E Title
    F Email
    G LinkedIn
    H Estimated Monthly Ad Spend
    I Estimated Revenue
    J Current Creative Style
    K Observed Weakness
    L Persona Depth
    M Current Agency?
    (then additional columns may exist; we’ll fill the first set consistently)
    """
    return [
        lead_type,              # A Lead Type
        company[:90],           # B Company Name
        url,                    # C Website
        "",                     # D Primary Contact
        "",                     # E Title
        "",                     # F Email
        "",                     # G LinkedIn
        "",                     # H Est Monthly Ad Spend
        "",                     # I Est Revenue
        signals_str[:200],      # J Current Creative Style (repurposed as detected stack/signals)
        weakness,               # K Observed Weakness
        "Low",                  # L Persona Depth
        "",                     # M Current Agency?
        # If your sheet has more columns, Sheets will keep them; we append left-to-right.
        f"Score={score} | id={lead_id} | domain={norm_domain(url)} | added={today}",  # N-ish Notes (if exists)
    ]


# =========================
# Common Crawl discovery
# =========================
def cc_latest_index_id() -> str:
    r = requests.get("https://index.commoncrawl.org/collinfo.json", timeout=25)
    r.raise_for_status()
    data = r.json()
    return data[0]["id"]  # typically latest


def cc_search(index_id: str, url_pattern: str, limit: int = 200) -> list[str]:
    """
    Query Common Crawl index for URLs matching a pattern.
    Returns raw URLs found in the index.
    """
    endpoint = f"https://index.commoncrawl.org/{index_id}-index"
    params = {
        "url": url_pattern,
        "output": "json",
        "limit": str(limit),
        "collapse": "urlkey",
    }
    r = requests.get(endpoint, params=params, timeout=40)
    if r.status_code == 404:
        return []
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


def homepages_from_urls(urls: list[str], max_domains: int) -> list[str]:
    """
    Convert arbitrary URLs into homepage candidates by unique domain.
    """
    out = []
    seen = set()
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


# =========================
# Main
# =========================
def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    svc = sheets_client(sa_json)
    existing = get_existing_domains(svc, sheet_id)

    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    new_rows = []

    index_id = cc_latest_index_id()
    print(f"Using Common Crawl index: {index_id}")

    def consider_url(lead_type: str, url: str):
        nonlocal new_rows, existing

        domain = norm_domain(url)
        if not domain or domain in existing:
            return
        if domain in BAD_DOMAINS:
            return

        html = fetch_html(url)
        if not html:
            return

        if lead_type == "DTC":
            sig = detect_signals(html)
            score = score_dtc(sig)

            # Looser threshold for first runs (you can tighten later)
            if score < 3:
                return

            observed = []
            for k in [
                "shopify", "woocommerce", "bigcommerce",
                "meta_pixel", "tiktok_pixel", "google_tag",
                "klaviyo", "attentive", "postscript",
                "triplewhale", "northbeam", "recharge"
            ]:
                if sig.get(k):
                    observed.append(k.replace("_", " ").title())

            weakness = "Likely needs scalable creative testing + fatigue prevention"
            company = domain

        else:
            score = score_agency(html)
            if score < 4:
                return

            hits = []
            for kw in AGENCY_KEYWORDS:
                if re.search(kw, html, re.I):
                    hits.append(re.sub(r"\W+", " ", kw).strip()[:28])
                if len(hits) >= 6:
                    break

            observed = ["Agency Signals"] + hits
            weakness = "Agency: potential creative partner (overflow creative system)"
            company = domain

        lead_id = stable_id(lead_type, domain)
        row = row_for_pipeline(
            lead_type=lead_type,
            company=company,
            url=url,
            signals_str=", ".join(observed),
            weakness=weakness,
            score=score,
            lead_id=lead_id,
            today=today
        )

        new_rows.append(row)
        existing.add(domain)
        time.sleep(SLEEP_SEC)

    # -------------------------
    # Discover DTC (Shopify-heavy)
    # -------------------------
    dtc_seed_urls = []
    dtc_seed_urls += cc_search(index_id, "*.myshopify.com/*", limit=CC_LIMIT_PER_QUERY)
    dtc_seed_urls += cc_search(index_id, "*cdn.shopify.com/*", limit=CC_LIMIT_PER_QUERY)

    dtc_homepages = homepages_from_urls(dtc_seed_urls, max_domains=MAX_DTC_DOMAINS)
    print(f"DTC candidate domains: {len(dtc_homepages)}")

    for u in dtc_homepages:
        consider_url("DTC", u)

    # -------------------------
    # Discover agencies (keyword-ish)
    # Note: Common Crawl index is URL-pattern based, so we use page slug patterns.
    # -------------------------
    agency_seed_urls = []
    agency_seed_urls += cc_search(index_id, "*case-studies*", limit=CC_LIMIT_PER_QUERY)
    agency_seed_urls += cc_search(index_id, "*case-studies/*", limit=CC_LIMIT_PER_QUERY)
    agency_seed_urls += cc_search(index_id, "*paid-social*", limit=CC_LIMIT_PER_QUERY)
    agency_seed_urls += cc_search(index_id, "*meta-ads*", limit=CC_LIMIT_PER_QUERY)
    agency_seed_urls += cc_search(index_id, "*facebook-ads*", limit=CC_LIMIT_PER_QUERY)

    agency_homepages = homepages_from_urls(agency_seed_urls, max_domains=MAX_AGENCY_DOMAINS)
    print(f"Agency candidate domains: {len(agency_homepages)}")

    for u in agency_homepages:
        consider_url("Agency", u)

    # -------------------------
    # Write results
    # -------------------------
    if new_rows:
        append_rows(svc, sheet_id, new_rows)
        print(f"Added {len(new_rows)} new leads.")
    else:
        print("No new leads found this run.")


if __name__ == "__main__":
    main()
