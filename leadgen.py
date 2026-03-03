import os
import json
import re
import time
import hashlib
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gbuild

# =========================================
# CONFIG
# =========================================
SHEET_NAME = "Pipeline"  # must match your Google Sheet tab name exactly

# Common Crawl discovery limits
CC_LIMIT_PER_QUERY = 250

# How many new candidate homepages to try per run
MAX_DTC_DOMAINS = 140
MAX_AGENCY_DOMAINS = 120

# Throttling
SLEEP_SEC = 0.45

# Retry behavior for transient CC issues
CC_MAX_RETRIES = 4
CC_BACKOFF_BASE_SEC = 1.35
CC_JITTER_SEC = 0.5

# Default pipeline values
DEFAULT_STAGE = "Lead Identified"
DEFAULT_PROBABILITY_PCT = 10
DEFAULT_FOLLOW_UP_STATUS = "Not Started"
DEFAULT_PERSONA_DEPTH = "Low"

DEFAULT_DEAL_SIZE_DTC = 9500
DEFAULT_DEAL_SIZE_AGENCY = 9500

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com", "reddit.com",
    "google.com", "shop.app", "apps.apple.com", "play.google.com"
}

# Directory sites to discover agencies from
AGENCY_DIRECTORY_PREFIXES = [
    "https://clutch.co/agencies",
    "https://themanifest.com",
    "https://www.designrush.com/agency",
    "https://sortlist.com",
    "https://agencyspotter.com",
]

# =========================================
# SIGNALS + SCORING
# =========================================
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


def infer_budget_fit(score: int) -> str:
    if score >= 7:
        return "High"
    if score >= 4:
        return "Med"
    return "Low"


# =========================================
# GOOGLE SHEETS
# =========================================
def sheets_client(sa_json: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
    return gbuild("sheets", "v4", credentials=creds)


def get_existing_domains(svc, sheet_id: str) -> set:
    rng = f"{SHEET_NAME}!C2:C"  # Website column
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
    deal_size: int,
    probability_pct: int,
    first_contact_date: str,
    last_contact_date: str,
    next_follow_up_date: str,
    notes: str,
    follow_up_status: str
):
    """
    EXACT mapping to your headers (A..Y):

    A  Lead Type (DTC / Agency)
    B  Company Name
    C  Website
    D  Primary Contact
    E  Title
    F  Email
    G  LinkedIn
    H  Estimated Monthly Ad Spend
    I  Estimated Revenue
    J  Current Creative Style
    K  Observed Weakness
    L  Persona Depth (Low/Med/High)
    M  Current Agency? (Y/N)
    N  Media Buyer In-House? (Y/N)
    O  Budget Fit (High/Med/Low)
    P  Stage
    Q  Deal Size ($)
    R  Probability %
    S  Weighted Value ($)
    T  First Contact Date
    U  Last Contact Date
    V  Next Follow-Up Date
    W  Days in Stage
    X  Notes
    Y  Follow-Up Status
    """
    weighted_value = round((deal_size or 0) * (probability_pct or 0) / 100)
    days_in_stage = ""  # recommend formula in sheet

    return [
        lead_type,                    # A
        company[:120],                # B
        website,                      # C
        "",                           # D
        "",                           # E
        "",                           # F
        "",                           # G
        "",                           # H
        "",                           # I
        current_creative_style[:250], # J
        observed_weakness[:250],      # K
        persona_depth,                # L
        current_agency,               # M
        media_buyer_inhouse,          # N
        budget_fit,                   # O
        stage,                        # P
        deal_size,                    # Q
        probability_pct,              # R
        weighted_value,               # S
        first_contact_date,           # T
        last_contact_date,            # U
        next_follow_up_date,          # V
        days_in_stage,                # W
        notes[:700],                  # X
        follow_up_status              # Y
    ]


# =========================================
# COMMON CRAWL (correct 404 behavior + retries for transient errors)
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

            # 404 from CC index means "no results" for that query, not a real failure
            if r.status_code == 404:
                return r

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

    # if we got here, give up (treat as empty results)
    print(f"[CC] giving up on query after retries: {params.get('url')}")
    return None


def cc_search_prefix(index_id: str, prefix_url: str, limit: int = 200) -> list[str]:
    """
    Search CC index by URL prefix.
    """
    endpoint = f"https://index.commoncrawl.org/{index_id}-index"
    params = {
        "url": prefix_url,
        "matchType": "prefix",
        "output": "json",
        "limit": str(limit),
        "collapse": "urlkey",
    }

    r = request_with_retries(endpoint, params=params, timeout=45)
    if r is None:
        return []
    if r.status_code == 404:
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


def cc_search_wild(index_id: str, wild_pattern: str, limit: int = 200) -> list[str]:
    """
    CC index supports wildcard patterns in some cases (like *.myshopify.com/*).
    We keep this only for DTC where it already works.
    """
    endpoint = f"https://index.commoncrawl.org/{index_id}-index"
    params = {
        "url": wild_pattern,
        "output": "json",
        "limit": str(limit),
        "collapse": "urlkey",
    }

    r = request_with_retries(endpoint, params=params, timeout=45)
    if r is None:
        return []
    if r.status_code == 404:
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


# =========================================
# AGENCY EXTRACTION FROM DIRECTORY PAGES
# =========================================
def extract_candidate_websites_from_html(html: str) -> set:
    """
    Heuristic extraction of candidate agency websites from directory pages.
    We extract http(s) links, then filter out the directory domain + known socials.
    """
    urls = set(re.findall(r'https?://[^\s"\'<>]+', html))
    cleaned = set()

    for u in urls:
        # strip junk
        u = u.split("#")[0]
        u = u.split("?")[0]
        d = norm_domain(u)
        if not d:
            continue
        if d in BAD_DOMAINS:
            continue
        # ignore directory domains themselves
        if any(d.endswith(norm_domain(p)) for p in AGENCY_DIRECTORY_PREFIXES):
            continue
        # ignore obvious tracking/outbound wrappers
        if "googleadservices" in d or "doubleclick" in d:
            continue

        # likely actual company site if TLD-ish and not the directory
        cleaned.add(f"https://{d}/")

    return cleaned


# =========================================
# MAIN
# =========================================
def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    svc = sheets_client(sa_json)
    existing_domains = get_existing_domains(svc, sheet_id)

    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    new_rows = []

    index_id = cc_latest_index_id()
    print(f"Using Common Crawl index: {index_id}")

    def add_lead(lead_type: str, homepage_url: str):
        nonlocal new_rows, existing_domains

        domain = norm_domain(homepage_url)
        if not domain or domain in existing_domains:
            return
        if domain in BAD_DOMAINS:
            return

        html = fetch_html(homepage_url)
        if not html:
            return

        lead_id = stable_id(lead_type, domain)

        persona_depth = DEFAULT_PERSONA_DEPTH
        current_agency = ""         # unknown
        media_buyer_inhouse = ""    # unknown
        stage = DEFAULT_STAGE
        probability_pct = DEFAULT_PROBABILITY_PCT
        first_contact_date = today
        last_contact_date = today
        next_follow_up_date = ""
        follow_up_status = DEFAULT_FOLLOW_UP_STATUS

        company = domain

        if lead_type == "DTC":
            sig = detect_signals(html)
            score = score_dtc(sig)

            # keep open; you can tighten later
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

            current_creative_style = ", ".join(observed) if observed else "Ecommerce stack detected"
            observed_weakness = "Likely needs scalable creative testing + fatigue prevention"
            deal_size = DEFAULT_DEAL_SIZE_DTC
            budget_fit = infer_budget_fit(score)
            notes = f"Score={score} | id={lead_id} | domain={domain}"

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

            current_creative_style = "Agency Signals: " + ", ".join(hits) if hits else "Agency site detected"
            observed_weakness = "Agency: potential creative partner (overflow creative system)"
            deal_size = DEFAULT_DEAL_SIZE_AGENCY
            budget_fit = infer_budget_fit(score)
            notes = f"Score={score} | id={lead_id} | domain={domain}"

        row = row_for_pipeline(
            lead_type=lead_type,
            company=company,
            website=homepage_url,
            current_creative_style=current_creative_style,
            observed_weakness=observed_weakness,
            persona_depth=persona_depth,
            current_agency=current_agency,
            media_buyer_inhouse=media_buyer_inhouse,
            budget_fit=budget_fit,
            stage=stage,
            deal_size=deal_size,
            probability_pct=probability_pct,
            first_contact_date=first_contact_date,
            last_contact_date=last_contact_date,
            next_follow_up_date=next_follow_up_date,
            notes=notes,
            follow_up_status=follow_up_status
        )

        new_rows.append(row)
        existing_domains.add(domain)
        time.sleep(SLEEP_SEC)

    # -------------------------
    # DTC Discovery (Shopify)
    # -------------------------
    dtc_seed_urls = []
    dtc_seed_urls += cc_search_wild(index_id, "*.myshopify.com/*", limit=CC_LIMIT_PER_QUERY)

    dtc_homepages = homepages_from_urls(dtc_seed_urls, max_domains=MAX_DTC_DOMAINS)
    print(f"DTC candidate domains: {len(dtc_homepages)}")

    for u in dtc_homepages:
        add_lead("DTC", u)

    # -------------------------
    # Agency Discovery via directories (more reliable than guessing slugs)
    # -------------------------
    dir_pages = []
    for prefix in AGENCY_DIRECTORY_PREFIXES:
        dir_pages += cc_search_prefix(index_id, prefix, limit=120)

    # Dedup directory pages
    dir_pages = list(dict.fromkeys(dir_pages))[:180]
    print(f"Directory pages discovered: {len(dir_pages)}")

    candidate_agency_sites = set()
    for page in dir_pages[:120]:
        html = fetch_html(page)
        if not html:
            continue
        candidate_agency_sites |= extract_candidate_websites_from_html(html)
        time.sleep(SLEEP_SEC)

        if len(candidate_agency_sites) >= 600:
            break

    # Convert to a bounded list
    agency_homepages = list(candidate_agency_sites)
    random.shuffle(agency_homepages)
    agency_homepages = agency_homepages[:MAX_AGENCY_DOMAINS]

    print(f"Agency candidate domains: {len(agency_homepages)}")

    for u in agency_homepages:
        add_lead("Agency", u)

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
