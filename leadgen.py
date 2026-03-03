import os, json, re, time, hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from duckduckgo_search import DDGS
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gbuild

SHEET_NAME = "Pipeline"  # <-- change if your tab name differs

# Weekly volume controls (keep modest to avoid rate limiting)
MAX_RESULTS_PER_QUERY = 20
SLEEP_BETWEEN_REQUESTS_SEC = 0.6

# Broad-spectrum discovery queries (no niches)
DTC_QUERIES = [
    '("powered by shopify" OR "myshopify.com") ("shop now" OR "buy now")',
    '"powered by shopify" ("free shipping" OR "limited time" OR "bundle")',
    '"powered by shopify" ("subscribe & save" OR "subscription")',
    'site:shop.app ("Shop" OR "Brand")',
]

AGENCY_QUERIES = [
    '"performance marketing agency" ("ecommerce" OR "shopify" OR "dtc")',
    '("paid social agency" OR "meta ads agency" OR "facebook ads agency") ("ecommerce" OR "shopify" OR "dtc")',
    '"DTC growth agency"',
    '"ecommerce growth agency" "paid social"',
]

# Site signal detection (for scoring)
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

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com"
}

def norm_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host
    except Exception:
        return url.strip().lower()

def stable_id(lead_type: str, domain: str) -> str:
    raw = f"{lead_type}:{domain}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:10]

def web_search(query: str, max_results: int = 20) -> list[dict]:
    items = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=max_results):
            link = r.get("href") or r.get("url")
            title = r.get("title") or ""
            if link:
                items.append({"link": link, "title": title})
    return items

def fetch_html(url: str, timeout=14) -> str:
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LeadGenBot/1.0)"},
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
    if re.search(r"case studies|portfolio|clients|results", html, re.I): score += 3
    if re.search(r"paid social|meta ads|facebook ads|tiktok ads", html, re.I): score += 3
    if re.search(r"shopify|ecommerce|dtc", html, re.I): score += 2
    if re.search(r"creative strategy|ugc|creative testing", html, re.I): score += 1
    if re.search(r"scale|scaling|growth", html, re.I): score += 1
    return score

def sheets_client(sa_json: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
    return gbuild("sheets", "v4", credentials=creds)

def get_existing_domains(svc, sheet_id: str):
    # Assumes Website is column C in Pipeline (your template)
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

def row_for_pipeline(lead_type: str, company: str, url: str, signals_str: str, weakness: str,
                     score: int, lead_id: str, today: str):
    """
    Maps into your Pipeline columns A..X (based on the CRM template we built earlier).
    If your columns differ, paste your header row and I’ll remap this.
    """
    return [
        lead_type,                 # A Lead Type (DTC / Agency)
        company[:80],              # B Company Name
        url,                       # C Website
        "", "", "", "",            # D-G Contact fields
        "", "",                    # H-I Ad spend / Revenue (blank)
        signals_str[:200],         # J Current Creative Style (repurposed as signals/stack)
        weakness,                  # K Observed Weakness
        "Low",                     # L Persona Depth guess
        "", "",                    # M-N
        "Medium",                  # O Budget Fit
        "Lead Identified",         # P Stage
        9500,                      # Q Deal Size placeholder
        "",                        # R Probability (optional auto in sheet)
        "",                        # S Weighted value (optional auto in sheet)
        today,                     # T First Contact Date
        today,                     # U Last Contact Date
        "",                        # V Next Follow-Up Date
        "",                        # W Days in Stage (formula in sheet)
        f"Score={score} | id={lead_id} | domain={norm_domain(url)}"  # X Notes
    ]

def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    svc = sheets_client(sa_json)
    existing = get_existing_domains(svc, sheet_id)

    new_rows = []
    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")

    def consider_url(lead_type: str, title: str, url: str):
        nonlocal new_rows, existing

        if not url:
            return
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
            if score < 4:
                return
            observed = []
            for k in ["shopify","woocommerce","bigcommerce","meta_pixel","tiktok_pixel",
                      "klaviyo","attentive","postscript","triplewhale","northbeam","recharge"]:
                if sig.get(k):
                    observed.append(k.replace("_", " ").title())
            weakness = "Likely needs creative testing system / fatigue prevention"
            company = title or domain
        else:
            score = score_agency(html)
            if score < 4:
                return
            hits = []
            for kw in AGENCY_KEYWORDS:
                if re.search(kw, html, re.I):
                    hits.append(re.sub(r"\\W+", " ", kw).strip()[:28])
                if len(hits) >= 6:
                    break
            observed = ["Agency Signals"] + hits
            weakness = "Agency: potential creative partner / overflow creative system"
            company = title or domain

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
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    # DTC discovery
    for q in DTC_QUERIES:
        results = web_search(q, max_results=MAX_RESULTS_PER_QUERY)
        for r in results:
            consider_url("DTC", r.get("title", ""), r.get("link", ""))

    # Agency discovery
    for q in AGENCY_QUERIES:
        results = web_search(q, max_results=MAX_RESULTS_PER_QUERY)
        for r in results:
            consider_url("Agency", r.get("title", ""), r.get("link", ""))

    if new_rows:
        append_rows(svc, sheet_id, new_rows)
        print(f"Added {len(new_rows)} new leads.")
    else:
        print("No new leads found this run.")

if __name__ == "__main__":
    main()
