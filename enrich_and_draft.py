# enrich_and_draft.py
#
# What this does:
# - Reads your "Pipeline" tab in Google Sheets
# - For each row missing a LinkedIn DM Draft (or Human Review Status), it:
#   1) Pulls light research from the company website (home + about + work/case-studies if present)
#   2) Detects marketing stack signals (Meta pixel, Klaviyo, etc.)
#   3) Detects ROLE SEGMENT (Founder / VP Marketing / Head of Growth / Agency Owner / etc.) from the "Title" column
#   4) Uses OpenAI to produce: company summary + hooks + best approach + DM + email
# - Writes back into new columns it creates if missing (to the far right)
#
# Requirements (pip):
#   pip install requests beautifulsoup4 google-api-python-client google-auth openai
#
# GitHub Secrets required:
#   GSHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON
#   OPENAI_API_KEY

import os
import json
import re
import time
import random
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gbuild

from openai import OpenAI


# ==============================
# CONFIG
# ==============================
SHEET_NAME = "Pipeline"
MAX_ROWS_PER_RUN = 25           # safety: how many leads to process per run
HTTP_TIMEOUT = 18
SLEEP_BETWEEN_LEADS = 0.35

# Sheet column names you already have (based on your headers)
LEAD_TYPE_COL = "Lead Type (DTC / Agency)"
COMPANY_COL = "Company Name"
WEBSITE_COL = "Website"
TITLE_COL = "Title"  # optional but recommended
NOTES_COL = "Notes"  # optional

# Columns this script will ensure exist (appended to the right if missing)
REQUIRED_HEADERS = [
    "Company Summary",
    "ICP Fit Notes",
    "Likely Pain",
    "Detected Signals",
    "Personalization Hooks",
    "Approach Type",
    "Recommended CTA",
    "LinkedIn DM Draft",
    "Cold Email Subject",
    "Cold Email Draft",
    "Confidence (0-100)",
    "Human Review Status",
    # Helpful debugging columns:
    "Role Segment",
    "Role-Based Style",
]

# Stack signals
SIGNALS = {
    "Meta Pixel": re.compile(r"connect\.facebook\.net|fbq\(", re.I),
    "TikTok Pixel": re.compile(r"tiktok.*pixel|ttq\(", re.I),
    "Google Tag Manager": re.compile(r"googletagmanager\.com|gtag\(", re.I),
    "Klaviyo": re.compile(r"klaviyo", re.I),
    "Attentive": re.compile(r"attentive", re.I),
    "Postscript": re.compile(r"postscript", re.I),
    "Triple Whale": re.compile(r"triplewhale", re.I),
    "Northbeam": re.compile(r"northbeam", re.I),
    "Recharge": re.compile(r"recharge", re.I),
}

BAD_DOMAINS = {
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "pinterest.com", "snapchat.com", "reddit.com",
    "google.com", "shop.app", "apps.apple.com", "play.google.com",
}


# ==============================
# Helpers
# ==============================
def norm_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        return host
    except Exception:
        return (url or "").strip().lower()


def safe_get(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-Enrich/1.1)"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return r.text[:350000]
    except Exception:
        return ""


def extract_readable_text(html: str, limit_chars: int = 6500) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit_chars]


def detect_signals(html: str) -> list[str]:
    if not html:
        return []
    found = []
    for name, rx in SIGNALS.items():
        if rx.search(html):
            found.append(name)
    return found


def extract_canonical_domain(html: str) -> str | None:
    if not html:
        return None
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m:
        return norm_domain(m.group(1))
    return None


def fetch_company_snippets(website: str) -> dict:
    """
    Pull homepage + a couple likely pages for richer personalization.
    Keep it lightweight.
    """
    domain = norm_domain(website)
    if not domain or domain in BAD_DOMAINS:
        return {"domain": domain, "base_url": "", "homepage_text": "", "about_text": "", "work_text": "", "signals": []}

    base = f"https://{domain}/"
    home_html = safe_get(base)
    if not home_html:
        # try http fallback
        base = f"http://{domain}/"
        home_html = safe_get(base)

    # canonical redirect (sometimes ddg points to subdomain)
    canon = extract_canonical_domain(home_html)
    if canon and canon != domain and canon not in BAD_DOMAINS:
        base2 = f"https://{canon}/"
        html2 = safe_get(base2)
        if html2:
            domain = canon
            base = base2
            home_html = html2

    home_text = extract_readable_text(home_html, 7000)
    sigs = detect_signals(home_html)

    # Common pages (best-effort)
    about_html = (
        safe_get(f"{base}about")
        or safe_get(f"{base}about-us")
        or safe_get(f"{base}company")
    )
    work_html = (
        safe_get(f"{base}case-studies")
        or safe_get(f"{base}work")
        or safe_get(f"{base}portfolio")
        or safe_get(f"{base}clients")
    )

    about_text = extract_readable_text(about_html, 3500)
    work_text = extract_readable_text(work_html, 3500)

    return {
        "domain": domain,
        "base_url": base,
        "homepage_text": home_text,
        "about_text": about_text,
        "work_text": work_text,
        "signals": sigs,
    }


# ==============================
# Role detection + style guidance
# ==============================
def classify_role_segment(title: str, lead_type: str) -> tuple[str, str]:
    """
    Returns (role_segment, style_guidance) used to tailor outreach.
    """
    t = (title or "").strip().lower()
    lt = (lead_type or "").strip().lower()

    # Agency-side heuristics
    if "agency" in lt:
        if any(k in t for k in ["owner", "founder", "co-founder", "principal", "partner", "managing director", "ceo"]):
            return (
                "Agency Owner/Principal",
                "Peer-to-peer operator tone. Focus on collaboration/overflow/creative engine. Avoid pitching. Ask how they handle creative testing for client accounts today."
            )
        if any(k in t for k in ["vp", "vice president", "head of", "director", "growth", "marketing", "strategy"]):
            return (
                "Agency Marketing/Strategy Lead",
                "Respectful, curious tone. Talk about how agencies position + deliver creative iteration for DTC clients. Ask what clients are asking for most (volume vs structured testing)."
            )
        return (
            "Agency Team Member",
            "Friendly curiosity. Ask what service line is growing fastest and where creative becomes a bottleneck."
        )

    # DTC-side heuristics
    if any(k in t for k in ["founder", "co-founder", "owner", "ceo", "president"]):
        return (
            "Founder/Owner",
            "Short, direct, founder-friendly. One concrete observation + one simple question. No fluff. No meeting ask in first touch."
        )

    if any(k in t for k in ["vp marketing", "vice president of marketing", "vp, marketing", "svp marketing", "evp marketing", "cmo"]):
        return (
            "VP Marketing/CMO",
            "Operator-to-operator. Show you noticed something specific. Ask a smart question about creative testing process (volume vs experimentation, fatigue, iteration loop)."
        )

    if any(k in t for k in ["head of growth", "vp growth", "director of growth", "growth lead", "acquisition", "performance marketing", "paid social"]):
        return (
            "Head of Growth/Performance",
            "Technical-but-not-jargony. Mention experimentation/creative fatigue/iteration velocity. Ask about how they decide what to test next."
        )

    if any(k in t for k in ["marketing manager", "brand manager", "social", "content", "creative"]):
        return (
            "Marketing/Brand Manager",
            "Supportive + curious. Ask how they source creative ideas and keep fresh variants coming. Make it easy to reply."
        )

    return (
        "Unknown",
        "Default to peer curiosity. One concrete observation + a simple question. Keep it human and non-salesy."
    )


# ==============================
# Google Sheets
# ==============================
def sheets_client(sa_json: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
    return gbuild("sheets", "v4", credentials=creds)


def col_to_a1(col_idx_0: int) -> str:
    """0->A, 25->Z, 26->AA ..."""
    s = ""
    n = col_idx_0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def get_sheet_values(svc, sheet_id: str, rng: str):
    return svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=rng).execute().get("values", [])


def update_sheet_values(svc, sheet_id: str, rng: str, values):
    body = {"values": values}
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()


def ensure_headers(svc, sheet_id: str) -> dict:
    """
    Read header row, append missing REQUIRED_HEADERS, and return header->index map.
    """
    header = get_sheet_values(svc, sheet_id, f"{SHEET_NAME}!1:1")
    header_row = header[0] if header else []
    existing = {h: i for i, h in enumerate(header_row) if h}

    # Validate core columns exist (do not auto-create these)
    for must in [LEAD_TYPE_COL, COMPANY_COL, WEBSITE_COL]:
        if must not in existing:
            raise RuntimeError(f"Missing required column header in sheet: '{must}'")

    changed = False
    for h in REQUIRED_HEADERS:
        if h not in existing:
            header_row.append(h)
            existing[h] = len(header_row) - 1
            changed = True

    if changed:
        update_sheet_values(svc, sheet_id, f"{SHEET_NAME}!1:1", [header_row])

    return existing


def rows_to_process(svc, sheet_id: str, header_map: dict):
    """
    Pull rows; process those missing DM Draft or Human Review Status.
    """
    values = get_sheet_values(svc, sheet_id, f"{SHEET_NAME}!A2:ZZ")
    out = []

    dm_i = header_map["LinkedIn DM Draft"]
    status_i = header_map["Human Review Status"]

    for idx, row in enumerate(values, start=2):
        max_i = max(dm_i, status_i)
        if len(row) <= max_i:
            row = row + [""] * (max_i + 1 - len(row))

        dm = (row[dm_i] or "").strip()
        status = (row[status_i] or "").strip()

        if dm and status:
            continue

        out.append((idx, row))
        if len(out) >= MAX_ROWS_PER_RUN:
            break

    return out


# ==============================
# Prompt + OpenAI call
# ==============================
def build_prompt(
    lead_type: str,
    company_name: str,
    website: str,
    title: str,
    role_segment: str,
    role_style: str,
    snippets: dict,
    notes: str
) -> str:
    signals = snippets.get("signals", [])
    home = snippets.get("homepage_text", "")
    about = snippets.get("about_text", "")
    work = snippets.get("work_text", "")

    return f"""
You are writing sincere, human outreach for Joseph at Augusta Productions (performance creative studio / fractional creative director).
Your job is NOT to pitch. Your job is to start a real conversation with another operator.

Audience context:
- This person receives dozens of vendor messages weekly.
- If it sounds templated, they will ignore it.
- Your goal is to sound like Joseph actually looked at their company.

Lead context:
Lead Type: {lead_type}
Company: {company_name}
Website: {website}
Person Title: {title}
Role Segment: {role_segment}
Role-Based Style Guidance: {role_style}

Detected marketing signals/stack:
{", ".join(signals) if signals else "None detected"}

Existing notes (if any):
{notes}

Company website text (homepage excerpt):
{home}

About excerpt:
{about}

Work/case-studies excerpt:
{work}

Hard rules:
- No buzzwords: do NOT use "scale", "growth engine", "synergy", "we help brands", "unlock growth"
- No generic compliments ("love what you're doing", "impressed by your work") unless you reference a specific detail from the text above
- Do NOT ask for a call in the first message
- LinkedIn DM must be <= 420 characters
- End with a question that is easy to answer in one sentence
- Start with a concrete observation (do NOT start with "I came across" or "I wanted to reach out")
- Tone: casual, curious, operator-to-operator

Return ONLY valid JSON with exactly these keys:
company_summary (string, 1-2 sentences)
icp_fit_notes (string, 1-2 sentences)
likely_pain (string, 1 sentence)

one_thing_noticed (string, specific observation tied to provided text)
why_it_matters (string, 1 sentence)

personalization_hooks (array of exactly 3 short bullets as strings; each must reference concrete text)

approach_type (one of: "Peer curiosity", "Specific compliment", "Partner angle", "Problem hypothesis", "Referral-style")
recommended_cta (string, a single question)

linkedin_dm (string, <= 420 chars)
email_subject (string, <= 60 chars)
cold_email (string, <= 120 words, ends with a question)

confidence (integer 0-100)
""".strip()


def call_openai_json(client: OpenAI, prompt: str) -> dict:
    # Uses Responses API via OpenAI Python SDK
    resp = client.responses.create(
        model="gpt-5",
        instructions="You output only JSON. No markdown. No extra text.",
        input=prompt,
    )
    text = (resp.output_text or "").strip()
    m = re.search(r"\{.*\}\s*$", text, flags=re.S)
    if not m:
        raise ValueError(f"Model did not return JSON. Got: {text[:300]}")
    return json.loads(m.group(0))


# ==============================
# Write back
# ==============================
def write_back_row(svc, sheet_id: str, header_map: dict, row_number: int, payload: dict, signals: list[str], role_segment: str, role_style: str):
    def get(k, default=""):
        v = payload.get(k, default)
        if isinstance(v, list):
            return "\n".join(f"- {x}" for x in v)
        return v

    updates = {
        "Company Summary": get("company_summary"),
        "ICP Fit Notes": get("icp_fit_notes"),
        "Likely Pain": get("likely_pain"),
        "Detected Signals": ", ".join(signals) if signals else "",
        "Personalization Hooks": get("personalization_hooks"),
        "Approach Type": get("approach_type"),
        "Recommended CTA": get("recommended_cta"),
        "LinkedIn DM Draft": get("linkedin_dm"),
        "Cold Email Subject": get("email_subject"),
        "Cold Email Draft": get("cold_email"),
        "Confidence (0-100)": int(payload.get("confidence", 0)),
        "Human Review Status": "Needs Review",
        "Role Segment": role_segment,
        "Role-Based Style": role_style,
    }

    max_col = max(header_map[h] for h in updates.keys())
    min_col = min(header_map[h] for h in updates.keys())
    start_a1 = col_to_a1(min_col)
    end_a1 = col_to_a1(max_col)

    # Read the row so we preserve other cells
    existing = get_sheet_values(svc, sheet_id, f"{SHEET_NAME}!{row_number}:{row_number}")
    row_vals = existing[0] if existing else []
    if len(row_vals) <= max_col:
        row_vals = row_vals + [""] * (max_col + 1 - len(row_vals))

    for h, v in updates.items():
        row_vals[header_map[h]] = v

    slice_vals = row_vals[min_col:max_col + 1]
    update_sheet_values(svc, sheet_id, f"{SHEET_NAME}!{start_a1}{row_number}:{end_a1}{row_number}", [slice_vals])


# ==============================
# MAIN
# ==============================
def main():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    openai_key = os.environ["OPENAI_API_KEY"]

    svc = sheets_client(sa_json)
    header_map = ensure_headers(svc, sheet_id)

    client = OpenAI(api_key=openai_key)

    candidates = rows_to_process(svc, sheet_id, header_map)
    if not candidates:
        print("No rows need enrichment/drafting.")
        return

    for (row_number, row) in candidates:
        def cell(col_name: str) -> str:
            if col_name not in header_map:
                return ""
            i = header_map[col_name]
            return (row[i] if i < len(row) else "") or ""

        lead_type = cell(LEAD_TYPE_COL).strip()
        company = cell(COMPANY_COL).strip()
        website = cell(WEBSITE_COL).strip()
        title = cell(TITLE_COL).strip() if TITLE_COL in header_map else ""
        notes = cell(NOTES_COL).strip() if NOTES_COL in header_map else ""

        if not website:
            print(f"Row {row_number}: missing website, skipping.")
            continue

        # Role detection
        role_segment, role_style = classify_role_segment(title, lead_type)

        # Research
        snippets = fetch_company_snippets(website)
        signals = snippets.get("signals", [])

        prompt = build_prompt(
            lead_type=lead_type or "Unknown",
            company_name=company or snippets.get("domain", "") or "Unknown",
            website=website,
            title=title or "Unknown",
            role_segment=role_segment,
            role_style=role_style,
            snippets=snippets,
            notes=notes
        )

        try:
            payload = call_openai_json(client, prompt)
            write_back_row(svc, sheet_id, header_map, row_number, payload, signals, role_segment, role_style)
            print(f"Row {row_number}: drafted successfully ({company or website}) [{role_segment}]")
        except Exception as e:
            print(f"Row {row_number}: failed drafting. err={e}")

        time.sleep(SLEEP_BETWEEN_LEADS + random.random() * 0.15)


if __name__ == "__main__":
    main()
