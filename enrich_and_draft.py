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

from openai import OpenAI  # OpenAI Python SDK (Responses API)

# ==============================
# CONFIG
# ==============================
SHEET_NAME = "Pipeline"
MAX_ROWS_PER_RUN = 25          # safety: process only N new leads per run
HTTP_TIMEOUT = 18
SLEEP_BETWEEN_LEADS = 0.35

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
]

# Your existing lead columns (assumed)
H_COL = "Lead Type (DTC / Agency)"
B_COL = "Company Name"
C_COL = "Website"
X_COL = "Notes"

# Signal detection (reuse your idea)
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
            headers={"User-Agent": "Mozilla/5.0 (compatible; AP-Enrich/1.0)"},
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return ""
        return r.text[:350000]
    except Exception:
        return ""


def extract_readable_text(html: str, limit_chars: int = 6000) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    # Remove noisy tags
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


def fetch_company_snippets(website: str) -> dict:
    """
    Pull homepage + a couple likely pages for richer personalization.
    Keep it lightweight.
    """
    domain = norm_domain(website)
    if not domain or domain in BAD_DOMAINS:
        return {"domain": domain, "homepage_text": "", "about_text": "", "work_text": "", "signals": []}

    base = f"https://{domain}/"
    home_html = safe_get(base)
    home_text = extract_readable_text(home_html, 7000)
    sigs = detect_signals(home_html)

    # Try a couple common pages (best-effort)
    about_html = safe_get(f"https://{domain}/about") or safe_get(f"https://{domain}/about-us")
    work_html = safe_get(f"https://{domain}/case-studies") or safe_get(f"https://{domain}/work") or safe_get(f"https://{domain}/portfolio")

    about_text = extract_readable_text(about_html, 3500)
    work_text = extract_readable_text(work_html, 3500)

    return {
        "domain": domain,
        "homepage_text": home_text,
        "about_text": about_text,
        "work_text": work_text,
        "signals": sigs,
        "base_url": base,
    }


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

    changed = False
    for h in REQUIRED_HEADERS:
        if h not in existing:
            header_row.append(h)
            existing[h] = len(header_row) - 1
            changed = True

    # Also ensure the core columns exist (won't create them, but validates)
    for must in [H_COL, B_COL, C_COL]:
        if must not in existing:
            raise RuntimeError(f"Missing required column header in sheet: '{must}'")

    if changed:
        update_sheet_values(svc, sheet_id, f"{SHEET_NAME}!1:1", [header_row])

    return existing


def rows_to_process(svc, sheet_id: str, header_map: dict):
    """
    Pull all rows; choose those missing DM Draft or Human Review Status.
    """
    values = get_sheet_values(svc, sheet_id, f"{SHEET_NAME}!A2:ZZ")
    out = []
    dm_i = header_map["LinkedIn DM Draft"]
    status_i = header_map["Human Review Status"]

    for idx, row in enumerate(values, start=2):
        # Pad row to length
        if len(row) <= max(dm_i, status_i):
            row = row + [""] * (max(dm_i, status_i) + 1 - len(row))

        dm = row[dm_i].strip() if row[dm_i] else ""
        status = row[status_i].strip() if row[status_i] else ""

        if dm and status:
            continue  # already enriched

        out.append((idx, row))
        if len(out) >= MAX_ROWS_PER_RUN:
            break

    return out


def build_prompt(lead_type: str, company_name: str, website: str, snippets: dict, notes: str) -> str:
    signals = snippets.get("signals", [])
    home = snippets.get("homepage_text", "")
    about = snippets.get("about_text", "")
    work = snippets.get("work_text", "")

    return f"""
You are helping write sincere, non-generic outreach for Augusta Productions (performance creative studio / fractional creative director).
Goal: start a real conversation (no hard pitch). Keep it human.

Lead type: {lead_type}
Company: {company_name}
Website: {website}
Detected signals/stack: {", ".join(signals) if signals else "None detected"}
Existing notes: {notes}

Company text (homepage excerpt):
{home}

About excerpt:
{about}

Work/case-studies excerpt:
{work}

Return ONLY valid JSON with exactly these keys:
company_summary (string, 1-2 sentences)
icp_fit_notes (string, 1-2 sentences)
likely_pain (string, 1 sentence)
personalization_hooks (array of 3 short bullets as strings, each must reference something concrete from the text above)
approach_type (one of: "Peer curiosity", "Specific compliment", "Partner angle", "Problem hypothesis", "Referral-style")
recommended_cta (string, a single question that feels casual)
linkedin_dm (string, <= 420 characters, no emojis, no buzzwords, no "we help brands scale", must feel sincere)
email_subject (string, <= 60 chars)
cold_email (string, <= 120 words, conversational, ends with a question)
confidence (integer 0-100)
""".strip()


def call_openai_json(client: OpenAI, prompt: str) -> dict:
    """
    Uses Responses API and parses JSON.
    """
    # The Responses API is the recommended primitive in OpenAI docs. :contentReference[oaicite:2]{index=2}
    resp = client.responses.create(
        model="gpt-5",
        instructions="You output only JSON. No markdown. No extra text.",
        input=prompt,
    )
    text = (resp.output_text or "").strip()
    # Attempt to locate JSON in case of leading/trailing whitespace
    m = re.search(r"\{.*\}\s*$", text, flags=re.S)
    if not m:
        raise ValueError(f"Model did not return JSON. Got: {text[:200]}")
    return json.loads(m.group(0))


def write_back_row(svc, sheet_id: str, header_map: dict, row_number: int, payload: dict, signals: list[str]):
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
    }

    # Build a single row update across only the necessary columns
    max_col = max(header_map[h] for h in updates.keys())
    start_col = min(header_map[h] for h in updates.keys())
    start_a1 = col_to_a1(start_col)
    end_a1 = col_to_a1(max_col)

    # Read existing row so we preserve other cells
    existing = get_sheet_values(svc, sheet_id, f"{SHEET_NAME}!{row_number}:{row_number}")
    row_vals = existing[0] if existing else []
    if len(row_vals) <= max_col:
        row_vals = row_vals + [""] * (max_col + 1 - len(row_vals))

    for h, v in updates.items():
        row_vals[header_map[h]] = v

    # Write the slice back
    slice_vals = row_vals[start_col:max_col + 1]
    update_sheet_values(svc, sheet_id, f"{SHEET_NAME}!{start_a1}{row_number}:{end_a1}{row_number}", [slice_vals])


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
        lead_type = (row[header_map[H_COL]] if len(row) > header_map[H_COL] else "").strip()
        company = (row[header_map[B_COL]] if len(row) > header_map[B_COL] else "").strip()
        website = (row[header_map[C_COL]] if len(row) > header_map[C_COL] else "").strip()
        notes = ""
        if X_COL in header_map and len(row) > header_map[X_COL]:
            notes = (row[header_map[X_COL]] or "").strip()

        if not website:
            print(f"Row {row_number}: no website, skipping.")
            continue

        snippets = fetch_company_snippets(website)
        signals = snippets.get("signals", [])

        prompt = build_prompt(lead_type, company or snippets.get("domain", ""), website, snippets, notes)

        try:
            payload = call_openai_json(client, prompt)
            write_back_row(svc, sheet_id, header_map, row_number, payload, signals)
            print(f"Row {row_number}: drafted successfully ({company or website})")
        except Exception as e:
            print(f"Row {row_number}: failed drafting. err={e}")

        time.sleep(SLEEP_BETWEEN_LEADS + random.random() * 0.15)


if __name__ == "__main__":
    main()
