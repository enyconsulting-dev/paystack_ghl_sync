"""
GHL pipeline  ->  Google Sheets lead sync (FastAPI)

Endpoints
  GET  /health        liveness check
  POST /sync          backfill: pull every opportunity in the pipeline, append the missing ones
  POST /webhook/ghl   real-time: GHL calls this on each new registration, row is appended

Dedupe is by Opportunity ID (column B), so running /sync repeatedly or receiving
duplicate webhooks never creates duplicate rows.
"""
import logging
import threading
from datetime import datetime, timezone
from functools import lru_cache

import gspread
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from google.oauth2.service_account import Credentials
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ghl-sync")


# ----------------------------------------------------------------- settings
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ghl_api_token: str
    ghl_location_id: str
    ghl_pipeline_name: str = "FREE AI MASTERCLASS {OCT 17TH 2026}"
    google_service_account_file: str = "service_account.json"
    google_sheet_id: str
    google_worksheet_name: str = "Leads"
    webhook_secret: str
    admin_api_key: str


@lru_cache
def settings() -> Settings:
    return Settings()


GHL_BASE = "https://services.leadconnectorhq.com"
HEADERS_ROW = [
    "Date Added", "Opportunity ID", "Contact ID", "Name", "Email",
    "Phone", "Stage", "Status", "Source", "Tags",
]
ID_COL = 2  # Opportunity ID column (1-indexed)

app = FastAPI(title="GHL → Google Sheets Lead Sync")
sheet_lock = threading.Lock()  # serialises read-check-append so no duplicates


# ---------------------------------------------------------------------- GHL
def ghl_client() -> httpx.Client:
    s = settings()
    return httpx.Client(
        base_url=GHL_BASE,
        timeout=30,
        headers={
            "Authorization": f"Bearer {s.ghl_api_token}",
            "Version": "2021-07-28",
            "Accept": "application/json",
        },
    )


@lru_cache
def get_pipeline() -> dict:
    """Find the pipeline by name; returns {'id':..., 'stages': {stage_id: name}}."""
    s = settings()
    with ghl_client() as c:
        r = c.get("/opportunities/pipelines", params={"locationId": s.ghl_location_id})
        r.raise_for_status()
    for p in r.json().get("pipelines", []):
        if p["name"].strip().lower() == s.ghl_pipeline_name.strip().lower():
            return {"id": p["id"], "stages": {st["id"]: st["name"] for st in p.get("stages", [])}}
    names = [p["name"] for p in r.json().get("pipelines", [])]
    raise RuntimeError(f"Pipeline '{s.ghl_pipeline_name}' not found. Available: {names}")


def fetch_all_opportunities() -> list[dict]:
    """Page through every opportunity in the pipeline."""
    s, pipe = settings(), get_pipeline()
    out: list[dict] = []
    params = {"location_id": s.ghl_location_id, "pipeline_id": pipe["id"], "limit": 100}
    with ghl_client() as c:
        while True:
            r = c.get("/opportunities/search", params=params)
            r.raise_for_status()
            data = r.json()
            batch = data.get("opportunities", [])
            out.extend(batch)
            meta = data.get("meta") or {}
            if not batch or not meta.get("startAfterId"):
                break
            params["startAfter"] = meta["startAfter"]
            params["startAfterId"] = meta["startAfterId"]
    return out


def fetch_opportunity(opp_id: str) -> dict:
    with ghl_client() as c:
        r = c.get(f"/opportunities/{opp_id}")
        r.raise_for_status()
    return r.json().get("opportunity", r.json())


def fetch_contact(contact_id: str) -> dict:
    with ghl_client() as c:
        r = c.get(f"/contacts/{contact_id}")
        r.raise_for_status()
    return r.json().get("contact", {})


def opp_to_row(opp: dict, contact: dict | None = None) -> list:
    pipe = get_pipeline()
    contact = contact or opp.get("contact") or {}
    name = contact.get("name") or " ".join(
        filter(None, [contact.get("firstName"), contact.get("lastName")])
    ) or opp.get("name", "")
    return [
        opp.get("createdAt") or datetime.now(timezone.utc).isoformat(),
        opp.get("id", ""),
        opp.get("contactId") or contact.get("id", ""),
        name,
        contact.get("email", ""),
        contact.get("phone", ""),
        pipe["stages"].get(opp.get("pipelineStageId"), opp.get("pipelineStageId", "")),
        opp.get("status", ""),
        opp.get("source") or contact.get("source", ""),
        ", ".join(contact.get("tags") or []),
    ]


# ------------------------------------------------------------ Google Sheets
@lru_cache
def get_worksheet() -> gspread.Worksheet:
    s = settings()
    creds = Credentials.from_service_account_file(
        s.google_service_account_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sh = gspread.authorize(creds).open_by_key(s.google_sheet_id)
    try:
        ws = sh.worksheet(s.google_worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(s.google_worksheet_name, rows=1000, cols=len(HEADERS_ROW))
    if ws.row_values(1) != HEADERS_ROW:
        ws.update(range_name="A1", values=[HEADERS_ROW])
    return ws


def append_new_rows(rows: list[list]) -> int:
    """Append rows whose Opportunity ID isn't already in the sheet. Returns count added."""
    if not rows:
        return 0
    with sheet_lock:
        ws = get_worksheet()
        existing = set(ws.col_values(ID_COL))
        fresh, seen = [], set()
        for row in rows:
            oid = row[ID_COL - 1]
            if oid and oid not in existing and oid not in seen:
                fresh.append(row)
                seen.add(oid)
        if fresh:
            ws.append_rows(fresh, value_input_option="USER_ENTERED")
        return len(fresh)


# ----------------------------------------------------------------- security
def require_webhook_secret(x_webhook_secret: str = Header(default="")):
    if x_webhook_secret != settings().webhook_secret:
        raise HTTPException(401, "Invalid webhook secret")


def require_admin(x_api_key: str = Header(default="")):
    if x_api_key != settings().admin_api_key:
        raise HTTPException(401, "Invalid API key")


# ------------------------------------------------------------------- routes
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/sync", dependencies=[Depends(require_admin)])
def sync_all():
    """Backfill: fetch all leads in the pipeline and append the ones not yet in the sheet."""
    opps = fetch_all_opportunities()
    added = append_new_rows([opp_to_row(o) for o in opps])
    log.info("Sync complete: %d fetched, %d added", len(opps), added)
    return {"fetched": len(opps), "added": added}


@app.post("/webhook/ghl", dependencies=[Depends(require_webhook_secret)])
async def ghl_webhook(request: Request):
    """
    Accepts either:
      • native GHL event  (type = OpportunityCreate), or
      • a Workflow 'Webhook' action with custom data containing opportunity_id / pipeline_id.
    """
    payload = await request.json()
    log.info("Webhook payload: %s", payload)
    return handle_event(payload)


def handle_event(payload: dict) -> dict:
    # Normalise field names from the different payload shapes GHL can send
    opp_id = (
        payload.get("id") if payload.get("type", "").startswith("Opportunity") else None
    ) or payload.get("opportunity_id") or payload.get("opportunityId")
    pipeline_id = payload.get("pipelineId") or payload.get("pipeline_id")

    if not opp_id:
        raise HTTPException(400, "No opportunity id in payload")

    target = get_pipeline()["id"]
    if pipeline_id and pipeline_id != target:
        return {"status": "ignored", "reason": "different pipeline"}

    opp = fetch_opportunity(opp_id)  # authoritative copy; also verifies pipeline
    if opp.get("pipelineId") != target:
        return {"status": "ignored", "reason": "different pipeline"}

    contact = fetch_contact(opp["contactId"]) if opp.get("contactId") else opp.get("contact")
    added = append_new_rows([opp_to_row(opp, contact)])
    return {"status": "ok", "added": added}
