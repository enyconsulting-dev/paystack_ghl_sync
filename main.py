# paystack_ghl_sync/main.py
import csv, hashlib, hmac, json, os, re, secrets, smtplib, threading, time
from datetime import datetime, timezone
from email.message import EmailMessage
import base64

import gspread
import httpx
from fastapi import FastAPI, HTTPException, Request

app = FastAPI()
LOCK = threading.Lock()


def env(k, d=""):
    return os.environ.get(k, d)


def load_sa():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is empty in Render env vars")

    if raw[0] in "'\"" and raw[-1] == raw[0]:
        raw = raw[1:-1].strip()

    if not raw.startswith("{"):
        try:
            raw = base64.b64decode(raw, validate=True).decode("utf-8").strip()
        except (ValueError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_JSON must contain the full service-account "
                "JSON key file contents, optionally base64-encoded"
            ) from exc

    try:
        credentials = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON; paste the complete "
            "service-account key file contents into Render without extra text"
        ) from exc

    if (
        not isinstance(credentials, dict)
        or credentials.get("type") != "service_account"
        or not credentials.get("client_email")
        or not credentials.get("private_key")
    ):
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON must be a Google service-account key "
            "containing type, client_email, and private_key"
        )
    return credentials


# ---------------- config (Render environment variables) ----------------
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SA_JSON = load_sa()
ADMIN_KEY = os.environ["ADMIN_KEY"]                      # long random string, also used as ?key= in GHL webhook
PAYSTACK_SECRET = env("PAYSTACK_SECRET_KEY")
STRIPE_WHSEC = env("STRIPE_WEBHOOK_SECRET")
GHL_TOKEN = env("GHL_API_TOKEN")
GHL_LOCATION = env("GHL_LOCATION_ID")

KAJABI_CLIENT_ID = env("KAJABI_CLIENT_ID")
KAJABI_CLIENT_SECRET = env("KAJABI_CLIENT_SECRET")
KAJABI_SITE_ID = env("KAJABI_SITE_ID", "83263")
KAJABI_TAG_TYPE = env("KAJABI_TAG_TYPE", "contact_tags")
KAJABI_TAG_IDS = [t.strip() for t in env("KAJABI_TAG_IDS").split(",") if t.strip()]  # tag ids added to every buyer
KAJABI_VAULT_TAG_ID = env("KAJABI_VAULT_TAG_ID")          # extra tag id for vault buyers
VAULT_OFFER_ID = env("VAULT_OFFER_ID")                    # kajabi offer id of the full vault

MAIL_FROM = env("MAIL_FROM")                              # e.g. "Business Analysis School <support@businessanalysisschool.com>"
REPLY_TO = env("REPLY_TO", "support@businessanalysisschool.com")
TEAM_EMAILS = [e.strip() for e in env("TEAM_EMAILS").split(",") if e.strip()]
COACH_EMAILS = [e.strip() for e in env("COACH_EMAILS").split(",") if e.strip()]
LIBRARY_URL = env("LIBRARY_URL", "")
SUPPORT_EMAIL = env("SUPPORT_EMAIL", "support@businessanalysisschool.com")

NAME_COL = env("MAP_NAME_COL", "ghl_name")
LINK_COL = env("MAP_LINK_COL", "paystack_link")
OFFER_COL = env("MAP_OFFER_COL", "kajabi_offer_id")


# ---------------- access map ----------------
def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def slug(u):
    return (u or "").split("?")[0].rstrip("/").split("/")[-1].lower()


with open("access_map.csv", newline="", encoding="utf-8-sig") as f:
    ROWS = [r for r in csv.DictReader(f) if r.get(NAME_COL)]
BY_NAME = {norm(r[NAME_COL]): r for r in ROWS}
BY_SLUG = {slug(r[LINK_COL]): r for r in ROWS if r.get(LINK_COL)}


# ---------------- google sheets ----------------
gc = gspread.service_account_from_dict(SA_JSON)
sh = gc.open_by_key(SHEET_ID)


def ws(title, header):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        w = sh.add_worksheet(title, rows=2000, cols=len(header))
        w.append_row(header)
        return w


PAYMENTS = ws("Payments", ["key", "time_utc", "source", "reference", "buyer_name", "email",
                           "product", "amount", "currency", "kajabi_offer_id", "status", "note"])
RAW = ws("Raw", ["time_utc", "source", "payload"])
STRIPE_LEDGER = ws("StripeLedger", ["id", "time_utc", "type", "email", "name",
                                    "amount", "currency", "description"])


def now():
    return datetime.now(timezone.utc).isoformat()


def log_raw(source, payload):
    try:
        RAW.append_row([now(), source, json.dumps(payload)[:45000]])
    except Exception:
        pass


# ---------------- kajabi ----------------
KAJ = "https://api.kajabi.com/v1"


def kajabi_token():
    r = httpx.post(f"{KAJ}/oauth/token", timeout=20, data={
        "grant_type": "client_credentials",
        "client_id": KAJABI_CLIENT_ID,
        "client_secret": KAJABI_CLIENT_SECRET})
    if r.status_code >= 300:
        raise RuntimeError(f"kajabi token {r.status_code}: {r.text[:200]}")
    return r.json()["access_token"]


def kh(token):
    return {"Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"}


def kajabi_contact(email, name):
    token = kajabi_token()
    h = kh(token)
    # 1. look for an existing contact
    try:
        r = httpx.get(f"{KAJ}/contacts", headers=h, timeout=20,
                      params={"filter[search]": email})
        if r.status_code < 300:
            for c in r.json().get("data", []):
                if (c.get("attributes", {}).get("email") or "").lower() == email.lower():
                    return token, c["id"]
    except Exception:
        pass
    # 2. create one
    body = {"data": {"type": "contacts",
                     "attributes": {"name": name, "email": email},
                     "relationships": {"site": {"data": {"type": "sites", "id": KAJABI_SITE_ID}}}}}
    r = httpx.post(f"{KAJ}/contacts", headers=h, json=body, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"kajabi create contact {r.status_code}: {r.text[:250]}")
    return token, r.json()["data"]["id"]


def kajabi_grant(ctx, offer_id):
    token, cid = ctx
    h = kh(token)
    r = httpx.post(f"{KAJ}/contacts/{cid}/relationships/offers", headers=h, timeout=30,
                   json={"data": [{"type": "offers", "id": str(offer_id)}]})
    if r.status_code >= 300:
        raise RuntimeError(f"kajabi grant {r.status_code}: {r.text[:250]}")
    # tags are best effort: they never block the grant
    note = ""
    tag_ids = list(KAJABI_TAG_IDS)
    if KAJABI_VAULT_TAG_ID and VAULT_OFFER_ID and str(offer_id) == VAULT_OFFER_ID:
        tag_ids.append(KAJABI_VAULT_TAG_ID)
    for tid in tag_ids:
        t = httpx.post(f"{KAJ}/contacts/{cid}/relationships/tags", headers=h, timeout=20,
                       json={"data": [{"type": KAJABI_TAG_TYPE, "id": str(tid)}]})
        if t.status_code >= 300:
            note += f" TAG_FAILED({tid}): {t.status_code} {t.text[:120]}"
    return note.strip()


# ---------------- email ----------------
def send_email(to, subject, text):
    to = [to] if isinstance(to, str) else [x for x in to if x]
    if not to or not MAIL_FROM:
        return "skipped"
    try:
        if env("RESEND_API_KEY"):
            r = httpx.post("https://api.resend.com/emails", timeout=20,
                           headers={"Authorization": f"Bearer {env('RESEND_API_KEY')}"},
                           json={"from": MAIL_FROM, "to": to, "subject": subject,
                                 "text": text, "reply_to": REPLY_TO})
            return "sent" if r.status_code < 300 else f"failed {r.status_code} {r.text[:150]}"
        msg = EmailMessage()
        msg["From"], msg["To"], msg["Subject"], msg["Reply-To"] = MAIL_FROM, ", ".join(to), subject, REPLY_TO
        msg.set_content(text)
        with smtplib.SMTP(env("SMTP_HOST"), int(env("SMTP_PORT", "587")), timeout=20) as s:
            s.starttls()
            s.login(env("SMTP_USER"), env("SMTP_PASS"))
            s.send_message(msg)
        return "sent"
    except Exception as e:
        return f"failed {e}"


def notify(source, ref, name, email, amount, currency, results):
    first = (name or "").split()[0] if name else "there"
    lines = []
    for r in results:
        label = "your recent purchase" if r["status"] == "UNMATCHED" else r["product"]
        if r["status"] == "GRANTED":
            lines.append(f"- {label}: access is active")
        elif r["status"] == "MANUAL":
            lines.append(f"- {label}: our team will contact you personally to set this up")
        else:
            lines.append(f"- {label}: our support team is activating this and will confirm by email")
    lib = f"Sign in to your library at {LIBRARY_URL}. " if LIBRARY_URL else ""
    buyer_text = (
        f"Hello {first},\n\n"
        "Thank you for your purchase from Business Analysis School. Here is what we received:\n\n"
        + "\n".join(lines) +
        f"\n\nReference: {ref}\n\n"
        f"{lib}If you have not yet received your login details, check your spam folder or reply to this email. "
        f"You can also write to {SUPPORT_EMAIL} or message us on WhatsApp at +234 908 551 5969.\n\n"
        "Business Analysis School\n")
    buyer_status = send_email(email, "Your masterclass purchase is confirmed", buyer_text) if email else "no email"

    bad = [r for r in results if r["status"] != "GRANTED" and r["status"] != "MANUAL"]
    tag = "[ACTION NEEDED] " if bad else "[Sale] "
    team_lines = "\n".join(f"- {r['product']} | {r['status']} | offer {r['offer'] or 'none'} {r['note']}"
                           for r in results)
    team_text = (f"Source: {source}\nReference: {ref}\nBuyer: {name} <{email}>\n"
                 f"Amount: {amount} {currency}\nBuyer email: {buyer_status}\n\nItems:\n{team_lines}\n")
    send_email(TEAM_EMAILS, f"{tag}{', '.join(r['product'] for r in results)[:120]}", team_text)

    manual = [r for r in results if r["status"] == "MANUAL"]
    if manual and COACH_EMAILS:
        send_email(COACH_EMAILS, "[Coach] Manual fulfilment needed",
                   f"Buyer: {name} <{email}>\nReference: {ref}\nPlease contact this buyer for:\n"
                   + "\n".join(f"- {r['product']}" for r in manual) + "\n")


# ---------------- core fulfilment ----------------
def fulfil(source, ref, name, email, amount, currency, products, resolver):
    with LOCK:
        seen = set(PAYMENTS.col_values(1))
        new = [(f"{source}:{ref}:{norm(p)}", p) for p in products
               if f"{source}:{ref}:{norm(p)}" not in seen]
        if not new:
            return {"duplicate": True}
        name = name or (email.split("@")[0] if email else "")
        ctx, results = None, []
        for key, p in new:
            row = resolver(p)
            product = row[NAME_COL] if row else p
            offer = (row.get(OFFER_COL) or "").strip() if row else ""
            note = ""
            if not row:
                status = "UNMATCHED"
            elif not offer:
                status = "MANUAL"
            else:
                try:
                    if ctx is None:
                        ctx = kajabi_contact(email, name)
                    note = kajabi_grant(ctx, offer)
                    status = "GRANTED"
                except Exception as e:
                    status, note = "GRANT_FAILED", str(e)[:300]
            PAYMENTS.append_row([key, now(), source, ref, name, email, product, amount,
                                 currency, offer, status, note], value_input_option="USER_ENTERED")
            results.append({"product": product, "offer": offer, "status": status, "note": note})
        notify(source, ref, name, email, amount, currency, results)
        return {"items": results}


def check(key):
    if not secrets.compare_digest(key or "", ADMIN_KEY):
        raise HTTPException(401)


# ---------------- GHL (USD) ----------------
@app.post("/webhook/ghl")
async def ghl(request: Request):
    check(request.query_params.get("key"))
    d = await request.json()
    log_raw("ghl", d)
    c = d.get("customData") or d
    email = c.get("email") or d.get("email") or ""
    name = c.get("name") or d.get("full_name") or d.get("name") or ""
    ref = str(c.get("order_id") or d.get("order_id") or d.get("id") or "")
    pn = c.get("product_names") or d.get("product_names") or c.get("items") or []
    if isinstance(pn, str):
        pn = [x for x in re.split(r"[|\n]", pn) if x.strip()]
    prods = [(i.get("name") if isinstance(i, dict) else str(i)).strip() for i in pn]
    if not email or not prods or not ref:
        return {"ok": False, "reason": "missing email, product or order_id. Read the Raw tab."}
    return {"ok": True, **fulfil("ghl", ref, name, email, c.get("amount", ""),
                                 c.get("currency", "USD"), prods,
                                 lambda p: BY_NAME.get(norm(p)))}


# ---------------- Paystack (NGN) ----------------
def paystack_item(x):
    cust = x.get("customer") or {}
    meta = x.get("metadata") or {}
    name = " ".join(filter(None, [cust.get("first_name"), cust.get("last_name")]))
    s = slug(meta.get("referrer") or "")
    return fulfil("paystack", x["reference"], name, cust.get("email", ""),
                  x["amount"] / 100, x.get("currency", "NGN"), [s or "unknown-page"],
                  lambda _p: BY_SLUG.get(s))


@app.post("/webhook/paystack")
async def paystack(request: Request):
    body = await request.body()
    good = hmac.new(PAYSTACK_SECRET.encode(), body, hashlib.sha512).hexdigest()
    if not hmac.compare_digest(request.headers.get("x-paystack-signature", ""), good):
        raise HTTPException(401)
    d = json.loads(body)
    log_raw("paystack", d)
    if d.get("event") != "charge.success":
        return {"ok": True, "ignored": d.get("event")}
    return {"ok": True, **paystack_item(d["data"])}


@app.api_route("/sweep/paystack", methods=["GET", "POST"])
def sweep_paystack(key: str, since: str = "2026-10-08"):
    """Re-reads Paystack successes since a date and fulfils any missed. Safe to repeat."""
    check(key)
    page, new = 1, 0
    while True:
        r = httpx.get("https://api.paystack.co/transaction", timeout=30,
                      params={"status": "success", "from": since, "perPage": 100, "page": page},
                      headers={"Authorization": f"Bearer {PAYSTACK_SECRET}"}).json()
        for x in r.get("data", []):
            if not paystack_item(x).get("duplicate"):
                new += 1
        if page >= (r.get("meta") or {}).get("pageCount", 1):
            break
        page += 1
    return {"ok": True, "new_orders": new}


# ---------------- Stripe (ledger only) ----------------
def stripe_ok(body: bytes, header: str) -> bool:
    pairs = [p.split("=", 1) for p in header.split(",") if "=" in p]
    t = next((v for k, v in pairs if k == "t"), "")
    sigs = [v for k, v in pairs if k == "v1"]
    if not t or abs(time.time() - int(t)) > 300:
        return False
    good = hmac.new(STRIPE_WHSEC.encode(), f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(good, s) for s in sigs)


@app.post("/webhook/stripe")
async def stripe_hook(request: Request):
    body = await request.body()
    if not stripe_ok(body, request.headers.get("stripe-signature", "")):
        raise HTTPException(401)
    e = json.loads(body)
    log_raw("stripe", e)
    if e["type"] not in ("payment_intent.succeeded", "checkout.session.completed"):
        return {"ok": True, "ignored": e["type"]}
    o = e["data"]["object"]
    if o["id"] in set(STRIPE_LEDGER.col_values(1)):
        return {"ok": True, "duplicate": True}
    cd = o.get("customer_details") or {}
    STRIPE_LEDGER.append_row([o["id"], now(), e["type"],
                              cd.get("email") or o.get("receipt_email") or "", cd.get("name") or "",
                              (o.get("amount_total") or o.get("amount") or 0) / 100,
                              o.get("currency", ""), o.get("description") or ""])
    return {"ok": True}


# ---------------- admin / debug ----------------
@app.api_route("/admin/fulfil", methods=["GET", "POST"])
def admin_fulfil(key: str, email: str, product: str, name: str = "", ref: str = ""):
    """Manually fulfil one item, for example when a webhook payload could not be parsed."""
    check(key)
    ref = ref or f"manual-{int(time.time())}"
    return fulfil("manual", ref, name, email, "", "", [product], lambda p: BY_NAME.get(norm(p)))


@app.get("/test/kajabi")
def test_kajabi(key: str):
    check(key)
    try:
        t = kajabi_token()
        r = httpx.get(f"{KAJ}/me", headers=kh(t), timeout=20)
        return {"token": "ok", "me_status": r.status_code, "me": r.text[:300]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/test/email")
def test_email(key: str, to: str):
    check(key)
    return {"result": send_email(to, "Test from the sync service", "This is a test email.")}


@app.get("/probe/ghl")
def probe_ghl(key: str):
    """Temporary: raw GHL orders, to map the product name field. Remove once confirmed."""
    check(key)
    r = httpx.get("https://services.leadconnectorhq.com/payments/orders", timeout=30,
                  params={"altId": GHL_LOCATION, "altType": "location", "limit": 5},
                  headers={"Authorization": f"Bearer {GHL_TOKEN}", "Version": "2021-07-28"})
    return r.json()


@app.get("/health")
def health():
    return {"ok": True}