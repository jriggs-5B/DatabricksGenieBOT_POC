"""
Databricks Genie Bot

Author: Luiz Carrossoni Neto
Revision: 1.0

This script implements an experimental chatbot that interacts with Databricks' Genie API. The bot facilitates conversations with Genie,
Databricks' AI assistant, through a chat interface.

Note: This is experimental code and is not intended for production use.


Update on May 02 to reflect Databricks API Changes https://www.databricks.com/blog/genie-conversation-apis-public-preview
"""

import os
import json
import logging
import time
import tempfile
import csv
from typing import Dict, List, Optional
from dotenv import load_dotenv
from aiohttp import web
from botbuilder.core import BotFrameworkAdapterSettings, BotFrameworkAdapter, ActivityHandler, TurnContext, MessageFactory
from botbuilder.schema import Activity, ChannelAccount, Attachment, ActivityTypes
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI, MessageStatus
import asyncio
import requests
import re
import html
# RE-ENABLE FOR EMAIL GRAPH SOLUTION
import base64
from urllib.parse import urlencode, quote
from contextlib import suppress
import sqlparse
from supervisor import supervisor_summarize, supervisor_insights
from storage import save_user_profile, save_user_pref, get_user_prefs, clear_user_pref

# Env vars
load_dotenv()

DATABRICKS_SPACE_ID = os.getenv("DATABRICKS_SPACE_ID")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
APP_ID = os.getenv("MicrosoftAppId", "")
APP_PASSWORD = os.getenv("MicrosoftAppPassword", "")
MAX_ROWS = 200
SESSION_FILES: Dict[str, str] = {}
# In‑memory store for full Genie JSON per session
SESSION_DATA: Dict[str, Dict] = {}
DASH_URL = os.environ["DASH_URL"]
BOT_URL = os.environ["BOT_URL"]
DATABRICKS_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA  = os.getenv("DATABRICKS_SCHEMA")
# RE-ENABLE FOR EMAIL GRAPH SOLUTION
MS_CLIENT_ID     = os.getenv("MicrosoftAppId", "")
MS_CLIENT_SECRET = os.getenv("MicrosoftAppPassword", "")
MS_TENANT_ID     = os.getenv("MS_TENANT_ID")  # or your tenant GUID
MS_REDIRECT_URI  = os.getenv("MS_REDIRECT_URI")         # e.g. https://<bot-host>/graph/callback
MS_SCOPES        = "openid profile offline_access Mail.ReadWrite User.Read.All"
GRAPH_TOKENS: Dict[str, Dict[str, str]] = {}
# OWA_MAX_URL_LEN = 1400  # keep the final URL comfortably below Safe Links limits
PREVIEW_MAX_ROWS = 50
TYPING_INTERVAL = 4.0
DRAFT_CACHE_TTL = 120  # seconds to treat a draft as 'recent'
DRAFTS_BY_KEY: dict[str, dict] = {}
LLM_SUPERVISOR_INSIGHTS_ENABLED = os.getenv("LLM_SUPERVISOR_INSIGHTS_ENABLED", "0") == "1"
EXPLICIT_INTENT = re.compile(
    r"\b(remember|set|save|store|make\s+(?:it\s+)?(?:my\s+)?default|default\s+to|prefer|use)\b",
    re.IGNORECASE,)
CLEAR_INTENT = re.compile(r"\b(clear|forget|delete|reset)\b", re.IGNORECASE)

VALID_DEPTS =   {"32", "41", "42", "44", "45", "46",
                "51", "52", "53", "55", "61", "63",
                "64", "66", "67", "82", "83", "84",
                "85", "86", "91", "93", "95", "96"}
VALID_DCS =     {"3", "4", "5", "6", "7"}
VALID_REGIONS = {"FLORIDA", "GREAT LAKES", "MID ATLANTIC",
                "MID SOUTH", "MIDWEST", "MOUNTAIN",
                "NEW YORK CITY METRO", "NORTHEAST",
                "SOUTH ATLANTIC", "SOUTHEAST", "SOUTHERN",
                "TEXAS", "WEST"}

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

# Module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ---- Genie per-turn instructions (applied on every message) -----------------
# Toggle with GENIE_INSTRUCTIONS_ENABLED=1 to enable; leave unset/0 to disable.
GENIE_INSTRUCTIONS_ENABLED = os.getenv("GENIE_INSTRUCTIONS_ENABLED", "1") == "1"

GENIE_INSTRUCTIONS = """\
You are Databricks Genie SQL assistant. Follow these rules carefully when generating SQL:

- When looking up string values, use ILIKE function
- Always use ILIKE instead of LIKE function
- Never use SELECT * in SQL queries
- For counts, use DISTINCT counts only unless specified otherwise
- For showing data by week, refer to Fiscal week
- To calculate Fiscal week, use the retail fiscal calendar, which starts in February

Business Logic Rules:
- Cargo ready delay = Latest_Vendor_Cargo_Ready_DT - Target_Cargo_Ready_DT, only if Target_Cargo_Ready_DT < Latest_Vendor_Cargo_Ready_DT
- Delay to ship center/DC = SUGGESTED_NEW_ANTICIPATE_DT - PO_ANTICIPATE_DT (must be positive; never negative)
- Bookings are due when booked_dt is NULL/blank AND current date > REQUEST_DT + 21 days
- Orders are past due to ship if REQUEST_DT > current date
- Import orders departed late if Actual Departed Date > PO Ship Date + 7 days
- Container is on-time if max(SUGGESTED_NEW_ANTICIPATE_DT) < min(PO_ANTICIPATE_DT); otherwise late
- For orders not yet arrived at DC, use Balance Quantity for all quantity/units questions

Terminology and Conversions:
- FEU = Forty-Foot Equivalent Unit
- If booked_date IS NULL: 1 FEU = Ordered_Line_Volume / 61
- If booked_date IS NOT NULL: 1 FEU = Booked_Line_Volume / 61
- Order is at risk if Risk_Flag = 'Y'; otherwise not at risk
- Actual ocean transit time = Port_ATA - ATD
- Expected ocean transit time = Transit_Days_O_Ocean_and_Air
- For transit times: Origin Port = start; Discharge Port = end

Lead Times and OTIF:
- Average lead time = ACTUAL_DC_ARRIVAL_DT - ORDER_DT
- OTIF = On-Time and In Full
- Order is on time if ACTUAL_DC_ARRIVAL_DT <= PO_ANTICIPATE_DT
- Order is in full if ORDERED_QTY <= RECEIVED_QTY

Dataset References:
- For container, FEU, TEU, shipments → use table: inbound_otw_report_20250721
- For orders → use table: inbound_po_supply_chain_20250721
- For receipts or received orders → use table: inbound_otw_report_20250721
- Apply all rules above when interpreting the request and generating SQL.
"""

# logger.info(
#     "Graph config at startup: client_id=%r tenant=%r redirect=%r",
#     MS_CLIENT_ID, MS_TENANT_ID, MS_REDIRECT_URI
# )

workspace_client = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN
)

# 2) Register healthz **before** all your other routes
async def healthz(request):
    return web.Response(status=200)

app = web.Application()
app.router.add_get("/healthz", healthz)

genie_api = GenieAPI(workspace_client.api_client)

def get_attachment_query_result(space_id, conversation_id, message_id, attachment_id):
    url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Message endpoint returned status {response.status_code}: {response.text}")
        return {}
    
    try:
        message_data = response.json()
        logger.info(f"Message data: {message_data}")
        
        statement_id = None
        if "attachments" in message_data:
            for attachment in message_data["attachments"]:
                if attachment.get("attachment_id") == attachment_id:
                    if "query" in attachment and "statement_id" in attachment["query"]:
                        statement_id = attachment["query"]["statement_id"]
                        break
        
        if not statement_id:
            logger.error("No statement_id found in message data")
            return {}
            
        query_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
        query_headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
            "X-Databricks-Statement-Id": statement_id
        }
        
        query_response = requests.get(query_url, headers=query_headers)
        if query_response.status_code != 200:
            logger.error(f"Query result endpoint returned status {query_response.status_code}: {query_response.text}")
            return {}
            
        if not query_response.text.strip():
            logger.error(f"Empty response from Genie API: {query_response.status_code}")
            return {}
            
        result = query_response.json()
        logger.info(f"Raw query result response: {result}")
        
        if isinstance(result, dict):
            if "data_array" in result:
                if not isinstance(result["data_array"], list):
                    result["data_array"] = []
            if "schema" in result:
                if not isinstance(result["schema"], dict):
                    result["schema"] = {}
                    
            if "schema" in result and "columns" in result["schema"]:
                if not isinstance(result["schema"]["columns"], list):
                    result["schema"]["columns"] = []
                    
            if "data_array" in result and result["data_array"] and "schema" not in result:
                first_row = result["data_array"][0]
                if isinstance(first_row, dict):
                    result["schema"] = {
                        "columns": [{"name": key} for key in first_row.keys()]
                    }
                elif isinstance(first_row, list):
                    result["schema"] = {
                        "columns": [{"name": f"Column {i}"} for i in range(len(first_row))]
                    }
                    
        return result
    except Exception as e:
        logger.error(f"Failed to process Genie API response: {e}, text: {response.text}")
        return {}


# Per-key async locks to avoid race conditions when two hits arrive at once
_DRAFT_LOCKS: dict[str, asyncio.Lock] = {}
def _get_draft_lock(key: str) -> asyncio.Lock:
    lock = _DRAFT_LOCKS.get(key)
    if not lock:
        lock = asyncio.Lock()
        _DRAFT_LOCKS[key] = lock
    return lock

def execute_attachment_query(space_id, conversation_id, message_id, attachment_id, payload):
    url = f"{DATABRICKS_HOST}/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        logger.error(f"Execute query endpoint returned status {response.status_code}: {response.text}")
        return {}
    if not response.text.strip():
        logger.error(f"Empty response from Genie API: {response.status_code}")
        return {}
    try:
        return response.json()
    except Exception as e:
        logger.error(f"Failed to parse JSON from Genie API: {e}, text: {response.text}")
        return {}
    
def count_total_rows_via_sql_warehouse(raw_sql: str) -> Optional[int]:
    """
    Run: SELECT COUNT(*) FROM (<raw_sql>) t
    against the Databricks SQL Statements API (warehouse).
    Returns an int or None.
    """
    if not raw_sql or not DATABRICKS_WAREHOUSE_ID:
        return None

    submit_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    stmt = f"SELECT COUNT(*) AS total_count FROM (\n{raw_sql.strip().rstrip(';')}\n) t"
    payload = {"statement": stmt, "warehouse_id": DATABRICKS_WAREHOUSE_ID}
    if DATABRICKS_CATALOG:
        payload["catalog"] = DATABRICKS_CATALOG
    if DATABRICKS_SCHEMA:
        payload["schema"] = DATABRICKS_SCHEMA

    # submit
    r = requests.post(submit_url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    statement_id = (r.json() or {}).get("statement_id")
    if not statement_id:
        return None

    # poll
    get_url = f"{submit_url}/{statement_id}"
    for _ in range(60):  # up to ~60s
        g = requests.get(get_url, headers=headers, timeout=15)
        g.raise_for_status()
        data = g.json() or {}
        state = ((data.get("status") or {}).get("state")) or ""
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            if state != "SUCCEEDED":
                return None
            res = data.get("result") or {}
            arr = res.get("data_array") or []
            if arr and isinstance(arr[0], (list, tuple)) and len(arr[0]) >= 1:
                try:
                    return int(arr[0][0])
                except Exception:
                    return None
            return None
        time.sleep(1)

    return None

def _compose_genie_prompt(user_question: str, aad_id: str | None = None) -> str:
    """
    Build the full Genie prompt including base instructions,
    user-specific preferences (if any), and the actual request.
    """
    if not GENIE_INSTRUCTIONS_ENABLED:
        return user_question

    system_prompt = GENIE_INSTRUCTIONS

    # Inject user preferences if available
    if aad_id:
        prefs = get_user_prefs(aad_id)
        system_prompt = apply_user_prefs_to_prompt(system_prompt, prefs, user_question)

        # ---- NEW: replace invalid mentions with valid prefs ----
        for key, valid_values in prefs.items():
            if key == "dept":
                # remove any non-valid dept mentions
                for v in re.findall(r"\b\d+\b", user_question):
                    if v not in VALID_DEPTS:
                        user_question = re.sub(rf"\b{re.escape(v)}\b", "", user_question)
            elif key == "dc":
                for v in re.findall(r"\b\d+\b", user_question):
                    if v not in VALID_DCS:
                        user_question = re.sub(rf"\b{re.escape(v)}\b", "", user_question)
            elif key == "region":
                for v in re.findall(r"\b\w+\b", user_question):
                    if v.capitalize() not in VALID_REGIONS:
                        user_question = re.sub(rf"\b{re.escape(v)}\b", "", user_question)

    return f"{system_prompt}\nREQUEST:\n{user_question}"

logger = logging.getLogger(__name__)

async def ask_genie(
    question: str,
    space_id: str,
    conversation_id: Optional[str] = None,
    aad_id: Optional[str] = None
) -> tuple[str, str]:
    logger.debug("🔥 ENTERING ask_genie v2! 🔥")
    logger.debug("🛠️  ASK_GENIE PAYLOAD HOTFIX DEPLOYED 🛠️")
    try:
        loop = asyncio.get_running_loop()

        # ───────────────────────────────────────────────────────────────────────────
        # 1) start or continue conversation
        # ───────────────────────────────────────────────────────────────────────────
        # Compose prompt with per-turn instructions
        composed_question = _compose_genie_prompt(question, aad_id)

        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, composed_question
            )
            conversation_id = initial_message.conversation_id
        else:
            initial_message = await loop.run_in_executor(
                None,
                genie_api.create_message_and_wait,
                space_id, conversation_id, composed_question
            )

        message_id = initial_message.message_id

        # ───────────────────────────────────────────────────────────────────────────
        # 2) poll for COMPLETED with exponential backoff
        # ───────────────────────────────────────────────────────────────────────────
        max_attempts = 5
        backoff_base = 2
        for attempt in range(1, max_attempts + 1):
            message_content = await loop.run_in_executor(
                None,
                genie_api.get_message,
                space_id, conversation_id, message_id
            )
            status = getattr(message_content, "status", None)
            logger.debug(f"[Poll {attempt}/{max_attempts}] status={status}")

            if status == MessageStatus.COMPLETED:
                break
            if status == MessageStatus.FAILED:
                err = getattr(message_content, "error_message", "<no error>")
                logger.error(f"Genie FAILED on attempt {attempt}: {err}")
            else:
                logger.debug(f"Sleeping {backoff_base**attempt}s before retry")

            if attempt < max_attempts:
                time.sleep(backoff_base ** attempt)
            else:
                raise RuntimeError(f"Genie did not complete after {max_attempts} attempts")

        logger.info(f"Raw message content: {message_content}")

        # ───────────────────────────────────────────────────────────────────────────
        # 3) handle any plain‑text attachments first
        # ───────────────────────────────────────────────────────────────────────────
        if message_content.attachments:
            for attachment in message_content.attachments:

                # 3a) Plain‑text cards first
                text_obj = getattr(attachment, "text", None)
                if text_obj and hasattr(text_obj, "content"):
                    return json.dumps({"message": text_obj.content}), conversation_id

                # 3b) SQL cards next
                attachment_id = getattr(attachment, "attachment_id", None)
                query_obj     = getattr(attachment, "query", None)
                if attachment_id and query_obj:
                    # — pull description & raw SQL —
                    desc    = getattr(query_obj, "description", None) or ""
                    raw_sql = getattr(query_obj, "query",      None)

                    if raw_sql:
                        raw_sql = raw_sql.strip().rstrip(";")
                        raw_sql_limited = f"{raw_sql} LIMIT {MAX_ROWS + 1}"  # ask for one extra row
                    else:
                        raw_sql_limited = raw_sql

                    # ───────────────────────────────────────────────────────
                    # A) FULL result (for CSV) — re-execute raw_sql, then fetch
                    # ───────────────────────────────────────────────────────
                    await loop.run_in_executor(
                        None,
                        execute_attachment_query,
                        space_id, conversation_id, message_id, attachment_id,
                        {"query": raw_sql}
                    )
                    full_result = await loop.run_in_executor(
                        None,
                        get_attachment_query_result,
                        space_id,
                        conversation_id,
                        message_id,
                        attachment_id
                    )
                    full_stmt    = (full_result or {}).get("statement_response", {}) or {}
                    full_rows    = ((full_stmt.get("result") or {}).get("data_array") or [])
                    full_schema  = ((full_stmt.get("manifest") or {}).get("schema") or {}).get("columns", []) or []
                    csv_rows     = len(full_rows)

                    # Authoritative total via SQL Warehouse (independent of Genie)
                    db_total_rows = await loop.run_in_executor(
                        None,
                        count_total_rows_via_sql_warehouse,
                        raw_sql
                    )

                    # 3) write them to a temp CSV file
                    tf = tempfile.NamedTemporaryFile(
                        mode="w", newline="", delete=False, suffix=".csv", dir="/tmp"
                    )
                    writer = csv.writer(tf)
                    # header row
                    writer.writerow([col["name"] for col in full_schema])
                    # data rows
                    for row in full_rows:
                        writer.writerow(row)
                    tf.close()

                    # 4) record the file path for this session
                    SESSION_FILES[conversation_id] = tf.name

                    # ──────────────────────────────────────────────────────────
                    # 4) LIMITED preview for Teams — execute limited SQL, fetch, compute sizes
                    # ──────────────────────────────────────────────────────────
                    try:
                        # Re-execute the limited SQL on this attachment
                        await loop.run_in_executor(
                        None,
                        execute_attachment_query,
                        space_id, conversation_id, message_id, attachment_id,
                        {"query": raw_sql}
                        )

                        # Fetch the limited result
                        query_result = await loop.run_in_executor(
                            None,
                            get_attachment_query_result,
                            space_id, conversation_id, message_id, attachment_id,
                        )

                        # Normalize & enforce MAX_ROWS defensively
                        rows = query_result["statement_response"]["result"].get("data_array", []) or []
                        if len(rows) > MAX_ROWS:
                            rows = rows[:MAX_ROWS]
                            query_result["statement_response"]["result"]["data_array"] = rows

                        shown_rows = len(rows)

                        # Derive counts (handle missing COUNT by falling back to csv_rows)
                        if db_total_rows is None:
                            db_total_rows = csv_rows

                        teams_truncated = max(csv_rows - shown_rows, 0)       # CSV → Teams
                        csv_truncated   = max(db_total_rows - csv_rows, 0)    # DB → CSV (Genie cap)
                        total_truncated = max(db_total_rows - shown_rows, 0)  # DB → Teams
                        truncated       = total_truncated > 0

                    except Exception as e:
                        logger.warning(f"Genie error or payload too large: {e}")
                        shown_rows = 0
                        if db_total_rows is None:
                            db_total_rows = 0
                        csv_rows = 0
                        teams_truncated = 0
                        csv_truncated   = max(db_total_rows - csv_rows, 0)
                        total_truncated = max(db_total_rows - shown_rows, 0)
                        truncated       = total_truncated > 0
                        query_result = {
                            "query_result_metadata": {},
                            "statement_response": {
                                "result": {"data_array": []},
                                "manifest": {"schema": {"columns": []}}
                            }
                        }

                    if db_total_rows is None or db_total_rows < csv_rows:
                        logger.warning("Warehouse COUNT < CSV rows, correcting total to CSV size")
                        db_total_rows = csv_rows

                    logger.debug(f"🔍 query_result after truncate/error: {query_result!r}")

                    # Build the answer JSON dict
                    answer_json = {
                        "query_description": desc or "",
                        "query_result_metadata": query_result.get("query_result_metadata", {}),
                        "statement_response":    query_result.get("statement_response", {}),
                        "raw_sql":               raw_sql or "",
                        "raw_sql_executed":      raw_sql_limited or "",
                        "truncated":             truncated,

                        # NEW: sizes
                        "db_total_rows":   int(db_total_rows or 0),  # true total
                        "csv_rows":        int(csv_rows or 0),       # rows in downloadable CSV
                        "shown_rows":      int(shown_rows or 0),     # rows in Teams

                        # NEW: breakdown
                        "teams_truncated": int(teams_truncated or 0),  # csv_rows - shown_rows
                        "csv_truncated":   int(csv_truncated or 0),    # db_total_rows - csv_rows
                        "total_truncated": int(total_truncated or 0),  # db_total_rows - shown_rows
                    }
                    # Store for Dash to fetch later
                    logger.debug("🚀 FINAL GENIE PAYLOAD: %r", answer_json)
                    SESSION_DATA[conversation_id] = answer_json

                    logger.info(
                        "Sizes: db_total=%s csv=%s shown=%s | truncated total=%s (csv=%s, teams=%s)",
                        answer_json["db_total_rows"], answer_json["csv_rows"], answer_json["shown_rows"],
                        answer_json["total_truncated"], answer_json["csv_truncated"], answer_json["teams_truncated"]
                    )
                    
                    return json.dumps(answer_json), conversation_id

        # ────────────────
        # Fallback if no attachments at all
        # ────────────────
        return json.dumps({"error": "No data available."}), conversation_id

    except Exception as e:
        logger.error(f"Error in ask_genie: {e}", exc_info=True)
        return json.dumps({"error": "An error occurred while processing your request."}), conversation_id

SUBJECT_PREFIX = "Five Below - "
SUBJECT_MAX = 72  # keep it inbox-friendly

def build_business_subject(answer_json: Dict) -> str:
    desc = (answer_json.get("query_description") or "").strip()
    if not desc:
        return SUBJECT_PREFIX + "Results Summary"

    # trim boilerplate words
    desc = re.sub(r"\b(this|a|an|the|of|for|to|that|which)\b", "", desc, flags=re.I)
    desc = re.sub(r"\s+", " ", desc).strip(" -—:.,")
    # kill quotes/brackets that clutter subjects
    desc = desc.replace('"', '').replace("'", "").replace("[", "").replace("]", "")
    subj = SUBJECT_PREFIX + desc

    # nicely truncate on word boundary
    if len(subj) > SUBJECT_MAX:
        cut = subj[:SUBJECT_MAX].rsplit(" ", 1)[0]
        subj = cut + "…"
    return subj

def build_email_bodies(answer_json: Dict, preview_max: int = PREVIEW_MAX_ROWS) -> tuple[str, str, bool]:
    """
    Build both PLAINTEXT and HTML versions of the email body from the Genie `answer_json`.

    Policy:
      • If TOTAL rows ≤ preview_max (default 50): include a preview table (no CSV attachment).
      • If TOTAL rows  > preview_max: omit preview table (CSV should be attached by caller).

    Returns: (plain_body, html_body, included_preview)
      - included_preview == True → NO attachment should be added by the caller
      - included_preview == False → Caller should attach CSV
    """
    # Sizes computed earlier in ask_genie
    db_total_rows   = int(answer_json.get("db_total_rows") or 0)
    csv_rows        = int(answer_json.get("csv_rows") or 0)
    shown_rows      = int(answer_json.get("shown_rows") or 0)
    teams_truncated = int(answer_json.get("teams_truncated") or 0)
    csv_truncated   = int(answer_json.get("csv_truncated") or 0)

    desc     = (answer_json.get("query_description") or "Results Summary").strip()
    stmt     = answer_json.get("statement_response", {}) or {}
    result   = (stmt.get("result") or {})
    rows     = (result.get("data_array") or [])
    manifest = (stmt.get("manifest") or {})
    schema   = ((manifest.get("schema") or {}).get("columns") or [])
    col_names: List[str] = [c.get("name", f"Col{i}") for i, c in enumerate(schema)]

    include_preview = db_total_rows <= preview_max

    def fmt_cell(val, col_type_name: str | None) -> str:
        if val is None:
            return "NULL"
        t = (col_type_name or "").upper()
        try:
            if t in ("DECIMAL", "DOUBLE", "FLOAT", "REAL", "NUMERIC"):
                return f"{float(val):,.2f}"
            if t in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "LONG"):
                return f"{int(float(val)):,.0f}"
        except Exception:
            # fall through to str if conversion fails
            pass
        return str(val)

    # ----------------- PLAINTEXT -----------------
    lines = []
    lines.append("")
    lines.append("")
    lines.append("")
    lines.append("Results Summary")
    lines.append("")
    lines.append(f"Description: {desc}")
    lines.append(f"Total rows in dataset: {db_total_rows:,}")
    lines.append(f"Rows shown in Teams: {shown_rows:,}")
    lines.append(f"Rows in CSV: {csv_rows:,}")
    if teams_truncated:
        lines.append(f"Teams view truncated: {teams_truncated:,}")
    if csv_truncated:
        lines.append(f"CSV capped by Genie: {csv_truncated:,}")
    if include_preview:
        lines.append("")
        lines.append(f"All rows are shown inline (≤ {preview_max}).")
        # Optional: add a tiny plaintext preview header and first few lines
        # (kept minimal since HTML carries the full 50-row preview)
        header_txt = " | ".join(col_names)
        lines.append("")
        lines.append(header_txt)
        lines.append("-" * len(header_txt))
        for r in rows[:min(len(rows), preview_max, 5)]:
            formatted = []
            for v, c in zip(r, schema):
                formatted.append(fmt_cell(v, c.get("type_name")))
            lines.append(" | ".join(formatted))
    else:
        lines.append("")
        lines.append(f"Preview omitted due to size (> {preview_max} rows). CSV attached.")

    plain_body = "\n".join(lines).strip("\n")

    # ------------------- HTML --------------------
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    html_parts = [
        '<p style="margin:0 0 12px 0;">&nbsp;</p>',
        '<p style="margin:0 0 12px 0;">&nbsp;</p>',
        '<p style="margin:0 0 12px 0;">&nbsp;</p>',
        '<h2 style="margin:0 0 12px 0;">Results Summary</h2>',
        f"<p><strong>Description:</strong> {esc(desc)}</p>",
        f"<p><strong>Total rows in dataset:</strong> {db_total_rows:,}</p>",
        f"<p><strong>Rows shown in Teams:</strong> {shown_rows:,} &nbsp; <strong>Rows in CSV:</strong> {csv_rows:,}</p>",
    ]
    if teams_truncated or csv_truncated:
        html_parts.append("<ul>")
        if teams_truncated:
            html_parts.append(f"<li>Teams view truncated: <strong>{teams_truncated:,}</strong></li>")
        if csv_truncated:
            html_parts.append(f"<li>CSV capped by Genie: <strong>{csv_truncated:,}</strong></li>")
        html_parts.append("</ul>")

    if include_preview and rows and col_names:
        # Full preview table (all rows since total ≤ preview_max)
        thead = "".join(
            f'<th style="border:1px solid #ddd;padding:6px;background:#f5f5f5;text-align:left">{esc(c)}</th>'
            for c in col_names
        )
        body_rows = []
        for r in rows[:preview_max]:
            cells = []
            for v, c in zip(r, schema):
                cells.append(esc(fmt_cell(v, c.get("type_name"))))
            tds = "".join(f'<td style="border:1px solid #ddd;padding:6px">{cell}</td>' for cell in cells)
            body_rows.append(f"<tr>{tds}</tr>")
        table_html = (
            '<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;border:1px solid #ddd;margin-top:8px">'
            f"<thead><tr>{thead}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"
        )
        html_parts.append(table_html)
    else:
        html_parts.append(f'<p><em>Preview omitted due to size (&gt; {preview_max} rows). CSV attached.</em></p>')

    html_body = "".join(html_parts)

    return plain_body, html_body, include_preview

# RE-ENABLE FOR EMAIL GRAPH SOLUTION
def _oauth_authorize_url(state: str) -> str:
    params = {
        "client_id": MS_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": MS_REDIRECT_URI,
        "response_mode": "query",
        "scope": MS_SCOPES,
        "state": state
        # "prompt": "consent"
    }
    return f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/authorize?{urlencode(params)}"

def _oauth_token_request(data: Dict[str, str]) -> Dict:
    url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

def _now_epoch() -> int:
    return int(time.time())

def _save_tokens_for_user(user_id: str, tok: Dict):
    GRAPH_TOKENS[user_id] = {
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token", ""),
        "expires_at": str(_now_epoch() + int(tok.get("expires_in", 3600) - 60)),
    }

def _get_valid_access_token(user_id: str) -> Optional[str]:
    info = GRAPH_TOKENS.get(user_id)
    if not info:
        return None
    if _now_epoch() < int(info.get("expires_at", "0")) and info.get("access_token"):
        return info["access_token"]
    if info.get("refresh_token"):
        try:
            tok = _oauth_token_request({
                "client_id": MS_CLIENT_ID,
                "scope": MS_SCOPES,
                "refresh_token": info["refresh_token"],
                "grant_type": "refresh_token",
                "client_secret": MS_CLIENT_SECRET,
                "redirect_uri": MS_REDIRECT_URI,
            })
            _save_tokens_for_user(user_id, tok)
            return tok["access_token"]
        except Exception:
            logger.exception("Graph token refresh failed")
    return None

KEY_NORMALIZATION = {
    "department": "dept",
    "departments": "dept",
    "dept": "dept",
    "depts": "dept",
    "dc": "dc",
    "distribution center": "dc",
    "distribution centers": "dc",
    "region": "region",
    "regions": "region",
}

async def maybe_handle_pref_command(turn_context, aad_id: str, question: str) -> bool:
    """
    Handle explicit commands like 'remember my dept is 42'
    or 'set dc 3 and 7'.
    Returns True if handled, False otherwise.
    """
    text = question.lower()

    CONNECTOR = r"(?:are|is|=|as|to|:)?"   # <-- add as|to|: here

    patterns = {
        # dept / departments …
        "dept": re.compile(
            rf"\b(?:dept|depts|department|departments)\s*{CONNECTOR}\s*([\d\s, and]+)",
            re.IGNORECASE,
        ),
        # dc / dcs / distribution center(s)
        "dc": re.compile(
            rf"\b(?:dc|dcs|distribution\s+center(?:s)?)\s*{CONNECTOR}\s*([\d\s, and]+)",
            re.IGNORECASE,
        ),
        # region(s) – words allowed
        "region": re.compile(
            rf"\bregion(?:s)?\s*{CONNECTOR}\s*([\w\s, and]+)",
            re.IGNORECASE,
        ),
    }

    for key, pattern in patterns.items():
        match = pattern.search(question)
        if match:
            key = KEY_NORMALIZATION.get(key, key)
            raw_value = match.group(1).strip()

            # Split multiple values on commas or "and"
            if key in ("dept", "dc"):
                # numeric lists (comma or "and")
                values = re.split(r"\s*(?:,|and)\s*", raw_value)
                values = [v.strip() for v in values if v.strip().isdigit()]
            else:  # region
                # region names: allow multi-word tokens like "great lakes"
                values = re.split(r"\s*(?:,|and)\s*", raw_value)
                values = [v.strip().upper() for v in values if v.strip()]

            if not values:
                continue  # nothing valid parsed

            # ---- VALIDATION ----
            valid, invalid = validate_pref(key, values)
            if not valid:
                await turn_context.send_activity(
                    f"⚠️ Sorry, I can’t save {key}={', '.join(invalid)}. "
                    f"Valid values are: {', '.join(sorted(VALID_DCS if key=='dc' else VALID_DEPTS if key=='dept' else VALID_REGIONS))}."
                )
                return True  # handled; don’t call Genie

            # Save list if multiple, single string if one
            pref_value = values if len(valid) > 1 else valid[0]

            prefs = save_user_pref(aad_id, key, pref_value)

            # Build confirmation message
            label = key
            if isinstance(pref_value, list):
                values_str = ", ".join(pref_value)
                msg = f"✅ Got it — I’ll remember your default {label}s = [{values_str}]."
            else:
                msg = f"✅ Got it — I’ll remember your default {label} = {pref_value}."

            if invalid:
                msg = f"⚠️ Ignoring invalid {key}(s): {', '.join(invalid)}. " + msg

            
            # Natural summary of all prefs (only if there are others to show)
            remaining = []
            for k, v in prefs.items():
                if isinstance(v, list):
                    remaining.append(f"{k} = [{', '.join(v)}]")
                else:
                    remaining.append(f"{k} = {v}")

            if remaining:
                msg += f" Current saved preferences: {', '.join(remaining)}."

            await turn_context.send_activity(msg)
            return True

    return False

async def maybe_handle_clear_command(turn_context, aad_id: str, question: str) -> bool:
    """
    Handle explicit commands like 'clear my dept', 'forget region', 'reset all'.
    Returns True if handled, False otherwise.
    """
    if not CLEAR_INTENT.search(question):
        return False

    text = question.lower()
    prefs = None
    cleared_key = None
    message = ""

    if "dept" in text or "department" in text:
        prefs = clear_user_pref(aad_id, "dept")
        cleared_key = "dept"
        message = "🗑️ Cleared your saved department preference(s)."
    elif "dc" in text or "distribution center" in text:
        prefs = clear_user_pref(aad_id, "dc")
        cleared_key = "dc"
        message = "🗑️ Cleared your saved distribution center preference(s)."
    elif "region" in text:
        prefs = clear_user_pref(aad_id, "region")
        cleared_key = "region"
        message = "🗑️ Cleared your saved region preference(s)."
    elif "all" in text or "prefs" in text or "preferences" in text:
        prefs = clear_user_pref(aad_id, None)
        cleared_key = "all"
        message = "🗑️ All saved preferences have been reset."
    else:
        return False

    # Build a natural summary of what’s left
    if prefs:
        remaining = []
        for k, v in prefs.items():
            if isinstance(v, list):
                remaining.append(f"{k} = [{', '.join(v)}]")
            else:
                remaining.append(f"{k} = {v}")
        if remaining:
            message += f" Current saved preferences: {', '.join(remaining)}."
        else:
            message += " You now have no saved preferences."
    else:
        message += " You now have no saved preferences."

    await turn_context.send_activity(message)
    logger.info(f"User {aad_id} cleared {cleared_key} → remaining prefs {prefs}")
    return True

def extract_depts(question: str) -> list[str]:
    import re, logging
    logger = logging.getLogger(__name__)

    matches = re.findall(r"\b(?:dept|depts|department|departments)\s*(?:=)?\s*(\d+)", question, re.I)
    logger.info(f"extract_depts: regex direct matches = {matches}")

    if not matches:
        combo = re.search(r"\b(?:dept|depts|department|departments)\s*(.+)", question, re.I)
        if combo:
            logger.info(f"extract_depts: fallback combo = {combo.group(1)}")
            vals = re.split(r"[,\s]+and\s+|,|\s+and\s+", combo.group(1))
            cleaned = [v.strip() for v in vals if v.strip().isdigit()]
            logger.info(f"extract_depts: cleaned fallback = {cleaned}")
            return cleaned

    return matches

def extract_dcs(question: str) -> list[str]:
    # Match "dc7", "dc 7", "distribution center 4, 5"
    matches = re.findall(r"\b(?:dc|distribution center)\s*(\d+)", question, re.I)
    return matches

def extract_regions(question: str) -> list[str]:
    # Match "region east", "region=west", "region great lakes"
    matches = re.findall(r"\bregion(?:s)?\s*(?:=)?\s*([a-zA-Z\s]+)", question, re.I)
    return [m.strip().upper() for m in matches]

def validate_pref(key: str, values: list[str]) -> tuple[list[str], list[str]]:
    """
    Returns (valid, invalid) lists for given key/values.
    """
    if key == "dept":
        valid = [v for v in values if v in VALID_DEPTS]
    elif key == "dc":
        valid = [v for v in values if v in VALID_DCS]
    elif key == "region":
        valid = [v.upper() for v in values if v.upper() in VALID_REGIONS]
    else:
        return [], values
    invalid = [v for v in values if v not in valid]
    return valid, invalid

def apply_user_prefs_to_prompt(system_prompt: str, prefs: dict, question: str) -> str:
    # --- Dept ---
    q_depts = extract_depts(question)
    if q_depts:
        valid, invalid = validate_pref("dept", q_depts)
        if valid:
            system_prompt += f"\nAlways filter by dept IN ({', '.join(valid)}) unless user specifies otherwise."
        else:
            logger.warning(f"User specified invalid dept(s): {q_depts}")
            # keep question as-is; do not override with prefs
    elif "dept" in prefs:
        val = prefs["dept"]
        if isinstance(val, list):
            system_prompt += f"\nAlways filter by dept IN ({', '.join(val)}) unless user specifies otherwise."
        else:
            system_prompt += f"\nAlways filter by dept={val} unless user specifies otherwise."

    # --- DC ---
    q_dcs = extract_dcs(question)
    if q_dcs:
        valid, invalid = validate_pref("dc", q_dcs)
        if valid:
            system_prompt += f"\nAlways filter by DC IN ({', '.join(valid)}) unless user specifies otherwise."
        else:
            logger.warning(f"User specified invalid DC(s): {q_dcs}")
    elif "dc" in prefs:
        val = prefs["dc"]
        if isinstance(val, list):
            system_prompt += f"\nAlways filter by DC IN ({', '.join(val)}) unless user specifies otherwise."
        else:
            system_prompt += f"\nAlways filter by DC={val} unless user specifies otherwise."

    # --- Region ---
    q_regions = extract_regions(question)
    if q_regions:
        valid, invalid = validate_pref("region", q_regions)
        if valid:
            system_prompt += f"\nAlways filter by region IN ({', '.join(valid)}) unless user specifies otherwise."
        else:
            logger.warning(f"User specified invalid region(s): {q_regions}")
    elif "region" in prefs:
        val = prefs["region"]
        if isinstance(val, list):
            system_prompt += f"\nAlways filter by region IN ({', '.join(val)}) unless user specifies otherwise."
        else:
            system_prompt += f"\nAlways filter by region={val} unless user specifies otherwise."

    return system_prompt

def get_graph_user_details(aad_id: str, token: str) -> dict:
    """
    Fetch full Graph user object using the user's AAD Object ID.
    """
    try:
        headers = {"Authorization": f"Bearer {token}"}
        url = f"https://graph.microsoft.com/v1.0/users/{aad_id}"
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            user_data = resp.json()
            logger.debug(f"Graph user lookup for {aad_id}: displayName={user_data.get('displayName')} mail={user_data.get('mail')}")
            return user_data
        else:
            logger.warning(f"Graph lookup failed for {aad_id}: {resp.text}")
            return {}
    except Exception:
        logger.exception(f"Error calling Graph for {aad_id}")
        return {}

def create_draft_via_graph(access_token: str, subject: str, html_body: str) -> Dict:
    url = "https://graph.microsoft.com/v1.0/me/messages"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    payload = {
        "subject": subject,
        "body": { "contentType": "HTML", "content": html_body },
        "toRecipients": [], "ccRecipients": [], "bccRecipients": []
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def attach_csv_via_graph(access_token: str, message_id: str, csv_bytes: bytes, filename: str = "results.csv") -> Dict:
    url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}/attachments"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    payload = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": filename,
        "contentType": "text/csv",
        "contentBytes": base64.b64encode(csv_bytes).decode()
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def format_sql_for_card(raw_sql: str) -> str:
    """
    Prettify SQL for display (IDE-like). Falls back to raw on any error.
    """
    try:
        return sqlparse.format(
            raw_sql or "",
            reindent=True,
            keyword_case="upper",     # UPPER keywords
            identifier_case=None,     # keep identifiers as-is
            strip_comments=False,     # keep comments
            use_space_around_operators=True
        ).strip()
    except Exception:
        return raw_sql or ""

def escape_md_for_card(text: str) -> str:
    """
    Adaptive Card TextBlock parses a subset of Markdown. Escape chars that
    often break SQL (underscore/pipe/asterisk/backtick).
    """
    return (
        (text or "")
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("`", "\\`")
    )

def chunk_text(s: str, limit: int = 2400):
    """
    Split long SQL into chunks so each TextBlock stays under card limits.
    (Teams/Adaptive Cards can truncate overly long TextBlocks.)
    """
    s = s or ""
    return [s[i:i+limit] for i in range(0, len(s), limit)]

def build_sql_toggle_card(
    raw_sql: str,
    conversation_id: str,
    truncated: bool,
    user_id: str
) -> Attachment:
    pretty_sql = format_sql_for_card(raw_sql)
    safe_sql   = escape_md_for_card(pretty_sql)
    chunks     = chunk_text(safe_sql, limit=2400)

    # Base card
    card = {
      "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
      "type": "AdaptiveCard",
      "version": "1.5",
      "body": [
        {
          "type": "Container",
          "id": "sqlContainer",
          "isVisible": False,
          "items": [
            {"type": "TextBlock", "text": "**Generated SQL**", "weight": "Bolder"}
          ] + [
            {
              "type": "TextBlock",
              "text": chunk,
              "wrap": True,
              "fontType": "Monospace",     # ← IDE-like look
              "spacing": "Small"
            } for chunk in chunks
          ]
        }
      ],
      "actions": [
        {
          "type": "Action.ToggleVisibility",
          "title": "Show SQL",
          "targetElements": ["sqlContainer"]
        },
        {
          "type":  "Action.OpenUrl",
          "title": "Show Chart",
          "url":   f"{DASH_URL}/chart?session={conversation_id}"
        },
        {
          "type":  "Action.OpenUrl",
          "title": "Email Results",
          "url":   f"{BOT_URL}/graph/login?session={conversation_id}&user={user_id}"
        }
      ]
    }

    if truncated:
        card["actions"].append({
          "type":  "Action.OpenUrl",
          "title": "Download CSV",
          "url":   f"{BOT_URL}/download_csv?session={conversation_id}"
        })

    return Attachment(
      content_type="application/vnd.microsoft.card.adaptive",
      content=card
    )

def process_query_results(answer_json: Dict) -> str:
    sections: List[str] = []
    logger.info(f"Processing answer JSON: {answer_json}")

    # 0) Plain-text summaries (e.g., "explain this dataset") come back as `message`
    msg = answer_json.get("message")
    if isinstance(msg, str) and msg.strip():
        # Keep it simple and safe; Teams supports basic markdown.
        sections.append("## Dataset Summary\n\n" + msg.strip() + "\n")
        # Stitch and return immediately; there’s no table to render.
        return "\n".join(sections)

    # 1) Metadata (row_count, execution_time_ms)
    meta = answer_json.get("query_result_metadata", {})
    if isinstance(meta, dict):
        meta_bits = []
        if "row_count" in meta:
            meta_bits.append(f"**Row Count:** {meta['row_count']}")
        if "execution_time_ms" in meta:
            meta_bits.append(f"**Execution Time:** {meta['execution_time_ms']}ms")
        if meta_bits:
            sections.append("\n".join(meta_bits) + "\n")

    # 2) Results table
    stmt = answer_json.get("statement_response", {})
    result = stmt.get("result", {})              # <-- this is the dict you saw
    rows   = result.get("data_array", [])
    # the schema columns live under manifest.schema.columns
    manifest = stmt.get("manifest", {})
    schema   = manifest.get("schema", {}).get("columns", [])

    # 3) Query Description
    desc = answer_json.get("query_description") or ""
    if desc:
        sections.append(f"## Query Description\n\n{desc}\n")

    # 4) Truncation / size info (all computed in ask_genie)
    db_total_rows   = answer_json.get("db_total_rows", 0)
    csv_rows        = answer_json.get("csv_rows", 0)
    shown_rows      = answer_json.get("shown_rows", len(rows) if isinstance(rows, list) else 0)
    teams_truncated = answer_json.get("teams_truncated", 0)
    csv_truncated   = answer_json.get("csv_truncated", 0)

    notice_parts = []
    if db_total_rows and db_total_rows > shown_rows:
        notice_parts.append(f"Showing first {shown_rows:,} of {db_total_rows:,} rows")
        # if teams_truncated:
        #     notice_parts.append(f"Teams view cut {teams_truncated:,}")
        if csv_truncated:
            notice_parts.append(f"CSV contains the first 5,000 rows of {db_total_rows:,} ({csv_truncated:,} rows are truncated)")
    truncation_notice = "_" + " • ".join(notice_parts) + "._" if notice_parts else ""

    if rows and schema:
        # build header
        header = "| " + " | ".join(col["name"] for col in schema) + " |"
        sep    = "|" + "|".join(" --- " for _ in schema) + "|"
        table = [header, sep]

        # populate rows
        for row in rows:
            out = []
            for val, col in zip(row, schema):
                if val is None:
                    out.append("NULL")
                elif col.get("type_name") in ("DECIMAL", "DOUBLE", "FLOAT"):
                    out.append(f"{float(val):,.2f}")
                elif col.get("type_name") in ("INT", "BIGINT", "LONG"):
                    out.append(f"{int(val):,}")
                else:
                    out.append(str(val))
            table.append("| " + " | ".join(out) + " |")

        results_block = "## Query Results\n\n"
        if truncation_notice:
            results_block += truncation_notice + "\n\n"
        results_block += "\n".join(table) + "\n"
        sections.append(results_block)

                # --- NEW: Supervisor Insights ---
        if LLM_SUPERVISOR_INSIGHTS_ENABLED:
            insights = supervisor_insights(answer_json)
            if insights.get("insights_text"):
                sections.append("## Insights\n\n" + insights["insights_text"] + "\n")
    else:
        logger.debug("No results table to render")

    # 4) Collapsible SQL block
    raw_sql_md = answer_json.get("raw_sql_markdown")
    if raw_sql_md:
        sections.append(raw_sql_md + "\n")

    # 5) If nothing at all…
    if not sections:
        logger.error("No data available to show in process_query_results")
        return "_No tabular results for this request._\n"

    # stitch and return
    return "\n".join(sections)

def safe_md(text: str) -> str:
    return text.replace("|", "\\|").replace("_", "\\_")

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)

class MyBot(ActivityHandler):
    def __init__(self):
        self.conversation_ids: Dict[str, str] = {}
        self.user_state: Dict[str, Dict] = {}

    async def _typing_pump(self, turn_context: TurnContext, interval: float = TYPING_INTERVAL):
        """
        Periodically send a 'typing' activity until cancelled.
        """
        while True:
            try:
                await turn_context.send_activity(Activity(type=ActivityTypes.typing))
            except Exception:
                logger.exception("Typing pump failed to send typing activity")
            await asyncio.sleep(interval)

    async def on_message_activity(self, turn_context: TurnContext):
        from_prop = turn_context.activity.from_property
        user_id   = getattr(from_prop, "id", None)                      # BF channel id
        aad_id    = getattr(from_prop, "aad_object_id", None)           # Entra object id
        aad_name  = getattr(from_prop, "name", None) or "Unknown"       # display name fallback

        try:
            from storage import save_user_profile
            logger.info(f"[USER] upserting UserPrefs for aad_id={aad_id} name={aad_name!r}")
            save_user_profile(aad_id, aad_name)
            logger.info("[USER] save_user_profile completed")
        except Exception:
            logger.exception(f"[USER] save_user_profile failed (aad_id={aad_id})")

        state = self.user_state.setdefault(user_id, {})

        question = turn_context.activity.text

        pending = state.get("pending_pref")
        if pending:
            lower_q = question.strip().lower()
            if lower_q in ("yes", "y", "ok", "sure"):
                # normalize key
                key = KEY_NORMALIZATION.get(pending["key"].lower(), pending["key"])

                save_user_pref(aad_id, key, pending["value"])

                vals = pending["value"]
                if isinstance(vals, list):
                    pretty_vals = ", ".join(vals)
                else:
                    pretty_vals = str(vals)

                await turn_context.send_activity(
                    f"✅ Got it — I’ll remember your {key}(s): {pretty_vals}."
                )
                state.pop("pending_pref", None)
                return

            elif lower_q in ("no", "n", "nope", "nah", "not now", "cancel", "stop"):
                await turn_context.send_activity(
                    f"👍 Okay — I won’t save that {pending['key']}."
                )
                state.pop("pending_pref", None)
                return

        if EXPLICIT_INTENT.search(question):
            if await maybe_handle_pref_command(turn_context, aad_id, question):
                return  # handled; skip Genie for this turn
            
        if CLEAR_INTENT.search(question):
            if await maybe_handle_clear_command(turn_context, aad_id, question):
                return  # handled; skip Genie for this turn

        typing_task = asyncio.create_task(self._typing_pump(turn_context, interval=TYPING_INTERVAL))

        try:
            # 1) call Genie
            answer, new_conversation_id = await ask_genie(
                question,
                DATABRICKS_SPACE_ID,
                self.conversation_ids.get(user_id),
                aad_id=aad_id,
            )
            self.conversation_ids[user_id] = new_conversation_id

            # 2) parse JSON
            answer_json = json.loads(answer)

            # map of preference keys to the trigger words we’ll look for in the question
            prefs = get_user_prefs(aad_id)
            logger.info(f"Loaded prefs for {aad_id}: {prefs}")
            if not state.get("pending_pref"):
                if "dept" not in prefs:
                    vals = extract_depts(question)
                    if vals:
                        # normalize the key before saving
                        key = KEY_NORMALIZATION.get("dept", "dept")
                        valid, invalid = validate_pref(key, vals)
                        if not valid:
                            await turn_context.send_activity(
                                f"⚠️ I didn’t recognize dept(s): {', '.join(invalid)}. "
                                f"Valid values are: {', '.join(sorted(VALID_DEPTS))}."
                            )
                        else:
                            state["pending_pref"] = {"key": key, "value": valid}
                            if invalid:
                                await turn_context.send_activity(
                                    f"⚠️ Ignoring invalid dept(s): {', '.join(invalid)}. "
                                    f"Proceeding with valid dept(s): {', '.join(valid)}.\n"
                                    f"(Reply `yes` or `no` to confirm.)"
                                )
                            else:
                                await turn_context.send_activity(
                                    f"💡 I noticed you mentioned dept(s) {', '.join(valid)} in this query. "
                                    f"Want me to remember {', '.join(valid)} as your default dept(s)?\n"
                                    f"(Reply `yes` or `no` to confirm.)"
                                )

            if "dc" not in prefs:
                vals = extract_dcs(question)
                if vals:
                    key = KEY_NORMALIZATION.get("dc", "dc")
                    valid, invalid = validate_pref(key, vals)
                    if not valid:
                        await turn_context.send_activity(
                            f"⚠️ I didn’t recognize DC(s): {', '.join(invalid)}. "
                            f"Valid values are: {', '.join(sorted(VALID_DCS))}."
                        )
                    else:
                        state["pending_pref"] = {"key": key, "value": valid}
                        if invalid:
                            await turn_context.send_activity(
                                f"⚠️ Ignoring invalid DC(s): {', '.join(invalid)}. "
                                f"Proceeding with valid DC(s): {', '.join(valid)}.\n"
                                f"(Reply `yes` or `no` to confirm.)"
                            )
                        else:
                            await turn_context.send_activity(
                                f"💡 I noticed you mentioned DC(s) {', '.join(valid)} in this query. "
                                f"Want me to remember {', '.join(valid)} as your default DC(s)?\n"
                                f"(Reply `yes` or `no` to confirm.)"
                            )

                if "region" not in prefs:
                    vals = extract_regions(question)
                    if vals:
                        key = KEY_NORMALIZATION.get("region", "region")
                        valid, invalid = validate_pref(key, vals)
                        if not valid:
                            await turn_context.send_activity(
                                f"⚠️ I didn’t recognize region(s): {', '.join(invalid)}. "
                                f"Valid values are: {', '.join(sorted(VALID_REGIONS))}."
                            )
                        else:
                            state["pending_pref"] = {"key": key, "value": valid}
                            if invalid:
                                await turn_context.send_activity(
                                    f"⚠️ Ignoring invalid region(s): {', '.join(invalid)}. "
                                    f"Proceeding with valid region(s): {', '.join(valid)}.\n"
                                    f"(Reply `yes` or `no` to confirm.)"
                                )
                            else:
                                await turn_context.send_activity(
                                    f"💡 I noticed you mentioned region(s) {', '.join(valid)} in this query. "
                                    f"Want me to remember {', '.join(valid)} as your default region(s)?\n"
                                    f"(Reply `yes` or `no` to confirm.)"
                                )

            # safely add user info without affecting Genie response
            aad_id = getattr(turn_context.activity.from_property, "aad_object_id", None)
            aad_name = turn_context.activity.from_property.name
            answer_json["user_info"] = {
                "id": turn_context.activity.from_property.id,
                "name": aad_name,
                "aad_id": aad_id,
            }

            token = _get_valid_access_token(user_id)
            if not token:
                logger.warning(f"No Graph token available for user_id={user_id} aad_id={aad_id}")
            if token and aad_id:
                graph_user = get_graph_user_details(aad_id, token)
                answer_json["user_info"]["graph_raw"] = graph_user

            logger.info(f"Captured user info: {answer_json['user_info']}")

            # persist user profile + Graph enrichment into Table Storage
            if aad_id and aad_name:
                try:
                    from storage import save_user_profile
                    save_user_profile(aad_id, aad_name)
                except Exception:
                    logger.warning(f"Skipped saving user profile for {aad_id} until Application User.Read.All is granted")

            # 3a) send plain‑text markdown (description + results)
            plain_markdown = process_query_results(answer_json)

            plain_markdown += f"\n\n*Session ID: `{new_conversation_id}`*"

            await turn_context.send_activity(plain_markdown)

            # 3b) Send the SQL toggle card only if we actually have SQL
            raw_sql = answer_json.get("raw_sql", "") or ""
            if raw_sql.strip():
                conversation  = self.conversation_ids[user_id]
                truncated     = answer_json.get("truncated", False)
                # If your build_sql_toggle_card now expects user_id too, pass it
                # sql_card = build_sql_toggle_card(raw_sql, conversation, truncated, user_id)
                sql_card = build_sql_toggle_card(raw_sql, conversation, truncated, user_id)
                await turn_context.send_activity(MessageFactory.attachment(sql_card))

        except json.JSONDecodeError as jde:
            logger.exception("Failed to parse JSON from Genie")
            await turn_context.send_activity(
                "⚠️ I got something I couldn’t understand back from Genie."
            )

        except Exception as e:
            # **this will now log the full stacktrace to your container logs**
            logger.exception("Unhandled error in on_message_activity")
            await turn_context.send_activity(
                "❗️ I’m sorry—I ran into an unexpected error processing your request. "
                "Please try again in a moment."
            )
        finally:
            typing_task.cancel()
            with suppress(asyncio.CancelledError):
                await typing_task

    async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Supply Chain KNOWLEDGE Agent!")

BOT = MyBot()

async def _create_draft_for_session(user_id: str, session: str) -> web.Response:
    access_token = _get_valid_access_token(user_id)
    if not access_token:
        return web.Response(status=401, text="No cached token; interactive sign-in required.")

    data = SESSION_DATA.get(session)
    if not data:
        return web.Response(status=404, text="No session data to summarize.")

    # ---- idempotency key for this user+session
    key = f"{user_id}:{session}"
    now = time.time()
    cached = DRAFTS_BY_KEY.get(key)
    if cached and (now - cached.get("ts", 0) <= DRAFT_CACHE_TTL):
        logger.info("Reusing recent draft for %s", key)
        web_link = cached["web_link"]
        html = f"""<!doctype html>
        <html><head><meta charset="utf-8"></head>
        <body style="font-family:Segoe UI, Arial; margin:16px">
        <h3>Draft created</h3>
        <p><a href="{web_link}" target="_blank" rel="noopener">Open draft in Outlook (web)</a></p>
        <p>You can also find it in your Drafts folder in desktop Outlook.</p>
        </body></html>"""
        return web.Response(text=html, content_type="text/html")

    # Ensure only one creator runs for this key at a time
    lock = _get_draft_lock(key)
    async with lock:
        # Double-check inside the lock (another request may have created it)
        cached = DRAFTS_BY_KEY.get(key)
        if cached and (time.time() - cached.get("ts", 0) <= DRAFT_CACHE_TTL):
            logger.info("Reusing recent draft for %s (post-lock)", key)
            web_link = cached["web_link"]
            html = f"""<!doctype html>
            <html><head><meta charset="utf-8"></head>
            <body style="font-family:Segoe UI, Arial; margin:16px">
            <h3>Draft created</h3>
            <p><a href="{web_link}" target="_blank" rel="noopener">Open draft in Outlook (web)</a></p>
            <p>You can also find it in your Drafts folder in desktop Outlook.</p>
            </body></html>"""
            return web.Response(text=html, content_type="text/html")

                # ----- Supervisor (LLM) -----
        try:
            sup = supervisor_summarize(data)  # {'subject','summary_html','summary_text'}
            subject_override = (sup.get("subject") or "").strip()
            summary_html = (sup.get("summary_html") or "").strip()
            summary_text = (sup.get("summary_text") or "").strip()
        except Exception:
            logger.exception("Supervisor failed; proceeding without overrides.")
            subject_override = ""
            summary_html = ""
            summary_text = ""

        # ----- Build bodies (use 50-row rule) -----
        # Preview inline if TOTAL rows <= 50; otherwise no preview + attach CSV
        plain_body, html_body, included_preview = build_email_bodies(data, preview_max=PREVIEW_MAX_ROWS)

        # Subject: supervisor override if present; else your existing deterministic subject
        subject = subject_override or build_business_subject(data)

        # Inject executive summary ONLY if supervisor provided it (LLM enabled and succeeded)
        if summary_html:
            html_body = (
                '<h2 style="margin:0 0 12px 0;">Executive Summary</h2>'
                f'{summary_html}'
            )
        else:
            # deterministic version
            html_body = html_body

        # Plain text body: if LLM summary exists, use only that; else fallback
        if summary_text:
            plain_body = (
                "Executive Summary\n\n"
                f"{summary_text}\n"
            )
        else:
            plain_body = plain_body

        if summary_text:
            plain_body = (
                "Executive Summary\n\n"
                f"{summary_text}\n"
                + ("-" * 24) + "\n"
            ) + plain_body

        # ----- Create draft + optional CSV attachment (unchanged) -----
        try:
            draft = create_draft_via_graph(access_token, subject, html_body)
            msg_id   = draft.get("id")
            web_link = draft.get("webLink")

            if not included_preview:
                csv_path = SESSION_FILES.get(session)
                if csv_path and os.path.exists(csv_path):
                    with open(csv_path, "rb") as f:
                        csv_bytes = f.read()
                    _ = attach_csv_via_graph(access_token, msg_id, csv_bytes, os.path.basename(csv_path))
                else:
                    logger.warning("CSV path missing for session %s; skipping attachment", session)

            DRAFTS_BY_KEY[key] = {"msg_id": msg_id, "web_link": web_link, "ts": time.time()}

        except Exception:
            logger.exception("Failed to create draft or attach CSV via Graph")
            return web.Response(status=500, text="Failed to create Outlook draft.")

    # Success page
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Segoe UI, Arial; margin:16px">
  <h3>Draft created</h3>
  <p><a href="{web_link}" target="_blank" rel="noopener">Open draft in Outlook (web)</a></p>
  <p>You can also find it in your Drafts folder in desktop Outlook.</p>
</body></html>"""
    return web.Response(text=html, content_type="text/html")

# Prefer cached token; otherwise interactive login
async def graph_login(request: web.Request) -> web.Response:
    session = request.query.get("session") or ""
    user_id = request.query.get("user") or ""
    if not session or not user_id:
        return web.Response(status=400, text="Missing session or user.")
    if _get_valid_access_token(user_id):
        raise web.HTTPFound(f"{BOT_URL}/graph/draft?session={session}&user={user_id}")
    state = json.dumps({"session": session, "user": user_id})
    raise web.HTTPFound(_oauth_authorize_url(state))

app.router.add_get("/graph/login", graph_login)

# Silent path using cached token
async def graph_draft(request: web.Request) -> web.Response:
    session = request.query.get("session") or ""
    user_id = request.query.get("user") or ""
    if not session or not user_id:
        return web.Response(status=400, text="Missing session or user.")
    return await _create_draft_for_session(user_id, session)

app.router.add_get("/graph/draft", graph_draft)

# After AAD callback, cache tokens then delegate to helper
async def graph_callback(request: web.Request) -> web.Response:
    code  = request.query.get("code")
    state = request.query.get("state")
    if not code or not state:
        return web.Response(status=400, text="Missing code or state.")
    try:
        s = json.loads(state)
        session = s["session"]
        user_id = s["user"]
    except Exception:
        return web.Response(status=400, text="Invalid state")

    try:
        tok = _oauth_token_request({
            "client_id": MS_CLIENT_ID,
            "scope": MS_SCOPES,
            "code": code,
            "grant_type": "authorization_code",
            "client_secret": MS_CLIENT_SECRET,
            "redirect_uri": MS_REDIRECT_URI,
        })
        _save_tokens_for_user(user_id, tok)
    except Exception:
        logger.exception("Token exchange failed")
        return web.Response(status=500, text="Failed to sign in to Microsoft Graph.")

    return await _create_draft_for_session(user_id, session)

app.router.add_get("/graph/callback", graph_callback)

async def messages(req: web.Request) -> web.Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    try:
        response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
        if response:
            return web.json_response(data=response.body, status=response.status)
        return web.Response(status=201)
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return web.Response(status=500)

app.router.add_post("/api/messages", messages)

# ────────────────────────────────────────────────────────────
# New: CSV download endpoint
# ────────────────────────────────────────────────────────────
async def download_csv(request: web.Request) -> web.Response:
    session = request.query.get("session")
    path    = SESSION_FILES.get(session)
    if not path or not os.path.exists(path):
        return web.Response(status=404, text="No data for that session.")

    # Clean up after serving:
    # del SESSION_FILES[session]

    return web.FileResponse(
        path,
        headers={
            "Content-Disposition": f"attachment; filename=\"results_{session}.csv\""
        }
    )

app.router.add_get("/download_csv", download_csv)

# ────────────────────────────────────────────────────────────────
# New: serve full Genie JSON for a session
# ────────────────────────────────────────────────────────────────
async def download_json(request: web.Request) -> web.Response:
    session = request.query.get("session")
    data    = SESSION_DATA.get(session)
    if not data:
        return web.Response(status=404, text="No data for that session.")
    return web.json_response(data)

app.router.add_get("/download_json", download_json)

# ────────────────────────────────────────────────────────────
# App start
# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        host = os.getenv("HOST", "localhost")
        port = int(os.environ.get("PORT", 3978))
        web.run_app(app, host=host, port=port)
    except Exception as error:
        logger.exception("Error running app")
