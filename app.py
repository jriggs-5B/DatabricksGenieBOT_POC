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
from botbuilder.schema import Activity, ChannelAccount, Attachment
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI, MessageStatus
import asyncio
import requests

# Log for prod
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# logging.getLogger("databricks_genie").setLevel(logging.DEBUG)

#Log for development
# 1) Enable DEBUG everywhere (you can narrow this later)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

# 2) Your module logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 3) Turn on the Databricks SDK + HTTP internals
logging.getLogger("databricks").setLevel(logging.DEBUG)
logging.getLogger("databricks.sdk").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

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



logger = logging.getLogger(__name__)

async def ask_genie(
    question: str,
    space_id: str,
    conversation_id: Optional[str] = None
) -> tuple[str, str]:
    logger.debug("🔥 ENTERING ask_genie v2! 🔥")
    logger.debug("🛠️  ASK_GENIE PAYLOAD HOTFIX DEPLOYED 🛠️")
    try:
        loop = asyncio.get_running_loop()

        # ───────────────────────────────────────────────────────────────────────────
        # 1) start or continue conversation
        # ───────────────────────────────────────────────────────────────────────────
        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )
            conversation_id = initial_message.conversation_id
        else:
            initial_message = await loop.run_in_executor(
                None,
                genie_api.create_message_and_wait,
                space_id, conversation_id, question
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
                    # A) fetch and cache the full resultset as CSV
                    # ───────────────────────────────────────────────────────
                    # 1) temporarily override the query to be the full SQL
                    query_obj.query = raw_sql

                    # 2) ask Genie for the full rows
                    full_result = await loop.run_in_executor(
                        None,
                        get_attachment_query_result,
                        space_id,
                        conversation_id,
                        message_id,
                        attachment_id
                    )
                    full_rows  = full_result["statement_response"]["result"]["data_array"]
                    full_schema= full_result["statement_response"]["manifest"]["schema"]["columns"]

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
                    # 4) fetch, truncate, and handle “too large” errors
                    # ──────────────────────────────────────────────────────────
                    query_obj.query = raw_sql_limited

                    truncated = False
                    try:
                        # execute the limited SQL
                        query_result = await loop.run_in_executor(
                            None,
                            get_attachment_query_result,
                            space_id,
                            conversation_id,
                            message_id,
                            attachment_id,
                        )

                        # truncate client‑side if necessary
                        rows = query_result["statement_response"]["result"].get("data_array", [])

                        truncated = False
                        if len(rows) > MAX_ROWS:
                            truncated = True
                            query_result["statement_response"]["result"]["data_array"] = rows[:MAX_ROWS]

                    except Exception as e:
                        # catch “payload too large” or other errors
                        logger.warning(f"Genie error or payload too large: {e}")
                        truncated = True
                        query_result = {
                            "query_result_metadata": {},
                            "statement_response": {
                                "result": {"data_array": []},
                                "manifest": {"schema": {"columns": []}}
                            }
                        }

                    logger.debug(f"🔍 query_result after truncate/error: {query_result!r}")

                    # # build the JSON payload, including the `truncated` flag
                    # return json.dumps({
                    #     "query_description": desc or "",
                    #     "query_result_metadata": query_result.get("query_result_metadata", {}),
                    #     "statement_response":    query_result.get("statement_response", {}),
                    #     "raw_sql":               raw_sql or "",
                    #     "raw_sql_executed":      raw_sql_limited or "",
                    #     "truncated":             truncated
                    # }), conversation_id

                    # Build the answer JSON dict
                    answer_json = {
                        "query_description": desc or "",
                        "query_result_metadata": query_result.get("query_result_metadata", {}),
                        "statement_response":    query_result.get("statement_response", {}),
                        "raw_sql":               raw_sql or "",
                        "raw_sql_executed":      raw_sql_limited or "",
                        "truncated":             truncated
                    }
                    # Store for Dash to fetch later
                    logger.debug("🚀 FINAL GENIE PAYLOAD: %r", answer_json)
                    SESSION_DATA[conversation_id] = answer_json
                    
                    return json.dumps(answer_json), conversation_id

                    # logger.debug("🚀 FINAL GENIE PAYLOAD: %r", payload)
                    # return json.dumps(payload), conversation_id

        # ────────────────
        # Fallback if no attachments at all
        # ────────────────
        return json.dumps({"error": "No data available."}), conversation_id

    except Exception as e:
        logger.error(f"Error in ask_genie: {e}", exc_info=True)
        return json.dumps({"error": "An error occurred while processing your request."}), conversation_id


def build_sql_toggle_card(
    raw_sql: str,
    conversation_id: str,
    truncated: bool
) -> Attachment:
    # 1) base card
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
            {"type": "TextBlock", "text": "**Generated SQL**", "weight": "Bolder"},
            {"type": "TextBlock", "text": raw_sql, "wrap": True}
          ]
        }
      ],
      # start with just the Show SQL button
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
        }
      ]
    }

    # 2) conditionally add the Download CSV button
    if truncated:
        card["actions"].append({
          "type":  "Action.OpenUrl",
          "title": "Download CSV",
          "url":   f"{BOT_URL}/download_csv?session={conversation_id}"
        })

    # 3) wrap and return
    return Attachment(
      content_type="application/vnd.microsoft.card.adaptive",
      content=card
    )

def process_query_results(answer_json: Dict) -> str:
    sections: List[str] = []
    logger.info(f"Processing answer JSON: {answer_json}")

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

    truncated = False
    if len(rows) > MAX_ROWS:
        truncated = True
        rows = rows[:MAX_ROWS]

    # 3) Query Description
    truncated = answer_json.get("truncated", False)
    desc = answer_json.get("query_description")
    if truncated:
        desc += f"\n\n*Showing first {MAX_ROWS} rows; results truncated.*"
    if desc:
        sections.append(f"## Query Description\n\n{desc}\n")

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

        sections.append("## Query Results\n\n" + "\n".join(table) + "\n")
    else:
        logger.debug("No results table to render")

    # 4) Collapsible SQL block
    raw_sql_md = answer_json.get("raw_sql_markdown")
    if raw_sql_md:
        sections.append(raw_sql_md + "\n")

    # 5) If nothing at all…
    if not sections:
        logger.error("No data available to show in process_query_results")
        return "No data available.\n\n"

    # stitch and return
    return "\n".join(sections)


SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)

class MyBot(ActivityHandler):
    def __init__(self):
        self.conversation_ids: Dict[str, str] = {}

    async def on_message_activity(self, turn_context: TurnContext):
        user_id = turn_context.activity.from_property.id
        question = turn_context.activity.text

        try:
            # 1) call Genie
            answer, new_conversation_id = await ask_genie(
                question,
                DATABRICKS_SPACE_ID,
                self.conversation_ids.get(user_id),
            )
            self.conversation_ids[user_id] = new_conversation_id

            # 2) parse JSON
            answer_json = json.loads(answer)

            # 3a) send plain‑text markdown (description + results)
            plain_markdown = process_query_results(answer_json)

            plain_markdown += f"\n\n*Session ID: `{new_conversation_id}`*"

            await turn_context.send_activity(plain_markdown)

            

            # 3b) send only the SQL toggle card
            raw_sql = answer_json.get("raw_sql", "")
            conversation  = self.conversation_ids[user_id]
            truncated     = answer_json.get("truncated", False)

            sql_card = build_sql_toggle_card(raw_sql, conversation, truncated)
            await turn_context.send_activity(
                MessageFactory.attachment(sql_card)
            )

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

    async def on_members_added_activity(self, members_added: List[ChannelAccount], turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Supply Chain KNOWLEDGE Agent!")

BOT = MyBot()

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
