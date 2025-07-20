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
from typing import Dict, List, Optional
from dotenv import load_dotenv
from aiohttp import web
from botbuilder.core import BotFrameworkAdapterSettings, BotFrameworkAdapter, ActivityHandler, TurnContext
from botbuilder.schema import Activity, ChannelAccount
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
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

workspace_client = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN
)

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
    try:
        loop = asyncio.get_running_loop()

        # 1) Start or continue the conversation
        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )
            conversation_id = initial_message.conversation_id
        else:
            initial_message = await loop.run_in_executor(
                None, genie_api.create_message_and_wait,
                space_id, conversation_id, question
            )

        message_id = initial_message.message_id

        # 2) Poll for COMPLETED with retries
        max_attempts = 5
        backoff_base = 2  # seconds
        message_content = None

        for attempt in range(1, max_attempts + 1):
            # fetch the message status & payload
            message_content = await loop.run_in_executor(
                None,
                genie_api.get_message,
                space_id, conversation_id, message_id
            )

            status = getattr(message_content, "status", None)
            logger.debug(f"[Poll {attempt}/{max_attempts}] status={status}")

            if status == "COMPLETED":
                logger.debug("Genie returned COMPLETED")
                break
            elif status == "FAILED":
                error_msg = getattr(message_content, "error_message", "<no error>")
                logger.error(f"Genie FAILED on attempt {attempt}: {error_msg}")
                # retry up to max_attempts
            else:
                # still running (e.g. PENDING/RUNNING)
                logger.debug(f"Genie not ready, sleeping {backoff_base**attempt}s")
            
            if attempt < max_attempts:
                time.sleep(backoff_base ** attempt)
            else:
                # give up
                raise RuntimeError(f"Genie did not complete after {max_attempts} attempts")

        # 3) Process the completed message_content exactly as before
        logger.info(f"Raw message content: {message_content}")

        if message_content.attachments:
            for attachment in message_content.attachments:
                text_obj = getattr(attachment, "text", None)
                if text_obj and hasattr(text_obj, "content"):
                    return json.dumps({"message": text_obj.content}), conversation_id
            
                attachment_id = getattr(attachment, "attachment_id", None)
                query_obj     = getattr(attachment, "query", None)

                if attachment_id and query_obj:
                    query_result = await loop.run_in_executor(
                        None,
                        get_attachment_query_result,
                        space_id,
                        conversation_id,
                        message_id,
                        attachment_id
                    )
                    # ... (rest of your attachment logic) ...
                    # build and return JSON payload with statement_response, etc.
                    # (unchanged)

                text_obj = getattr(attachment, "text", None)
                if text_obj and hasattr(text_obj, "content"):
                    return json.dumps({"message": text_obj.content}), conversation_id

        # fallback to plain content
        return json.dumps({"message": message_content.content}), conversation_id

    except Exception as e:
        logger.error(f"Error in ask_genie: {str(e)}")
        return json.dumps({"error": "An error occurred while processing your request."}), conversation_id


def process_query_results(answer_json: Dict) -> str:
    response = ""
    
    logger.info(f"Processing answer JSON: {answer_json}")
    
    if "query_description" in answer_json and answer_json["query_description"]:
        response += f"## Query Description\n\n{answer_json['query_description']}\n\n"

    if "query_result_metadata" in answer_json:
        metadata = answer_json["query_result_metadata"]
        if isinstance(metadata, dict):
            if "row_count" in metadata:
                response += f"**Row Count:** {metadata['row_count']}\n\n"
            if "execution_time_ms" in metadata:
                response += f"**Execution Time:** {metadata['execution_time_ms']}ms\n\n"

    if "statement_response" in answer_json:
        statement_response = answer_json["statement_response"]
        logger.info(f"Found statement_response: {statement_response}")
        
        if "result" in statement_response and "data_array" in statement_response["result"]:
            response += "## Query Results\n\n"
            
            schema = statement_response.get("manifest", {}).get("schema", {})
            columns = schema.get("columns", [])
            logger.info(f"Schema columns: {columns}")
            
            header = "| " + " | ".join(col["name"] for col in columns) + " |"
            separator = "|" + "|".join(["---" for _ in columns]) + "|"
            response += header + "\n" + separator + "\n"
            
            data_array = statement_response["result"]["data_array"]
            logger.info(f"Data array: {data_array}")
            
            for row in data_array:
                formatted_row = []
                for value, col in zip(row, columns):
                    if value is None:
                        formatted_value = "NULL"
                    elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                        formatted_value = f"{float(value):,.2f}"
                    elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                        formatted_value = f"{int(value):,}"
                    else:
                        formatted_value = str(value)
                    formatted_row.append(formatted_value)
                response += "| " + " | ".join(formatted_row) + " |\n"
        else:
            logger.error(f"Missing result or data_array in statement_response: {statement_response}")
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "No data available.\n\n"
        logger.error("No statement_response or message found in answer_json")
    
    return response

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD
                                       )
ADAPTER = BotFrameworkAdapter(SETTINGS)

class MyBot(ActivityHandler):
    def __init__(self):
        self.conversation_ids: Dict[str, str] = {}

    async def on_message_activity(self, turn_context: TurnContext):
        question = turn_context.activity.text
        user_id = turn_context.activity.from_property.id
        conversation_id = self.conversation_ids.get(user_id)

        try:
            answer, new_conversation_id = await ask_genie(question, DATABRICKS_SPACE_ID, conversation_id)
            self.conversation_ids[user_id] = new_conversation_id

            answer_json = json.loads(answer)
            response = process_query_results(answer_json)

             # ────────────────────────────────────────────────
             # If Genie explicitly returned “No data available.”, fallback
             # ────────────────────────────────────────────────
            if "No data available." in response:
                 raise RuntimeError("Genie returned no data")

            await turn_context.send_activity(response)

        except json.JSONDecodeError:
            await turn_context.send_activity("Failed to decode response from the server.")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await turn_context.send_activity("An error occurred while processing your request.")
            logger.error(f"ask_genie failed or empty result: {e}")
            await turn_context.send_activity(
                 "❗️ I’m sorry—I wasn’t able to fetch an answer from Genie right now. "
                 "Please try again in a moment, or reach out if the problem persists."
             )

            await turn_context.send_activity(response)
        except json.JSONDecodeError:
            await turn_context.send_activity("Failed to decode response from the server.")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await turn_context.send_activity("An error occurred while processing your request.")

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

app = web.Application()
app.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        host = os.getenv("HOST", "localhost")
        port = int(os.environ.get("PORT", 3978))
        web.run_app(app, host=host, port=port)
    except Exception as error:
        logger.exception("Error running app")
