import os, requests, logging
import re
import json
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

logger = logging.getLogger(__name__)

TABLE_NAME = "BotUserPreferences"

# ------------------------
# Azure Table Storage setup
# ------------------------
_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
_service = TableServiceClient.from_connection_string(_conn_str)

def _get_table():
    return _service.create_table_if_not_exists(TABLE_NAME)

def _log_http_error(where: str, err: Exception):
    if isinstance(err, HttpResponseError):
        # HttpResponseError has status_code, error_code, and response
        status = getattr(err, "status_code", None)
        code = getattr(err, "error_code", None)
        try:
            body = err.response.text() if err.response else None
        except Exception:
            body = "<unavailable>"
        logger.error(f"[{where}] HttpResponseError status={status} error_code={code} body={body}")
    else:
        logger.exception(f"[{where}] {type(err).__name__}: {err}")

# ------------------------
# App-only Graph auth
# ------------------------
tenant_id = os.getenv("MS_TENANT_ID")
client_id = os.getenv("MicrosoftAppId")
client_secret = os.getenv("MicrosoftAppPassword")

def get_app_graph_token() -> str:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,  # don't log this
        "scope": "https://graph.microsoft.com/.default"
    }
    logger.info(f"Graph token request: tenant={tenant_id}, client_id={client_id}, scope={data['scope']}")
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return token

def get_user_profile_app(aad_id: str) -> dict:
    token = get_app_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/users/{aad_id}"

    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        user = resp.json()
        logger.debug(f"Graph lookup for {aad_id}: displayName={user.get('displayName')} mail={user.get('mail')}")
        return user
    else:
        logger.error(
            f"Graph (app) lookup failed for {aad_id}. "
            f"Status={resp.status_code}, Body={resp.text}"
        )
        return {}

def save_user_profile(aad_id: str, name: str):
    graph_user = {}
    try:
        graph_user = get_user_profile_app(aad_id)
    except Exception:
        logger.exception(f"Graph enrichment raised exception for {aad_id} ({name})")
        graph_user = {}

    t = _get_table()

    try:
        # Try to read existing row; if it exists, DO NOT touch userPrefs
        e = t.get_entity("UserPrefs", aad_id)
        patch = {
            "PartitionKey": "UserPrefs",
            "RowKey": aad_id,
            "displayName": name,
        }
        if graph_user:
            patch["graphProfile"] = json.dumps(graph_user)
        t.upsert_entity(patch, mode=UpdateMode.MERGE)
        logger.info(f"Updated profile for {aad_id} without altering userPrefs")
    except ResourceNotFoundError:
        # Create fresh row — only here we initialize userPrefs
        entity = {
            "PartitionKey": "UserPrefs",
            "RowKey": aad_id,
            "displayName": name,
            "graphProfile": json.dumps(graph_user) if graph_user else None,
            "userPrefs": "{}",
        }
        t.upsert_entity(entity, mode=UpdateMode.MERGE)  # insert/merge
        logger.info(f"Created profile for {aad_id} with empty userPrefs")

def save_user_pref(aad_id: str, key: str, value: str | list[str]) -> dict:
    """
    Save or update a single user preference.
    Handles both single string values and lists of strings.
    """
    table = _get_table()
    entity = table.get_entity("UserPrefs", aad_id)
    prefs = json.loads(entity.get("userPrefs", "{}"))

    # Always normalize lists to a list type, not comma-joined strings
    if isinstance(value, list):
        prefs[key] = value
    else:
        prefs[key] = str(value)

    entity["userPrefs"] = json.dumps(prefs)
    table = _get_table()
    table.upsert_entity(entity)
    logger.info(f"Updated {aad_id} prefs: {prefs}")
    return prefs

def get_user_prefs(aad_id: str) -> dict:
    """
    Fetch preferences as dict.
    Values may be strings or lists depending on what was stored.
    """
    try:
        table = _get_table()
        entity = table.get_entity("UserPrefs", aad_id)
        prefs = json.loads(entity.get("userPrefs", "{}"))
        return prefs
    except Exception:
        return {}
    
def clear_user_pref(aad_id: str, key: str | None = None) -> dict:
    """
    Remove one preference (key) or all prefs for a user.
    Returns the updated prefs dict.
    """
    table = _get_table()
    entity = table.get_entity("UserPrefs", aad_id)
    prefs = json.loads(entity.get("userPrefs", "{}"))

    if key:
        prefs.pop(key, None)
    else:
        prefs = {}  # wipe all

    entity["userPrefs"] = json.dumps(prefs)
    table.upsert_entity(entity)
    logger.info(f"Cleared prefs for {aad_id}, key={key or 'ALL'} → {prefs}")
    return prefs