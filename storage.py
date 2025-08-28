import os, requests, logging
import json
from azure.data.tables import TableServiceClient

logger = logging.getLogger(__name__)

# ------------------------
# Azure Table Storage setup
# ------------------------
_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
_service = TableServiceClient.from_connection_string(_conn_str)
_table = _service.create_table_if_not_exists("BotUserPreferences")

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

    logger.info(f"Graph lookup with app token: url={url}")

    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        user = resp.json()
        logger.info(f"Graph (app) user lookup for {aad_id}: {user}")
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
    except Exception as e:
        logger.exception(
            f"Graph enrichment raised exception for {aad_id} ({name})"
        )
        graph_user = {}

    entity = {
        "PartitionKey": "UserPrefs",
        "RowKey": aad_id,
        "displayName": name,
        "graphProfile": json.dumps(graph_user) if graph_user else None,
    }

    try:
        _table.upsert_entity(entity)
        logger.info(f"Saved/updated user profile for {aad_id} ({name})")
    except Exception as e:
        logger.exception(f"Failed to upsert user profile for {aad_id}")