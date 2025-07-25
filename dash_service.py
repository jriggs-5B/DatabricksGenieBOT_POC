import os
import json
import requests
import pandas as pd
from flask import Flask, request
from dash import Dash, dcc, html, Input, Output, State, dash_table
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("dash_service")

# URL where your bot is listening (override via env var if needed)
BOT_URL = os.environ["BOT_URL"]

# Flask server to host Dash
server = Flask(__name__)

# Dash app mounted at /chart/
dash_app = Dash(
    __name__,
    server=server,
    url_base_pathname="/chart/",
    suppress_callback_exceptions=True,
)

dash_app.layout = html.Div([
    # This component lets you read the URL (including query string)
    dcc.Location(id="url", refresh=False),

    html.Div(
        html.Img(src="/assets/5B_logo_medallion.png", style={"height": "60px"}),
        style={"textAlign": "center", "padding": "10px 0"}
    ),

    html.H2("Supply Chain KNOWLEDGE Agent Results Visuaization"),
    dcc.Dropdown(
        id="chart-type",
        options=[
            {"label": "Bar",  "value": "bar"},
            {"label": "Line", "value": "line"},
            {"label": "Pie",  "value": "pie"},
        ],
        value="bar",
        clearable=False,
        style={"width": "200px", "margin": "0 auto 20px auto"},
    ),
    dcc.Graph(id="main-chart"),

     # 4) Data table placeholder
    html.H3("Underlying Data"),
    dash_table.DataTable(
        id="data-table",
        columns=[],   # will be set in callback
        data=[],      # will be set in callback
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "5px"},
    ),
])

@dash_app.callback(
    [
        Output("main-chart", "figure"),
        Output("data-table", "columns"),
        Output("data-table", "data"),
    ],
    [Input("chart-type", "value")],
    [State("url", "search")]
)

def update_chart_and_table(chart_type, url_search):
    import urllib.parse

    # parse out ?session=xxx
    query = urllib.parse.parse_qs((url_search or "").lstrip("?"))
    session = query.get("session", [None])[0]
    logger.debug(f"🛰️  Dash fetching JSON for session={session}")

    # fetch data
    resp = requests.get(f"{BOT_URL}/download_json", params={"session": session})
    logger.debug(f"🛰️  Response {resp.status_code}: {resp.text[:200]}…")

    if resp.status_code != 200 or not session:
        # nothing to chart
        return {"data": []}

    data = resp.json()
    cols = [c["name"] for c in data["statement_response"]["manifest"]["schema"]["columns"]]
    rows = data["statement_response"]["result"]["data_array"]
    df   = pd.DataFrame(rows, columns=cols)

    x, y = df.columns[:2]
    if chart_type == "bar":
        fig_data = [{"type": "bar",  "x": df[x], "y": df[y]}]
    elif chart_type == "line":
        fig_data = [{"type": "line", "x": df[x], "y": df[y]}]
    else:
        fig_data = [{"type": "pie",  "labels": df[x], "values": df[y]}]
    fig = {"data": data, "layout": {"margin": {"t": 30, "b": 30}}}

    # prepare table columns+data
    table_columns = [{"name": c, "id": c} for c in df.columns]
    table_data    = df.to_dict("records")

    return fig, table_columns, table_data

if __name__ == "__main__":
    # Use DASH_PORT env var or default to 8050
    port = int(os.getenv("PORT", os.getenv("DASH_PORT", 8050)))
    server.run(host="0.0.0.0", port=port)