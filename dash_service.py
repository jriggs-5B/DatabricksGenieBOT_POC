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
        style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "padding": "10px 0"
        },
        children=[
            # 1) Logo on the left
            html.Img(
                src=dash_app.get_asset_url("5B_Logo_Medallion.png"),
                style={"height": "60px", "marginRight": "20px"}
            ),
            # 2) Title on the right
            html.H2(
                "Supply Chain KNOWLEDGE Agent Results Visualization",
                style={"margin": 0}
            ),
        ],
    ),
    
    # html.Div(
    #     html.Img(src="/assets/5B_Logo_Medallion.png", style={"height": "60px"}),
    #     style={"textAlign": "center", "padding": "10px 0"}
    # ),

    # html.H2("Supply Chain KNOWLEDGE Agent Results Visualization"),

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

    # 2) X‑column selector
    html.Div([
        html.Label("X‑axis:"),
        dcc.Dropdown(id="x-col", clearable=False)
    ], style={"display":"inline-block","width":"40%","margin":"0 5%"}),

    # 3) Y‑column selector
    html.Div([
        html.Label("Y‑axis:"),
        dcc.Dropdown(id="y-col", clearable=False)
    ], style={"display":"inline-block","width":"40%","margin":"0 5%"}),

    dcc.Graph(id="main-chart"),

     # 4) Data table placeholder
    html.H3("Underlying Data", style={"textAlign":"center","marginTop":"40px"}),
    dash_table.DataTable(
        id="data-table",
        columns=[],   # will be set in callback
        data=[],      # will be set in callback
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "5px"},
    ),
])
# 1) Populate x‑col and y‑col options + initial values once we know the session

@dash_app.callback(
    [
      Output("x-col", "options"),
      Output("x-col", "value"),
      Output("y-col", "options"),
      Output("y-col", "value"),
    ],
    Input("url", "search")
)
def populate_column_dropdowns(url_search):
    import urllib.parse
    query   = urllib.parse.parse_qs((url_search or "").lstrip("?"))
    session = query.get("session", [None])[0]

    # fetch the raw JSON once
    resp = requests.get(f"{BOT_URL}/download_json", params={"session": session})
    if resp.status_code != 200:
        return [], None, [], None

    j = resp.json()
    cols = [c["name"] for c in j["statement_response"]["manifest"]["schema"]["columns"]]
    options = [{"label": c, "value": c} for c in cols]

    # default to first two columns if available
    x0 = cols[0] if len(cols) > 0 else None
    y0 = cols[1] if len(cols) > 1 else None

    return options, x0, options, y0


@dash_app.callback(
    [
      Output("main-chart", "figure"),
      Output("data-table", "columns"),
      Output("data-table", "data"),
    ],
    [
      Input("chart-type", "value"),
      Input("x-col",       "value"),
      Input("y-col",       "value"),
    ],
    [State("url", "search")]
)

def update_chart_and_table(chart_type, x_col, y_col, url_search):
    logger.debug(f"Inputs → chart={chart_type!r}, x_col={x_col!r}, y_col={y_col!r}, url={url_search!r}")
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

    logger.debug("DF columns: %r", list(df.columns))
    logger.debug("Requested x_col=%r, y_col=%r", x_col, y_col)

    if chart_type == "bar":
        fig_data = [{"type": "bar",  "x": df[x_col], "y": df[y_col]}]
    elif chart_type == "line":
        fig_data = [{"type": "line", "x": df[x_col], "y": df[y_col]}]
    else:
        fig_data = [{"type": "pie",  "labels": df[x_col], "values": df[y_col]}]

    fig = {
      "data": fig_data,
      "layout": {
        "margin": {"t": 30, "b": 50},
        "xaxis": {"title": x_col, "tickangle": -45},
        "yaxis": {"title": y_col},
      }  
    }

    # prepare table columns+data
    table_columns = [{"name": c, "id": c} for c in df.columns]
    table_data    = df.to_dict("records")

    return fig, table_columns, table_data

if __name__ == "__main__":
    # Use DASH_PORT env var or default to 8050
    port = int(os.getenv("PORT", os.getenv("DASH_PORT", 8050)))
    server.run(host="0.0.0.0", port=port)