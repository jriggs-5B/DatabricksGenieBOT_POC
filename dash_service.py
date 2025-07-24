import os
import json
import requests
import pandas as pd
from flask import Flask, request
from dash import Dash, dcc, html, Input, Output

# URL where your bot is listening (override via env var if needed)
BOT_URL = os.getenv("BOT_URL", "http://bot-service:8181")

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
    html.H2("Genie Results Chart"),
    dcc.Dropdown(
        id="chart-type",
        options=[
            {"label": "Bar",  "value": "bar"},
            {"label": "Line", "value": "line"},
            {"label": "Pie",  "value": "pie"},
        ],
        value="bar",
        clearable=False,
    ),
    dcc.Graph(id="main-chart"),
])

@dash_app.callback(
    Output("main-chart", "figure"),
    Input("chart-type", "value"),
)
def update_chart(chart_type):
    # 1) Fetch the full Genie JSON from your bot
    session = request.args.get("session")
    resp = requests.get(f"{BOT_URL}/download_json", params={"session": session})
    if resp.status_code != 200:
        return {"data": []}

    answer_json = resp.json()

    # 2) Build DataFrame from the returned rows & schema
    cols = [c["name"] for c in answer_json["statement_response"]["manifest"]["schema"]["columns"]]
    rows = answer_json["statement_response"]["result"]["data_array"]
    df   = pd.DataFrame(rows, columns=cols)

    # 3) Render the selected chart type
    x, y = df.columns[:2]
    if chart_type == "bar":
        fig = {"data": [{"type": "bar",   "x": df[x], "y": df[y]}]}
    elif chart_type == "line":
        fig = {"data": [{"type": "line",  "x": df[x], "y": df[y]}]}
    else:
        fig = {"data": [{"type": "pie",   "labels": df[x], "values": df[y]}]}

    return fig

if __name__ == "__main__":
    # Use DASH_PORT env var or default to 8050
    port = int(os.getenv("DASH_PORT", 8050))
    server.run(host="0.0.0.0", port=port)