"""
Supervisor agent for concise subjects + executive summaries.

Usage:
    from supervisor import supervisor_summarize

    sup = supervisor_summarize(answer_json)
    # sup: {"subject": str, "summary_html": str, "summary_text": str}

Environment:
    OPENAI_API_KEY          (required)
    LLM_SUPERVISOR_ENABLED  (default: "1")
    LLM_MODEL               (default: "gpt-5")
    LLM_TIMEOUT_S           (default: "15")
"""

from __future__ import annotations
from typing import Any, Dict, List
import os
import json
import html
import logging
import time

logger = logging.getLogger(__name__)

# --- Config ---
LLM_ENABLED   = os.getenv("LLM_SUPERVISOR_ENABLED", "1") == "1"
LLM_MODEL     = os.getenv("LLM_MODEL", "gpt-5")
LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "15"))
ORG_NAME      = os.getenv("ORG_NAME", "Five Below")

# Optional: install via `pip install openai>=1.0.0`
try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # handled at call time

import inspect
from openai import OpenAI
_client = OpenAI()
import openai as _pkg
logger.warning("SUP: chat.completions.create signature=%s",
               inspect.signature(_client.chat.completions.create))

def _first_json_object(text: str) -> str:
    """Return the first balanced {...} JSON object from a string, or ''."""
    if not text:
        return ""
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    return text[start:i+1]
    return ""

# ---------------- Sanitization ----------------
# Keep HTML strictly limited; strip attributes and unknown tags
_ALLOWED_TAGS = {"p", "ul", "li", "strong", "em"}

def _empty_overrides() -> dict[str, str]:
    # No subject override and no summary blocks.
    return {"subject": "", "summary_html": "", "summary_text": ""}

def _sanitize_html(s: str) -> str:
    if not s:
        return ""
    out: List[str] = []
    i, N = 0, len(s)
    while i < N:
        ch = s[i]
        if ch != "<":
            out.append(html.escape(ch))
            i += 1
            continue
        j = s.find(">", i + 1)
        if j == -1:
            out.append(html.escape(s[i:]))
            break
        token = s[i + 1 : j].strip()
        is_close = token.startswith("/")
        tag = (token[1:] if is_close else token).split()[0].lower() if token else ""
        if tag in _ALLOWED_TAGS:
            out.append(f"</{tag}>" if is_close else f"<{tag}>")
        # else drop the tag entirely
        i = j + 1
    return "".join(out)


# ---------------- Packing dataset ----------------
def _pack_dataset(answer_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reduce the payload to what the model needs (schema + small sample + truncation note).
    """
    stmt = (answer_json.get("statement_response") or {})
    result = (stmt.get("result") or {})
    rows = (result.get("data_array") or [])[:20]
    cols = (((stmt.get("manifest") or {}).get("schema") or {}).get("columns") or [])
    shown = answer_json.get("shown_rows")
    total = answer_json.get("db_total_rows")
    csv_rows = answer_json.get("csv_rows")  # used only for the note, not raw data

    trunc_parts: List[str] = []
    if isinstance(shown, int) and isinstance(total, int) and total > shown:
        trunc_parts.append(f"Showing first {shown:,} of {total:,} rows")
    if isinstance(csv_rows, int) and isinstance(total, int) and csv_rows < total:
        trunc_parts.append(f"CSV contains first {csv_rows:,} rows")
    truncation = " • ".join(trunc_parts)

    return {
        "org": ORG_NAME,
        "description": (answer_json.get("query_description") or "").strip(),
        "columns": [c.get("name") for c in cols],
        "rows_sample": rows,
        "truncation": truncation
    }


# ---------------- Prompt + Schema ----------------
_SUPERVISOR_SYSTEM = (
    "You are a retail supply chain analyst writing email subjects and concise executive summaries "
    "from small tabular samples. Your audience is senior leadership.\n\n"
    "Output rules:\n"
    "- Subject: <= 120 chars, business-friendly, specific.\n"
    "- Summary: 2–5 sentences, plain business language (orders, containers, on-time, received in full, lead time). "
    "Do not include SQL, code, or IDs/PII. Mention truncation note if provided. "
    "Do not invent totals beyond what is visible in the sample.\n"
    "- If no rows: subject 'Five Below – No matching results' and a one-line summary.\n"
)

_SUPERVISOR_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string", "maxLength": 120},
        "summary_html": {
            "type": "string",
            "description": "HTML limited to <p>, <ul>, <li>, <strong>, <em>."
        },
        "summary_text": {"type": "string"}
    },
    "required": ["subject", "summary_html", "summary_text"],
    "additionalProperties": False
}


# ---------------- Public entry point ----------------
def supervisor_summarize(answer_json: Dict[str, Any]) -> Dict[str, str]:
    """
    Returns dict with keys: 'subject', 'summary_html', 'summary_text'.

    - Uses OpenAI Structured Outputs when LLM_SUPERVISOR_ENABLED=1 (default).
    - Gracefully degrades to a deterministic baseline if LLM not available or fails.
    """
    # If Genie sent only a text explanation (no table), return that directly.
    text_only = (answer_json.get("message") or "").strip()

    # If LLM is disabled, we do a strict no-op override
    if not LLM_ENABLED:
        return _empty_overrides()

    if OpenAI is None:
        logger.warning("OpenAI SDK missing; using no-op overrides.")
        return _empty_overrides()

    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))     
        content = _pack_dataset(answer_json)
        prompt = (
            _SUPERVISOR_SYSTEM
            + "\n\n"
            + "You will receive a JSON payload with dataset context.\n"
            + "Return ONLY a JSON object with EXACTLY these keys:\n"
            + "  - subject (string, <= 120 chars)\n"
            + "  - summary_html (string; allowed tags: <p>, <ul>, <li>, <strong>, <em>)\n"
            + "  - summary_text (string)\n"
            + "No prose, no code fences, no extra keys.\n\n"
            + "PAYLOAD:\n"
            + json.dumps(content, ensure_ascii=False)
        )

        # 1) Responses API with a single string input (no content parts, no response_format)
        resp = client.responses.create(
            model=LLM_MODEL,
            input=prompt,
            max_output_tokens=800,   # supported on Responses API
            # temperature omitted (defaults are fine in your runtime)
        )

        # 2) Collect response text (covers both modern and older SDK shapes)
        text_out = ""
        out = getattr(resp, "output", None)

        if isinstance(out, list):
            # Modern 1.x often returns a list of items with messages/content blocks
            parts = []
            for item in out:
                if getattr(item, "type", "") == "message":
                    for c in getattr(item, "content", []) or []:
                        t = getattr(c, "type", "")
                        if t in ("output_text", "text"):
                            parts.append(getattr(c, "text", "") or "")
            text_out = "".join(parts).strip()

        # Fallbacks: older shapes / convenience properties
        if not text_out:
            # Some builds expose convenience properties
            text_out = getattr(resp, "output_text", "") or getattr(resp, "content", "") or ""
            text_out = (text_out or "").strip()

        # 3) Parse first JSON object from the text
        blob = _first_json_object(text_out)
        if not blob:
            logger.warning("SUP: no JSON found in Responses text (len=%s)", len(text_out))
            raise ValueError("empty content")

        parsed = json.loads(blob)

        # 4) Sanitize + validate
        subject = (parsed.get("subject") or "").strip()[:120]
        summary_html = _sanitize_html(parsed.get("summary_html") or "")
        summary_text = (parsed.get("summary_text") or "").strip()

        if not subject or not (summary_html and summary_text):
            logger.warning("SUP: parsed JSON missing keys: %s",
                        {k: parsed.get(k) for k in ("subject","summary_html","summary_text")})
            raise ValueError("Supervisor output missing required fields")

        return {"subject": subject, "summary_html": summary_html, "summary_text": summary_text}

    except Exception as e:
        logger.exception("LLM supervisor failed; using no-op overrides. Error: %s", e)
        return _empty_overrides()
    

def supervisor_insights(answer_json: dict) -> dict:
    """
    Generate business insights from Genie results.
    Accepts the full answer_json for flexibility.
    Returns { "insights_html": str, "insights_text": str }
    """
    try:
        # Try the "direct" fields first (if process_query_results or other code sets them)
        data_sample = answer_json.get("data_array")
        schema = answer_json.get("schema")

        # Otherwise drill into the Genie shape
        if not data_sample:
            data_sample = (
                answer_json.get("statement_response", {})
                .get("result", {})
                .get("data_array", [])
            )
        if not schema:
            schema = (
                answer_json.get("statement_response", {})
                .get("manifest", {})
                .get("schema", {})
                .get("columns", [])
            )

        if not data_sample or not schema:
            return {"insights_html": "", "insights_text": ""}

        prompt = f"""
        You are an experienced retail supply chain analyst. Your audience is inventory planners, buyers, supply chain analysts, 
        and merchandise operations analysts who use this tool to locate product in the supply chain and determine where there 
        are potential issues.

        Your task is to review the provided data and generate 2–4 concise, high-value insights that go beyond description:
        - Identify potential supply chain risks, delays, shortages, or imbalances.
        - Highlight notable contributors (e.g., top vendors, DCs, SKUs, origins) driving issues or concentration of volume.
        - Suggest practical next steps, possible root causes, or follow-up questions to explore.

        Data schema: {schema}
        Data sample (first 20 rows max): {data_sample[:20]}

        Guidelines:
        - Use bullet points.
        - Business-friendly, plain English.
        - Each point should either highlight a potential issue OR propose a potential action/question.
        - Keep strictly grounded in the data provided (no assumptions or hallucinations).
        - Do not include PII.
        """
        resp = _client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": "You generate business insights for retail supply chain data."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=250,
        )

        text_out = resp.choices[0].message.content.strip()
        html_out = "<ul>" + "".join(
            [f"<li>{line.strip()}</li>" for line in text_out.split("\n") if line.strip()]
        ) + "</ul>"

        return {"insights_html": html_out, "insights_text": text_out}

    except Exception as e:
        return {"insights_html": "", "insights_text": f"[Supervisor error: {e}]"}