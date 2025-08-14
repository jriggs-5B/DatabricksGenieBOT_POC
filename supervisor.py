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

        messages = [
            {"role": "system", "content": _SUPERVISOR_SYSTEM},
            {"role": "user", "content": json.dumps(content, ensure_ascii=False)}
        ]

        resp = client.responses.create(
            model=LLM_MODEL,
            input=messages,
            temperature=0.2,
            max_output_tokens=800,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "SupervisorSummary",
                    "schema": _SUPERVISOR_SCHEMA,
                    "strict": True,
                }
            },
            timeout=LLM_TIMEOUT_S,
        )

        # Prefer parsed output if available
        parsed = getattr(getattr(resp, "output", None), "parsed", None)
        if parsed is None:
            # Fallback: try to locate text content and parse it
            text_blob = None
            out = getattr(resp, "output", None) or []
            for item in out:
                if getattr(item, "type", "") == "message":
                    for c in (item.content or []):
                        if getattr(c, "type", "") == "output_text":
                            text_blob = c.text
                            break
                if text_blob:
                    break
            parsed = json.loads(text_blob or "{}")

        # Final sanitize & trim
        subj = (parsed.get("subject") or "").strip()[:120]
        s_html = _sanitize_html(parsed.get("summary_html") or "")
        s_text = (parsed.get("summary_text") or "").strip()

        if not subj or not (s_html and s_text):
            raise ValueError("Structured output missing required fields")

        return {"subject": subj, "summary_html": s_html, "summary_text": s_text}

    except Exception as e:
        logger.exception("LLM supervisor failed; using no-op overrides. Error: %s", e)
        return _empty_overrides()