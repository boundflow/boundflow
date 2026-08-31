"""HTML for the operator console.

No template engine and no CDN: the console has to work on a laptop with no network
and no build step, so the markup is built here and the CSS/JS are inlined by
`page()`. Everything user-supplied goes through `esc`.

`detail_rows` derives its rows from dataclass fields rather than a hand-written list,
for the same reason `boundflow.cli.output` derives its columns from the data: a field
added to WorkflowInfo shows up on its own instead of being silently dropped by a
renderer nobody remembered to update.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from enum import Enum
from html import escape
from typing import Any

# Lifecycle/workflow states that colour a row. Anything unlisted renders neutral,
# so a state added server-side degrades to plain text instead of raising.
_TONE = {
    "awaiting_approval": "warn",
    "awaiting_input": "warn",
    "interrupted": "bad",
    "halted": "bad",
    "blocked": "bad",
    "disabled": "bad",
    "suspended": "warn",
    "paused": "warn",
    "cooldown": "warn",
    "active": "good",
    "scheduled": "good",
    "completed": "good",
    "successful": "good",
    "failed": "bad",
    "in_progress": "info",
    "invoking": "info",
}


def esc(value: Any) -> str:
    """Render any value as escaped HTML text."""
    return escape(fmt(value), quote=True)


def fmt(value: Any) -> str:
    """Plain-text rendering, shared by esc() and the JSON view."""
    if value is None or value == "":
        return "—"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.6f}".rstrip("0").rstrip(".")
    if isinstance(value, dict):
        return ", ".join(f"{k}={fmt(v)}" for k, v in value.items()) or "—"
    if isinstance(value, (list, tuple)):
        return ", ".join(fmt(v) for v in value) or "—"
    return str(value)


def pill(value: Any) -> str:
    """A state as a coloured pill."""
    text = fmt(value)
    tone = _TONE.get(text.lower(), "")
    return f'<span class="pill {tone}">{escape(text)}</span>'


def detail_rows(obj: Any, skip: tuple[str, ...] = ()) -> str:
    """A definition list of a dataclass's fields, minus `skip`.

    Nested dataclasses (a workflow's config, suspension, pending gates) are rendered
    inline rather than as a repr, since a repr in a table cell is unreadable.
    """
    if obj is None:
        return '<p class="muted">none</p>'
    out = []
    for f in dataclasses.fields(obj):
        if f.name in skip:
            continue
        value = getattr(obj, f.name)
        if dataclasses.is_dataclass(value):
            value = ", ".join(
                f"{sub.name}={fmt(getattr(value, sub.name))}"
                for sub in dataclasses.fields(value)
            )
        label = f.name.replace("_", " ")
        out.append(f"<dt>{escape(label)}</dt><dd>{esc(value)}</dd>")
    return f"<dl>{''.join(out)}</dl>"


def table(headers: list[str], rows: list[list[str]], empty: str = "Nothing here.") -> str:
    """A table of pre-rendered cells. Cells are inserted raw, so callers escape."""
    if not rows:
        return f'<p class="muted">{escape(empty)}</p>'
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


_CSS = """
*{box-sizing:border-box}
body{margin:0;font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
     background:var(--bg);color:var(--fg)}
:root{--bg:#fbfbfa;--fg:#1c1c1a;--muted:#6b6b66;--line:#e3e3df;--card:#fff;
      --good:#1a7f4b;--goodbg:#e6f4ec;--bad:#a32b2b;--badbg:#fbeaea;
      --warn:#8a5a00;--warnbg:#fdf1dc;--info:#1f5c8a;--infobg:#e6f0f7;--accent:#1c1c1a}
@media (prefers-color-scheme:dark){:root{--bg:#161614;--fg:#ecece8;--muted:#9a9a92;
      --line:#2e2e2a;--card:#1d1d1b;--good:#6ee7a8;--goodbg:#12301f;--bad:#f29b9b;
      --badbg:#331616;--warn:#f0c47a;--warnbg:#332614;--info:#93c5e8;--infobg:#132633;
      --accent:#ecece8}}
header{padding:16px 24px;border-bottom:1px solid var(--line);display:flex;
       align-items:baseline;gap:16px;flex-wrap:wrap}
header h1{font-size:15px;margin:0;letter-spacing:.02em}
header .muted{font-size:12px}
main{padding:24px;max-width:1200px;margin:0 auto}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);
   margin:32px 0 12px}
h2:first-child{margin-top:0}
a{color:inherit}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;
      padding:16px;margin-bottom:12px}
.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13px}
th{text-align:left;font-weight:600;color:var(--muted);font-size:11px;
   text-transform:uppercase;letter-spacing:.06em;padding:8px 12px 8px 0;
   border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:10px 12px 10px 0;border-bottom:1px solid var(--line);vertical-align:top}
tr:last-child td{border-bottom:none}
.pill{display:inline-block;padding:1px 8px;border-radius:99px;font-size:12px;
      background:var(--line)}
.pill.good{background:var(--goodbg);color:var(--good)}
.pill.bad{background:var(--badbg);color:var(--bad)}
.pill.warn{background:var(--warnbg);color:var(--warn)}
.pill.info{background:var(--infobg);color:var(--info)}
.muted{color:var(--muted)}
code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
dl{display:grid;grid-template-columns:minmax(140px,auto) 1fr;gap:6px 20px;margin:0}
dt{color:var(--muted);font-size:12px}
dd{margin:0;word-break:break-word}
form{display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;margin-top:12px}
label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted)}
input[type=text]{padding:6px 8px;border:1px solid var(--line);border-radius:6px;
                 background:var(--bg);color:var(--fg);font:inherit;min-width:200px}
button{padding:7px 14px;border:1px solid var(--line);border-radius:6px;
       background:var(--accent);color:var(--bg);font:inherit;font-weight:600;
       cursor:pointer}
button.ghost{background:transparent;color:var(--fg)}
button:hover{opacity:.85}
.err{background:var(--badbg);color:var(--bad);padding:10px 14px;border-radius:6px;
     margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.stat b{display:block;font-size:20px;font-weight:600}
.stat span{font-size:11px;color:var(--muted);text-transform:uppercase;
           letter-spacing:.06em}
"""

# Reloads the fleet fragment in place so a long-lived tab tracks the fleet without
# the operator refreshing. Left alone while a form has focus, so a poll can't wipe
# a half-typed reason out from under someone.
_JS = """
(function(){
 var el=document.getElementById('fleet');if(!el)return;
 setInterval(function(){
  var a=document.activeElement;
  if(a&&(a.tagName==='INPUT'||a.tagName==='BUTTON'))return;
  fetch('/fragment/fleet').then(function(r){return r.ok?r.text():null})
   .then(function(t){if(t!==null)el.innerHTML=t}).catch(function(){});
 },4000);
})();
"""


def page(title: str, body: str, *, server: str, error: str = "") -> str:
    """Wrap body content in the full document."""
    banner = f'<div class="err">{escape(error)}</div>' if error else ""
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>{escape(title)} · BoundFlow</title><style>{_CSS}</style></head><body>"
        "<header><h1><a href='/' style='text-decoration:none'>BoundFlow</a></h1>"
        f"<span class='muted mono'>{escape(server)}</span>"
        "<span class='muted'>local operator console</span></header>"
        f"<main>{banner}{body}</main><script>{_JS}</script></body></html>"
    )
