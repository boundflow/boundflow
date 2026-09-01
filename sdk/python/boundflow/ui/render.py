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


def _nested(value: Any) -> str:
    """A dataclass or dict as its own definition list, nested inside a <dd>.

    Comma-joining these produced a repr — `version=1, invoke_timeout_seconds=300,
    triggerable=yes` — which is unreadable exactly where the detail matters, on a
    workflow's config and its suspension.
    """
    if dataclasses.is_dataclass(value):
        items = [(f.name, getattr(value, f.name)) for f in dataclasses.fields(value)]
    else:
        items = list(value.items())
    if not items:
        return '<span class="muted">—</span>'
    rows = "".join(
        f"<dt>{escape(str(k).replace('_', ' '))}</dt><dd>{esc(v)}</dd>"
        for k, v in items
    )
    return f'<dl class="sub">{rows}</dl>'


def detail_rows(obj: Any, skip: tuple[str, ...] = ()) -> str:
    """A definition list of a dataclass's fields, minus `skip`.

    Nested dataclasses and non-empty dicts become their own indented lists rather
    than one flattened line.
    """
    if obj is None:
        return '<p class="muted">none</p>'
    out = []
    for f in dataclasses.fields(obj):
        if f.name in skip:
            continue
        value = getattr(obj, f.name)
        if dataclasses.is_dataclass(value) or (isinstance(value, dict) and value):
            cell = _nested(value)
        else:
            cell = esc(value)
        out.append(f"<dt>{escape(f.name.replace('_', ' '))}</dt><dd>{cell}</dd>")
    return f"<dl>{''.join(out)}</dl>"


def table(headers: list[str], rows: list[list[str]], empty: str = "Nothing here.",
          *, raw_headers: bool = False) -> str:
    """A table of pre-rendered cells. Cells are inserted raw, so callers escape.

    raw_headers lets a caller pass markup (a sort link); it escapes them itself.
    """
    if not rows:
        return f'<p class="muted">{escape(empty)}</p>'
    head = "".join(f"<th>{h if raw_headers else escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


_CSS = """
*{box-sizing:border-box}
:root{--bg:#0f1115;--panel:#151922;--fg:#d7dce5;--dim:#6c7686;--line:#232936;
      --sel:#1c2331;--acc:#5eead4;--on-acc:#0f1115;
      --good:#4ade80;--bad:#f87171;--warn:#fbbf24;--info:#60a5fa}
@media (prefers-color-scheme:light){
 :root{--bg:#fcfcfb;--panel:#f2f2ef;--fg:#1c1f26;--dim:#6b7280;--line:#e0e0da;
       --sel:#e8e8e3;--acc:#0d7d6b;--on-acc:#fff;
       --good:#15803d;--bad:#b91c1c;--warn:#a16207;--info:#1d4ed8}}
body{margin:0;background:var(--bg);color:var(--fg);
     font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
a{color:inherit;text-decoration:none}
.wrap{display:grid;grid-template-columns:190px 1fr;min-height:100vh}
aside{background:var(--panel);border-right:1px solid var(--line);padding:14px 0;
      display:flex;flex-direction:column}
aside .brand{padding:0 16px 14px;font-size:14px;font-weight:700;letter-spacing:.06em;
             color:var(--acc);text-transform:uppercase}
aside nav a{display:flex;justify-content:space-between;padding:6px 16px;color:var(--dim)}
aside nav a:hover{background:var(--sel);color:var(--fg)}
aside nav a.on{color:var(--fg);border-left:2px solid var(--acc);
               background:var(--sel);padding-left:14px}
aside nav a b{color:var(--acc);font-weight:600}
aside .foot{margin-top:auto;padding:12px 16px 0;border-top:1px solid var(--line);
            color:var(--dim);font-size:11px;line-height:1.7;word-break:break-all}
main{display:flex;flex-direction:column;min-width:0}
.bar{display:flex;align-items:center;gap:10px;padding:9px 16px;
     border-bottom:1px solid var(--line);background:var(--panel)}
.bar .ctx{color:var(--acc);font-weight:600;white-space:nowrap}
.bar input{flex:1;background:var(--bg);border:1px solid var(--line);border-radius:3px;
           color:var(--fg);font:inherit;padding:4px 8px;min-width:0}
.bar input::placeholder{color:var(--dim)}
.body{padding:14px 16px;overflow:auto;flex:1}
h2{font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:var(--dim);
   margin:18px 0 8px;font-weight:600}
h2:first-child{margin-top:0}
h3{font-size:13px;margin:14px 0 4px;font-weight:600}
table{border-collapse:collapse;width:100%}
th a{color:inherit}
th a:hover{color:var(--acc)}
th{text-align:left;color:var(--dim);font-size:10px;letter-spacing:.14em;
   text-transform:uppercase;font-weight:600;padding:0 14px 6px 0;
   border-bottom:1px solid var(--line);white-space:nowrap}
td{padding:3px 14px 3px 0;border-bottom:1px solid var(--line);white-space:nowrap;
   vertical-align:top}
tbody tr:hover{background:var(--sel)}
tbody tr.cur{background:var(--sel);box-shadow:inset 2px 0 0 var(--acc)}
.pill{font-weight:600}
.pill:before{content:"\\25cf  ";font-size:9px;vertical-align:1px}
.pill.good{color:var(--good)}.pill.bad{color:var(--bad)}
.pill.warn{color:var(--warn)}.pill.info{color:var(--info)}
.muted,.dim{color:var(--dim)}
.card{background:var(--panel);border:1px solid var(--line);
      border-left:2px solid var(--warn);padding:12px 14px;margin-bottom:10px}
dl{display:grid;grid-template-columns:minmax(110px,auto) 1fr;gap:3px 18px;margin:0}
dt{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.08em}
dd{margin:0;word-break:break-word;white-space:normal}
dl.sub{grid-column:1/-1;margin:2px 0 6px;padding-left:12px;gap:2px 14px;
       border-left:1px solid var(--line)}
dl.sub dt{font-size:10px;opacity:.85}
dl.sub dd{font-size:12px}
dd:has(dl.sub){grid-column:1/-1}
form{display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;margin-top:10px}
label{display:flex;flex-direction:column;gap:3px;font-size:10px;color:var(--dim);
      text-transform:uppercase;letter-spacing:.08em}
input[type=text]{background:var(--bg);border:1px solid var(--line);border-radius:3px;
                 color:var(--fg);font:inherit;padding:5px 8px;min-width:190px}
button{background:var(--acc);color:var(--on-acc);border:0;border-radius:3px;
       padding:6px 14px;font:inherit;font-weight:700;cursor:pointer;
       text-transform:uppercase;letter-spacing:.06em;font-size:11px}
button.ghost{background:transparent;color:var(--fg);border:1px solid var(--line)}
button:hover{filter:brightness(1.12)}
.err{border-left:2px solid var(--bad);background:var(--panel);color:var(--bad);
     padding:8px 12px;margin-bottom:12px}
.grid{display:flex;border:1px solid var(--line);margin-bottom:12px;
      background:var(--panel);flex-wrap:wrap}
.stat{padding:9px 18px;border-right:1px solid var(--line)}
.stat b{display:block;font-size:17px;color:var(--acc)}
.stat span{font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em}
.dhead{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;
       flex-wrap:wrap;margin-bottom:12px}
.dhead h2{margin:0}
.actions{display:flex;gap:8px;align-items:flex-start}
.actions form{margin:0}
label.check{flex-direction:row;align-items:center;gap:6px;text-transform:none;
  letter-spacing:0;font-size:12px}
.callout{border-left:2px solid var(--warn);background:var(--panel);padding:10px 14px;
  margin-bottom:12px}
.callout.bad{border-left-color:var(--bad)}
.callout strong{display:block;font-size:13px;margin-bottom:2px}
.callout.bad strong{color:var(--bad)}
.callout.warn strong{color:var(--warn)}
.callout p{margin:0;color:var(--dim);font-size:12px}
.callout code{color:var(--fg)}
.block,.danger{border:1px solid var(--line);padding:12px 14px;margin-top:28px}
.danger{border-color:var(--bad)}
.block.warn{border-color:var(--warn)}
.block.warn strong{color:var(--warn)}
.block.warn button{background:transparent;color:var(--warn);
  border:1px solid var(--warn)}
.block+.danger{margin-top:12px}
.block strong,.danger strong{display:block;font-size:11px;text-transform:uppercase;
  letter-spacing:.12em;margin-bottom:6px;color:var(--dim)}
.danger strong{color:var(--bad)}
.block p,.danger p{margin:0 0 4px;color:var(--dim);font-size:12px}
.danger button{background:transparent;color:var(--bad);border:1px solid var(--bad)}
.status{border-top:1px solid var(--line);background:var(--panel);padding:6px 16px;
        color:var(--dim);font-size:11px;display:flex;gap:16px;flex-wrap:wrap}
.status kbd{color:var(--acc);font-weight:700;font-family:inherit}
.scroll{overflow-x:auto}
td details summary{cursor:pointer;list-style:none}
td details summary::-webkit-details-marker{display:none}
td details summary:before{content:"\\25b8  ";color:var(--dim)}
td details[open] summary:before{content:"\\25be  "}
td details[open]{white-space:normal}
td details dl{margin-top:6px;padding-left:12px;border-left:1px solid var(--line)}
@media (max-width:720px){.wrap{grid-template-columns:1fr}
 aside{flex-direction:row;align-items:center;gap:8px;padding:8px 12px;overflow-x:auto}
 aside .brand{padding:0}aside nav{display:flex;gap:4px}aside .foot{display:none}
 aside nav a{padding:4px 8px;gap:6px}}
"""

# Cursor + filter + the fleet poll. The cursor is re-marked after a poll swap, so a
# refresh under the operator's feet doesn't lose their place in the list.
_JS = """
(function(){
 function rows(){return Array.prototype.slice.call(
   document.querySelectorAll('tbody tr'))}
 var i=-1;
 function mark(){rows().forEach(function(r,n){r.classList.toggle('cur',n===i)});
  var r=rows()[i];if(r)r.scrollIntoView({block:'nearest'})}
 function open(){var r=rows()[i];if(!r)return;var a=r.querySelector('a');
  if(a)location.href=a.getAttribute('href')}
 function filter(q){q=q.toLowerCase();rows().forEach(function(r){
  r.style.display=r.innerText.toLowerCase().indexOf(q)>-1?'':'none'})}
 var f=document.getElementById('f');
 if(f)f.addEventListener('input',function(){filter(f.value)});
 document.addEventListener('keydown',function(e){
  if(e.metaKey||e.ctrlKey||e.altKey)return;
  if(e.target.tagName==='INPUT'){
   if(e.key==='Escape'){e.target.value='';filter('');e.target.blur()}return}
  if(e.key==='j'){i=Math.min(i+1,rows().length-1);mark();e.preventDefault()}
  else if(e.key==='k'){i=Math.max(i-1,0);mark();e.preventDefault()}
  else if(e.key==='Enter'){open();e.preventDefault()}
  else if(e.key==='/'){if(f){f.focus();e.preventDefault()}}
  else if(e.key==='g'){location.href='/'}
 });
 var el=document.getElementById('fleet');
 if(el)setInterval(function(){
  var a=document.activeElement;
  if(a&&(a.tagName==='INPUT'||a.tagName==='BUTTON'))return;
  fetch(el.dataset.src||'/fragment/fleet').then(function(r){return r.ok?r.text():null})
   .then(function(t){if(t!==null){el.innerHTML=t;mark();if(f&&f.value)filter(f.value)}})
   .catch(function(){});
 },4000);
})();
"""


def nav_links(items, current: str) -> str:
    """Sidebar entries: (href, label, count | None). A None count renders bare, for
    pages that don't have the fleet loaded."""
    out = []
    for href, label, count in items:
        on = " class='on'" if href == current else ""
        badge = f"<b>{escape(str(count))}</b>" if count is not None else ""
        out.append(f"<a{on} href='{escape(href)}'>{escape(label)}{badge}</a>")
    return "".join(out)


def page(title: str, body: str, *, server: str, error: str = "",
         labels: "Labels | None" = None, nav: str = "", filterable: bool = True) -> str:
    """Wrap body content in the full document."""
    from .labels import DEFAULT
    lb = labels or DEFAULT
    banner = f'<div class="err">{escape(error)}</div>' if error else ""
    search = ("<input id='f' placeholder='/  filter…'>" if filterable
              else "<span class='dim'>&nbsp;</span>")
    keys = (
        "<span><kbd>j</kbd>/<kbd>k</kbd> move</span><span><kbd>enter</kbd> open</span>"
        f"<span><kbd>/</kbd> filter</span><span><kbd>g</kbd> {escape(lb.fleet)}</span>"
        if filterable else f"<span><kbd>g</kbd> {escape(lb.fleet)}</span>"
    )
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>{escape(title)} · {escape(lb.brand)}</title>"
        f"<style>{_CSS}</style></head><body><div class='wrap'>"
        f"<aside><div class='brand'><a href='/'>{escape(lb.brand)}</a></div>"
        f"<nav>{nav}</nav>"
        f"<div class='foot'>{escape(server)}<br>{escape(lb.tagline)}</div></aside>"
        f"<main><div class='bar'><span class='ctx'>{escape(title)}</span>{search}</div>"
        f"<div class='body'>{banner}{body}</div>"
        f"<div class='status'>{keys}<span>auto-refresh 4s</span></div>"
        f"</main></div><script>{_JS}</script></body></html>"
    )
