from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union, Dict, Any, List

import pandas as pd

PathLike = Union[str, Path]


def _pct01(x: Optional[float]) -> str:
    if x is None:
        return ""
    try:
        return f"{x * 100:.2f}%"
    except Exception:
        return ""


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _html_escape(s: Any) -> str:
    s = "" if s is None else str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _preview_table_html(df: pd.DataFrame, n: int = 10) -> str:
    head = df.head(n)
    cols = list(head.columns)

    thead = "".join(f"<th>{_html_escape(c)}</th>" for c in cols)

    body_rows = []
    for _, row in head.iterrows():
        tds = "".join(f"<td>{_html_escape(row[c])}</td>" for c in cols)
        body_rows.append(f"<tr>{tds}</tr>")

    tbody = "".join(body_rows) if body_rows else ""
    shown = min(n, len(df))

    return f"""
    <div class="preview-block">
      <div class="preview-title">Data Preview (Top {shown} rows)</div>
      <div class="preview-wrap">
        <table class="preview-table">
          <thead><tr>{thead}</tr></thead>
          <tbody>{tbody}</tbody>
        </table>
      </div>
    </div>
    """


def _bar_html(
    null_pct: float,
    distinct_pct_total: float,
    non_distinct_pct_total: float,
    *,
    tooltip: str,
) -> str:
    """
    IICS-like value distribution bar.
    total width = 100% = distinct + non-distinct + null
    """

    def clamp(v: float) -> float:
        return max(0.0, min(1.0, v))

    z = clamp(null_pct)
    d = clamp(distinct_pct_total)
    n = clamp(non_distinct_pct_total)

    total = z + d + n
    if total > 0:
        z, d, n = z / total, d / total, n / total

    d_end = d * 100.0
    n_end = (d + n) * 100.0

    return f"""
    <div class="bar" title="{_html_escape(tooltip)}">
      <div class="bar-inner" style="
        background: linear-gradient(
          to right,
          var(--c-distinct) 0%,
          var(--c-distinct) {d_end:.2f}%,
          var(--c-nondistinct) {d_end:.2f}%,
          var(--c-nondistinct) {n_end:.2f}%,
          var(--c-null) {n_end:.2f}%,
          var(--c-null) 100%
        );
      "></div>
    </div>
    """


def _json_for_attr(obj: Any) -> str:
    """
    JSON -> safe string for embedding in HTML attribute.
    We escape &, <, >, ", ' via _html_escape so attribute stays valid.
    """
    s = json.dumps(obj, ensure_ascii=False, indent=None, separators=(",", ":"))
    return _html_escape(s)


def _json_pretty(obj: Any) -> str:
    """
    Pretty JSON for default display when needed.
    """
    return json.dumps(obj, ensure_ascii=False, indent=2)


def profile_to_html(
    df: pd.DataFrame,
    out_html: PathLike,
    *,
    dataset_name: str = "dataset",
    sample_rows: Optional[int] = None,
    title: Optional[str] = None,
    preview_rows: int = 10,
) -> str:
    """
    Generate an IICS-like "Columns and Rules" HTML report.

    New feature:
    - Click a row -> show underlying JSON (column-level profiling dict) below the table.
    """
    from .profiling import profile_dataframe

    prof = profile_dataframe(df, dataset_name=dataset_name, sample_rows=sample_rows)
    overview: Dict[str, Any] = prof.get("overview", {}) or []
    iics_rows: List[Dict[str, Any]] = prof.get("iics_table", []) or []
    cols_detailed: List[Dict[str, Any]] = prof.get("columns", []) or []

    # Map: col_name -> detailed dict
    detail_by_name: Dict[str, Dict[str, Any]] = {
        c.get("name"): c for c in cols_detailed if c.get("name") is not None
    }

    preview_html = _preview_table_html(df, n=preview_rows) if preview_rows and preview_rows > 0 else ""

    rows_html: List[str] = []
    first_click_payload: Optional[Dict[str, Any]] = None
    first_click_name: Optional[str] = None

    for idx, r in enumerate(iics_rows):
        col_name = str(r.get("column_name", ""))

        null_pct = _safe_float(r.get("null_pct")) or 0.0
        not_null_pct = _safe_float(r.get("not_null_pct")) or max(0.0, 1.0 - null_pct)

        distinct_count = _safe_int(r.get("distinct_count"), 0)

        row_count = _safe_int(overview.get("row_count"), 0) if isinstance(overview, dict) else 0
        null_count = int(round(null_pct * row_count)) if row_count else 0
        not_null_count = max(0, row_count - null_count)

        non_distinct_count = max(0, not_null_count - distinct_count)

        distinct_pct_nn = (distinct_count / not_null_count) if not_null_count > 0 else 0.0
        non_distinct_pct_nn = 1.0 - distinct_pct_nn if not_null_count > 0 else 0.0

        distinct_pct_total = distinct_pct_nn * not_null_pct
        non_distinct_pct_total = non_distinct_pct_nn * not_null_pct

        tooltip = (
            f"Distinct: {distinct_pct_total * 100:.2f}% ({distinct_count}/{row_count}) | "
            f"Non-distinct: {non_distinct_pct_total * 100:.2f}% ({non_distinct_count}/{row_count}) | "
            f"Null: {null_pct * 100:.2f}% ({null_count}/{row_count})"
        )

        bar = _bar_html(
            null_pct=null_pct,
            distinct_pct_total=distinct_pct_total,
            non_distinct_pct_total=non_distinct_pct_total,
            tooltip=tooltip,
        )

        d = detail_by_name.get(col_name, {}) or {}
        min_len = d.get("min_len", "")
        max_len = d.get("max_len", "")
        profile_type = d.get("profile_type", r.get("suggested_iics_type", "")) or ""

        pattern_summary = d.get("pattern_summary", []) or []
        pattern_count = len(pattern_summary)
        top_pattern_pct = _pct01(pattern_summary[0].get("pct")) if pattern_count > 0 else ""

        # Payload JSON shown when clicking this row: prefer detailed dict; fallback to r
        payload_obj = d if d else {"column_name": col_name, **r}
        payload_attr = _json_for_attr(payload_obj)

        if first_click_payload is None:
            first_click_payload = payload_obj
            first_click_name = col_name

        rows_html.append(
            f"""
            <tr class="data-row" data-col="{_html_escape(col_name)}" data-json="{payload_attr}">
              <td class="col-name">{_html_escape(col_name)}</td>
              <td class="dist-cell">{bar}</td>
              <td class="num">{_pct01(null_pct)}</td>
              <td class="num">{null_count}</td>
              <td class="num">{_pct01(distinct_pct_nn)}</td>
              <td class="num">{distinct_count}</td>
              <td class="num">{_pct01(non_distinct_pct_nn)}</td>
              <td class="num">{non_distinct_count}</td>
              <td class="num">{pattern_count if pattern_count else ""}</td>
              <td class="num">{top_pattern_pct}</td>
              <td class="num">{min_len if min_len is not None else ""}</td>
              <td class="num">{max_len if max_len is not None else ""}</td>
              <td class="pill">{_html_escape(profile_type)}</td>
            </tr>
            """
        )

    report_title = title or "Columns and Rules"
    dataset_label = dataset_name

    # Default panel content (show first column JSON by default)
    default_panel_title = _html_escape(first_click_name or "")
    default_panel_json = _html_escape(_json_pretty(first_click_payload or {}))

    # overview might not be dict if upstream changes; guard
    ov_row_count = _safe_int(overview.get("row_count"), 0) if isinstance(overview, dict) else 0
    ov_col_count = _safe_int(overview.get("column_count"), 0) if isinstance(overview, dict) else 0
    ov_miss_rate = _pct01(_safe_float(overview.get("missing_rate_overall")) or 0.0) if isinstance(overview, dict) else ""

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>PyDQ - {dataset_label}</title>
<style>
  :root {{
    --c-distinct: #1aa6a6;
    --c-nondistinct: #0b1b26;
    --c-null: #111111;

    --bg: #f3f4f6;
    --panel: #ffffff;
    --grid: #d9dee5;
    --text: #111827;
    --muted: #6b7280;
    --header: #f9fafb;
    --select: #dbeafe; /* selection highlight */
  }}

  body {{
    font-family: Segoe UI, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
  }}

  .topbar {{
    background: var(--panel);
    border-bottom: 1px solid var(--grid);
    padding: 10px 14px;
  }}

  .tabs {{
    display: flex;
    gap: 14px;
    font-size: 13px;
    align-items: center;
  }}

  .tab {{
    color: var(--muted);
    padding: 6px 8px;
    border-radius: 6px;
    user-select: none;
  }}

  .tab.active {{
    color: var(--text);
    font-weight: 600;
    background: var(--header);
    border: 1px solid var(--grid);
  }}

  .container {{
    padding: 12px 14px 18px 14px;
  }}

  .toolbar {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0 10px 0;
  }}

  .title {{
    font-size: 13px;
    font-weight: 600;
  }}

  .meta {{
    font-size: 12px;
    color: var(--muted);
    margin-left: 10px;
  }}

  .spacer {{
    flex: 1;
  }}

  .find {{
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  .find input {{
    padding: 6px 10px;
    border: 1px solid var(--grid);
    border-radius: 6px;
    width: 280px;
    background: white;
    font-size: 12px;
  }}

  /* ---- Data preview styles ---- */
  .preview-block {{
    background: var(--panel);
    border: 1px solid var(--grid);
    border-radius: 8px;
    padding: 10px 10px 8px 10px;
    margin-bottom: 10px;
  }}

  .preview-title {{
    font-size: 12px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 8px;
  }}

  .preview-wrap {{
    overflow: auto;
    max-height: 220px;
    border: 1px solid var(--grid);
    border-radius: 6px;
  }}

  .preview-table {{
    width: 100%;
    border-collapse: collapse;
    background: #ffffff;
  }}

  .preview-table thead th {{
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--header);
    border-bottom: 1px solid var(--grid);
    font-size: 12px;
    padding: 6px 8px;
    white-space: nowrap;
    text-align: left;
  }}

  .preview-table tbody td {{
    border-top: 1px solid var(--grid);
    font-size: 12px;
    padding: 6px 8px;
    white-space: nowrap;
  }}

  /* ---- Main table ---- */
  table {{
    width: 100%;
    border-collapse: collapse;
    background: var(--panel);
    border: 1px solid var(--grid);
  }}

  thead th {{
    position: sticky;
    top: 0;
    z-index: 2;
    background: var(--header);
    border-bottom: 1px solid var(--grid);
    font-size: 12px;
    text-align: left;
    padding: 8px 10px;
    white-space: nowrap;
  }}

  tbody td {{
    border-top: 1px solid var(--grid);
    padding: 8px 10px;
    font-size: 12px;
    vertical-align: middle;
  }}

  tbody tr:hover {{
    background: #eef2f7;
  }}

  tr.data-row {{
    cursor: pointer;
  }}

  tr.data-row.selected {{
    background: var(--select) !important;
  }}

  .num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
  }}

  .col-name {{
    font-weight: 600;
    color: #0f172a;
  }}

  .dist-cell {{
    min-width: 190px;
  }}

  .bar {{
    width: 170px;
    height: 12px;
    border: 1px solid var(--grid);
    border-radius: 3px;
    background: #ffffff;
    overflow: hidden;
  }}

  .bar-inner {{
    width: 100%;
    height: 100%;
  }}

  .pill span {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    background: #eeeeee;
    font-size: 11px;
    color: #111827;
    border: 1px solid #dddddd;
  }}

  /* ---- JSON panel ---- */
  .json-block {{
    margin-top: 10px;
    background: var(--panel);
    border: 1px solid var(--grid);
    border-radius: 8px;
    padding: 10px;
  }}

  .json-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 8px;
  }}

  .json-title {{
    font-size: 12px;
    font-weight: 600;
  }}

  .json-spacer {{
    flex: 1;
  }}

  .btn {{
    font-size: 12px;
    padding: 6px 10px;
    border-radius: 6px;
    border: 1px solid var(--grid);
    background: white;
    cursor: pointer;
  }}

  .btn:hover {{
    background: #f3f4f6;
  }}

  pre.json-pre {{
    margin: 0;
    padding: 10px;
    border-radius: 6px;
    border: 1px solid var(--grid);
    background: #0b1220;
    color: #e5e7eb;
    overflow: auto;
    max-height: 320px;
    font-size: 12px;
    line-height: 1.35;
  }}

  .footer-note {{
    margin-top: 10px;
    font-size: 12px;
    color: var(--muted);
  }}
</style>
</head>
<body>

<div class="topbar">
  <div class="tabs">
    <div class="tab active">Data Profiling Results</div>

  </div>
</div>

<div class="container">
  <div class="toolbar">
    <div class="title">{_html_escape(report_title)}</div>
    <div class="meta">
      Dataset: <b>{_html_escape(dataset_label)}</b> ·
      Rows: <b>{ov_row_count}</b> ·
      Columns: <b>{ov_col_count}</b> ·
      Overall Missing: <b>{ov_miss_rate}</b>
    </div>
    <div class="spacer"></div>
    <div class="find">
      <input id="findBox" type="text" placeholder="Find" />
    </div>
  </div>

  {preview_html}

  <table id="profileTable">
    <thead>
      <tr>
        <th>Columns</th>
        <th>Value Distribution</th>
        <th class="num">% Null</th>
        <th class="num"># Null</th>
        <th class="num">% Distinct</th>
        <th class="num"># Distinct</th>
        <th class="num">% Non-distinct</th>
        <th class="num"># Non-distinct</th>
        <th class="num"># Patterns</th>
        <th class="num">% of Top Pattern</th>
        <th class="num">Minimum Len</th>
        <th class="num">Maximum Len</th>
        <th>profile_type</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>

  <div class="json-block" id="jsonBlock">
    <div class="json-header">
      <div class="json-title" id="jsonTitle">Column JSON: {default_panel_title}</div>
      <div class="json-spacer"></div>
      <button class="btn" id="copyBtn" type="button">Copy JSON</button>
    </div>
    <pre class="json-pre" id="jsonPre">{default_panel_json}</pre>
  </div>

  <div class="footer-note">
    Tip: Use Find to filter columns. Click a row to view the underlying JSON.
  </div>
</div>

<script>
  const findBox = document.getElementById('findBox');
  const table = document.getElementById('profileTable');
  const jsonTitle = document.getElementById('jsonTitle');
  const jsonPre = document.getElementById('jsonPre');
  const copyBtn = document.getElementById('copyBtn');

  function pretty(obj) {{
    try {{
      return JSON.stringify(obj, null, 2);
    }} catch (e) {{
      return String(obj);
    }}
  }}

  function clearSelected() {{
    const rows = table.querySelectorAll('tbody tr.data-row');
    rows.forEach(r => r.classList.remove('selected'));
  }}

  // Find filter
  findBox.addEventListener('input', () => {{
    const q = findBox.value.trim().toLowerCase();
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(r => {{
      const name = (r.querySelector('.col-name')?.textContent || '').toLowerCase();
      r.style.display = name.includes(q) ? '' : 'none';
    }});
  }});

  // Click -> show JSON
  table.addEventListener('click', (evt) => {{
    const tr = evt.target.closest('tr.data-row');
    if (!tr) return;

    clearSelected();
    tr.classList.add('selected');

    const colName = tr.getAttribute('data-col') || '';
    const raw = tr.getAttribute('data-json') || '{{}}';

    try {{
      const obj = JSON.parse(raw);
      jsonTitle.textContent = colName ? ('Column JSON: ' + colName) : 'Column JSON';
      jsonPre.textContent = pretty(obj);
    }} catch (e) {{
      jsonTitle.textContent = colName ? ('Column JSON: ' + colName) : 'Column JSON';
      jsonPre.textContent = raw;
    }}
  }});


  // Copy JSON
  copyBtn.addEventListener('click', async () => {{
    try {{
      await navigator.clipboard.writeText(jsonPre.textContent || '');
      copyBtn.textContent = 'Copied';
      setTimeout(() => copyBtn.textContent = 'Copy JSON', 900);
    }} catch (e) {{
      // fallback: select text
      const range = document.createRange();
      range.selectNodeContents(jsonPre);
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
      copyBtn.textContent = 'Select and Copy';
      setTimeout(() => copyBtn.textContent = 'Copy JSON', 1200);
    }}
  }});

  // Render profile_type pills
  const pillCells = document.querySelectorAll('td.pill');
  pillCells.forEach(td => {{
    const t = td.textContent.trim();
    td.innerHTML = t ? `<span>${{t}}</span>` : '';
  }});

  // Auto-select first visible row to match default panel
  const firstRow = table.querySelector('tbody tr.data-row');
  if (firstRow) {{
    firstRow.classList.add('selected');
  }}
</script>

</body>
</html>
"""

    out_path = Path(out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return html


def profile_csv_to_html(
    csv_path: PathLike,
    out_html: PathLike,
    *,
    dataset_name: Optional[str] = None,
    sample_rows: Optional[int] = None,
    encoding: Optional[str] = None,
    title: Optional[str] = None,
    preview_rows: int = 10,
) -> str:
    """
    Convenience wrapper: read CSV then generate IICS-like HTML report.
    """
    df = pd.read_csv(csv_path, encoding=encoding) if encoding else pd.read_csv(csv_path)
    name = dataset_name or str(csv_path)
    return profile_to_html(
        df,
        out_html,
        dataset_name=name,
        sample_rows=sample_rows,
        title=title,
        preview_rows=preview_rows,
    )
