from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def _html_escape(s: Any) -> str:
    s = "" if s is None else str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _pct(x: Optional[float]) -> str:
    if x is None:
        return ""
    try:
        return f"{x * 100:.2f}%"
    except Exception:
        return ""


def _rate_badge_html(rate: Any) -> str:
    """
    Render pass_rate with a colored square:
      >= 0.80 -> green
      0.40~0.79 -> yellow
      < 0.40 -> red
    """
    if rate is None or (isinstance(rate, float) and pd.isna(rate)):
        return ""

    try:
        r = float(rate)
    except Exception:
        return ""

    if r >= 0.80:
        cls = "rate-green"
    elif r >= 0.40:
        cls = "rate-yellow"
    else:
        cls = "rate-red"

    # NOTE: this is HTML fragment; do NOT escape it again when placing into TD.
    return (
        "<span class='rate-badge'>"
        f"<span class='rate-dot {cls}'></span>"
        f"{_pct(r)}"
        "</span>"
    )


def build_summary_df(history: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for i, r in enumerate(history, start=1):
        if not r.get("ok"):
            continue
        c = r.get("counts") or {}
        rows.append(
            {
                "check_no": i,
                "rule_type": r.get("rule_type", "regex"),
                "column": r.get("column", ""),
                "pattern_or_expr": r.get("pattern", r.get("expr", "")),
                "mode": r.get("mode", ""),
                "allow_null": bool(r.get("allow_null", True)),
                "flags": ",".join(r.get("flags", []) or []),
                "total": c.get("total", 0),
                "pass": c.get("pass", 0),
                "fail": c.get("fail", 0),
                "null": c.get("null", 0),
                "non_null": c.get("non_null", 0),
                "pass_rate": r.get("pass_rate", None),
                "fail_samples": "|".join((r.get("samples") or {}).get("fail_values", []) or []),
            }
        )
    return pd.DataFrame(rows)


def build_failed_rows_df(df: pd.DataFrame, history: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    ALL failed rows across ALL rules; issue in the SAME row as _dq_issue.
    Used for Excel export.
    """
    fail_map: Dict[Any, List[str]] = {}

    for r in history:
        if not r.get("ok"):
            continue
        fail_idx = r.get("fail_indices") or []
        if not fail_idx:
            continue

        rule_type = r.get("rule_type", "regex")
        col = r.get("column", "")
        allow_null = bool(r.get("allow_null", True))

        if rule_type == "regex":
            pattern = r.get("pattern", "")
            desc = f"{col} !~ {pattern}" + ("" if allow_null else " (NULL not allowed)")
        else:
            expr = r.get("expr", r.get("pattern", ""))
            desc = f"{col}: {expr}" + ("" if allow_null else " (NULL not allowed)")

        for idx in fail_idx:
            fail_map.setdefault(idx, []).append(desc)

    if not fail_map:
        return pd.DataFrame()

    data_cols = list(df.columns)
    out_cols = data_cols + ["_dq_issue"]

    def _safe_cell(v: Any) -> Any:
        return "" if pd.isna(v) else v

    ordered_failed_indices = [idx for idx in df.index if idx in fail_map]

    out_rows: List[Dict[str, Any]] = []
    for idx in ordered_failed_indices:
        row = df.loc[idx]
        row_dict = {c: _safe_cell(row[c]) for c in data_cols}
        row_dict["_dq_issue"] = "FAILED: " + " || ".join(fail_map.get(idx, []))
        out_rows.append(row_dict)

    return pd.DataFrame(out_rows, columns=out_cols)


def export_excel(
    df: pd.DataFrame,
    history: List[Dict[str, Any]],
    out_path: str,
    *,
    include_failed_rows: bool = True,
) -> None:
    out_path = str(out_path)
    if not out_path.lower().endswith(".xlsx"):
        raise ValueError("Excel export requires .xlsx file extension.")

    summary_df = build_summary_df(history)
    failed_df = build_failed_rows_df(df, history) if include_failed_rows else pd.DataFrame()

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        if include_failed_rows:
            if len(failed_df) == 0:
                pd.DataFrame({"note": ["No failed rows."]}).to_excel(writer, sheet_name="FailedRows", index=False)
            else:
                failed_df.to_excel(writer, sheet_name="FailedRows", index=False)
        else:
            pd.DataFrame({"note": ["Failed rows not exported."]}).to_excel(writer, sheet_name="FailedRows", index=False)


def export_html(
    df: pd.DataFrame,
    history: List[Dict[str, Any]],
    out_path: str,
    *,
    include_failed_rows: bool = True,
    title: str = "PyDQKit DQ Dashboard",
) -> None:
    """
    Interactive HTML dashboard:
      - Summary table is clickable
      - Clicking a rule shows ONLY that rule's failed rows below
      - pass_rate shows a colored square badge
    """
    out_path = str(out_path)
    if not out_path.lower().endswith(".html"):
        raise ValueError("HTML export requires .html file extension.")

    summary_df = build_summary_df(history)

    def _safe_cell(v: Any) -> str:
        return "" if pd.isna(v) else str(v)

    # ---------- Preview (top 10) ----------
    preview_html = ""
    try:
        head = df.head(10)
        cols = list(head.columns)
        thead = "".join(f"<th>{_html_escape(c)}</th>" for c in cols)
        body_rows = []
        for _, row in head.iterrows():
            tds = "".join(f"<td>{_html_escape(_safe_cell(row[c]))}</td>" for c in cols)
            body_rows.append(f"<tr>{tds}</tr>")
        preview_html = f"""
        <div class="block">
          <div class="block-title">Data Preview (Top 10 rows)</div>
          <div class="table-wrap">
            <table>
              <thead><tr>{thead}</tr></thead>
              <tbody>{''.join(body_rows)}</tbody>
            </table>
          </div>
        </div>
        """
    except Exception:
        preview_html = ""

    # ---------- Summary table ----------
    # IMPORTANT: do NOT pre-format pass_rate into string here, because we need thresholds + badge
    summary_cols = list(summary_df.columns)
    _num_cols = {"total", "pass", "fail", "null", "non_null", "pass_rate"}

    sum_thead = "".join(
        f"<th class='num'>{_html_escape(c)}</th>" if c in _num_cols else f"<th>{_html_escape(c)}</th>"
        for c in summary_cols
    )

    sum_rows_html = []
    for _, row in summary_df.iterrows():
        check_no = int(row.get("check_no", 0) or 0)
        tds = []
        for c in summary_cols:
            if c == "pass_rate":
                # render badge HTML (do NOT html_escape this fragment)
                cell_html = _rate_badge_html(row.get(c))
                tds.append(f"<td class='num'>{cell_html}</td>")
                continue

            v = "" if pd.isna(row[c]) else str(row[c])
            cls = (
                "mono"
                if c in ("pattern_or_expr", "fail_samples")
                else ("num" if c in ("total", "pass", "fail", "null", "non_null") else "")
            )
            tds.append(f"<td class='{cls}'>{_html_escape(v)}</td>")
        sum_rows_html.append(f"<tr class='sum-row' data-check='{check_no}'>{''.join(tds)}</tr>")

    summary_table_html = f"""
    <div class="table-wrap">
      <table id="summaryTable">
        <thead><tr>{sum_thead}</tr></thead>
        <tbody>{''.join(sum_rows_html) if sum_rows_html else "<tr><td>No checks.</td></tr>"}</tbody>
      </table>
    </div>
    <div class="note" style="margin-top:8px;">
      Tip: Click a rule row to view its failed rows below.
    </div>
    """

    # ---------- Failed rows per rule (HTML fragments) ----------
    fragments_html = ""
    initial_failed_html = "<div class='note'>Failed rows not included.</div>"

    if include_failed_rows:
        df_cols = list(df.columns)

        def _render_failed_rows_for_check(failed_only: pd.DataFrame) -> str:
            if failed_only is None or len(failed_only) == 0:
                return "<div class='note'>No failed rows for this rule.</div>"

            cols = df_cols + ["_dq_issue"]
            thead = "".join(f"<th>{_html_escape(c)}</th>" for c in cols)

            body = []
            for _, r in failed_only.iterrows():
                tds = []
                for c in df_cols:
                    tds.append(f"<td>{_html_escape(_safe_cell(r.get(c, '')))}</td>")
                tds.append(f"<td class='issue mono'>{_html_escape(_safe_cell(r.get('_dq_issue', '')))}</td>")
                body.append("<tr>" + "".join(tds) + "</tr>")

            return f"""
            <div class="table-wrap">
              <table>
                <thead><tr>{thead}</tr></thead>
                <tbody>{''.join(body)}</tbody>
              </table>
            </div>
            """

        # build fragments
        check_failed_html: Dict[int, str] = {}

        for check_no, r in enumerate(history, start=1):
            if not r.get("ok"):
                check_failed_html[check_no] = "<div class='note'>Rule error. No failed rows.</div>"
                continue

            fail_idx = r.get("fail_indices") or []
            if not fail_idx:
                check_failed_html[check_no] = "<div class='note'>No failed rows for this rule.</div>"
                continue

            rule_type = r.get("rule_type", "regex")
            col = r.get("column", "")
            allow_null = bool(r.get("allow_null", True))

            if rule_type == "regex":
                pattern = r.get("pattern", "")
                issue_desc = f"{col} !~ {pattern}" + ("" if allow_null else " (NULL not allowed)")
            else:
                expr = r.get("expr", r.get("pattern", ""))
                issue_desc = f"{col}: {expr}" + ("" if allow_null else " (NULL not allowed)")

            fail_set = set(fail_idx)
            ordered = [idx for idx in df.index if idx in fail_set]
            if not ordered:
                check_failed_html[check_no] = "<div class='note'>No failed rows for this rule.</div>"
                continue

            failed_only = df.loc[ordered].copy()
            failed_only["_dq_issue"] = "FAILED: " + issue_desc
            check_failed_html[check_no] = _render_failed_rows_for_check(failed_only)

        for k, frag in check_failed_html.items():
            fragments_html += f"<div class='frag' id='frag-{k}' style='display:none'>{frag}</div>"

        if len(summary_df) > 0:
            first_check = int(summary_df.iloc[0]["check_no"])
            initial_failed_html = check_failed_html.get(first_check, "<div class='note'>No failed rows.</div>")
        else:
            initial_failed_html = "<div class='note'>No checks.</div>"

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_html_escape(title)}</title>
<style>
  :root {{
    --bg:#f3f4f6; --panel:#ffffff; --grid:#d9dee5; --text:#111827; --muted:#6b7280;
    --header:#f9fafb; --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    --select:#dbeafe;
  }}
  body {{ margin:0; font-family: Segoe UI, Arial, sans-serif; background:var(--bg); color:var(--text); }}
  .topbar {{ background:var(--panel); border-bottom:1px solid var(--grid); padding:12px 14px; }}
  .title {{ font-size:14px; font-weight:700; }}
  .meta {{ font-size:12px; color:var(--muted); margin-top:4px; }}
  .container {{ padding:12px 14px 18px 14px; }}
  .block {{ background:var(--panel); border:1px solid var(--grid); border-radius:10px; padding:10px; margin-bottom:10px; }}
  .block-title {{ font-size:12px; font-weight:700; margin-bottom:8px; display:flex; gap:10px; align-items:center; }}
  .block-subtitle {{ font-size:12px; color:var(--muted); font-weight:600; }}
  .table-wrap {{ overflow:auto; border:1px solid var(--grid); border-radius:8px; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; }}
  thead th {{ position:sticky; top:0; background:var(--header); border-bottom:1px solid var(--grid); font-size:12px; text-align:left; padding:8px 10px; white-space:nowrap; }}
  tbody td {{ border-top:1px solid var(--grid); font-size:12px; padding:8px 10px; vertical-align:top; }}
  tbody tr:hover {{ background:#eef2f7; }}
  .mono {{ font-family: var(--mono); }}
  .issue {{ color:#991b1b; }}
  .note {{ font-size:12px; color:var(--muted); }}
  .num {{ text-align:center; font-variant-numeric: tabular-nums; }}
  tr.sum-row {{ cursor:pointer; }}
  tr.sum-row.selected {{ background:var(--select) !important; }}

  /* ---- pass_rate badge ---- */
  .rate-badge {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-variant-numeric: tabular-nums;
  }}
  .rate-dot {{
    width: 10px;
    height: 10px;
    border-radius: 2px; /* small square */
    display: inline-block;
  }}
  .rate-green {{ background: #22c55e; }}
  .rate-yellow {{ background: #facc15; }}
  .rate-red {{ background: #ef4444; }}
</style>
</head>
<body>
  <div class="topbar">
    <div class="title">{_html_escape(title)}</div>
    <div class="meta">Rows: <b>{len(df)}</b> · Columns: <b>{df.shape[1]}</b> · Checks: <b>{len(history)}</b></div>
  </div>

  <div class="container">
    {preview_html}

    <div class="block">
      <div class="block-title">Summary <span class="block-subtitle">(click a rule to filter Failed Rows)</span></div>
      {summary_table_html}
    </div>

    <div class="block">
      <div class="block-title" id="failedTitle">Failed Rows</div>
      <div id="failedContainer">{initial_failed_html}</div>
      {fragments_html}
    </div>
  </div>

<script>
(function() {{
  const table = document.getElementById('summaryTable');
  const failedContainer = document.getElementById('failedContainer');
  const failedTitle = document.getElementById('failedTitle');

  function clearSelected() {{
    const rows = table.querySelectorAll('tbody tr.sum-row');
    rows.forEach(r => r.classList.remove('selected'));
  }}

  function showFailed(checkNo) {{
    const frag = document.getElementById('frag-' + checkNo);
    if (!frag) {{
      failedContainer.innerHTML = "<div class='note'>No failed rows for this rule.</div>";
      failedTitle.textContent = "Failed Rows (rule #" + checkNo + ")";
      return;
    }}
    failedContainer.innerHTML = frag.innerHTML;
    failedTitle.textContent = "Failed Rows (rule #" + checkNo + ")";
  }}

  table.addEventListener('click', (evt) => {{
    const tr = evt.target.closest('tr.sum-row');
    if (!tr) return;
    const checkNo = tr.getAttribute('data-check');
    clearSelected();
    tr.classList.add('selected');
    if (checkNo) showFailed(checkNo);
  }});

  const firstRow = table.querySelector('tbody tr.sum-row');
  if (firstRow) {{
    firstRow.classList.add('selected');
    const checkNo = firstRow.getAttribute('data-check');
    if (checkNo) showFailed(checkNo);
  }}
}})();
</script>

</body>
</html>
"""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(html, encoding="utf-8")
