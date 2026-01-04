from __future__ import annotations

import argparse
import re
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from pydqkit import exporters


# -----------------------------
# Regex / flags helpers
# -----------------------------
_FLAG_MAP = {
    "IGNORECASE": re.IGNORECASE,
    "I": re.IGNORECASE,
    "MULTILINE": re.MULTILINE,
    "M": re.MULTILINE,
    "DOTALL": re.DOTALL,
    "S": re.DOTALL,
    "ASCII": re.ASCII,
    "A": re.ASCII,
    "VERBOSE": re.VERBOSE,
    "X": re.VERBOSE,
}


def _compile(pattern: str, flags: Optional[Sequence[str]] = None) -> re.Pattern:
    f = 0
    for x in (flags or []):
        x = str(x).strip().upper()
        if x in _FLAG_MAP:
            f |= _FLAG_MAP[x]
    return re.compile(pattern, flags=f)


def _to_str_or_none(x: Any) -> Optional[str]:
    if pd.isna(x):
        return None
    return str(x)


def _parse_flags(flags_str: str) -> List[str]:
    """
    Accept:
      - "IGNORECASE,MULTILINE"
      - "IGNORECASE MULTILINE"
      - "i m"
    """
    s = (flags_str or "").strip()
    if not s:
        return []
    s = s.replace(",", " ")
    tokens = [t.strip().upper() for t in s.split() if t.strip()]
    out: List[str] = []
    seen = set()
    for t in tokens:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _normalize_mode(mode_raw: str) -> str:
    m = (mode_raw or "").strip().lower()
    if not m:
        return "fullmatch"
    if m in ("full", "match", "full_match", "full-match", "fullmatch"):
        return "fullmatch"
    if m in ("find", "search", "contains"):
        return "search"
    return "fullmatch"


def _parse_allow_null(x: str) -> bool:
    s = (x or "").strip().lower()
    if not s:
        return True
    return s in ("1", "true", "t", "yes", "y", "allow", "allowed")


def _split_arrow(stmt: str) -> List[str]:
    """
    Split a rule line by the token '=>', trimming outer whitespace.
    Example:
      status => ^(A|B)$ => fullmatch => false => IGNORECASE
    """
    parts = [p.strip() for p in stmt.split("=>")]
    # remove empty segments caused by accidental "a =>  => b"
    return [p for p in parts if p != ""]


# -----------------------------
# Core check
# -----------------------------
def check_regex_column(
    df: pd.DataFrame,
    column: str,
    pattern: str,
    *,
    mode: str = "fullmatch",
    allow_null: bool = True,
    flags: Optional[Sequence[str]] = None,
    sample_fail_values: int = 10,
) -> Dict[str, Any]:
    if column not in df.columns:
        return {"ok": False, "error": f"Column not found: {column}"}

    try:
        rx = _compile(pattern, flags=flags)
    except Exception as e:
        return {"ok": False, "error": f"Regex compile failed: {e}"}

    s = df[column].map(_to_str_or_none)
    is_null = s.isna()
    non_null = s[~is_null]

    mode_norm = _normalize_mode(mode)
    if mode_norm == "search":
        matched = non_null.map(lambda v: bool(rx.search(v)))
    else:
        matched = non_null.map(lambda v: bool(rx.fullmatch(v)))

    pass_non_null = matched
    fail_non_null = ~matched

    if allow_null:
        pass_null = is_null
        fail_null = pd.Series([False] * int(is_null.sum()), index=s[is_null].index)
    else:
        pass_null = pd.Series([False] * int(is_null.sum()), index=s[is_null].index)
        fail_null = is_null

    pass_count = int(pass_non_null.sum()) + int(pass_null.sum())
    fail_count = int(fail_non_null.sum()) + int(fail_null.sum())

    total = int(len(s))
    pass_rate = float(pass_count / total) if total else 1.0

    fail_values = non_null[fail_non_null].head(sample_fail_values).tolist()

    fail_indices: List[Any] = []
    if int(fail_non_null.sum()) > 0:
        fail_indices.extend(non_null[fail_non_null].index.tolist())
    if not allow_null and int(is_null.sum()) > 0:
        fail_indices.extend(s[is_null].index.tolist())

    return {
        "ok": True,
        "rule_type": "regex",
        "column": column,
        "pattern": pattern,
        "mode": mode_norm,
        "allow_null": allow_null,
        "flags": [str(x).strip().upper() for x in (flags or [])],
        "counts": {
            "total": total,
            "pass": pass_count,
            "fail": fail_count,
            "null": int(is_null.sum()),
            "non_null": int((~is_null).sum()),
        },
        "pass_rate": pass_rate,
        "samples": {"fail_values": fail_values},
        "fail_indices": fail_indices,
    }


def _fmt_pct(x: float) -> str:
    try:
        return f"{x * 100:.2f}%"
    except Exception:
        return ""


def _print_result(res: Dict[str, Any]) -> None:
    if not res.get("ok"):
        print(f"[ERROR] {res.get('error')}")
        return

    c = res["counts"]
    print("")
    print("=== Regex Check Result ===")
    print(f"Column     : {res['column']}")
    print(f"Mode       : {res['mode']}")
    print(f"Allow NULL : {res['allow_null']}")
    print(f"Flags      : {', '.join(res['flags']) if res['flags'] else '(none)'}")
    print(f"Pattern    : {res['pattern']}")
    print("")
    print(f"Total      : {c['total']}")
    print(f"Pass       : {c['pass']}  ({_fmt_pct(res['pass_rate'])})")
    print(f"Fail       : {c['fail']}")
    print(f"NULL       : {c['null']}")
    print("")
    fails = (res.get("samples") or {}).get("fail_values", []) or []
    if fails:
        print("Fail samples (first few):")
        for i, v in enumerate(fails, start=1):
            print(f"  {i}. {v}")
    else:
        print("Fail samples: (none)")
    print("")


def _ask_yes_no(prompt: str) -> bool:
    ans = input(prompt).strip().lower()
    return ans in ("y", "yes")


def _execute_rule_stmt(df: pd.DataFrame, stmt: str, history: List[Dict[str, Any]]) -> None:
    """
    stmt format:
      column => regex => [mode] => [allow_null] => [flags]
    """
    parts = _split_arrow(stmt)
    if len(parts) < 2:
        print("[ERROR] Invalid rule line. Expected at least: column => regex")
        return

    column = parts[0]
    pattern = parts[1]

    # Defaults
    mode = "fullmatch"
    allow_null = True
    flags: List[str] = []

    if len(parts) >= 3:
        mode = parts[2]
    if len(parts) >= 4:
        allow_null = _parse_allow_null(parts[3])
    if len(parts) >= 5:
        flags = _parse_flags(parts[4])

    res = check_regex_column(
        df,
        column,
        pattern,
        mode=mode,
        allow_null=allow_null,
        flags=flags,
    )
    _print_result(res)

    if res.get("ok"):
        history.append(res)


def interactive_shell(df: pd.DataFrame) -> None:
    history: List[Dict[str, Any]] = []

    print("")
    print("PyDQKit Regex Shell")
    print("")
    print("Rule block format (REQUIRED):")
    print("  RULE")
    print("    column => regex => mode => allow_null => flags")
    print("  END")
    print("")
    print("Minimal example (defaults used):")
    print(r"  RULE")
    print(r"    amount => ^\d+(\.\d+)?$")
    print(r"  END")
    print("")
    print("Example with options:")
    print(r"  RULE")
    print(r"    status => ^(ACTIVE|INACTIVE)$ => fullmatch => false")
    print(r"  END")
    print("")
    print("Defaults:")
    print("  mode=fullmatch, allow_null=true, flags=(none)")
    print("")
    print("Commands:")
    print("  :cols")
    print("  :export <path.xlsx>")
    print("  :export_dashboard <path.html>")
    print("  :q")
    print("")

    in_rule = False
    rule_lines: List[str] = []

    cols = list(df.columns)

    while True:
        line = input("pydqkit> ")
        if line is None:
            continue
        s = line.strip()

        if not in_rule:
            if not s:
                continue

            if s in (":q", "q", "quit", "exit"):
                break

            if s == ":cols":
                for c in cols:
                    print(f"  - {c}")
                continue

            if s.startswith(":export "):
                parts = s.split(maxsplit=1)
                if len(parts) != 2:
                    print("[ERROR] Usage: :export <path.xlsx>")
                    continue

                out_path = parts[1].strip().strip('"').strip("'")
                if not out_path.lower().endswith(".xlsx"):
                    print("[ERROR] Please use .xlsx extension, e.g. :export report.xlsx")
                    continue

                if not history:
                    print("[WARN] No checks to export yet.")
                    continue

                include_failed = _ask_yes_no("Export FailedRows sheet as well? (y/n): ")
                try:
                    exporters.export_excel(df, history, out_path, include_failed_rows=include_failed)
                    msg = f"[OK] Exported Excel report to {out_path}"
                    if include_failed:
                        msg += " (including FailedRows)"
                    print(msg)
                except Exception as e:
                    print(f"[ERROR] Export failed: {e}")
                continue

            if s.startswith(":export_dashboard"):
                parts = s.split(maxsplit=1)
                if len(parts) != 2:
                    print("[ERROR] Usage: :export_dashboard <path.html>")
                    continue

                out_path = parts[1].strip().strip('"').strip("'")
                if not out_path.lower().endswith(".html"):
                    print("[ERROR] Please use .html extension, e.g. :export_dashboard dashboard.html")
                    continue

                if not history:
                    print("[WARN] No checks to export yet.")
                    continue

                include_failed = _ask_yes_no("Include failed rows table in dashboard? (y/n): ")
                try:
                    exporters.export_html(df, history, out_path, include_failed_rows=include_failed)
                    print(f"[OK] Exported HTML dashboard to {out_path}")
                except Exception as e:
                    print(f"[ERROR] Export failed: {e}")
                continue

            if s.upper() == "RULE":
                in_rule = True
                rule_lines = []
                continue

            print("[ERROR] Please start a rule with 'RULE' (or use :cols / :export / :q).")
            continue

        # in_rule
        if s.upper() == "END":
            in_rule = False
            inner = "\n".join(rule_lines).strip()

            # pick first non-empty, non-comment line as rule statement
            stmt = ""
            for ln in inner.splitlines():
                t = ln.strip()
                if not t:
                    continue
                if t.startswith("#"):
                    continue
                stmt = t
                break

            if not stmt:
                print("[ERROR] Empty RULE block. Put: column => regex ... between RULE and END.")
                rule_lines = []
                continue

            _execute_rule_stmt(df, stmt, history)
            rule_lines = []
            continue

        rule_lines.append(line)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="PyDQKit interactive regex shell for checking column patterns.")
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument("--encoding", default=None, help="CSV encoding (optional)")
    parser.add_argument("--sample-rows", type=int, default=None, help="Load only first N rows (optional)")
    args = parser.parse_args(argv)

    df = pd.read_csv(args.csv_path, encoding=args.encoding) if args.encoding else pd.read_csv(args.csv_path)
    if args.sample_rows and args.sample_rows > 0 and len(df) > args.sample_rows:
        df = df.head(args.sample_rows).copy()

    interactive_shell(df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
