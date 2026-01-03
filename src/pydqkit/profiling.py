from __future__ import annotations

from typing import Any, Dict, List, Optional
import math

import pandas as pd
import pandas.api.types as ptypes


# -----------------------------
# Helpers
# -----------------------------
def _pattern_signature(s: str) -> str:
    """
    Turn a string into a simple signature for quick pattern discovery.

    Example:
      "AB123456" -> "LLDDDDDD"
      "ab-12"    -> "ll-DD" (punctuation kept)
    """
    out: List[str] = []
    for ch in s:
        if "A" <= ch <= "Z":
            out.append("L")
        elif "a" <= ch <= "z":
            out.append("l")
        elif "0" <= ch <= "9":
            out.append("D")
        elif ch.isspace():
            out.append("␠")
        else:
            out.append(ch)
    return "".join(out)


def _compress_signature(sig: str) -> str:
    """Compress signature by counting repeats: "LLDDDD" -> "L2D4"."""
    if not sig:
        return sig
    parts: List[str] = []
    cur = sig[0]
    n = 1
    for ch in sig[1:]:
        if ch == cur:
            n += 1
        else:
            parts.append(f"{cur}{n}" if n > 1 else cur)
            cur = ch
            n = 1
    parts.append(f"{cur}{n}" if n > 1 else cur)
    return "".join(parts)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    except Exception:
        return None


def _top_k_with_pct(series: pd.Series, k: int = 10) -> List[Dict[str, Any]]:
    """
    Top-K values including NA, with counts and percentages.
    Percent is computed w.r.t. total rows in the series.
    """
    total = int(len(series))
    if total == 0:
        return []

    vc = series.value_counts(dropna=False).head(k)
    out: List[Dict[str, Any]] = []
    for val, cnt in vc.items():
        key = "<NA>" if pd.isna(val) else str(val)
        out.append(
            {
                "value": key,
                "count": int(cnt),
                "pct": float(cnt / total),
            }
        )
    return out


def _pattern_top(series: pd.Series, k: int = 10, sig_samples: int = 2000) -> List[Dict[str, Any]]:
    """
    Discover frequent patterns for a string-like series, with counts and percentages.

    Computed on up to sig_samples non-null values to stay fast; percent is w.r.t. the sample size.
    """
    x = series.dropna().astype(str)
    if len(x) == 0:
        return []
    sample = x.head(sig_samples)
    sigs = sample.map(_pattern_signature).map(_compress_signature)
    total = int(len(sigs))
    vc = sigs.value_counts(dropna=False).head(k)
    out: List[Dict[str, Any]] = []
    for val, cnt in vc.items():
        out.append({"pattern": str(val), "count": int(cnt), "pct": float(cnt / total) if total else 0.0})
    return out


def _is_boolean_like(non_null: pd.Series) -> Optional[float]:
    """
    Return confidence in [0,1] if the series looks boolean-like, otherwise None.

    Designed to be conservative:
    - Definitive if pandas dtype is bool
    - For object/string: triggers only when unique tokens are <=2 and in a strict boolean vocabulary
    """
    if len(non_null) == 0:
        return None

    # Definitive bool dtype
    if ptypes.is_bool_dtype(non_null):
        return 1.0

    # Object/string tokens
    s = non_null.astype(str).str.strip().str.lower()
    uniq = set(s.unique())

    boolean_vocab = {
        "true", "false",
        "t", "f",
        "yes", "no",
        "y", "n",
        "0", "1",
    }

    if len(uniq) <= 2 and uniq.issubset(boolean_vocab):
        # "0/1" could also be codes, so slightly lower confidence
        if uniq.issubset({"0", "1"}):
            return 0.90
        return 0.95

    return None


def _suggest_iics_type(x: pd.Series) -> Dict[str, Any]:
    """
    Rough IICS-friendly type suggestion:
      - BOOLEAN if boolean-like
      - If datetime parse works -> DATETIME
      - If mostly numeric -> INTEGER / DECIMAL
      - Else STRING with max_len
    """
    non_null = x.dropna()
    if len(non_null) == 0:
        return {"suggested_type": "STRING", "note": "all_null"}

    # Boolean first (critical)
    bool_conf = _is_boolean_like(non_null)
    if bool_conf is not None and bool_conf >= 0.90:
        return {"suggested_type": "BOOLEAN", "confidence": float(bool_conf)}

    # datetime?
    dt = pd.to_datetime(non_null, errors="coerce", utc=False)
    dt_ratio = float(dt.notna().mean())
    if dt_ratio >= 0.98:
        return {"suggested_type": "DATETIME", "confidence": dt_ratio}

    # numeric?
    num = pd.to_numeric(non_null, errors="coerce")
    num_ratio = float(num.notna().mean())
    if num_ratio >= 0.98:
        as_float = num.dropna().astype(float)
        is_int_ratio = float((as_float % 1 == 0).mean()) if len(as_float) else 0.0
        if is_int_ratio >= 0.98:
            return {"suggested_type": "INTEGER", "confidence": num_ratio}
        return {"suggested_type": "DECIMAL", "confidence": num_ratio}

    # default string
    max_len = int(non_null.astype(str).map(len).max())
    return {
        "suggested_type": "STRING",
        "max_len": max_len,
        "confidence": 1.0 - max(num_ratio, dt_ratio),
    }


def _infer_profile_type(s: pd.Series) -> str:
    """
    A pragmatic profile type used for deciding which metrics apply.
    Returns one of: boolean, numeric, datetime, string, all_null.
    """
    non_null = s.dropna()
    if len(non_null) == 0:
        return "all_null"

    # Boolean first (critical: bool is also numeric-like)
    bool_conf = _is_boolean_like(non_null)
    if bool_conf is not None and bool_conf >= 0.95:
        return "boolean"

    # numeric?
    num = pd.to_numeric(non_null, errors="coerce")
    if float(num.notna().mean()) >= 0.98:
        return "numeric"

    # datetime?
    dt = pd.to_datetime(non_null, errors="coerce", utc=False)
    if float(dt.notna().mean()) >= 0.98:
        return "datetime"

    return "string"


def _boolean_to_int_series(x: pd.Series) -> pd.Series:
    """
    Convert a boolean or boolean-like series (non-null) into int 0/1 series.
    Assumes caller has already checked boolean-likeness.
    """
    if ptypes.is_bool_dtype(x):
        return x.astype(int)

    t = x.astype(str).str.strip().str.lower()
    mapped = t.map(
        {
            "true": 1, "t": 1, "yes": 1, "y": 1, "1": 1,
            "false": 0, "f": 0, "no": 0, "n": 0, "0": 0,
        }
    )
    return mapped.dropna().astype(int)


# -----------------------------
# Main API
# -----------------------------
def profile_dataframe(
    df: pd.DataFrame,
    *,
    dataset_name: str = "dataset",
    sample_rows: Optional[int] = None,
    top_k: int = 10,
    pattern_k: int = 10,
    pattern_sample: int = 2000,
    preview_rows: int = 10,
) -> Dict[str, Any]:
    """
    IICS-style data profiling report (JSON-serializable).

    Output contains:
      - overview: dataset-level stats
      - preview_top_rows: top N rows of raw data
      - iics_table: a compact "field as row" table with key metrics
      - columns: a more detailed per-column report
    """
    if sample_rows is not None and sample_rows > 0 and len(df) > sample_rows:
        df_work = df.head(sample_rows).copy()
        sampled = True
    else:
        df_work = df
        sampled = False

    rows = int(len(df_work))
    cols = int(df_work.shape[1])

    # Preview: keep it JSON-friendly (convert NaN to None)
    preview_df = df_work.head(preview_rows).copy()
    preview_records: List[Dict[str, Any]] = preview_df.where(pd.notna(preview_df), None).to_dict(orient="records")

    # Overview (dataset-level)
    total_missing = int(df_work.isna().sum().sum()) if rows and cols else 0
    missing_rate_overall = float(total_missing / (rows * cols)) if rows and cols else 0.0

    iics_table: List[Dict[str, Any]] = []
    columns_detailed: List[Dict[str, Any]] = []

    for c in df_work.columns:
        s = df_work[c]

        missing_count = int(s.isna().sum())
        missing_pct = float(missing_count / rows) if rows else 0.0
        not_null_count = int(rows - missing_count)
        not_null_pct = float(not_null_count / rows) if rows else 0.0

        non_null = s.dropna()

        # Distinct and duplicates (non-null semantics)
        distinct_count = int(non_null.nunique()) if len(non_null) else 0
        duplicate_count = int(non_null.duplicated().sum()) if len(non_null) else 0

        profile_type = _infer_profile_type(s)

        # Min/Max/Mean/Median/Std
        min_val: Any = None
        max_val: Any = None
        mean_val: Optional[float] = None
        median_val: Optional[float] = None
        std_val: Optional[float] = None

        if profile_type == "numeric":
            x_num = pd.to_numeric(s, errors="coerce").dropna()
            if len(x_num):
                min_val = _safe_float(x_num.min())
                max_val = _safe_float(x_num.max())
                mean_val = _safe_float(x_num.mean())
                median_val = _safe_float(x_num.median())
                std_val = _safe_float(x_num.std(ddof=1)) if len(x_num) > 1 else 0.0

        elif profile_type == "boolean":
            x_bool = s.dropna()
            if len(x_bool):
                xb = _boolean_to_int_series(x_bool)
                if len(xb):
                    min_val = int(xb.min())
                    max_val = int(xb.max())
                    mean_val = float(xb.mean())  # True rate
                    median_val = float(xb.median())
                    std_val = float(xb.std(ddof=1)) if len(xb) > 1 else 0.0

        elif profile_type == "datetime":
            x_dt = pd.to_datetime(s, errors="coerce", utc=False).dropna()
            if len(x_dt):
                min_val = x_dt.min().isoformat()
                max_val = x_dt.max().isoformat()

        # Length metrics for string-like columns
        min_len: Optional[int] = None
        max_len: Optional[int] = None
        avg_len: Optional[float] = None

        if profile_type != 0:
            x_str = non_null.astype(str)
            if len(x_str):
                lens = x_str.map(len)

                min_len_non_null = int(lens.min())
                max_len = int(lens.max())
                avg_len = float(lens.mean())

                # if it is null，Minimum Len = 0
                if missing_count > 0:
                    min_len = 0
                else:
                    min_len = min_len_non_null

        # Top values (including NA) and patterns
        top_values = _top_k_with_pct(s.astype(object), k=top_k)

        pattern_summary: List[Dict[str, Any]] = []
        if profile_type != 0:
            pattern_summary = _pattern_top(s, k=pattern_k, sig_samples=pattern_sample)

        type_suggest = _suggest_iics_type(s)

        iics_row: Dict[str, Any] = {
            "column_name": str(c),
            "pandas_dtype": str(s.dtype),
            "suggested_iics_type": type_suggest.get("suggested_type"),
            "null_pct": missing_pct,
            "not_null_pct": not_null_pct,
            "distinct_count": distinct_count,
            "duplicate_count": duplicate_count,
            "min": min_val,
            "max": max_val,
            "mean": mean_val,
            "median": median_val,
            "std": std_val,
            "min_length": min_len,
            "max_length": max_len,
            "pattern_summary": pattern_summary,
            "top_values": top_values,
        }
        iics_table.append(iics_row)

        columns_detailed.append(
            {
                "name": str(c),
                "profile_type": profile_type,
                "pandas_dtype": str(s.dtype),
                "row_count": rows,
                "missing_count": missing_count,
                "missing_rate": missing_pct,
                "non_null_count": not_null_count,
                "non_null_rate": not_null_pct,
                "distinct_count": distinct_count,
                "duplicate_count": duplicate_count,
                "min": min_val,
                "max": max_val,
                "mean": mean_val,
                "median": median_val,
                "std": std_val,
                "min_len": min_len,
                "max_len": max_len,
                "avg_len": avg_len,
                "pattern_summary": pattern_summary,
                "top_values": top_values,
                "type_suggestion": type_suggest,
            }
        )

    return {
        "dataset": dataset_name,
        "sampled": sampled,
        "sample_rows": int(len(df_work)),
        "total_rows_seen": rows,
        "total_columns": cols,
        "preview_top_rows": preview_records,
        "overview": {
            "row_count": rows,
            "column_count": cols,
            "total_cells": int(rows * cols),
            "total_missing": total_missing,
            "missing_rate_overall": missing_rate_overall,
        },
        "iics_table": iics_table,
        "columns": columns_detailed,
    }


def profile_csv(
    csv_path: str,
    *,
    dataset_name: Optional[str] = None,
    sample_rows: Optional[int] = None,
    encoding: Optional[str] = None,
    top_k: int = 10,
    pattern_k: int = 10,
    pattern_sample: int = 2000,
    preview_rows: int = 10,
) -> Dict[str, Any]:
    df = pd.read_csv(csv_path, encoding=encoding) if encoding else pd.read_csv(csv_path)
    name = dataset_name or csv_path
    return profile_dataframe(
        df,
        dataset_name=name,
        sample_rows=sample_rows,
        top_k=top_k,
        pattern_k=pattern_k,
        pattern_sample=pattern_sample,
        preview_rows=preview_rows,
    )
