# transform.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, List, Any
import pandas as pd

from data_contract import DATA_CONTRACT, REQUIRED_COLUMNS

# ---------------------------
# Public report for batch layer
# ---------------------------
@dataclass(frozen=True)
class TransformReport:
    rows_in: int
    rows_valid: int
    rows_rejected: int
    reject_rate: float
    missing_required_columns: List[str]
    critical_failure: bool
    rejection_reason_counts: Dict[str, int]

# ---------------------------
# Configuration: synonyms mapping
# ---------------------------
SYNONYMS: Dict[str, str] = {
    # Spanish → canonical
    "nombre": "name",
    "edad": "age",
    "ciudad": "city",
    # Common variants → canonical
    "Name": "name",
    "AGE": "age",
    "City": "city",
}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize raw names: strip + lower
    df2 = df.copy()
    df2.columns = [str(c).strip() for c in df2.columns]

    # First map exact synonyms (case-sensitive keys as given)
    renamed = {c: SYNONYMS[c] for c in df2.columns if c in SYNONYMS}
    df2 = df2.rename(columns=renamed)

    # Then normalize to lower snake-ish (basic)
    df2.columns = [str(c).strip().lower() for c in df2.columns]
    return df2

def ensure_required_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    missing = sorted(list(REQUIRED_COLUMNS - set(df.columns)))
    if missing:
        # Add missing columns as NA so downstream logic can mark them invalid,
        # but we also expose missing columns as "critical_failure" in report.
        df2 = df.copy()
        for col in missing:
            df2[col] = pd.NA
        return df2, missing
    return df, []

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce columns according to contract types using vectorized operations.
    Anything that can't be coerced becomes NA (and will fail validation).
    """
    df2 = df.copy()

    for col, spec in DATA_CONTRACT.items():
        if col not in df2.columns:
            continue

        if spec.dtype == "string":
            df2[col] = df2[col].astype("string")

        elif spec.dtype == "int":
            # Convert to numeric first, then to nullable Int64
            num = pd.to_numeric(df2[col], errors="coerce")
            df2[col] = num.astype("Int64")

        elif spec.dtype == "float":
            df2[col] = pd.to_numeric(df2[col], errors="coerce").astype("Float64")

        elif spec.dtype == "datetime":
            df2[col] = pd.to_datetime(df2[col], errors="coerce")

        else:
            raise ValueError(f"Unsupported dtype in contract: {spec.dtype} for column {col}")

    return df2

def build_rejection_reasons(df: pd.DataFrame, missing_required_cols: List[str]) -> pd.Series:
    """
    Returns a string Series with semicolon-separated reasons per row, or "" if none.
    Rules:
    - Missing / null violations produce reasons like: "null:name" or "missing_column:age"
    - Type coercion failures typically show as null after coercion, so they are covered by null checks.
    - Validator failures produce reasons like: "rule:age_in_0_120"
    """
    reasons = pd.Series([""] * len(df), index=df.index, dtype="string")

    # Missing required columns: mark all rows (pipeline-level issue)
    # We still encode per-row reasons for consistency, but batch can treat missing columns as critical_failure.
    for col in missing_required_cols:
        reasons = _append_reason(reasons, f"missing_column:{col}")

    # Nullability checks
    for col, spec in DATA_CONTRACT.items():
        if col not in df.columns:
            continue
        if spec.nullable:
            continue
        is_null = df[col].isna()
        if is_null.any():
            reasons = _append_reason_masked(reasons, is_null, f"null:{col}")

    # Semantic validators
    for col, spec in DATA_CONTRACT.items():
        if col not in df.columns:
            continue
        if spec.validator is None:
            continue
        ok = spec.validator(df[col])
        # validator must return boolean Series (True means OK). Anything NA counts as fail.
        ok = ok.fillna(False)
        fail = ~ok
        if fail.any():
            rule_name = spec.validator_name or f"{col}_validator"
            reasons = _append_reason_masked(reasons, fail, f"rule:{rule_name}")

    return reasons.fillna("").astype("string")

def _append_reason(reasons: pd.Series, reason: str) -> pd.Series:
    # Append reason to all rows (used for missing columns)
    has_any = reasons.str.len().gt(0)
    out = reasons.copy()
    out.loc[~has_any] = reason
    out.loc[has_any] = out.loc[has_any] + ";" + reason
    return out

def _append_reason_masked(reasons: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    out = reasons.copy()
    masked = mask.fillna(False)
    if not masked.any():
        return out
    has_any = out.str.len().gt(0) & masked
    no_any = masked & ~out.str.len().gt(0)

    out.loc[no_any] = reason
    out.loc[has_any] = out.loc[has_any] + ";" + reason
    return out

def split_valid_rejected(df: pd.DataFrame, rejection_reason: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rejected_mask = rejection_reason.str.len().gt(0)
    df2 = df.copy()
    df2["rejection_reason"] = rejection_reason

    valid = df2.loc[~rejected_mask].copy()
    rejected = df2.loc[rejected_mask].copy()
    return valid, rejected

def compute_reason_counts(rejected: pd.DataFrame) -> Dict[str, int]:
    if rejected.empty:
        return {}

    # Count each reason token (semicolon separated)
    tokens = (
        rejected["rejection_reason"]
        .astype("string")
        .str.split(";")
        .explode()
        .dropna()
    )
    counts = tokens.value_counts()
    return {k: int(v) for k, v in counts.items()}

def is_critical_failure(missing_required_cols: List[str], rejected: pd.DataFrame) -> bool:
    """
    Critical failure rules:
    - Missing required columns is always critical.
    - Any rejected reason related to a CRITICAL column can be considered critical at batch level,
      but we keep this conservative: missing columns only.
    You can tighten this later (Day 34) to include critical field violations.
    """
    return len(missing_required_cols) > 0

def transform(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, TransformReport]:
    # 1) normalize + ensure required + coerce
    df = normalize_columns(raw_df)
    df, missing_cols = ensure_required_columns(df)
    df = coerce_types(df)

    # 2) build rejection reasons vectorized
    rejection_reason = build_rejection_reasons(df, missing_cols)

    # 3) split
    valid, rejected = split_valid_rejected(df, rejection_reason)

    # 4) report
    rows_in = int(len(df))
    rows_valid = int(len(valid))
    rows_rejected = int(len(rejected))
    reject_rate = (rows_rejected / rows_in) if rows_in else 0.0

    report = TransformReport(
        rows_in=rows_in,
        rows_valid=rows_valid,
        rows_rejected=rows_rejected,
        reject_rate=float(reject_rate),
        missing_required_columns=missing_cols,
        critical_failure=is_critical_failure(missing_cols, rejected),
        rejection_reason_counts=compute_reason_counts(rejected),
    )

    return valid, rejected, report
