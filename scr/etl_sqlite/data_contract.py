# data_contract.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Set, Literal
import pandas as pd

Severity = Literal["critical", "non_critical"]

@dataclass(frozen=True)
class ColumnSpec:
    # Canonical name already (e.g., name, age, city)
    dtype: str  # "string" | "int" | "float" | "datetime" (extensible)
    nullable: bool
    severity: Severity = "critical"

    # Optional semantic validation (must be vectorized function returning boolean Series)
    validator: Optional[Callable[[pd.Series], pd.Series]] = None
    validator_name: Optional[str] = None  # used in rejection_reason

# ---- Vectorized validators (examples) ----
def v_non_empty_str(s: pd.Series) -> pd.Series:
    # Accept pandas "string" dtype; handle NaN safely
    return s.fillna("").astype("string").str.strip().str.len().ge(2)

def v_age_range(s: pd.Series) -> pd.Series:
    # expects numeric (after coercion); invalids will be NaN and fail range
    return s.between(0, 120, inclusive="both")

# ---- Contract definition (edit to match your domain) ----
DATA_CONTRACT: Dict[str, ColumnSpec] = {
    "name": ColumnSpec(
        dtype="string",
        nullable=False,
        severity="critical",
        validator=v_non_empty_str,
        validator_name="name_non_empty",
    ),
    "age": ColumnSpec(
        dtype="int",
        nullable=False,
        severity="critical",
        validator=v_age_range,
        validator_name="age_in_0_120",
    ),
    "city": ColumnSpec(
        dtype="string",
        nullable=False,
        severity="non_critical",
        validator=v_non_empty_str,
        validator_name="city_non_empty",
    ),
}

REQUIRED_COLUMNS: Set[str] = set(DATA_CONTRACT.keys())
