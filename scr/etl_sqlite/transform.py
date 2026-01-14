from __future__ import annotations

import re
import unicodedata
import pandas as pd


def _normalize_col(col: str) -> str:
    """
    Normaliza nombres de columnas:
    - lower
    - strip
    - quita acentos
    - espacios y guiones -> underscore
    - colapsa underscores múltiples
    """
    col = str(col).strip().lower()
    col = unicodedata.normalize("NFKD", col)
    col = "".join(ch for ch in col if not unicodedata.combining(ch))
    col = col.replace("-", "_").replace(" ", "_")
    col = re.sub(r"_+", "_", col)
    return col


def _auto_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas a name/age/city usando sinónimos comunes.
    """
    df = df.copy()
    df.columns = [_normalize_col(c) for c in df.columns]

    synonyms = {
        "name": {"name", "nombre", "full_name", "fullname", "person", "persona", "customer", "cliente"},
        "age": {"age", "edad", "years", "anios", "anos"},
        "city": {"city", "ciudad", "municipio", "location", "localidad", "city_name"},
    }

    rename_map = {}

    # Para cada columna destino (name/age/city), busca si existe alguna de sus variantes en df.columns
    cols_set = set(df.columns)
    for target, options in synonyms.items():
        found = None
        for opt in options:
            if opt in cols_set:
                found = opt
                break
        if found is not None:
            rename_map[found] = target

    df = df.rename(columns=rename_map)
    return df


def transform_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Recibe DF crudo y devuelve:
      - df_valid (name, age, city)
      - df_rejected con rejection_reason

    Acepta CSVs con nombres de columnas en español/variantes y las mapea automáticamente.
    """
    df = _auto_rename_columns(df)

    required = {"name", "age", "city"}
    missing = required - set(df.columns)
    if missing:
        # Mensaje útil para depuración
        cols = list(df.columns)
        raise ValueError(
            f"Faltan columnas requeridas: {missing}. "
            f"Columnas detectadas en tu CSV (normalizadas): {cols}"
        )

    # Limpieza básica
    df = df.copy()
    df["name"] = df["name"].astype(str).str.strip()
    df["city"] = df["city"].astype(str).str.strip()
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # Reglas
    valid_name = df["name"].str.len() >= 2
    valid_city = df["city"].str.len() >= 2
    valid_age = df["age"].between(0, 120)

    is_valid = valid_name & valid_city & valid_age

    df_valid = df[is_valid].copy()
    df_rejected = df[~is_valid].copy()

    # Razones específicas
    if not df_rejected.empty:
        # vectorizado por prioridad
        reason = pd.Series("unknown", index=df_rejected.index)

        reason[df_rejected["name"].str.len() < 2] = "invalid_name"
        reason[df_rejected["city"].str.len() < 2] = "invalid_city"
        reason[df_rejected["age"].isna() | ~df_rejected["age"].between(0, 120)] = "invalid_age"

        df_rejected["rejection_reason"] = reason.values

    return df_valid, df_rejected
