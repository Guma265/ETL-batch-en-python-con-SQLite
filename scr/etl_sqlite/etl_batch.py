from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from transform import transform_dataframe


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_files (
            file_name TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            city_id INTEGER NOT NULL,
            FOREIGN KEY (city_id) REFERENCES cities(id)
        )
        """
    )

    # ✅ NUEVO: métricas de calidad (por corrida + archivo)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_quality_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            total_rows INTEGER NOT NULL,
            valid_rows INTEGER NOT NULL,
            rejected_rows INTEGER NOT NULL,
            reject_rate REAL NOT NULL,
            top_rejection_reason TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
        )
        """
    )

    conn.commit()


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def run_batch(
    input_dir: Path | str,
    db_path: Path | str,
    rejected_dir: Path | str,
    max_reject_rate: Optional[float] = None,   # ✅ quality gate
) -> dict:
    input_dir = Path(input_dir)
    db_path = Path(db_path)
    rejected_dir = Path(rejected_dir)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat(timespec="seconds")

    summary = {
        "run_id": run_id,
        "files_total": 0,
        "files_processed": 0,
        "files_skipped": 0,
        "files_failed_quality": 0,
        "rows_valid": 0,
        "rows_rejected": 0,
        "db_path": str(db_path),
        "max_reject_rate": max_reject_rate,
    }

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn)
    cur = conn.cursor()

    # Registrar corrida
    cur.execute(
        "INSERT INTO etl_runs (run_id, started_at) VALUES (?, ?)",
        (run_id, started_at),
    )
    conn.commit()

    csv_files = sorted(input_dir.glob("*.csv"))
    summary["files_total"] = len(csv_files)

    for csv_path in csv_files:
        file_name = csv_path.name

        # Idempotencia por archivo (nombre)
        already = cur.execute(
            "SELECT 1 FROM etl_files WHERE file_name = ?",
            (file_name,),
        ).fetchone()
        if already:
            summary["files_skipped"] += 1
            continue

        # --- Extract ---
        df_raw = pd.read_csv(csv_path)

        # --- Transform ---
        df_valid, df_rejected = transform_dataframe(df_raw)

        total_rows = int(len(df_raw))
        valid_rows = int(len(df_valid))
        rejected_rows = int(len(df_rejected))
        reject_rate = (rejected_rows / total_rows) if total_rows > 0 else 0.0

        top_reason = None
        if rejected_rows > 0 and "rejection_reason" in df_rejected.columns:
            top_reason = str(df_rejected["rejection_reason"].value_counts().idxmax())

        created_at = datetime.utcnow().isoformat(timespec="seconds")

        try:
            # Guardar métricas SIEMPRE (aunque falle calidad)
            cur.execute(
                """
                INSERT INTO etl_quality_metrics
                (run_id, file_name, total_rows, valid_rows, rejected_rows, reject_rate, top_rejection_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, file_name, total_rows, valid_rows, rejected_rows, float(reject_rate), top_reason, created_at),
            )

            # ✅ Quality gate (si se configura)
            if max_reject_rate is not None and reject_rate > max_reject_rate:
                # Guardar rechazados para diagnóstico
                if not df_rejected.empty:
                    (rejected_dir / f"rejected_{file_name}").parent.mkdir(parents=True, exist_ok=True)
                    df_rejected.to_csv(rejected_dir / f"rejected_{file_name}", index=False)

                cur.execute(
                    """
                    INSERT INTO etl_files (file_name, run_id, processed_at, status)
                    VALUES (?, ?, ?, ?)
                    """,
                    (file_name, run_id, created_at, "failed_quality"),
                )

                conn.commit()
                summary["files_failed_quality"] += 1
                # No cargamos a persons/cities cuando falla calidad
                continue

            # --- Load: ciudades ---
            for city in df_valid["city"].dropna().unique():
                city = str(city).strip()
                if not city:
                    continue
                cur.execute("INSERT OR IGNORE INTO cities (name) VALUES (?)", (city,))

            # --- Load: personas ---
            for _, row in df_valid.iterrows():
                city_name = str(row["city"]).strip()
                city_id = cur.execute(
                    "SELECT id FROM cities WHERE name = ?",
                    (city_name,),
                ).fetchone()[0]

                cur.execute(
                    "INSERT INTO persons (name, age, city_id) VALUES (?, ?, ?)",
                    (str(row["name"]).strip(), int(row["age"]), int(city_id)),
                )

            # Guardar rechazados (si hubo)
            if not df_rejected.empty:
                df_rejected.to_csv(rejected_dir / f"rejected_{file_name}", index=False)

            # Auditoría del archivo
            cur.execute(
                """
                INSERT INTO etl_files (file_name, run_id, processed_at, status)
                VALUES (?, ?, ?, ?)
                """,
                (file_name, run_id, created_at, "processed"),
            )

            conn.commit()

            summary["files_processed"] += 1
            summary["rows_valid"] += valid_rows
            summary["rows_rejected"] += rejected_rows

        except Exception:
            conn.rollback()
            raise

    conn.close()
    return summary


if __name__ == "__main__":
    result = run_batch(
        input_dir="in",
        db_path="etl.db",
        rejected_dir="data/rejected",
        max_reject_rate=None,  # prueba sin gate
    )
    print("ETL OK:", result)
