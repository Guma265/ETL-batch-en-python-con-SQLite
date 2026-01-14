from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from transform import transform_dataframe


def ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Crea las tablas necesarias si no existen.
    Puede ejecutarse múltiples veces (idempotente).
    """
    cur = conn.cursor()

    # Corridas ETL
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL
        )
        """
    )

    # Auditoría por archivo (idempotencia por file_name)
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

    # Catálogo de ciudades
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )

    # Hechos: personas
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

    conn.commit()


def run_batch(
    input_dir: Path | str,
    db_path: Path | str,
    rejected_dir: Path | str,
) -> dict:
    """
    ETL batch:
    - procesa múltiples CSV en input_dir
    - transforma con pandas (transform_dataframe)
    - carga a SQLite (cities/persons)
    - genera rechazados a rejected_dir
    - idempotencia por archivo (etl_files.file_name)
    - auditoría por corrida (etl_runs) y archivo (etl_files)
    """
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
        "rows_valid": 0,
        "rows_rejected": 0,
        "db_path": str(db_path),
    }

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # ✅ Asegura esquema ANTES de insertar cualquier cosa
    ensure_schema(conn)

    cur = conn.cursor()

    # Registrar corrida
    cur.execute(
        """
        INSERT INTO etl_runs (run_id, started_at)
        VALUES (?, ?)
        """,
        (run_id, started_at),
    )
    conn.commit()

    csv_files = sorted(input_dir.glob("*.csv"))
    summary["files_total"] = len(csv_files)

    for csv_path in csv_files:
        file_name = csv_path.name

        # Idempotencia por archivo (si ya existe en etl_files, se salta)
        already = cur.execute(
            "SELECT 1 FROM etl_files WHERE file_name = ?",
            (file_name,),
        ).fetchone()

        if already:
            summary["files_skipped"] += 1
            continue

        # Extract
        df_raw = pd.read_csv(csv_path)

        # Transform (pandas)
        df_valid, df_rejected = transform_dataframe(df_raw)

        try:
            # Load: ciudades (insert or ignore)
            for city in df_valid["city"].dropna().unique():
                city = str(city).strip()
                if not city:
                    continue
                cur.execute(
                    "INSERT OR IGNORE INTO cities (name) VALUES (?)",
                    (city,),
                )

            # Load: personas
            for _, row in df_valid.iterrows():
                city_name = str(row["city"]).strip()
                city_row = cur.execute(
                    "SELECT id FROM cities WHERE name = ?",
                    (city_name,),
                ).fetchone()

                if city_row is None:
                    # Esto no debería pasar si insertamos cities arriba, pero por seguridad:
                    cur.execute(
                        "INSERT OR IGNORE INTO cities (name) VALUES (?)",
                        (city_name,),
                    )
                    city_row = cur.execute(
                        "SELECT id FROM cities WHERE name = ?",
                        (city_name,),
                    ).fetchone()

                city_id = int(city_row[0])

                cur.execute(
                    """
                    INSERT INTO persons (name, age, city_id)
                    VALUES (?, ?, ?)
                    """,
                    (str(row["name"]).strip(), int(row["age"]), city_id),
                )

            # Guardar rechazados
            if not df_rejected.empty:
                rejected_path = rejected_dir / f"rejected_{file_name}"
                df_rejected.to_csv(rejected_path, index=False)

            # Auditoría del archivo
            cur.execute(
                """
                INSERT INTO etl_files (file_name, run_id, processed_at, status)
                VALUES (?, ?, ?, ?)
                """,
                (
                    file_name,
                    run_id,
                    datetime.utcnow().isoformat(timespec="seconds"),
                    "processed",
                ),
            )

            conn.commit()

            summary["files_processed"] += 1
            summary["rows_valid"] += len(df_valid)
            summary["rows_rejected"] += len(df_rejected)

        except Exception:
            conn.rollback()
            raise

    conn.close()
    return summary


if __name__ == "__main__":
    # Defaults para correr sin CLI (ajusta si tu carpeta se llama distinto)
    result = run_batch(input_dir="in", db_path="etl.db", rejected_dir="data/rejected")
    print("ETL OK:", result)
