from __future__ import annotations

import json
import sqlite3
import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Dict, List

import pandas as pd

from transform import transform, TransformReport


# =========================
# Exit codes
# =========================
EXIT_OK = 0
EXIT_CONTRACT_FAIL = 2
EXIT_QUALITY_GATE_FAIL = 3
EXIT_RUNTIME_FAIL = 10


# =========================
# Result models
# =========================
@dataclass(frozen=True)
class FileResult:
    file_path: str
    file_hash: str
    status: str  # OK | SKIPPED | FAILED_CONTRACT | FAILED_QUALITY_GATE | FAILED_RUNTIME
    rows_in: int
    rows_valid: int
    rows_rejected: int
    reject_rate: float
    duration_ms: int
    missing_required_columns: List[str]
    rejection_reason_counts: Dict[str, int]
    error: Optional[str] = None


@dataclass(frozen=True)
class BatchResult:
    run_id: Optional[int]
    started_at_utc: str
    finished_at_utc: str
    duration_ms: int
    total_files_seen: int
    total_files_processed: int
    total_files_skipped: int
    total_files_failed: int
    exit_code: int
    file_results: List[FileResult]


def _file_result(
    path: Path,
    file_hash: str,
    status: str,
    report: TransformReport,
    duration_ms: int,
    error: Optional[str],
) -> FileResult:
    return FileResult(
        file_path=str(path),
        file_hash=file_hash,
        status=status,
        rows_in=report.rows_in,
        rows_valid=report.rows_valid,
        rows_rejected=report.rows_rejected,
        reject_rate=report.reject_rate,
        duration_ms=duration_ms,
        missing_required_columns=report.missing_required_columns,
        rejection_reason_counts=report.rejection_reason_counts,
        error=error,
    )


# =========================
# Logger fallback
# =========================
class SimpleLogger:
    def info(self, msg: str, *args) -> None:
        print(msg % args if args else msg)

    def warning(self, msg: str, *args) -> None:
        print("[WARN] " + (msg % args if args else msg))

    def error(self, msg: str, *args) -> None:
        print("[ERROR] " + (msg % args if args else msg))


# =========================
# Public entry
# =========================
def run_batch(
    input_dir: str | Path,
    db_path: str | Path,
    max_reject_rate: float = 0.2,
    dry_run: bool = False,
    logger=None,
) -> BatchResult:
    """
    Day 33:
      - structured logs (key=value)
      - SQL-ready metrics already stored in etl_quality_metrics
      - schema drift protection in _ensure_schema
    """
    logger = logger or SimpleLogger()
    input_dir = Path(input_dir)
    db_path = Path(db_path)

    started_utc = _utc_now_iso()
    t0 = time.time()

    csv_files = sorted(_discover_csv_files(input_dir))
    total_files_seen = len(csv_files)
    file_results: List[FileResult] = []

    logger.info(
        "event=batch_start input_dir=%s db=%s dry_run=%s max_reject_rate=%.3f files_seen=%d started_at_utc=%s",
        str(input_dir),
        str(db_path),
        dry_run,
        float(max_reject_rate),
        total_files_seen,
        started_utc,
    )

    if dry_run:
        # No DB writes
        for p in csv_files:
            fr = _process_one_file_dry(p, max_reject_rate, logger)
            file_results.append(fr)

        finished_utc = _utc_now_iso()
        duration_ms = int((time.time() - t0) * 1000)

        exit_code = _exit_code_from_results(file_results)
        processed = sum(1 for r in file_results if r.status == "OK")
        skipped = sum(1 for r in file_results if r.status == "SKIPPED")
        failed = sum(1 for r in file_results if r.status.startswith("FAILED"))

        logger.info(
            "event=batch_finish run_id=%s status=%s exit_code=%d duration_ms=%d files_seen=%d files_processed=%d files_skipped=%d files_failed=%d finished_at_utc=%s",
            "null",
            _run_status(exit_code),
            int(exit_code),
            int(duration_ms),
            int(total_files_seen),
            int(processed),
            int(skipped),
            int(failed),
            finished_utc,
        )

        return BatchResult(
            run_id=None,
            started_at_utc=started_utc,
            finished_at_utc=finished_utc,
            duration_ms=duration_ms,
            total_files_seen=total_files_seen,
            total_files_processed=processed,
            total_files_skipped=skipped,
            total_files_failed=failed,
            exit_code=exit_code,
            file_results=file_results,
        )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        _ensure_schema(conn)

        run_id = _insert_run_start(conn, input_dir, max_reject_rate)
        logger.info("event=run_created run_id=%d", int(run_id))

        processed = skipped = failed = 0

        for p in csv_files:
            fr = _process_one_file(conn, run_id, p, max_reject_rate, logger)
            file_results.append(fr)

            if fr.status == "OK":
                processed += 1
            elif fr.status == "SKIPPED":
                skipped += 1
            else:
                failed += 1

        finished_utc = _utc_now_iso()
        duration_ms = int((time.time() - t0) * 1000)
        exit_code = _exit_code_from_results(file_results)

        _update_run_finish(
            conn,
            run_id,
            _run_status(exit_code),
            finished_utc,
            duration_ms,
            total_files_seen,
            processed,
            skipped,
            failed,
        )

        logger.info(
            "event=batch_finish run_id=%d status=%s exit_code=%d duration_ms=%d files_seen=%d files_processed=%d files_skipped=%d files_failed=%d finished_at_utc=%s",
            int(run_id),
            _run_status(exit_code),
            int(exit_code),
            int(duration_ms),
            int(total_files_seen),
            int(processed),
            int(skipped),
            int(failed),
            finished_utc,
        )

        return BatchResult(
            run_id=run_id,
            started_at_utc=started_utc,
            finished_at_utc=finished_utc,
            duration_ms=duration_ms,
            total_files_seen=total_files_seen,
            total_files_processed=processed,
            total_files_skipped=skipped,
            total_files_failed=failed,
            exit_code=exit_code,
            file_results=file_results,
        )

    except Exception as e:
        # Structured failure
        logger.error("event=batch_runtime_failure error=%s", str(e))
        raise
    finally:
        conn.close()


# =========================
# Schema + migration (PRO)
# =========================
def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    return r is not None


def _get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {r[1] for r in rows}


def _rename_table(conn: sqlite3.Connection, old: str, new: str) -> None:
    conn.execute(f"ALTER TABLE {old} RENAME TO {new};")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                status TEXT NOT NULL,
                input_dir TEXT NOT NULL,
                max_reject_rate REAL NOT NULL,
                duration_ms INTEGER,
                files_seen INTEGER,
                files_processed INTEGER,
                files_skipped INTEGER,
                files_failed INTEGER
            );
            """
        )

        # --- Schema drift protection: etl_files ---
        if _table_exists(conn, "etl_files"):
            cols = _get_columns(conn, "etl_files")
            required = {
                "file_path",
                "file_hash",
                "status",
                "rows_in",
                "rows_valid",
                "rows_rejected",
                "reject_rate",
                "duration_ms",
                "created_at_utc",
                "run_id",
            }
            if not required.issubset(cols):
                legacy = f"etl_files_legacy_{int(time.time())}"
                _rename_table(conn, "etl_files", legacy)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                rows_in INTEGER NOT NULL,
                rows_valid INTEGER NOT NULL,
                rows_rejected INTEGER NOT NULL,
                reject_rate REAL NOT NULL,
                duration_ms INTEGER NOT NULL,
                error TEXT,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
            );
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_etl_files_path_hash_status
            ON etl_files(file_path, file_hash, status);
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_quality_metrics (
                metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                rows_in INTEGER NOT NULL,
                rows_valid INTEGER NOT NULL,
                rows_rejected INTEGER NOT NULL,
                reject_rate REAL NOT NULL,
                duration_ms INTEGER NOT NULL,
                reason_counts_json TEXT NOT NULL,
                missing_required_columns_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
            );
            """
        )

        # Domain tables
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cities (
                city_id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_name TEXT NOT NULL UNIQUE
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS persons (
                person_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                city_id INTEGER,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (city_id) REFERENCES cities(city_id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS persons_rejected (
                rejected_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_file TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                rejection_reason TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES etl_runs(run_id)
            );
            """
        )


# =========================
# File processing
# =========================
def _process_one_file_dry(path: Path, max_reject_rate: float, logger) -> FileResult:
    t0 = time.time()
    h = _sha256_file(path)

    try:
        raw = pd.read_csv(path)
        valid, rejected, report = transform(raw)
        ms = int((time.time() - t0) * 1000)

        status = "OK"
        if report.critical_failure:
            status = "FAILED_CONTRACT"
        elif report.reject_rate > max_reject_rate:
            status = "FAILED_QUALITY_GATE"

        # Day 33: structured per-file log
        logger.info(
            "event=file_result mode=dry file=%s status=%s rows_in=%d rows_valid=%d rows_rejected=%d reject_rate=%.3f duration_ms=%d",
            path.name,
            status,
            report.rows_in,
            report.rows_valid,
            report.rows_rejected,
            report.reject_rate,
            ms,
        )

        return _file_result(path, h, status, report, ms, None)

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        logger.error(
            "event=file_result mode=dry file=%s status=FAILED_RUNTIME duration_ms=%d error=%s",
            path.name,
            ms,
            str(e),
        )
        return FileResult(
            file_path=str(path),
            file_hash=h,
            status="FAILED_RUNTIME",
            rows_in=0,
            rows_valid=0,
            rows_rejected=0,
            reject_rate=0.0,
            duration_ms=ms,
            missing_required_columns=[],
            rejection_reason_counts={},
            error=str(e),
        )


def _process_one_file(
    conn: sqlite3.Connection,
    run_id: int,
    path: Path,
    max_reject_rate: float,
    logger,
) -> FileResult:
    t0 = time.time()
    h = _sha256_file(path)

    # Idempotency: if already OK for same file_path+hash, skip
    if _already_ok(conn, path, h):
        logger.info(
            "event=file_result mode=real file=%s status=SKIPPED reason=idempotent",
            path.name,
        )
        return FileResult(
            file_path=str(path),
            file_hash=h,
            status="SKIPPED",
            rows_in=0,
            rows_valid=0,
            rows_rejected=0,
            reject_rate=0.0,
            duration_ms=0,
            missing_required_columns=[],
            rejection_reason_counts={},
            error=None,
        )

    try:
        raw = pd.read_csv(path)
        valid, rejected, report = transform(raw)
        ms = int((time.time() - t0) * 1000)

        status = "OK"
        if report.critical_failure:
            status = "FAILED_CONTRACT"
        elif report.reject_rate > max_reject_rate:
            status = "FAILED_QUALITY_GATE"

        # Persist audits + metrics always (even on failures)
        _insert_file_audit(conn, run_id, path, h, status, report, ms)
        _insert_quality(conn, run_id, path, h, report, ms)

        if status == "OK":
            # Keep load atomic-ish per file
            with conn:
                _load_valid(conn, valid)
                _load_rejected(conn, rejected, run_id, path, h)

        # Day 33: structured per-file log (real)
        logger.info(
            "event=file_result mode=real run_id=%d file=%s status=%s rows_in=%d rows_valid=%d rows_rejected=%d reject_rate=%.3f duration_ms=%d",
            int(run_id),
            path.name,
            status,
            report.rows_in,
            report.rows_valid,
            report.rows_rejected,
            report.reject_rate,
            ms,
        )

        # Optional: show missing required columns (contract fail)
        if status == "FAILED_CONTRACT":
            logger.error(
                "event=contract_failure run_id=%d file=%s missing_required_columns=%s",
                int(run_id),
                path.name,
                json.dumps(report.missing_required_columns, ensure_ascii=False),
            )

        return _file_result(path, h, status, report, ms, None)

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        status = "FAILED_RUNTIME"

        # Persist audit record of runtime failure
        _insert_file_audit_runtime_error(conn, run_id, path, h, status, ms, str(e))

        logger.error(
            "event=file_result mode=real run_id=%d file=%s status=FAILED_RUNTIME duration_ms=%d error=%s",
            int(run_id),
            path.name,
            ms,
            str(e),
        )

        return FileResult(
            file_path=str(path),
            file_hash=h,
            status="FAILED_RUNTIME",
            rows_in=0,
            rows_valid=0,
            rows_rejected=0,
            reject_rate=0.0,
            duration_ms=ms,
            missing_required_columns=[],
            rejection_reason_counts={},
            error=str(e),
        )


# =========================
# DB helpers
# =========================
def _insert_run_start(conn: sqlite3.Connection, input_dir: Path, max_reject_rate: float) -> int:
    cur = conn.execute(
        """
        INSERT INTO etl_runs(started_at_utc, status, input_dir, max_reject_rate)
        VALUES (?, ?, ?, ?)
        """,
        (_utc_now_iso(), "RUNNING", str(input_dir), float(max_reject_rate)),
    )
    return int(cur.lastrowid)


def _update_run_finish(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    finished_utc: str,
    duration_ms: int,
    files_seen: int,
    files_processed: int,
    files_skipped: int,
    files_failed: int,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE etl_runs
            SET finished_at_utc=?,
                status=?,
                duration_ms=?,
                files_seen=?,
                files_processed=?,
                files_skipped=?,
                files_failed=?
            WHERE run_id=?
            """,
            (
                finished_utc,
                status,
                int(duration_ms),
                int(files_seen),
                int(files_processed),
                int(files_skipped),
                int(files_failed),
                int(run_id),
            ),
        )


def _insert_file_audit(
    conn: sqlite3.Connection,
    run_id: int,
    path: Path,
    file_hash: str,
    status: str,
    report: TransformReport,
    duration_ms: int,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO etl_files
            (run_id, file_path, file_hash, status,
             rows_in, rows_valid, rows_rejected, reject_rate,
             duration_ms, error, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(run_id),
                str(path),
                file_hash,
                status,
                int(report.rows_in),
                int(report.rows_valid),
                int(report.rows_rejected),
                float(report.reject_rate),
                int(duration_ms),
                None,
                _utc_now_iso(),
            ),
        )


def _insert_file_audit_runtime_error(
    conn: sqlite3.Connection,
    run_id: int,
    path: Path,
    file_hash: str,
    status: str,
    duration_ms: int,
    error: str,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO etl_files
            (run_id, file_path, file_hash, status,
             rows_in, rows_valid, rows_rejected, reject_rate,
             duration_ms, error, created_at_utc)
            VALUES (?, ?, ?, ?, 0, 0, 0, 0.0, ?, ?, ?)
            """,
            (
                int(run_id),
                str(path),
                file_hash,
                status,
                int(duration_ms),
                error,
                _utc_now_iso(),
            ),
        )


def _insert_quality(
    conn: sqlite3.Connection,
    run_id: int,
    path: Path,
    file_hash: str,
    report: TransformReport,
    duration_ms: int,
) -> None:
    reason_json = json.dumps(report.rejection_reason_counts, ensure_ascii=False, sort_keys=True)
    missing_json = json.dumps(report.missing_required_columns, ensure_ascii=False)

    with conn:
        conn.execute(
            """
            INSERT INTO etl_quality_metrics
            (run_id, file_path, file_hash,
             rows_in, rows_valid, rows_rejected, reject_rate,
             duration_ms, reason_counts_json, missing_required_columns_json,
             created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(run_id),
                str(path),
                file_hash,
                int(report.rows_in),
                int(report.rows_valid),
                int(report.rows_rejected),
                float(report.reject_rate),
                int(duration_ms),
                reason_json,
                missing_json,
                _utc_now_iso(),
            ),
        )


def _load_valid(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    if df.empty:
        return

    # Minimal load (name, age) - city mapping lo dejamos para una iteración posterior si lo tienes ya hecho
    created_at = _utc_now_iso()
    rows = []
    for _, r in df.iterrows():
        rows.append((str(r["name"]), int(r["age"]), created_at))

    conn.executemany(
        "INSERT INTO persons(name, age, created_at_utc) VALUES (?, ?, ?)",
        rows,
    )


def _load_rejected(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    run_id: int,
    path: Path,
    file_hash: str,
) -> None:
    if df.empty:
        return

    created_at = _utc_now_iso()
    rows = []
    for _, r in df.iterrows():
        rr = str(r.get("rejection_reason", "")).strip()
        raw = r.drop(labels=["rejection_reason"], errors="ignore").to_dict()
        raw_json = json.dumps(raw, ensure_ascii=False, default=str)
        rows.append((int(run_id), str(path), file_hash, rr, raw_json, created_at))

    conn.executemany(
        """
        INSERT INTO persons_rejected
        (run_id, source_file, file_hash, rejection_reason, raw_json, created_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _already_ok(conn: sqlite3.Connection, path: Path, file_hash: str) -> bool:
    r = conn.execute(
        """
        SELECT 1
        FROM etl_files
        WHERE file_path=? AND file_hash=? AND status='OK'
        LIMIT 1
        """,
        (str(path), file_hash),
    ).fetchone()
    return r is not None


# =========================
# Utils
# =========================
def _discover_csv_files(d: Path) -> Iterable[Path]:
    yield from d.rglob("*.csv")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _exit_code_from_results(results: List[FileResult]) -> int:
    if any(r.status == "FAILED_RUNTIME" for r in results):
        return EXIT_RUNTIME_FAIL
    if any(r.status == "FAILED_CONTRACT" for r in results):
        return EXIT_CONTRACT_FAIL
    if any(r.status == "FAILED_QUALITY_GATE" for r in results):
        return EXIT_QUALITY_GATE_FAIL
    return EXIT_OK


def _run_status(code: int) -> str:
    return {
        EXIT_OK: "OK",
        EXIT_CONTRACT_FAIL: "FAILED_CONTRACT",
        EXIT_QUALITY_GATE_FAIL: "FAILED_QUALITY_GATE",
        EXIT_RUNTIME_FAIL: "FAILED_RUNTIME",
    }.get(code, "FAILED_RUNTIME")
