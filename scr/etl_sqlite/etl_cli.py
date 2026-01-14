from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from etl_batch2 import run_batch


class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[90m",
        "INFO": "\033[94m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "CRITICAL": "\033[95m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        return f"{color}{super().format(record)}{self.RESET}"


def setup_logging(level: str) -> None:
    level = level.upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter("%(asctime)s | %(levelname)s | %(message)s"))

    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    root.handlers.clear()
    root.addHandler(handler)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CLI para ejecutar el ETL batch (CSV -> SQLite)")

    p.add_argument("--input-dir", default="in", help="Carpeta con CSVs (relativa al script). Default: in")
    p.add_argument("--db", default="etl.db", help="Archivo SQLite (relativo al script). Default: etl.db")
    p.add_argument("--rejected-dir", default="data/rejected", help="Carpeta rechazados. Default: data/rejected")
    p.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR. Default: INFO")
    p.add_argument("--dry-run", action="store_true", help="Solo lista CSVs, no carga DB.")

    # ✅ NUEVO: Quality gate
    p.add_argument(
        "--max-reject-rate",
        type=float,
        default=None,
        help="Umbral máximo de rechazo permitido (0.0–1.0). Si se excede, el archivo falla calidad.",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    log = logging.getLogger("etl_cli")

    base_dir = Path(__file__).resolve().parent
    input_dir = (base_dir / args.input_dir).resolve()
    db_path = (base_dir / args.db).resolve()
    rejected_dir = (base_dir / args.rejected_dir).resolve()

    log.info("ETL CLI iniciado")
    log.info(
        "input_dir=%s | db=%s | rejected_dir=%s | dry_run=%s | max_reject_rate=%s",
        input_dir, db_path, rejected_dir, args.dry_run, args.max_reject_rate
    )

    if not input_dir.exists():
        log.error("No existe la carpeta de entrada: %s", input_dir)
        return 2

    csvs = sorted(input_dir.glob("*.csv"))
    log.info("Encontré %d archivo(s) CSV", len(csvs))

    if args.dry_run:
        for f in csvs:
            log.info(" - %s", f.name)
        log.info("DRY-RUN finalizado (no se tocó la DB).")
        return 0

    if not csvs:
        log.warning("No hay CSVs para procesar.")
        return 0

    summary = run_batch(
        input_dir=input_dir,
        db_path=db_path,
        rejected_dir=rejected_dir,
        max_reject_rate=args.max_reject_rate,
    )

    log.info("ETL terminado ✅")
    log.info("Resumen: %s", summary)

    # ✅ Si falló calidad en algún archivo, regresa código distinto (útil en automatizaciones)
    if summary.get("files_failed_quality", 0) > 0:
        log.warning("Hubo %d archivo(s) que fallaron quality gate.", summary["files_failed_quality"])
        return 4

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
