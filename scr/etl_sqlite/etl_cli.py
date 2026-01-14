from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from etl_batch import run_batch


# -------------------------------------------------
# Logging con colores
# -------------------------------------------------
class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[90m",     # gris
        "INFO": "\033[94m",      # azul
        "WARNING": "\033[93m",   # amarillo
        "ERROR": "\033[91m",     # rojo
        "CRITICAL": "\033[95m",  # magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


def setup_logging(level: str) -> None:
    level = level.upper()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ColorFormatter("%(asctime)s | %(levelname)s | %(message)s")
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    root.handlers.clear()
    root.addHandler(handler)


# -------------------------------------------------
# CLI args
# -------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CLI para ejecutar el ETL batch (CSV -> SQLite)"
    )

    parser.add_argument(
        "--input-dir",
        default="in",
        help="Carpeta con CSVs (relativa al script). Default: in",
    )
    parser.add_argument(
        "--db",
        default="etl.db",
        help="Archivo SQLite (relativo al script). Default: etl.db",
    )
    parser.add_argument(
        "--rejected-dir",
        default="data/rejected",
        help="Carpeta para rechazados (relativa al script). Default: data/rejected",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="DEBUG | INFO | WARNING | ERROR. Default: INFO",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo valida y lista CSVs, no carga DB.",
    )

    return parser.parse_args()


# -------------------------------------------------
# Main
# -------------------------------------------------
def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    log = logging.getLogger("etl_cli")

    BASE_DIR = Path(__file__).resolve().parent

    input_dir = (BASE_DIR / args.input_dir).resolve()
    db_path = (BASE_DIR / args.db).resolve()
    rejected_dir = (BASE_DIR / args.rejected_dir).resolve()

    log.info("ETL CLI iniciado")
    log.info(
        "input_dir=%s | db=%s | rejected_dir=%s | dry_run=%s",
        input_dir,
        db_path,
        rejected_dir,
        args.dry_run,
    )

    if not input_dir.exists():
        log.error("No existe la carpeta de entrada: %s", input_dir)
        return 2

    csv_files = sorted(input_dir.glob("*.csv"))
    log.info("Encontré %d archivo(s) CSV", len(csv_files))

    if args.dry_run:
        for csv in csv_files:
            log.info(" - %s", csv.name)
        log.info("DRY-RUN finalizado (no se tocó la DB).")
        return 0

    if not csv_files:
        log.warning("No hay CSVs para procesar.")
        return 0

    summary = run_batch(
        input_dir=input_dir,
        db_path=db_path,
        rejected_dir=rejected_dir,
    )

    log.info("ETL terminado correctamente ✅")
    log.info("Resumen: %s", summary)

    return 0


# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    raise SystemExit(main())
