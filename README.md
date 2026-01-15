ETL Batch (CSV → SQLite) con auditoría, rechazados y data quality gates

Descripción
Este proyecto implementa un ETL batch en Python que procesa múltiples archivos CSV desde una carpeta, aplica limpieza/validación con pandas, carga datos normalizados en SQLite, y registra auditoría completa por corrida y por archivo.
Incluye rechazados con rejection_reason y un mecanismo de quality gate (max_reject_rate) para bloquear cargas cuando la calidad del archivo excede un umbral.

Arquitectura
Flujo principal
CSV (in/) 
  → Extract (pandas.read_csv)
  → Transform (transform_dataframe)
      - normaliza columnas
      - mapea sinónimos (nombre/edad/ciudad → name/age/city)
      - valida filas
      - separa valid vs rejected + rejection_reason
  → Quality metrics (etl_quality_metrics)
  → Quality gate (opcional)
      - si reject_rate > max_reject_rate → status=failed_quality y NO carga
  → Load (SQLite)
      - cities (catalog) + persons (facts)
  → Auditoría (etl_runs, etl_files)
  → Output rechazados (data/rejected/)
  
Capas
etl_cli.py: interfaz por terminal + logging (colores) + parámetros
etl_batch.py: orquestación del pipeline por archivo, auditoría, carga en DB
transform.py: transformación y validación (función “pura”, fácil de testear)
SQLite: persistencia + trazabilidad

Estructura del proyecto (recomendada)
etl-python-sqlite/
├── README.md
├── requirements.txt
├── .gitignore
│
├── src/
│   └── etl_sqlite/
│       ├── __init__.py
│       ├── etl_cli.py
│       ├── etl_batch.py
│       └── transform.py
│
├── tests/
│   └── test_transform.py
│
├── data/
│   ├── in/           # (no subir CSV reales)
│   └── rejected/     # (outputs)
└── database/
    └── etl.db        # (no versionar)
    
Nota: En tu entorno local puedes tener in/ y etl.db en la raíz; el repo debe evitar subir datos reales y DB generadas.

Requisitos
Python 3.10+
pandas
Instalación:
pip install -r requirements.txt

requirements.txt mínimo:
pandas>=2.0

Uso
1) Dry-run (no toca DB)
Lista archivos CSV detectados:
python etl_cli.py --dry-run

2) Ejecutar ETL normal
python etl_cli.py

3) Activar quality gate
Ejemplo: permitir como máximo 20% de filas rechazadas por archivo:
python etl_cli.py --max-reject-rate 0.20

Comportamiento:
Si un archivo excede el umbral:
se guarda rejected_<archivo>.csv
se registra status = failed_quality
NO se carga a persons/cities
el CLI retorna exit code 4 (útil para automatización)
Modelo de datos (SQLite)

Tablas de auditoría
etl_runs(run_id, started_at)
Registro de cada corrida del ETL.
etl_files(file_name, run_id, processed_at, status)
Auditoría e idempotencia por archivo (si ya existe el file_name, se omite).
Tablas de negocio
cities(id, name UNIQUE)
persons(id, name, age, city_id FK)
Tabla de calidad
etl_quality_metrics(run_id, file_name, total_rows, valid_rows, rejected_rows, reject_rate, top_rejection_reason, created_at)
Consultas útiles
Ver métricas recientes:
SELECT file_name, total_rows, valid_rows, rejected_rows, reject_rate, top_rejection_reason
FROM etl_quality_metrics
ORDER BY id DESC
LIMIT 10;

Estados por archivo:
SELECT status, COUNT(*) FROM etl_files GROUP BY status;

Autor
Guillermo MR — proyecto de formación estructurada en Python aplicado a ETL/SQLite con buenas prácticas.
