# ETL-batch-en-python-con-SQLite
Este proyecto implementa un ETL batch (Extract–Transform–Load) en Python que procesa múltiples archivos CSV, aplica validaciones y transformaciones robustas con pandas, y carga los datos limpios en una base de datos SQLite, manteniendo auditoría completa e idempotencia por archivo. El objetivo es demostrar buenas prácticas de ingeniería de datos

Arquitectura del ETL

El flujo general es:

CSV (input) 
  → Transformaciones (pandas)
  → Validación y separación de rechazados
  → Carga en SQLite
  → Auditoría por corrida y por archivo
  
Capas del sistema
Extract
Lectura de múltiples archivos CSV desde una carpeta de entrada.
Transform
Limpieza y validación de datos usando pandas:
Normalización de nombres de columnas
Soporte para columnas en español/variantes
Reglas de validación vectorizadas
Separación explícita de registros válidos y rechazados
Load
Inserción en SQLite con claves foráneas y tablas normalizadas.
Auditoría

Registro de:
Corridas del ETL (etl_runs)
Archivos procesados (etl_files)
Idempotencia por nombre de archivo

Estructura del proyecto
dia28_etl/
├── etl_cli.py          # CLI para ejecutar el ETL con logging
├── etl_batch.py        # Lógica principal del ETL
├── transform.py        # Transformaciones y validaciones con pandas
├── etl.db              # Base de datos SQLite
├── in/                 # CSV de entrada
├── data/
│   └── rejected/       # Registros rechazados
└── README.md

Transformaciones y validación
La función central de transformación es:
transform_dataframe(df) -> (df_valid, df_rejected)

Características:
Normaliza nombres de columnas (lowercase, sin acentos, _)
Mapea automáticamente sinónimos comunes:
nombre → name
edad → age
ciudad → city
Reglas de validación:
name: longitud mínima
age: rango válido (0–120)
city: longitud mínima
Registros inválidos se guardan con rejection_reason
Esto permite:
Trazabilidad
Análisis de calidad de datos
Reprocesamiento controlado

Modelo de datos (SQLite)
Tablas principales
etl_runs
run_id
started_at
etl_files
file_name
run_id
processed_at
status
cities
id
name
persons
id
name
age
city_id

El diseño separa catálogos y hechos, evitando duplicación y facilitando análisis posteriores.

Idempotencia

El ETL es idempotente por archivo:

Si un archivo CSV ya fue procesado (registrado en etl_files), se omite automáticamente.
Esto permite ejecutar el ETL múltiples veces sin duplicar datos.
Archivos nuevos o con nombre distinto sí se procesan.

Interfaz de línea de comandos (CLI)
El proyecto incluye un CLI con argparse y logging en colores:
Ejecución normal
python etl_cli.py
Dry-run (no toca la base de datos)
python etl_cli.py --dry-run
Cambiar nivel de logging
python etl_cli.py --log-level DEBUG

Los logs están coloreados para facilitar el diagnóstico:
INFO → azul
WARNING → amarillo
ERROR → rojo

Requisitos
Python 3.10+
pandas
sqlite3 (incluido en Python)
Instalación de dependencias:
pip install pandas

Resultados esperados

Al ejecutar el ETL:
Los registros válidos se insertan en SQLite
Los registros inválidos se guardan como CSV en data/rejected/
Se registra una nueva corrida en etl_runs
Se mantiene auditoría completa por archivo

Ejemplo de resumen:
files_total: 4
files_processed: 2
files_skipped: 2
rows_valid: 8
rows_rejected: 2

Próximas mejoras
Tests unitarios con pytest
Idempotencia basada en hash del contenido
Exportación de métricas de calidad de datos
Soporte para otras fuentes (APIs, JSON)

Autor
Proyecto desarrollado como parte de un plan de formación estructurado en Python aplicado a Data Engineering, con énfasis en buenas prácticas y diseño profesional.
