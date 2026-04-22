# Data Project Hub

Caja de herramientas unificada para ingeniería de datos y procesos ETL.

## 🏗️ Arquitectura por Capas

- **Connections (`connections/`):** Adaptadores para PostgreSQL, MS SQL Server y SQLite.
- **Readers (`utils/readers/`):** Lectura e ingesta estandarizada de Excel y CSV (con detección automática de separadores y codificación).
- **Writers (`utils/writers/`):** Exportación inteligente a CSV y Excel (preservación de tipos nativos y auto-ajuste de columnas).
- **Logs (`utils/logs/`):** Sistema centralizado con Loguru. Separa el flujo operativo de los errores críticos en `/logs`.

## ⚙️ Configuración Global (`.env`)

- **`LIMIT`**: Controla el volumen de extracción. 
  - `0` = Extraer todo (ALL).
  - `X` = Extraer solo X cantidad de filas.
- **Conexiones**: Se definen a nivel de servidor; la base de datos se especifica dinámicamente en el código.

## 🚀 Ejecución de Pruebas y ETL

### 1. Pruebas de Conexión
```bash
# PostgreSQL
python test\connections\test_postgres_connection.py --env DBPG_DWH

# SQL Server
python test\connections\test_sqlserver_connection.py --env DB_MSSQL_PROD --db MiBaseDeDatos
```

### 2. Pruebas de Lectura (Exploración)
```bash
# Excel (busca en data/input o sandbox)
python test\readers\test_excel_reader.py --file reporte.xlsx

# CSV (Detección automática de separadores)
python test\readers\test_csv_reader.py --file datos.csv
```

### 3. Flujo ETL (E2E)
```bash
# Extraer de PGSQL y volcar directamente a un Excel formateado
python test\etl\test_pg_to_excel.py --schema public --table mi_tabla --out reporte_final.xlsx
```

## 🛠️ Instalación Rápida

1. **Entorno:** `python -m venv venv` y activa con `.\venv\Scripts\activate`.
2. **Librerías:** `pip install -r requirements.txt`.
3. **Configuración:** Copia `.env.example` a `.env` y rellena tus credenciales.