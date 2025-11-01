# Guía de Instalación y Configuración
# Data Warehouse para Análisis de E-commerce - Olist Dataset

## Requisitos Previos

### Software Requerido
- Python 3.9 o superior
- PostgreSQL 12 o superior
- Git (opcional)

## Guía de Instalación Paso a Paso

### Paso 1: Instalar PostgreSQL

#### En Windows:
1. Descargar PostgreSQL desde: https://www.postgresql.org/download/windows/
2. Ejecutar el instalador
3. Durante la instalación, anotar:
   - Usuario: `postgres` (por defecto)
   - Contraseña: (la que elijas)
   - Puerto: `5432` (por defecto)

#### Verificar instalación:
```powershell
psql --version
```

### Paso 2: Crear las Bases de Datos

Abrir PowerShell y ejecutar:

```powershell
# Conectar a PostgreSQL
psql -U postgres

# Crear bases de datos
CREATE DATABASE olist_oltp;
CREATE DATABASE olist_dwh;

# Verificar
\l

# Salir
\q
```

### Paso 3: Configurar Python

#### Crear entorno virtual (recomendado):
```powershell
# Navegar al directorio del proyecto
cd "C:\Users\Alejandro\Documents\Cosas_Universidad\Doble_Titulo\Dasci_Brest\Programas Python\Análisis de E-commerce"

# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
.\venv\Scripts\Activate.ps1

# Si hay error de permisos, ejecutar:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

#### Instalar dependencias:
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

### Paso 4: Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```powershell
# Copiar el archivo de ejemplo
Copy-Item .env.example .env
```

Editar el archivo `.env` con tus credenciales:

```
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=TU_CONTRASEÑA_AQUI

# Database Names
OLTP_DB=olist_oltp
DWH_DB=olist_dwh

# Data Lake Configuration
DATA_LAKE_PATH=./data/staging
DATA_LAKE_TYPE=local
```

**IMPORTANTE:** Reemplazar `TU_CONTRASEÑA_AQUI` con tu contraseña de PostgreSQL.

### Paso 5: Verificar Conexión a la Base de Datos

```powershell
python config/db_config.py
```

Deberías ver:
```
✓ Conexión OLTP exitosa
✓ Conexión DWH exitosa
```

## Ejecución del Pipeline

### Opción 1: Ejecutar Pipeline Completo

```powershell
python run_pipeline.py
```

Este comando ejecutará todas las fases automáticamente:
1. Extracción (5-10 min)
2. Staging (3-5 min)
3. Transformación (10-15 min)
4. Análisis (2-3 min)

**Tiempo total estimado:** 20-35 minutos

### Opción 2: Ejecutar Paso a Paso

```powershell
# 1. Cargar CSVs a OLTP
python scripts/01_extract/load_csv_to_oltp.py

# 2. Extraer a Data Lake (Staging)
python scripts/02_staging/load_to_datalake.py

# 3. Limpiar datos
python scripts/03_transform/data_cleaning.py

# 4. Crear dimensiones
python scripts/03_transform/create_dimensions.py

# 5. Crear tabla de hechos
python scripts/03_transform/create_fact_table.py

# 6. Ejecutar análisis
python scripts/05_analysis/business_queries.py
```

## Verificar Resultados

### Verificar que los archivos CSV están en data/raw/:

```powershell
# Ver archivos CSV
ls data\raw\*.csv
```

Deberías ver 9 archivos CSV.

### Verificar tablas en PostgreSQL:

```powershell
# Conectar a la base DWH
psql -U postgres -d olist_dwh

# Ver tablas
\dt

# Ver registros
SELECT COUNT(*) FROM fct_orders;
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;

# Salir
\q
```

### Ver archivos generados:

```powershell
# Archivos Parquet (Data Lake)
ls data/staging/

# Datos procesados
ls data/processed/

# Reportes de análisis
ls data/analysis/

# Logs
ls logs/
```

## Configuración de Apache Airflow (Opcional)

Si quieres usar Airflow para orquestar el pipeline:

### Instalar Airflow:

```powershell
# Establecer AIRFLOW_HOME
$env:AIRFLOW_HOME = "$PWD\airflow"

# Instalar Airflow
pip install apache-airflow==2.8.0

# Inicializar base de datos
airflow db init

# Crear usuario admin
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com `
  --password admin
```

### Configurar DAG:

```powershell
# Copiar DAG al directorio de Airflow
mkdir $env:AIRFLOW_HOME\dags
Copy-Item dags\olist_etl_dag.py $env:AIRFLOW_HOME\dags\
```

### Iniciar Airflow:

```powershell
# Terminal 1: Webserver
airflow webserver -p 8080

# Terminal 2: Scheduler
airflow scheduler
```

Abrir navegador en: http://localhost:8080

## Solución de Problemas

### Error: "psycopg2 not found"
```powershell
pip install psycopg2-binary
```

### Error: "Permission denied" al activar venv
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Error: "Connection refused" a PostgreSQL
1. Verificar que PostgreSQL esté corriendo:
   ```powershell
   Get-Service postgresql*
   ```
2. Iniciar si está detenido:
   ```powershell
   Start-Service postgresql-x64-14  # Ajustar versión
   ```

### Error: "File not found"
Asegurarse de estar en el directorio correcto:
```powershell
cd "C:\Users\Alejandro\Documents\Cosas_Universidad\Doble_Titulo\Dasci_Brest\Programas Python\Análisis de E-commerce"
```

## Conectar Herramientas de BI

### Power BI:

1. Abrir Power BI Desktop
2. Obtener Datos → PostgreSQL
3. Servidor: `localhost`
4. Base de datos: `olist_dwh`
5. Seleccionar tablas: `fct_orders`, `dim_*`

### Tableau:

1. Conectar → PostgreSQL
2. Servidor: `localhost`
3. Base de datos: `olist_dwh`
4. Usar tablas del esquema star

### Looker Studio (Google Data Studio):

Requiere configurar connector a PostgreSQL o exportar a BigQuery.

## Soporte

Si encuentras problemas:
1. Revisar logs en `logs/`
2. Verificar configuración en `.env`
3. Verificar que PostgreSQL esté corriendo
4. Verificar conexión: `python config/db_config.py`

## Checklist de Instalación

- [ ] PostgreSQL instalado
- [ ] Bases de datos creadas (`olist_oltp`, `olist_dwh`)
- [ ] Python 3.9+ instalado
- [ ] Entorno virtual creado y activado
- [ ] Dependencias instaladas (`requirements.txt`)
- [ ] Archivo `.env` configurado
- [ ] Conexión a base de datos verificada
- [ ] Pipeline ejecutado exitosamente
- [ ] Resultados verificados en PostgreSQL
- [ ] Reportes generados en `data/analysis/`
