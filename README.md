# Data Warehouse para Análisis de E-commerce - Olist Dataset

## Descripción del Proyecto

Este proyecto implementa un pipeline ETL/ELT completo para construir un Data Warehouse con modelo en estrella, utilizando el dataset de Olist (e-commerce brasileño).

## Arquitectura del Pipeline

```
CSV Files → PostgreSQL (OLTP) → Data Lake (Parquet) → Transformación (Spark/dbt) → Data Warehouse (OLAP)
                                                                                          ↓
                                                                                    BI Tools (Looker/Tableau/Power BI)
```

## Estructura del Proyecto

```
Análisis de E-commerce/
├── config/
│   ├── config.yaml              # Configuración general
│   └── db_config.py             # Configuración de bases de datos
├── data/
│   ├── raw/                     # CSVs originales
│   ├── staging/                 # Parquet files
│   └── processed/               # Datos procesados
├── scripts/
│   ├── 01_extract/
│   │   ├── load_csv_to_oltp.py    # Carga CSVs a PostgreSQL
│   │   └── extract_from_oltp.py   # Extrae datos de OLTP
│   ├── 02_staging/
│   │   └── load_to_datalake.py    # Carga a Data Lake (Parquet)
│   ├── 03_transform/
│   │   ├── data_cleaning.py       # Limpieza de datos
│   │   ├── create_dimensions.py   # Crea tablas dimensión
│   │   └── create_fact_table.py   # Crea tabla de hechos
│   ├── 04_load/
│   │   └── load_to_dwh.py         # Carga al Data Warehouse
│   └── 05_analysis/
│       └── business_queries.py    # Queries de negocio
├── sql/
│   ├── oltp_schema.sql          # Schema OLTP
│   ├── dwh_schema.sql           # Schema DWH (Star Schema)
│   └── analysis_queries.sql     # Queries de análisis
├── dags/
│   └── olist_etl_dag.py         # DAG de Airflow
├── notebooks/
│   └── exploratory_analysis.ipynb
├── requirements.txt
└── README.md
```

## Modelo en Estrella

### Tabla de Hechos (Fact Table)
- **fct_orders**: Contiene métricas y claves foráneas a dimensiones

### Tablas de Dimensiones (Dimension Tables)
- **dim_customers**: Información de clientes
- **dim_products**: Información de productos
- **dim_sellers**: Información de vendedores
- **dim_geolocation**: Información geográfica
- **dim_date**: Dimensión de tiempo

## Instalación y Configuración

### 1. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar PostgreSQL

Instalar PostgreSQL y crear las bases de datos:
```sql
CREATE DATABASE olist_oltp;
CREATE DATABASE olist_dwh;
```

### 3. Configurar Variables de Entorno

Crear archivo `.env`:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_password
OLTP_DB=olist_oltp
DWH_DB=olist_dwh
```

### 4. Configurar Apache Airflow (Opcional)

```bash
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

## Ejecución del Pipeline

### Opción 1: Ejecutar paso a paso

```bash
# 1. Cargar CSVs a OLTP
python scripts/01_extract/load_csv_to_oltp.py

# 2. Extraer a Staging
python scripts/02_staging/load_to_datalake.py

# 3. Transformar datos
python scripts/03_transform/data_cleaning.py
python scripts/03_transform/create_dimensions.py
python scripts/03_transform/create_fact_table.py

# 4. Cargar a DWH
python scripts/04_load/load_to_dwh.py

# 5. Ejecutar análisis
python scripts/05_analysis/business_queries.py
```

### Opción 2: Usar Airflow

```bash
# Iniciar Airflow
airflow webserver -p 8080
airflow scheduler

# El DAG se ejecutará automáticamente según la programación
```

## Preguntas de Negocio Resueltas

1. **¿Cuáles son los productos más vendidos por mes?**
2. **¿De qué estados provienen los clientes más valiosos?**
3. **¿Cuál es el tiempo promedio de entrega por región?**
4. **¿Cuáles son las categorías de productos más rentables?**
5. **¿Cuál es el comportamiento de compra de clientes recurrentes?**

## Tecnologías Utilizadas

- **Extracción**: Python, Pandas, SQLAlchemy
- **Staging**: Parquet, Data Lake local/S3
- **Transformación**: PySpark / dbt
- **Data Warehouse**: PostgreSQL / BigQuery / Snowflake
- **Orquestación**: Apache Airflow
- **Visualización**: Looker Studio / Tableau / Power BI

## Notas

- El pipeline está diseñado para ejecutarse de forma batch (diariamente)
- Se puede adaptar para usar servicios en la nube (AWS, GCP, Azure)
- El modelo en estrella facilita queries analíticas eficientes
