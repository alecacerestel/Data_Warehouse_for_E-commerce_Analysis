# Pipeline ETL - Olist E-commerce Dataset

## Descripcion del Proyecto

Este proyecto implementa un pipeline ETL completo para procesar y analizar datos del e-commerce brasileno Olist. El sistema procesa mas de 1.5 millones de registros a traves de multiples fases de transformacion, culminando en un modelo estrella optimizado para analisis de negocio.

### Fases Implementadas

- **Fase 1: Extraccion** - Carga datos CSV a PostgreSQL OLTP (1.6M registros)
- **Fase 2: Staging** - Extrae datos OLTP a Data Lake local en formato Parquet (53 MB)
- **Fase 3: Transformacion** - Crea modelo estrella con 4 dimensiones y tabla de hechos

### Fases Futuras

- **Fase 4: Data Warehouse** - Carga a base de datos OLAP con optimizaciones
- **Fase 5: Analisis** - Queries de negocio, dashboards y KPIs


##  Fuente de Datos

Los datos utilizados en este proyecto provienen del dataset público de Olist disponible en Kaggle:

**[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

El dataset incluye información de aproximadamente 100,000 órdenes realizadas entre 2016 y 2018 en múltiples marketplaces de Brasil.

## Arquitectura del Pipeline

```
CSV Files (Raw) 
    ↓
PostgreSQL OLTP (1.6M registros)
    ↓
Data Lake Parquet (Staging - 53 MB)
    ↓
Modelo Estrella (Transformed)
    ├── dim_customers (99K)
    ├── dim_products (33K)
    ├── dim_sellers (3K)
    ├── dim_date (774)
    └── fct_orders (99K)
```
## Detalle de Fases

### Fase 1: Extraccion
- Lee 9 archivos CSV desde data/raw/
- Carga datos a PostgreSQL OLTP con validaciones
- Optimizacion especial para geolocation (1M+ registros) usando PostgreSQL COPY
- Total: 1,550,108 registros en ~99 segundos

### Fase 2: Staging
- Extrae todas las tablas desde OLTP
- Guarda en formato Parquet con compresion Snappy
- Reduccion de tamano: 75 por ciento vs CSV original
- Total: 53.51 MB en ~21 segundos

### Fase 3: Transformacion
- **Limpieza de datos**: Eliminacion de duplicados, manejo de nulos, normalizacion
- **Dimensiones creadas**:
  - dim_customers: 99,441 clientes con clasificacion regional
  - dim_products: 32,951 productos con categorias traducidas y clasificacion de tamano
  - dim_sellers: 3,095 vendedores con clasificacion regional
  - dim_date: 774 fechas con atributos de calendario completo
- **Tabla de hechos**: 
  - fct_orders: 99,441 ordenes con 19 metricas de negocio
  - Incluye: valores, tiempos de entrega, retrasos, scores de reviews
- Total: 235,361 registros en ~17 segundos

## Estructura del Proyecto

```
Analisis de E-commerce/
├── config/
│   ├── config.yaml              # Configuracion general
│   └── db_config.py             # Configuracion de base de datos
├── data/
│   ├── raw/                     # CSVs originales (9 archivos)
│   ├── staging/                 # Archivos Parquet (53 MB)
│   └── transformed/             # Modelo estrella (4 dims + 1 fact)
├── scripts/
│   ├── 01_extract/
│   │   └── load_csv_to_oltp.py  # Carga CSVs a PostgreSQL
│   ├── 02_staging/
│   │   └── load_to_staging.py   # Extrae OLTP a Parquet
│   └── 03_transform/
│       ├── data_cleaning.py     # Limpieza y validacion de datos
│       ├── create_dimensions.py # Creacion de dimensiones
│       └── create_fact_table.py # Creacion de tabla de hechos
├── sql/
│   └── oltp_schema.sql          # Schema de base de datos OLTP
├── logs/                         # Logs de ejecucion de todas las fases
├── requirements.txt              # Dependencias Python
├── run_pipeline.py               # Script principal del pipeline
├── truncate_all.py               # Limpia tablas OLTP
├── verify_staging.py             # Verifica archivos Parquet
├── verify_transformed.py         # Verifica modelo estrella
└── README.md
```

## Tablas en Base de Datos OLTP

El pipeline carga 9 tablas a PostgreSQL:

1. **customers** - Clientes 
2. **products** - Productos 
3. **sellers** - Vendedores
4. **orders** - Órdenes 
5. **order_items** - Items de orden 
6. **order_payments** - Pagos 
7. **order_reviews** - Reseñas 
8. **geolocation** - Geolocalización 
9. **product_category_name_translation** - Traducciones al Ingles

## Instalación y Configuración

### 1. Descargar Dataset

Descargar los archivos CSV desde [Kaggle - Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) y colocarlos en la carpeta `data/raw/`.

### 2. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar PostgreSQL

Instalar PostgreSQL y crear la base de datos:

```sql
CREATE DATABASE olist_oltp;
```

### 4. Configurar Variables de Entorno

Crear archivo `.env`:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_password
OLTP_DB=olist_oltp
```

### 5. Crear Schema

Ejecutar el script SQL para crear las tablas:

```bash
psql -U postgres -d olist_oltp -f sql/oltp_schema.sql
```

## Ejecucion del Pipeline

### Pipeline Completo (3 Fases)

Ejecuta todas las fases del pipeline en secuencia:

```bash
python truncate_all.py  # Limpia base de datos OLTP
python run_pipeline.py  # Ejecuta las 3 fases
```

El pipeline ejecutara:
1. **Fase 1 - Extraccion**: Carga 9 archivos CSV a PostgreSQL OLTP (1.6M registros)
2. **Fase 2 - Staging**: Extrae OLTP a Data Lake Parquet (53 MB)
3. **Fase 3 - Transformacion**: Crea modelo estrella con dimensiones y tabla de hechos

**Tiempo total**: Aproximadamente 2-3 minutos

### Ejecucion Parcial

Para ejecutar solo algunas fases, modifica los parametros en `run_pipeline.py`:

```python
run_pipeline(run_staging=True, run_transformation=True)
```

### Herramientas de Verificacion

**Verificar archivos Parquet del Data Lake:**

```bash
python verify_staging.py
```

Muestra informacion detallada de cada archivo Parquet del Data Lake: registros, columnas, tamano y muestra de datos.

**Verificar modelo estrella:**

```bash
python verify_transformed.py
```

Muestra estadisticas completas del modelo estrella:
- Resumen de cada dimension (registros, columnas, muestra)
- Estadisticas de la tabla de hechos
- Metricas de negocio: valores, tiempos de entrega, reviews, retrasos
- Distribucion por estado de orden y tipo de pago

## Tecnologias Utilizadas

- **Lenguaje**: Python 3.12
- **Base de Datos**: PostgreSQL 18
- **Librerias principales**:
  - pandas - Manipulacion de datos
  - SQL - ORM y conexiones
  - psycopg2 - Driver PostgreSQL
  - loguru - Logging
  - pyarrow - Lectura/escritura de Parquet
- **Formatos de datos**: CSV, Parquet

## Notas Tecnicas

### Fase 1 - Extraccion
- La tabla geolocation es la mas grande (1M+ registros) y usa PostgreSQL COPY para carga optimizada
- Coordenadas geograficas almacenadas con precision DECIMAL(11,8)
- Todas las tablas incluyen campos de auditoria (created_at, updated_at)

### Fase 2 - Staging
- Formato Parquet con compresion Snappy
- Reduccion de tamano: ~75% vs CSV original
- Archivos columnar para lectura eficiente
- Data Lake local en data/staging/

### Fase 3: Transformacion
- Eliminacion de duplicados por claves naturales
- Manejo de valores nulos con estrategias especificas
- Conversion de tipos de datos (fechas, numericos)
- Normalizacion de campos de texto (estados, ciudades)
- Validacion de rangos y valores permitidos

### Fase 4: Data Warehouse (Pendiente)
- Carga de modelo estrella a base de datos OLAP
- Optimizaciones: indices, particionamiento, vistas materializadas
- Estrategia para dimensiones de cambio lento (SCD)

### Fase 5: Analisis (Pendiente)
- Queries de negocio y metricas clave
- Dashboard interactivo con visualizaciones
- KPIs: conversion, retention, LTV, churn
