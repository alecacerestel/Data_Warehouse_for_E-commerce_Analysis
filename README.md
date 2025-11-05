# Pipeline ETL - Olist E-commerce Dataset

## Descripcion del Proyecto

Este proyecto implementa un pipeline ETL completo para procesar y analizar datos del e-commerce brasileno Olist. El sistema procesa mas de 1.5 millones de registros a traves de multiples fases de transformacion, culminando en un modelo estrella optimizado para analisis de negocio.

### Fases Implementadas

- **Fase 1: Extraccion** - Carga datos CSV a PostgreSQL OLTP (1.6M registros)
- **Fase 2: Staging** - Extrae datos OLTP a Data Lake local en formato Parquet (53 MB)
- **Fase 3: Transformacion** - Crea modelo estrella con 4 dimensiones y tabla de hechos
- **Fase 4: Data Warehouse** - Carga modelo estrella a PostgreSQL OLAP con optimizaciones

### Fases Futuras

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
        ↓
PostgreSQL OLAP (Data Warehouse - 236K registros)
    ├── Indices optimizados
    ├── Claves foraneas
    └── Vistas materializadas (8 vistas)
```
## Detalle de Fases

### Fase 1: Extraccion
- Lee 9 archivos CSV desde data/raw/
- Carga datos a PostgreSQL OLTP con validaciones
- Optimizacion especial para geolocation (1M+ registros) usando PostgreSQL COPY
- Total: 1,550,108 registros

### Fase 2: Staging
- Extrae todas las tablas desde OLTP
- Guarda en formato Parquet con compresion Snappy
- Reduccion de tamano: 75 por ciento vs CSV original
- Total: 53.51 MB

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
- Total: 235,361 registro

### Fase 4: Data Warehouse
- **Carga a PostgreSQL OLAP**: Todas las dimensiones y tabla de hechos
- **Optimizaciones implementadas**:
  - Indices en claves primarias y foraneas
  - Indices compuestos para queries frecuentes
  - Claves foraneas con verificacion de integridad referencial
  - Triggers para auditoria automatica (updated_at)
  - ANALYZE y VACUUM para estadisticas optimizadas
- **Vistas materializadas** (8 vistas para metricas de negocio):
  - mv_sales_by_month: Metricas mensuales de ventas
  - mv_sales_by_region: Analisis por region geografica
  - mv_top_products: Top 1000 productos por ingresos
  - mv_seller_performance: Desempeno de vendedores
  - mv_delivery_analysis: Analisis de entregas y retrasos
  - mv_payment_analysis: Metodos de pago y cuotas
  - mv_product_categories: Metricas por categoria
  - mv_customer_recurrence: Segmentacion de clientes
  - mv_executive_dashboard: KPIs principales
- Total: 235,702 registros 

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
│   │   └── load_to_postgres.py  # Carga CSV a PostgreSQL OLTP
│   ├── 02_stage/
│   │   └── export_to_parquet.py # Exporta desde OLTP a Parquet
│   ├── 03_transform/
│   │   ├── data_cleaning.py     # Limpieza y validacion de datos
│   │   ├── create_dimensions.py # Creacion de tablas dimensionales
│   │   └── create_fact_table.py # Creacion de tabla de hechos
│   └── 04_load/
│       └── load_to_dwh.py       # Carga modelo estrella a PostgreSQL OLAP
├── sql/
│   ├── oltp_schema.sql          # Schema de base de datos OLTP
│   ├── olap_schema.sql          # Schema de Data Warehouse OLAP
│   └── olap_views.sql           # Vistas materializadas OLAP
├── logs/                         # Logs de ejecucion de todas las fases
├── requirements.txt              # Dependencias Python
├── run_pipeline.py               # Script principal del pipeline (4 fases)
├── truncate_all.py               # Limpieza de tablas y archivos
├── verify_staging.py             # Verificacion de archivos Parquet staging
├── verify_transformed.py         # Verificacion de archivos Parquet transformados
├── verify_dwh.py                 # Verificacion de Data Warehouse OLAP
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
OLAP_DB=olist_olap
```

### 5. Crear Bases de Datos y Schemas

Crear bases de datos OLTP y OLAP, y aplicar schemas:

```bash
psql -U postgres -c "CREATE DATABASE olist_oltp;"
psql -U postgres -d olist_oltp -f sql/oltp_schema.sql

psql -U postgres -c "CREATE DATABASE olist_olap;"
psql -U postgres -d olist_olap -f sql/olap_schema.sql
```

Opcional: Aplicar vistas materializadas para metricas precalculadas (recomendado):

```bash
psql -U postgres -d olist_olap -f sql/olap_views.sql
```

## Ejecucion del Pipeline

### Pipeline Completo (4 Fases)

Ejecuta todas las fases del pipeline en secuencia:

```bash
python truncate_all.py  # Limpia bases de datos OLTP y OLAP
python run_pipeline.py  # Ejecuta las 4 fases
```

El pipeline ejecutara:
1. **Fase 1 - Extraccion**: Carga 9 archivos CSV a PostgreSQL OLTP (1.6M registros)
2. **Fase 2 - Staging**: Extrae OLTP a Data Lake Parquet (53 MB)
3. **Fase 3 - Transformacion**: Crea modelo estrella con dimensiones y tabla de hechos (235K registros)
4. **Fase 4 - Data Warehouse**: Carga modelo estrella a PostgreSQL OLAP con optimizaciones


### Ejecucion Parcial

Para ejecutar solo algunas fases, modifica los parametros en `run_pipeline.py`:

```python
# Ejecutar solo fases 1-3 (sin Data Warehouse)
run_pipeline(run_staging=True, run_transformation=True, run_dwh_load=False)

# Ejecutar solo fases 2-4 (sin extraccion)
run_pipeline(run_staging=True, run_transformation=True, run_dwh_load=True)
```

### Herramientas de Verificacion

**Verificar archivos Parquet del Data Lake:**

```bash
python verify_staging.py
```

Muestra informacion detallada de cada archivo Parquet del Data Lake: registros, columnas, tamano y muestra de datos.

**Verificar modelo estrella transformado:**

```bash
python verify_transformed.py
```

Muestra estadisticas completas del modelo estrella:

**Verificar Data Warehouse OLAP:**

```bash
python verify_dwh.py
```

Muestra metricas completas del Data Warehouse:
- Conteos de registros en dimensiones y tabla de hechos
- Metricas de negocio: ordenes totales, ingresos, valor promedio, tiempo de entrega
- Distribucion por estado de orden, categorias, regiones geograficas
- Validacion de integridad referencial (claves foraneas)
- Lista de vistas materializadas disponibles

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

### Fase 4: Data Warehouse
- Modelo estrella con 5 tablas: 4 dimensiones y 1 tabla de hechos
- Optimizaciones implementadas:
  - Indices en claves primarias (PRIMARY KEY)
  - Indices en claves foraneas para joins eficientes
  - Indices compuestos para queries frecuentes (fecha+estado, cliente+fecha, producto+fecha)
  - Claves foraneas con ON DELETE CASCADE
  - Triggers para actualizacion automatica de updated_at
  - ANALYZE y VACUUM para estadisticas optimizadas del query planner
- Validacion de integridad referencial en cada carga
- 8 vistas materializadas para metricas de negocio precalculadas
- Carga en chunks optimizada: 5000 para dimensiones, 1000 para hechos

### Vistas Materializadas OLAP
Las vistas materializadas precalculan metricas complejas para consultas rapidas:
- mv_sales_by_month: Ventas mensuales con metricas agregadas
- mv_sales_by_region: Desempeno por region geografica
- mv_top_products: Top 1000 productos por ingresos
- mv_seller_performance: Metricas de desempeno por vendedor
- mv_delivery_analysis: Analisis de tiempos de entrega y retrasos
- mv_payment_analysis: Distribucion de metodos de pago y cuotas
- mv_product_categories: Agregaciones por categoria de producto
- mv_customer_recurrence: Segmentacion de clientes por frecuencia de compra

Refrescar vistas: `SELECT refresh_all_materialized_views();`
- Carga de modelo estrella a base de datos OLAP
- Optimizaciones: indices, particionamiento, vistas materializadas
- Estrategia para dimensiones de cambio lento (SCD)

### Fase 5: Analisis (Pendiente)
- Queries de negocio y metricas clave
- Dashboard interactivo con visualizaciones
- KPIs: conversion, retention, LTV, churn
