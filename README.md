# Pipeline ETL - Olist E-commerce Dataset

## Descripcion del Proyecto

Este proyecto implementa un pipeline ETL completo para procesar datos del e-commerce brasileno Olist:

- Fase 1: Extraccion - Carga datos CSV a PostgreSQL OLTP
- Fase 2: Staging - Extrae datos OLTP a Data Lake local en formato Parquet
- Fase 3: Transformacion - XXXXX
- Fase 4: Data Warehouse - YYYYY
- Fase 5: Analisis - ZZZZZ


##  Fuente de Datos

Los datos utilizados en este proyecto provienen del dataset público de Olist disponible en Kaggle:

**[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

El dataset incluye información de aproximadamente 100,000 órdenes realizadas entre 2016 y 2018 en múltiples marketplaces de Brasil.

## Arquitectura del Pipeline

```
CSV Files -> PostgreSQL (OLTP) -> Data Lake Parquet (Staging)
```

## Fases Implementadas

### Fase 1: Extraccion
- Lee 9 archivos CSV
- Carga datos a PostgreSQL OLTP
- Total: 1.6M registros

### Fase 2: Staging
- Extrae datos desde OLTP
- Guarda en formato Parquet comprimido
- Tamano total: ~54 MB

## Estructura del Proyecto

```
Analisis de E-commerce/
 config/
    config.yaml              # Configuracion general
    db_config.py             # Configuracion de base de datos
 data/
    raw/                     # CSVs originales 
    staging/                 # Archivos Parquet 
 scripts/
    01_extract/
        load_csv_to_oltp.py  # Carga CSVs a PostgreSQL
    02_staging/
        load_to_staging.py   # Carga OLTP a Parquet
 sql/
    oltp_schema.sql          # Schema OLTP 
 logs/                        # Logs de ejecucion
 requirements.txt
 truncate_all.py              # Limpia tablas OLTP
 verify_staging.py            # Verifica archivos Parquet
 README.md
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

### Ejecutar pipeline completo

```bash
python truncate_all.py
python run_pipeline.py
```

El pipeline ejecutara:
1. Fase 1 - Extraccion: Carga 9 archivos CSV a PostgreSQL OLTP 
2. Fase 2 - Staging: Extrae OLTP a Data Lake Parquet 
3. Fase 3 - Transformacion: XXX
4. Total de ~1.6 millones de registros

### Verificar Data Lake

```bash
python verify_staging.py
```

Muestra informacion detallada de los archivos Parquet generados

## Tecnologias Utilizadas

- **Lenguaje**: Python 3.12
- **Base de Datos**: PostgreSQL 18
- **Librerias principales**:
  - pandas - Manipulacion de datos
  - SQLAlchemy - ORM y conexiones
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

### Fase 3: Transformacion (Pendiente)
- Limpieza y calidad de datos
- Creacion de dimensiones (customers, products, sellers, date)
- Creacion de tabla de hechos (orders)

### Fase 4: Data Warehouse (Pendiente)
- Carga a base de datos OLAP
- Modelo estrella optimizado
- Indices y particionamiento

### Fase 5: Analisis (Pendiente)
- Queries de negocio
- Dashboard con visualizaciones
- Metricas y KPIs

### Logs
- Todos los logs se guardan en logs/ para debugging
- logs/01_load_csv_to_oltp.log - Fase extraccion
- logs/02_load_to_staging.log - Fase staging
- logs/main_pipeline.log - Pipeline completo
