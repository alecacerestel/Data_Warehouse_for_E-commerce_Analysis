# Pipeline de Extracción - Olist E-commerce Dataset

## Descripción del Proyecto

Este proyecto implementa un pipeline de extracción para cargar datos CSV del e-commerce brasileño Olist a una base de datos PostgreSQL (OLTP).

##  Fuente de Datos

Los datos utilizados en este proyecto provienen del dataset público de Olist disponible en Kaggle:

**[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

El dataset incluye información de aproximadamente 100,000 órdenes realizadas entre 2016 y 2018 en múltiples marketplaces de Brasil.

## Arquitectura del Pipeline

```
CSV Files  PostgreSQL (OLTP)
```

## Estructura del Proyecto

```
Análisis de E-commerce/
 config/
    config.yaml              # Configuración general
    db_config.py             # Configuración de base de datos
 data/
    raw/                     # CSVs originales (9 archivos)
 scripts/
    01_extract/
        load_csv_to_oltp.py  # Carga CSVs a PostgreSQL
 sql/
    oltp_schema.sql          # Schema OLTP (9 tablas)
 logs/                        # Logs de ejecución
 requirements.txt
 README.md
```

## Tablas en Base de Datos OLTP

El pipeline carga 9 tablas a PostgreSQL:

1. **customers** - Clientes (99,441 registros)
2. **products** - Productos (32,951 registros)
3. **sellers** - Vendedores (3,095 registros)
4. **orders** - Órdenes (99,441 registros)
5. **order_items** - Items de orden (112,650 registros)
6. **order_payments** - Pagos (103,886 registros)
7. **order_reviews** - Reseñas (98,410 registros)
8. **geolocation** - Geolocalización (1,000,163 registros)
9. **product_category_name_translation** - Traducciones (71 registros)

## Instalaci�n y Configuraci�n

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

## Ejecuci�n del Pipeline

### Ejecutar pipeline completo

```bash
python run_pipeline.py
```

El pipeline ejecutará:
1. Carga de 9 archivos CSV a PostgreSQL
2. Total de ~1.6 millones de registros
3. Tiempo estimado: 78 segundos

## Tecnologías Utilizadas

- **Lenguaje**: Python 3.12
- **Base de Datos**: PostgreSQL 18
- **Librerías principales**:
  - pandas - Manipulación de datos
  - SQLAlchemy - ORM y conexiones
  - psycopg2 - Driver PostgreSQL
  - loguru - Logging
  - pyarrow - Lectura eficiente de CSV
- **Formato de datos**: CSV

## Notas Técnicas

- La tabla `geolocation` es la más grande (1M+ registros) y usa PostgreSQL COPY para carga optimizada
- Coordenadas geográficas almacenadas con precisión DECIMAL(11,8)
- Todas las tablas incluyen campos de auditoría (`created_at`, `updated_at`)
- Los logs se guardan en la carpeta `logs/` para debugging

## Próximos Pasos

Este pipeline sirve como base para:
- Análisis exploratorio de datos con Jupyter notebooks
- Construcción de dashboards con herramientas BI
- Desarrollo de modelos de Machine Learning
- Implementación de un Data Warehouse (futuro)
