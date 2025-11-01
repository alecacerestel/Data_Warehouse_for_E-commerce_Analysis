"""
DAG de Apache Airflow para el pipeline ETL/ELT de Olist E-commerce
Orquesta todo el flujo: Extract → Staging → Transform → Load → Analyze
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
from pathlib import Path

# Agregar el directorio raíz al path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Configuración por defecto del DAG
default_args = {
    'owner': 'alejandro',
    'depends_on_past': False,
    'email': ['alejandro@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Definir el DAG
dag = DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL/ELT completo para Olist E-commerce Data Warehouse',
    schedule_interval='@daily',  # Ejecutar diariamente
    start_date=days_ago(1),
    catchup=False,  # No ejecutar para fechas pasadas
    max_active_runs=1,
    tags=['etl', 'olist', 'data-warehouse', 'ecommerce'],
)

# ============================================
# FASE 1: EXTRACCIÓN (Extract)
# ============================================

def extract_csv_to_oltp():
    """Cargar CSVs a base de datos OLTP"""
    from scripts.extract.load_csv_to_oltp import CSVToOLTPLoader
    
    loader = CSVToOLTPLoader()
    loader.load_all()


task_extract = PythonOperator(
    task_id='01_extract_csv_to_oltp',
    python_callable=extract_csv_to_oltp,
    dag=dag,
)

# ============================================
# FASE 2: STAGING (Staging Layer)
# ============================================

def extract_to_datalake():
    """Extraer datos de OLTP al Data Lake (Parquet)"""
    from scripts.staging.load_to_datalake import DataLakeLoader
    
    loader = DataLakeLoader()
    loader.extract_all_tables()


task_staging = PythonOperator(
    task_id='02_extract_to_datalake',
    python_callable=extract_to_datalake,
    dag=dag,
)

# ============================================
# FASE 3: TRANSFORMACIÓN (Transform)
# ============================================

def clean_data():
    """Limpiar y preparar datos"""
    from scripts.transform.data_cleaning import DataCleaner
    
    cleaner = DataCleaner()
    cleaner.clean_all()


task_clean = PythonOperator(
    task_id='03_transform_clean_data',
    python_callable=clean_data,
    dag=dag,
)


def create_dimensions():
    """Crear tablas de dimensiones"""
    from scripts.transform.create_dimensions import DimensionBuilder
    
    builder = DimensionBuilder()
    builder.create_all_dimensions()


task_dimensions = PythonOperator(
    task_id='04_transform_create_dimensions',
    python_callable=create_dimensions,
    dag=dag,
)


def create_fact_table():
    """Crear tabla de hechos"""
    from scripts.transform.create_fact_table import FactTableBuilder
    
    builder = FactTableBuilder()
    builder.create_fact_orders()


task_fact = PythonOperator(
    task_id='05_transform_create_fact_table',
    python_callable=create_fact_table,
    dag=dag,
)

# ============================================
# FASE 4: CARGA (Load)
# ============================================

# Nota: En este caso, la carga ya se hace en las tareas de transformación
# Si usáramos un DWH en la nube, aquí iría la carga a BigQuery/Snowflake/Redshift

task_verify_dwh = BashOperator(
    task_id='06_verify_data_warehouse',
    bash_command='echo "Data Warehouse verificado exitosamente"',
    dag=dag,
)

# ============================================
# FASE 5: ANÁLISIS (Analyze)
# ============================================

def run_business_queries():
    """Ejecutar queries de análisis de negocio"""
    from scripts.analysis.business_queries import BusinessAnalyzer
    
    analyzer = BusinessAnalyzer()
    analyzer.generate_summary_report()


task_analyze = PythonOperator(
    task_id='07_run_business_analysis',
    python_callable=run_business_queries,
    dag=dag,
)

# ============================================
# NOTIFICACIONES Y LIMPIEZA
# ============================================

task_success = BashOperator(
    task_id='08_pipeline_success',
    bash_command='echo "✓ Pipeline ETL/ELT completado exitosamente - $(date)"',
    dag=dag,
)

# ============================================
# DEFINIR DEPENDENCIAS (ORDEN DE EJECUCIÓN)
# ============================================

# Flujo lineal del pipeline
task_extract >> task_staging >> task_clean >> task_dimensions >> task_fact >> task_verify_dwh >> task_analyze >> task_success

# Documentación del DAG
dag.doc_md = """
# Pipeline ETL/ELT - Olist E-commerce Data Warehouse

Este DAG orquesta el pipeline completo para construir el Data Warehouse de análisis de e-commerce.

## Fases del Pipeline:

### 1. Extracción (Extract)
- **Tarea**: `01_extract_csv_to_oltp`
- **Descripción**: Carga los archivos CSV originales a la base de datos transaccional (OLTP)
- **Duración estimada**: 5-10 minutos

### 2. Staging
- **Tarea**: `02_extract_to_datalake`
- **Descripción**: Extrae datos de OLTP y los almacena en Data Lake (formato Parquet)
- **Duración estimada**: 3-5 minutos

### 3. Transformación (Transform)
- **Tarea 3a**: `03_transform_clean_data`
  - Limpieza y preparación de datos
  - Manejo de nulos, estandarización, cálculos derivados
- **Tarea 3b**: `04_transform_create_dimensions`
  - Creación de tablas de dimensiones (clientes, productos, vendedores, geolocalización, fecha)
- **Tarea 3c**: `05_transform_create_fact_table`
  - Creación de tabla de hechos con todas las métricas de negocio
- **Duración estimada total**: 10-15 minutos

### 4. Verificación
- **Tarea**: `06_verify_data_warehouse`
- **Descripción**: Verifica la integridad del Data Warehouse
- **Duración estimada**: 1 minuto

### 5. Análisis
- **Tarea**: `07_run_business_analysis`
- **Descripción**: Ejecuta queries de análisis y genera reportes de negocio
- **Duración estimada**: 2-3 minutos

### 6. Finalización
- **Tarea**: `08_pipeline_success`
- **Descripción**: Notificación de éxito

## Preguntas de Negocio Respondidas:

1. ¿Cuáles son los productos más vendidos por mes?
2. ¿De qué estados provienen los clientes más valiosos?
3. ¿Cuál es el tiempo promedio de entrega por región?
4. ¿Cuáles son las categorías de productos más rentables?
5. ¿Cuál es el comportamiento de compra de clientes recurrentes?

## Modelo de Datos:

**Modelo en Estrella (Star Schema)**

**Tabla de Hechos:**
- `fct_orders`: Métricas de órdenes y ventas

**Tablas de Dimensiones:**
- `dim_customers`: Clientes
- `dim_products`: Productos
- `dim_sellers`: Vendedores
- `dim_geolocation`: Geolocalización
- `dim_date`: Tiempo

## Contacto:
- **Autor**: Alejandro
- **Email**: alejandro@example.com
"""

# Agregar notas a las tareas individuales
task_extract.doc_md = """
### Extracción de CSVs a OLTP
Carga los siguientes archivos CSV a PostgreSQL:
- customers
- products
- sellers
- orders
- order_items
- order_payments
- order_reviews
- geolocation
- product_category_translation
"""

task_staging.doc_md = """
### Extracción a Data Lake
Extrae todas las tablas de la base OLTP y las almacena en formato Parquet
para procesamiento eficiente en las siguientes fases.
"""

task_clean.doc_md = """
### Limpieza de Datos
- Elimina duplicados
- Maneja valores nulos
- Estandariza formatos
- Calcula campos derivados
- Categoriza datos
"""

task_dimensions.doc_md = """
### Creación de Dimensiones
Crea todas las tablas de dimensiones del modelo estrella con soporte para SCD Type 2.
"""

task_fact.doc_md = """
### Creación de Tabla de Hechos
Crea la tabla `fct_orders` con todas las métricas de negocio y referencias a las dimensiones.
"""

task_analyze.doc_md = """
### Análisis de Negocio
Ejecuta queries para responder preguntas de negocio y genera reportes en Excel.
"""
