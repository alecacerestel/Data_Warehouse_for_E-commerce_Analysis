"""
Script principal para ejecutar el pipeline ETL
Fase 1: Extraccion - Carga datos CSV a la base de datos OLTP
Fase 2: Staging - Extrae datos OLTP a Data Lake en formato Parquet
Fase 3: Transformacion - Crea modelo estrella con dimensiones y tabla de hechos
Fase 4: Data Warehouse - Carga modelo estrella a base de datos OLAP
"""
import sys
from pathlib import Path
from loguru import logger
import time
import importlib.util

# Configurar logging
logger.add("logs/main_pipeline.log", rotation="10 MB", level="INFO")

# Crear directorios necesarios
Path("logs").mkdir(exist_ok=True)

# Añadir el directorio raíz al path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))


def load_module(module_path, module_name):
    """Carga un módulo dinámicamente desde una ruta"""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_pipeline(run_staging=True, run_transformation=True, run_dwh_load=True):
    """
    Ejecuta el pipeline ETL completo
    
    Args:
        run_staging: Si True, ejecuta tambien la fase de staging
        run_transformation: Si True, ejecuta tambien la fase de transformacion
        run_dwh_load: Si True, ejecuta tambien la carga a Data Warehouse OLAP
    """
    
    logger.info("="*80)
    logger.info("PIPELINE ETL - OLIST E-COMMERCE")
    logger.info("="*80)
    
    start_time = time.time()
    
    try:
        # FASE 1: EXTRACCIÓN
        logger.info("\n" + "="*80)
        logger.info("EXTRACCIÓN - Cargar CSVs a base de datos OLTP")
        logger.info("="*80)
        extract_module = load_module(
            PROJECT_ROOT / "scripts" / "01_extract" / "load_csv_to_oltp.py",
            "load_csv_to_oltp"
        )
        loader = extract_module.CSVToOLTPLoader()
        loader.load_all()
        logger.success("Extracción completada")
        
        # FASE 2: STAGING (OPCIONAL)
        if run_staging:
            logger.info("\n" + "="*80)
            logger.info("STAGING - Cargar datos a Data Lake en formato Parquet")
            logger.info("="*80)
            staging_module = load_module(
                PROJECT_ROOT / "scripts" / "02_staging" / "load_to_staging.py",
                "load_to_staging"
            )
            staging_loader = staging_module.OLTPToStagingLoader()
            staging_loader.load_all_to_staging()
            logger.success("Staging completado")
        
        # FASE 3: TRANSFORMACION (OPCIONAL)
        if run_transformation and run_staging:
            logger.info("\n" + "="*80)
            logger.info("TRANSFORMACION - Crear modelo estrella (dimensiones y tabla de hechos)")
            logger.info("="*80)
            
            # Crear directorio para datos transformados
            transformed_path = PROJECT_ROOT / "data" / "transformed"
            transformed_path.mkdir(exist_ok=True)
            
            # Agregar directorio de transformacion al sys.path
            transform_dir = PROJECT_ROOT / "scripts" / "03_transform"
            sys.path.insert(0, str(transform_dir))
            
            transform_module = load_module(
                transform_dir / "create_fact_table.py",
                "create_fact_table"
            )
            builder = transform_module.FactTableBuilder()
            
            # Crear dimensiones
            logger.info("Creando dimensiones...")
            dim_customers = builder.dim_builder.create_dim_customers()
            dim_products = builder.dim_builder.create_dim_products()
            dim_sellers = builder.dim_builder.create_dim_sellers()
            dim_date = builder.dim_builder.create_dim_date()
            
            # Guardar dimensiones
            dim_customers.to_parquet(transformed_path / "dim_customers.parquet", compression='snappy')
            dim_products.to_parquet(transformed_path / "dim_products.parquet", compression='snappy')
            dim_sellers.to_parquet(transformed_path / "dim_sellers.parquet", compression='snappy')
            dim_date.to_parquet(transformed_path / "dim_date.parquet", compression='snappy')
            logger.success(f"Dimensiones guardadas en {transformed_path}")
            
            # Crear tabla de hechos
            logger.info("Creando tabla de hechos...")
            fct_orders = builder.create_fact_orders()
            fct_orders.to_parquet(transformed_path / "fct_orders.parquet", compression='snappy')
            logger.success(f"Tabla de hechos guardada en {transformed_path}")
            
            logger.success("Transformacion completada")
        elif run_transformation and not run_staging:
            logger.warning("Transformacion requiere staging. Se omite Fase 3.")
        
        # FASE 4: DATA WAREHOUSE (OPCIONAL)
        if run_dwh_load and run_transformation and run_staging:
            logger.info("\n" + "="*80)
            logger.info("DATA WAREHOUSE - Cargar modelo estrella a base de datos OLAP")
            logger.info("="*80)
            
            dwh_module = load_module(
                PROJECT_ROOT / "scripts" / "04_load" / "load_to_dwh.py",
                "load_to_dwh"
            )
            dwh_loader = dwh_module.DWHLoader()
            
            if dwh_loader.load_all():
                logger.success("Data Warehouse cargado correctamente")
            else:
                logger.error("Error al cargar Data Warehouse")
                
        elif run_dwh_load and (not run_transformation or not run_staging):
            logger.warning("Carga a DWH requiere staging y transformacion. Se omite Fase 4.")
        
        # RESUMEN FINAL
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("\n" + "="*80)
        logger.success("PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("="*80)
        logger.info(f"Tiempo total de ejecucion: {duration:.2f} segundos")
        logger.info("\nFases ejecutadas:")
        logger.info("  1. Extraccion: CSV -> PostgreSQL OLTP")
        if run_staging:
            logger.info("  2. Staging: OLTP -> Data Lake Parquet")
        if run_transformation and run_staging:
            logger.info("  3. Transformacion: Staging -> Modelo Estrella")
        if run_dwh_load and run_transformation and run_staging:
            logger.info("  4. Data Warehouse: Modelo Estrella -> PostgreSQL OLAP")
        logger.info("\nBase de datos OLTP:")
        logger.info("  - PostgreSQL: olist_oltp")
        if run_staging:
            logger.info("\nData Lake:")
            logger.info("  - Formato: Parquet")
            logger.info("  - Ubicacion: data/staging/")
        if run_transformation and run_staging:
            logger.info("\nModelo Estrella:")
            logger.info("  - Formato: Parquet")
            logger.info("  - Ubicacion: data/transformed/")
            logger.info("  - Dimensiones: dim_customers, dim_products, dim_sellers, dim_date")
            logger.info("  - Tabla de hechos: fct_orders")
        if run_dwh_load and run_transformation and run_staging:
            logger.info("\nData Warehouse OLAP:")
            logger.info("  - PostgreSQL: olist_olap")
            logger.info("  - Tablas: 4 dimensiones + 1 tabla de hechos")
            logger.info("  - Indices: Optimizados para consultas analiticas")
            logger.info("  - Vistas materializadas: 8 vistas con metricas de negocio")
        logger.info("\nTablas procesadas:")
        logger.info("  - customers (clientes)")
        logger.info("  - products (productos)")
        logger.info("  - sellers (vendedores)")
        logger.info("  - orders (ordenes)")
        logger.info("  - order_items (items de orden)")
        logger.info("  - order_payments (pagos)")
        logger.info("  - order_reviews (resenas)")
        logger.info("  - geolocation (geolocalizacion)")
        logger.info("  - product_category_name_translation (traducciones)")
        logger.info("\nLogs disponibles en: logs/")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"\n ERROR EN EL PIPELINE: {e}")
        logger.exception("Detalles del error:")
        return False


def main():
    """Función principal"""
    success = run_pipeline()
    
    if success:
        logger.success("\n Pipeline ejecutado correctamente")
        sys.exit(0)
    else:
        logger.error("\n Pipeline falló")
        sys.exit(1)


if __name__ == "__main__":
    main()
