"""
Script principal para ejecutar el pipeline ETL
Fase 1: Extraccion - Carga datos CSV a la base de datos OLTP
Fase 2: Staging - Extrae datos OLTP a Data Lake en formato Parquet
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


def run_pipeline(run_staging=True):
    """
    Ejecuta el pipeline ETL completo
    
    Args:
        run_staging: Si True, ejecuta tambien la fase de staging
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
        logger.info("\nBase de datos OLTP:")
        logger.info("  - PostgreSQL: olist_oltp")
        if run_staging:
            logger.info("\nData Lake:")
            logger.info("  - Formato: Parquet")
            logger.info("  - Ubicacion: data/staging/")
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
