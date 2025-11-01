"""
Script principal para ejecutar el pipeline de EXTRACCIÓN
Carga datos CSV a la base de datos OLTP (PostgreSQL)
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


def run_pipeline():
    """Ejecuta el pipeline de extracción (carga CSV a OLTP)"""
    
    logger.info("="*80)
    logger.info("PIPELINE DE EXTRACCIÓN - OLIST E-COMMERCE")
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
        
        # RESUMEN FINAL
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("\n" + "="*80)
        logger.success("PIPELINE COMPLETADO EXITOSAMENTE 🎉")
        logger.info("="*80)
        logger.info(f"Tiempo total de ejecución: {duration:.2f} segundos")
        logger.info("\nBase de datos OLTP:")
        logger.info("  - PostgreSQL: olist_oltp")
        logger.info("\nTablas cargadas:")
        logger.info("  - customers (clientes)")
        logger.info("  - products (productos)")
        logger.info("  - sellers (vendedores)")
        logger.info("  - orders (órdenes)")
        logger.info("  - order_items (items de orden)")
        logger.info("  - order_payments (pagos)")
        logger.info("  - order_reviews (reseñas)")
        logger.info("  - geolocation (geolocalización)")
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
