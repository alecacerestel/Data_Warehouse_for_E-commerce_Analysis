"""
Script principal para ejecutar el pipeline ETL/ELT completo
Ejecuta todas las fases en secuencia
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
Path("data/staging").mkdir(parents=True, exist_ok=True)
Path("data/processed").mkdir(parents=True, exist_ok=True)
Path("data/analysis").mkdir(parents=True, exist_ok=True)

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
    """Ejecuta el pipeline completo ETL/ELT"""
    
    logger.info("="*80)
    logger.info("INICIANDO PIPELINE ETL/ELT - OLIST E-COMMERCE DATA WAREHOUSE")
    logger.info("="*80)
    
    start_time = time.time()
    
    try:
        # FASE 1: EXTRACCIÓN
        logger.info("\n" + "="*80)
        logger.info("FASE 1: EXTRACCIÓN - Cargar CSVs a OLTP")
        logger.info("="*80)
        extract_module = load_module(
            PROJECT_ROOT / "scripts" / "01_extract" / "load_csv_to_oltp.py",
            "load_csv_to_oltp"
        )
        loader = extract_module.CSVToOLTPLoader()
        loader.load_all()
        logger.success("Fase 1 completada")
        
        # FASE 2: STAGING
        logger.info("\n" + "="*80)
        logger.info("FASE 2: STAGING - Extraer a Data Lake")
        logger.info("="*80)
        staging_module = load_module(
            PROJECT_ROOT / "scripts" / "02_staging" / "load_to_datalake.py",
            "load_to_datalake"
        )
        staging_loader = staging_module.DataLakeLoader()
        staging_loader.extract_all_tables()
        logger.success("Fase 2 completada")
        
        # FASE 3: TRANSFORMACIÓN - Limpieza
        logger.info("\n" + "="*80)
        logger.info("FASE 3A: TRANSFORMACIÓN - Limpieza de Datos")
        logger.info("="*80)
        cleaning_module = load_module(
            PROJECT_ROOT / "scripts" / "03_transform" / "data_cleaning.py",
            "data_cleaning"
        )
        cleaner = cleaning_module.DataCleaner()
        cleaner.clean_all()
        logger.success("Fase 3A completada")
        
        # FASE 3: TRANSFORMACIÓN - Dimensiones
        logger.info("\n" + "="*80)
        logger.info("FASE 3B: TRANSFORMACIÓN - Creación de Dimensiones")
        logger.info("="*80)
        dimensions_module = load_module(
            PROJECT_ROOT / "scripts" / "03_transform" / "create_dimensions.py",
            "create_dimensions"
        )
        dim_builder = dimensions_module.DimensionBuilder()
        dim_builder.create_all_dimensions()
        logger.success("Fase 3B completada")
        
        # FASE 3: TRANSFORMACIÓN - Tabla de Hechos
        logger.info("\n" + "="*80)
        logger.info("FASE 3C: TRANSFORMACIÓN - Creación de Tabla de Hechos")
        logger.info("="*80)
        fact_module = load_module(
            PROJECT_ROOT / "scripts" / "03_transform" / "create_fact_table.py",
            "create_fact_table"
        )
        fact_builder = fact_module.FactTableBuilder()
        fact_builder.create_fact_orders()
        logger.success("Fase 3C completada")
        
        # FASE 4: ANÁLISIS
        logger.info("\n" + "="*80)
        logger.info("FASE 4: ANÁLISIS - Queries de Negocio")
        logger.info("="*80)
        analysis_module = load_module(
            PROJECT_ROOT / "scripts" / "05_analysis" / "business_queries.py",
            "business_queries"
        )
        analyzer = analysis_module.BusinessAnalyzer()
        analyzer.generate_summary_report()
        logger.success("Fase 4 completada")
        
        # RESUMEN FINAL
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("\n" + "="*80)
        logger.success("PIPELINE COMPLETADO EXITOSAMENTE 🎉")
        logger.info("="*80)
        logger.info(f"Tiempo total de ejecución: {duration/60:.2f} minutos")
        logger.info("\nResultados disponibles en:")
        logger.info("  - Data Lake: data/staging/")
        logger.info("  - Datos procesados: data/processed/")
        logger.info("  - Análisis: data/analysis/")
        logger.info("  - Logs: logs/")
        logger.info("\nBase de datos DWH:")
        logger.info("  - PostgreSQL: olist_dwh")
        logger.info("\nTablas creadas:")
        logger.info("  Dimensiones: dim_customers, dim_products, dim_sellers, dim_geolocation, dim_date")
        logger.info("  Hechos: fct_orders")
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
