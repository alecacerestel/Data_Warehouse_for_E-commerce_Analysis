"""
Script para limpiar datos y archivos generados
Útil para reiniciar el proyecto desde cero
"""
import os
import shutil
from pathlib import Path
from loguru import logger

logger.add("logs/cleanup.log", rotation="1 MB")


def clean_data_directories():
    """Limpia directorios de datos generados"""
    directories = [
        "data/staging",
        "data/processed",
        "data/analysis"
    ]
    
    for directory in directories:
        dir_path = Path(directory)
        if dir_path.exists():
            # Eliminar todos los archivos excepto .gitkeep
            for file in dir_path.glob("*"):
                if file.name != ".gitkeep":
                    try:
                        file.unlink()
                        logger.info(f"Eliminado: {file}")
                    except Exception as e:
                        logger.error(f"Error eliminando {file}: {e}")
            
            logger.success(f"✓ Directorio limpiado: {directory}")


def clean_logs():
    """Limpia archivos de log antiguos"""
    log_path = Path("logs")
    if log_path.exists():
        for file in log_path.glob("*.log"):
            try:
                file.unlink()
                logger.info(f"Log eliminado: {file}")
            except Exception as e:
                logger.error(f"Error eliminando log {file}: {e}")
        
        logger.success("✓ Logs limpiados")


def clean_pycache():
    """Elimina archivos __pycache__"""
    for pycache in Path(".").rglob("__pycache__"):
        try:
            shutil.rmtree(pycache)
            logger.info(f"Eliminado: {pycache}")
        except Exception as e:
            logger.error(f"Error eliminando {pycache}: {e}")
    
    logger.success("✓ __pycache__ limpiados")


def truncate_databases():
    """Limpia las tablas de las bases de datos (opcional)"""
    from config.db_config import DatabaseConfig
    from sqlalchemy import text
    
    response = input("\n⚠️  ¿Deseas ELIMINAR todos los datos de las bases de datos? (s/n): ")
    
    if response.lower() != 's':
        logger.info("Bases de datos no modificadas")
        return
    
    try:
        db_config = DatabaseConfig()
        
        # Limpiar DWH
        logger.info("Limpiando Data Warehouse...")
        dwh_engine = db_config.get_dwh_engine()
        
        tables = ['fct_orders', 'dim_customers', 'dim_products', 'dim_sellers', 
                  'dim_geolocation', 'dim_date']
        
        with dwh_engine.connect() as conn:
            for table in tables:
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                    conn.commit()
                    logger.info(f"  Tabla truncada: {table}")
                except Exception as e:
                    logger.warning(f"  Error truncando {table}: {e}")
        
        logger.success("✓ Data Warehouse limpiado")
        
        # Limpiar OLTP
        response = input("\n¿Deseas también limpiar la base OLTP? (s/n): ")
        if response.lower() == 's':
            logger.info("Limpiando base OLTP...")
            oltp_engine = db_config.get_oltp_engine()
            
            oltp_tables = ['order_reviews', 'order_payments', 'order_items', 
                          'orders', 'geolocation', 'product_category_translation',
                          'sellers', 'products', 'customers']
            
            with oltp_engine.connect() as conn:
                for table in oltp_tables:
                    try:
                        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                        conn.commit()
                        logger.info(f"  Tabla truncada: {table}")
                    except Exception as e:
                        logger.warning(f"  Error truncando {table}: {e}")
            
            logger.success("✓ Base OLTP limpiada")
        
    except Exception as e:
        logger.error(f"Error limpiando bases de datos: {e}")


def main():
    """Función principal"""
    logger.info("="*60)
    logger.info("INICIANDO LIMPIEZA DEL PROYECTO")
    logger.info("="*60)
    
    print("\n🧹 Script de Limpieza - Data Warehouse Olist\n")
    print("Este script eliminará:")
    print("  - Archivos Parquet generados")
    print("  - Archivos de análisis (Excel/CSV)")
    print("  - Logs antiguos")
    print("  - Archivos __pycache__")
    print("  - (Opcional) Datos de las bases de datos\n")
    
    response = input("¿Deseas continuar? (s/n): ")
    
    if response.lower() != 's':
        print("Operación cancelada")
        return
    
    # Limpiar directorios
    print("\n[1/4] Limpiando directorios de datos...")
    clean_data_directories()
    
    # Limpiar logs
    print("\n[2/4] Limpiando logs...")
    clean_logs()
    
    # Limpiar __pycache__
    print("\n[3/4] Limpiando __pycache__...")
    clean_pycache()
    
    # Limpiar bases de datos (opcional)
    print("\n[4/4] Limpieza de bases de datos...")
    truncate_databases()
    
    print("\n" + "="*60)
    logger.success("✓ LIMPIEZA COMPLETADA")
    print("="*60)
    print("\nEl proyecto ha sido limpiado. Puedes ejecutar el pipeline nuevamente.")


if __name__ == "__main__":
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    main()
