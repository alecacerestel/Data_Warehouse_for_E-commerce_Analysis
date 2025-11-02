"""
Script para verificar los datos en el Data Lake Parquet
Muestra informacion sobre los archivos y su contenido
"""
import pandas as pd
from pathlib import Path
from loguru import logger

logger.add("logs/verify_staging.log", rotation="1 MB", level="INFO")


def verify_parquet_files(staging_path: str = "data/staging"):
    """Verifica los archivos Parquet en staging"""
    staging_path = Path(staging_path)
    
    if not staging_path.exists():
        logger.error(f"Directorio {staging_path} no existe")
        return
    
    parquet_files = list(staging_path.glob("*.parquet"))
    
    if not parquet_files:
        logger.warning("No se encontraron archivos Parquet")
        return
    
    logger.info("="*80)
    logger.info(f"VERIFICACION DE DATA LAKE - {len(parquet_files)} archivos encontrados")
    logger.info("="*80)
    
    total_size = 0
    total_records = 0
    
    for file_path in sorted(parquet_files):
        df = pd.read_parquet(file_path)
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        total_size += file_size_mb
        total_records += len(df)
        
        logger.info(f"\n{file_path.name}")
        logger.info(f"  Registros: {len(df):,}")
        logger.info(f"  Columnas: {len(df.columns)}")
        logger.info(f"  Tamaño: {file_size_mb:.2f} MB")
        logger.info(f"  Columnas: {', '.join(df.columns.tolist())}")
        
        # Mostrar primeras filas
        logger.info(f"  Muestra de datos:")
        logger.info(f"\n{df.head(3).to_string()}\n")
    
    logger.info("="*80)
    logger.info(f"RESUMEN TOTAL")
    logger.info(f"  Total registros: {total_records:,}")
    logger.info(f"  Tamaño total: {total_size:.2f} MB")
    logger.info("="*80)


if __name__ == "__main__":
    verify_parquet_files()
