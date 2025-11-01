"""
Script para extraer datos de OLTP y cargarlos al Data Lake (Staging)
Fase 2: Staging - Cargar datos en formato Parquet
"""
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import text
from loguru import logger
import os

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig, load_config

# Configurar logging
logger.add("logs/02_load_to_datalake.log", rotation="1 MB", level="INFO")


class DataLakeLoader:
    """Clase para extraer datos de OLTP y cargarlos al Data Lake"""
    
    def __init__(self, staging_path: str = "data/staging"):
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_oltp_engine()
        self.staging_path = Path(staging_path)
        self.config = load_config()
        
        # Crear directorio de staging si no existe
        self.staging_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data Lake path: {self.staging_path}")
    
    def extract_table_to_parquet(self, table_name: str, chunk_size: int = 10000):
        """Extrae una tabla de OLTP y la guarda como Parquet"""
        logger.info(f"Extrayendo tabla: {table_name}")
        
        try:
            # Extraer datos en chunks para manejar tablas grandes
            query = f"SELECT * FROM {table_name}"
            
            # Leer en chunks y concatenar
            chunks = []
            for chunk in pd.read_sql(query, self.engine, chunksize=chunk_size):
                chunks.append(chunk)
            
            df = pd.concat(chunks, ignore_index=True)
            
            logger.info(f"  Registros extraídos: {len(df):,}")
            
            # Guardar como Parquet
            output_file = self.staging_path / f"{table_name}.parquet"
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Calcular tamaño del archivo
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            logger.success(f"✓ {table_name}.parquet creado ({file_size_mb:.2f} MB)")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error extrayendo {table_name}: {e}")
            raise
    
    def extract_all_tables(self):
        """Extrae todas las tablas a Data Lake"""
        logger.info("="*60)
        logger.info("INICIANDO EXTRACCIÓN A DATA LAKE (STAGING)")
        logger.info("="*60)
        
        tables = [
            'customers',
            'products',
            'sellers',
            'orders',
            'order_items',
            'order_payments',
            'order_reviews',
            'geolocation',
            'product_category_translation'
        ]
        
        total_records = 0
        
        for table in tables:
            try:
                records = self.extract_table_to_parquet(table)
                total_records += records
            except Exception as e:
                logger.error(f"Error en tabla {table}: {e}")
                continue
        
        logger.info("="*60)
        logger.success(f"✓ EXTRACCIÓN COMPLETADA: {total_records:,} registros totales")
        logger.info("="*60)
        
        # Listar archivos creados
        self.list_staging_files()
    
    def list_staging_files(self):
        """Lista los archivos en el Data Lake"""
        logger.info("\nArchivos en Data Lake:")
        
        total_size = 0
        for file in sorted(self.staging_path.glob("*.parquet")):
            size_mb = file.stat().st_size / (1024 * 1024)
            total_size += size_mb
            logger.info(f"  {file.name}: {size_mb:.2f} MB")
        
        logger.info(f"\nTamaño total: {total_size:.2f} MB")
    
    def validate_parquet_files(self):
        """Valida que los archivos Parquet se puedan leer correctamente"""
        logger.info("\nValidando archivos Parquet...")
        
        for file in self.staging_path.glob("*.parquet"):
            try:
                df = pd.read_parquet(file)
                logger.info(f"✓ {file.name}: {len(df):,} registros, {len(df.columns)} columnas")
            except Exception as e:
                logger.error(f"✗ Error leyendo {file.name}: {e}")


def main():
    """Función principal"""
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    loader = DataLakeLoader()
    loader.extract_all_tables()
    loader.validate_parquet_files()
    
    logger.success("\n¡Proceso de staging completado exitosamente!")


if __name__ == "__main__":
    main()
