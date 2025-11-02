"""
Script para extraer datos de OLTP y cargarlos al Data Lake local en formato Parquet
Fase 2: Staging - Almacenamiento intermedio
"""
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import text
from tqdm import tqdm
from loguru import logger
import time

# Agregar el directorio raiz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig

# Configurar logging
logger.add("logs/02_load_to_staging.log", rotation="1 MB", level="INFO")


class OLTPToStagingLoader:
    """Clase para extraer datos de OLTP y guardarlos en formato Parquet"""
    
    def __init__(self, staging_path: str = "data/staging"):
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_oltp_engine()
        self.staging_path = Path(staging_path)
        
        # Crear directorio si no existe
        self.staging_path.mkdir(parents=True, exist_ok=True)
        
    def extract_table_to_parquet(self, table_name: str, query: str = None):
        """
        Extrae una tabla de OLTP y la guarda en formato Parquet
        
        Args:
            table_name: Nombre de la tabla
            query: Query SQL personalizada (opcional)
        """
        try:
            logger.info(f"Extrayendo tabla {table_name} desde OLTP...")
            
            # Si no se proporciona query, extraer toda la tabla
            if query is None:
                query = f"SELECT * FROM {table_name}"
            
            # Extraer datos
            df = pd.read_sql(query, self.engine)
            
            logger.info(f"Registros extraidos: {len(df)}")
            
            # Guardar en formato Parquet
            output_path = self.staging_path / f"{table_name}.parquet"
            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Calcular tamaño del archivo
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            
            logger.success(f"Tabla {table_name} guardada en Parquet ({file_size_mb:.2f} MB)")
            
            return len(df), file_size_mb
            
        except Exception as e:
            logger.error(f"Error procesando tabla {table_name}: {e}")
            raise
    
    def load_all_to_staging(self):
        """Extrae todas las tablas de OLTP al Data Lake"""
        logger.info("="*60)
        logger.info("INICIANDO CARGA A DATA LAKE - FORMATO PARQUET")
        logger.info("="*60)
        
        # Definir tablas a extraer
        tables = [
            'customers',
            'products',
            'sellers',
            'product_category_translation',
            'orders',
            'order_items',
            'order_payments',
            'order_reviews',
            'geolocation'
        ]
        
        total_records = 0
        total_size_mb = 0
        start_time = time.time()
        
        # Procesar cada tabla
        for table in tqdm(tables, desc="Extrayendo tablas"):
            records, size_mb = self.extract_table_to_parquet(table)
            total_records += records
            total_size_mb += size_mb
        
        elapsed_time = time.time() - start_time
        
        logger.info("="*60)
        logger.success(f"EXTRACCION COMPLETADA: {total_records} registros totales")
        logger.info(f"Tamaño total en Parquet: {total_size_mb:.2f} MB")
        logger.info(f"Tiempo de ejecucion: {elapsed_time:.2f} segundos")
        logger.info("="*60)
        
        return total_records, total_size_mb
    
    def verify_staging_data(self):
        """Verifica los datos en el Data Lake"""
        logger.info("Verificando datos en staging...")
        
        parquet_files = list(self.staging_path.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning("No se encontraron archivos Parquet en staging")
            return
        
        logger.info(f"Archivos encontrados: {len(parquet_files)}")
        
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"  {file_path.name}: {len(df):,} registros ({file_size_mb:.2f} MB)")
        
        logger.success("Verificacion de staging completada")


def main():
    """Funcion principal"""
    try:
        logger.info("Iniciando proceso de staging...")
        
        # Crear loader
        loader = OLTPToStagingLoader()
        
        # Cargar datos a staging
        total_records, total_size = loader.load_all_to_staging()
        
        # Verificar datos
        loader.verify_staging_data()
        
        logger.success("Proceso de staging completado exitosamente")
        logger.info(f"Total: {total_records:,} registros en {total_size:.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Error en el proceso de staging: {e}")
        logger.exception("Detalles del error:")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
