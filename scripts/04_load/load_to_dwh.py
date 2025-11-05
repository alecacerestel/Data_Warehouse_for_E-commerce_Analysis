"""
Script para cargar modelo estrella a Data Warehouse OLAP
Lee archivos Parquet transformados y carga a PostgreSQL OLAP
"""
import pandas as pd
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import os

# Agregar directorio raiz al path
current_dir = Path(__file__).parent
root_dir = current_dir.parent.parent
sys.path.insert(0, str(root_dir))
os.chdir(str(root_dir))

from config.db_config import get_olap_connection_string

logger.add("logs/04_load_to_dwh.log", rotation="1 MB", level="INFO")


class DWHLoader:
    """Clase para cargar datos del modelo estrella a Data Warehouse OLAP"""
    
    def __init__(self, transformed_path: str = "data/transformed"):
        self.transformed_path = Path(transformed_path)
        self.engine = None
        self.connection_string = get_olap_connection_string()
        
    def connect(self):
        """Establece conexion con la base de datos OLAP"""
        try:
            self.engine = create_engine(self.connection_string)
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Conectado a PostgreSQL OLAP: {version}")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error al conectar con base de datos OLAP: {e}")
            return False
    
    def truncate_tables(self):
        """Limpia todas las tablas antes de cargar"""
        logger.info("Limpiando tablas OLAP...")
        
        truncate_queries = [
            "TRUNCATE TABLE fct_orders CASCADE",
            "TRUNCATE TABLE dim_customers CASCADE",
            "TRUNCATE TABLE dim_products CASCADE",
            "TRUNCATE TABLE dim_sellers CASCADE",
            "TRUNCATE TABLE dim_date CASCADE"
        ]
        
        try:
            with self.engine.connect() as conn:
                for query in truncate_queries:
                    conn.execute(text(query))
                conn.commit()
            logger.success("Tablas OLAP limpiadas correctamente")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error al limpiar tablas OLAP: {e}")
            return False
    
    def load_dimension(self, dimension_name: str, table_name: str):
        """
        Carga una dimension al DWH
        
        Args:
            dimension_name: Nombre del archivo Parquet (sin extension)
            table_name: Nombre de la tabla en la base de datos
        """
        logger.info(f"Cargando dimension {dimension_name}...")
        
        try:
            # Leer archivo Parquet
            parquet_file = self.transformed_path / f"{dimension_name}.parquet"
            df = pd.read_parquet(parquet_file)
            
            initial_count = len(df)
            logger.info(f"Registros leidos: {initial_count}")
            
            # Cargar a base de datos
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False,
                method=None,
                chunksize=5000
            )
            
            # Verificar carga
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                loaded_count = result.fetchone()[0]
            
            if loaded_count == initial_count:
                logger.success(f"Dimension {dimension_name} cargada: {loaded_count} registros")
                return True
            else:
                logger.warning(f"Discrepancia en {dimension_name}: esperados {initial_count}, cargados {loaded_count}")
                return False
                
        except Exception as e:
            logger.error(f"Error al cargar dimension {dimension_name}: {e}")
            return False
    
    def load_fact_table(self):
        """Carga la tabla de hechos al DWH"""
        logger.info("Cargando tabla de hechos fct_orders...")
        
        try:
            # Leer archivo Parquet
            parquet_file = self.transformed_path / "fct_orders.parquet"
            df = pd.read_parquet(parquet_file)
            
            initial_count = len(df)
            logger.info(f"Registros leidos: {initial_count}")
            
            # Convertir columnas booleanas a texto para evitar problemas
            if 'is_delayed' in df.columns:
                df['is_delayed'] = df['is_delayed'].astype(bool)
            
            # Cargar a base de datos en chunks mas pequenos por ser tabla grande
            df.to_sql(
                'fct_orders',
                self.engine,
                if_exists='append',
                index=False,
                method=None,
                chunksize=1000
            )
            
            # Verificar carga
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM fct_orders"))
                loaded_count = result.fetchone()[0]
            
            if loaded_count == initial_count:
                logger.success(f"Tabla de hechos cargada: {loaded_count} registros")
                return True
            else:
                logger.warning(f"Discrepancia en fct_orders: esperados {initial_count}, cargados {loaded_count}")
                return False
                
        except Exception as e:
            logger.error(f"Error al cargar tabla de hechos: {e}")
            return False
    
    def analyze_tables(self):
        """Ejecuta ANALYZE en todas las tablas para actualizar estadisticas"""
        logger.info("Actualizando estadisticas de tablas...")
        
        analyze_queries = [
            "ANALYZE dim_customers",
            "ANALYZE dim_products",
            "ANALYZE dim_sellers",
            "ANALYZE dim_date",
            "ANALYZE fct_orders"
        ]
        
        try:
            with self.engine.connect() as conn:
                for query in analyze_queries:
                    conn.execute(text(query))
                conn.commit()
            logger.success("Estadisticas actualizadas correctamente")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error al actualizar estadisticas: {e}")
            return False
    
    def verify_referential_integrity(self):
        """Verifica la integridad referencial de las claves foraneas"""
        logger.info("Verificando integridad referencial...")
        
        checks = [
            ("Customer FK", "SELECT COUNT(*) FROM fct_orders WHERE customer_key NOT IN (SELECT customer_key FROM dim_customers)"),
            ("Product FK", "SELECT COUNT(*) FROM fct_orders WHERE product_key IS NOT NULL AND product_key NOT IN (SELECT product_key FROM dim_products)"),
            ("Seller FK", "SELECT COUNT(*) FROM fct_orders WHERE seller_key IS NOT NULL AND seller_key NOT IN (SELECT seller_key FROM dim_sellers)"),
            ("Date FK", "SELECT COUNT(*) FROM fct_orders WHERE purchase_date_key NOT IN (SELECT date_key FROM dim_date)")
        ]
        
        all_valid = True
        
        try:
            with self.engine.connect() as conn:
                for check_name, query in checks:
                    result = conn.execute(text(query))
                    invalid_count = result.fetchone()[0]
                    
                    if invalid_count > 0:
                        logger.error(f"{check_name}: {invalid_count} referencias invalidas encontradas")
                        all_valid = False
                    else:
                        logger.success(f"{check_name}: OK")
            
            if all_valid:
                logger.success("Integridad referencial verificada correctamente")
            
            return all_valid
            
        except SQLAlchemyError as e:
            logger.error(f"Error al verificar integridad referencial: {e}")
            return False
    
    def get_load_summary(self):
        """Obtiene resumen de registros cargados"""
        logger.info("Obteniendo resumen de carga...")
        
        summary_queries = {
            "dim_customers": "SELECT COUNT(*) FROM dim_customers",
            "dim_products": "SELECT COUNT(*) FROM dim_products",
            "dim_sellers": "SELECT COUNT(*) FROM dim_sellers",
            "dim_date": "SELECT COUNT(*) FROM dim_date",
            "fct_orders": "SELECT COUNT(*) FROM fct_orders"
        }
        
        summary = {}
        
        try:
            with self.engine.connect() as conn:
                for table_name, query in summary_queries.items():
                    result = conn.execute(text(query))
                    count = result.fetchone()[0]
                    summary[table_name] = count
                    logger.info(f"  {table_name}: {count:,} registros")
            
            return summary
            
        except SQLAlchemyError as e:
            logger.error(f"Error al obtener resumen: {e}")
            return None
    
    def load_all(self):
        """Carga completa del modelo estrella al DWH"""
        logger.info("="*60)
        logger.info("INICIANDO CARGA A DATA WAREHOUSE OLAP")
        logger.info("="*60)
        
        # Conectar a base de datos
        if not self.connect():
            logger.error("No se pudo establecer conexion con OLAP")
            return False
        
        # Limpiar tablas
        if not self.truncate_tables():
            logger.error("No se pudieron limpiar las tablas")
            return False
        
        # Cargar dimensiones en orden
        dimensions = [
            ("dim_customers", "dim_customers"),
            ("dim_products", "dim_products"),
            ("dim_sellers", "dim_sellers"),
            ("dim_date", "dim_date")
        ]
        
        for dim_file, dim_table in dimensions:
            if not self.load_dimension(dim_file, dim_table):
                logger.error(f"Error al cargar dimension {dim_file}")
                return False
        
        # Cargar tabla de hechos
        if not self.load_fact_table():
            logger.error("Error al cargar tabla de hechos")
            return False
        
        # Verificar integridad referencial
        if not self.verify_referential_integrity():
            logger.warning("Se encontraron problemas de integridad referencial")
        
        # Actualizar estadisticas
        if not self.analyze_tables():
            logger.warning("No se pudieron actualizar las estadisticas")
        
        # Resumen final
        logger.info("="*60)
        logger.info("RESUMEN DE CARGA:")
        logger.info("="*60)
        summary = self.get_load_summary()
        
        if summary:
            total_records = sum(summary.values())
            logger.success(f"Carga completada exitosamente: {total_records:,} registros totales")
            logger.info("="*60)
            return True
        else:
            logger.error("No se pudo obtener resumen de carga")
            return False


if __name__ == "__main__":
    loader = DWHLoader()
    success = loader.load_all()
    
    if success:
        logger.success("Data Warehouse cargado correctamente")
        sys.exit(0)
    else:
        logger.error("Error al cargar Data Warehouse")
        sys.exit(1)
