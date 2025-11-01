"""
Script para cargar archivos CSV a la base de datos OLTP (PostgreSQL)
Fase 1: Extracción - Simular base de datos transaccional
"""
import pandas as pd
import sys
import os
from pathlib import Path
from sqlalchemy import text
from tqdm import tqdm
from loguru import logger

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig, load_config

# Configurar logging
logger.add("logs/01_load_csv_to_oltp.log", rotation="1 MB", level="INFO")


class CSVToOLTPLoader:
    """Clase para cargar CSVs a la base de datos OLTP"""
    
    def __init__(self, data_path: str = "data/raw"):
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_oltp_engine()
        self.data_path = Path(data_path)
        self.config = load_config()
        
    def create_schema(self):
        """Crear schema de OLTP si no existe"""
        logger.info("Creando schema OLTP...")
        
        sql_file = Path("sql/oltp_schema.sql")
        if sql_file.exists():
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_commands = f.read()
            
            with self.engine.connect() as conn:
                # Ejecutar comando por comando
                for command in sql_commands.split(';'):
                    if command.strip():
                        try:
                            conn.execute(text(command))
                            conn.commit()
                        except Exception as e:
                            logger.warning(f"Error ejecutando comando SQL: {e}")
                            conn.rollback()  # Hacer rollback en caso de error
                            continue
            
            logger.success("Schema OLTP creado exitosamente")
        else:
            logger.warning("Archivo oltp_schema.sql no encontrado")
    
    def load_customers(self):
        """Cargar datos de clientes"""
        logger.info("Cargando datos de clientes...")
        
        file_path = self.data_path / self.config['source_files']['customers']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Eliminar duplicados basados en customer_id (primary key)
        original_count = len(df)
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        duplicates_removed = original_count - len(df)
        if duplicates_removed > 0:
            logger.warning(f"Se eliminaron {duplicates_removed} clientes duplicados")
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        # Cargar a la base de datos
        df.to_sql('customers', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} clientes cargados exitosamente")
        return len(df)
    
    def load_products(self):
        """Cargar datos de productos"""
        logger.info("Cargando datos de productos...")
        
        file_path = self.data_path / self.config['source_files']['products']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('products', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} productos cargados exitosamente")
        return len(df)
    
    def load_sellers(self):
        """Cargar datos de vendedores"""
        logger.info("Cargando datos de vendedores...")
        
        file_path = self.data_path / self.config['source_files']['sellers']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('sellers', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} vendedores cargados exitosamente")
        return len(df)
    
    def load_orders(self):
        """Cargar datos de órdenes"""
        logger.info("Cargando datos de órdenes...")
        
        file_path = self.data_path / self.config['source_files']['orders']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Convertir columnas de fecha
        date_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Reemplazar NaT por None para compatibilidad con SQLAlchemy 2.0
            df[col] = df[col].where(pd.notna(df[col]), None)
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('orders', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} órdenes cargadas exitosamente")
        return len(df)
    
    def load_order_items(self):
        """Cargar datos de items de órdenes"""
        logger.info("Cargando datos de items de órdenes...")
        
        file_path = self.data_path / self.config['source_files']['order_items']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Convertir fecha
        df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('order_items', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} items de órdenes cargados exitosamente")
        return len(df)
    
    def load_order_payments(self):
        """Cargar datos de pagos"""
        logger.info("Cargando datos de pagos...")
        
        file_path = self.data_path / self.config['source_files']['order_payments']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('order_payments', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} pagos cargados exitosamente")
        return len(df)
    
    def load_order_reviews(self):
        """Cargar datos de reviews"""
        logger.info("Cargando datos de reviews...")
        
        file_path = self.data_path / self.config['source_files']['order_reviews']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Convertir fechas
        df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
        df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        # Eliminar duplicados basados en review_id (primary key)
        original_count = len(df)
        df = df.drop_duplicates(subset=['review_id'], keep='first')
        duplicates_removed = original_count - len(df)
        
        if duplicates_removed > 0:
            logger.warning(f"Se eliminaron {duplicates_removed} reviews duplicadas")
        
        df.to_sql('order_reviews', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        logger.success(f"✓ {len(df)} reviews cargadas exitosamente")
        return len(df)
    
    def load_geolocation(self):
        """Cargar datos de geolocalización"""
        logger.info("Cargando datos de geolocalización...")
        
        file_path = self.data_path / self.config['source_files']['geolocation']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        # Cargar en chunks debido al tamaño
        df.to_sql('geolocation', self.engine, if_exists='append', index=False, method='multi', chunksize=5000)
        
        logger.success(f"✓ {len(df)} registros de geolocalización cargados")
        return len(df)
    
    def load_product_translation(self):
        """Cargar traducción de categorías de productos"""
        logger.info("Cargando traducción de categorías...")
        
        file_path = self.data_path / self.config['source_files']['product_translation']
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Registros encontrados: {len(df)}")
        
        df.to_sql('product_category_translation', self.engine, if_exists='append', index=False, method='multi')
        
        logger.success(f"✓ {len(df)} traducciones cargadas exitosamente")
        return len(df)
    
    def load_all(self):
        """Cargar todos los archivos CSV"""
        logger.info("="*60)
        logger.info("INICIANDO CARGA DE CSVs A BASE DE DATOS OLTP")
        logger.info("="*60)
        
        # Crear schema (comentado porque ya existe)
        # self.create_schema()
        
        # Orden de carga respetando las foreign keys
        load_order = [
            ("Clientes", self.load_customers),
            ("Productos", self.load_products),
            ("Vendedores", self.load_sellers),
            ("Traducción de Categorías", self.load_product_translation),
            ("Órdenes", self.load_orders),
            ("Items de Órdenes", self.load_order_items),
            ("Pagos", self.load_order_payments),
            ("Reviews", self.load_order_reviews),
            ("Geolocalización", self.load_geolocation),
        ]
        
        total_records = 0
        
        for name, load_func in tqdm(load_order, desc="Cargando tablas"):
            try:
                records = load_func()
                total_records += records
            except Exception as e:
                logger.error(f"Error cargando {name}: {e}")
                raise
        
        logger.info("="*60)
        logger.success(f"✓ CARGA COMPLETADA: {total_records} registros totales")
        logger.info("="*60)
        
        # Verificar integridad
        self.verify_data()
    
    def verify_data(self):
        """Verificar que los datos se cargaron correctamente"""
        logger.info("Verificando integridad de datos...")
        
        tables = [
            'customers', 'products', 'sellers', 'orders', 
            'order_items', 'order_payments', 'order_reviews', 
            'geolocation', 'product_category_translation'
        ]
        
        with self.engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info(f"  {table}: {count:,} registros")


def main():
    """Función principal"""
    # Ruta a los archivos CSV
    data_path = Path(__file__).parent.parent.parent
    
    loader = CSVToOLTPLoader(data_path=data_path)
    loader.load_all()
    
    logger.success("¡Proceso completado exitosamente!")


if __name__ == "__main__":
    main()
