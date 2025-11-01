"""
Script para crear tablas de dimensiones del modelo estrella
Fase 3: Transformación - Creación de dimensiones
"""
import pandas as pd
import sys
from pathlib import Path
from loguru import logger

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig

# Configurar logging
logger.add("logs/03_create_dimensions.log", rotation="1 MB", level="INFO")


class DimensionBuilder:
    """Clase para construir tablas de dimensiones"""
    
    def __init__(self, processed_path: str = "data/processed"):
        self.processed_path = Path(processed_path)
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_dwh_engine()
        
        logger.info(f"Processed data path: {self.processed_path}")
    
    def load_parquet(self, filename: str) -> pd.DataFrame:
        """Carga un archivo Parquet"""
        file_path = self.processed_path / filename
        return pd.read_parquet(file_path)
    
    def save_to_database(self, df: pd.DataFrame, table_name: str):
        """Guarda DataFrame en la base de datos"""
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.success(f"✓ {table_name} creada: {len(df):,} registros")
        except Exception as e:
            logger.error(f"Error guardando {table_name}: {e}")
            raise
    
    def create_dim_customers(self):
        """Crear dimensión de clientes"""
        logger.info("Creando dim_customers...")
        
        df = self.load_parquet("customers_clean.parquet")
        
        # Seleccionar y renombrar columnas
        dim_customers = df[[
            'customer_id',
            'customer_unique_id',
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state',
            'customer_region'
        ]].copy()
        
        # Agregar campos de auditoría SCD Type 2
        dim_customers['valid_from'] = pd.Timestamp.now()
        dim_customers['valid_to'] = pd.Timestamp('2999-12-31')
        dim_customers['is_current'] = True
        dim_customers['created_at'] = pd.Timestamp.now()
        dim_customers['updated_at'] = pd.Timestamp.now()
        
        logger.info(f"Clientes únicos: {dim_customers['customer_unique_id'].nunique():,}")
        logger.info(f"Distribución por región:\n{dim_customers['customer_region'].value_counts()}")
        
        self.save_to_database(dim_customers, 'dim_customers')
        return dim_customers
    
    def create_dim_products(self):
        """Crear dimensión de productos"""
        logger.info("Creando dim_products...")
        
        df = self.load_parquet("products_clean.parquet")
        
        # Seleccionar columnas
        dim_products = df[[
            'product_id',
            'product_category_name',
            'product_category_name_english',
            'product_name_lenght',
            'product_description_lenght',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm',
            'product_volume_cm3',
            'product_weight_category'
        ]].copy()
        
        # Agregar campos de auditoría
        dim_products['valid_from'] = pd.Timestamp.now()
        dim_products['valid_to'] = pd.Timestamp('2999-12-31')
        dim_products['is_current'] = True
        dim_products['created_at'] = pd.Timestamp.now()
        dim_products['updated_at'] = pd.Timestamp.now()
        
        logger.info(f"Productos únicos: {len(dim_products):,}")
        logger.info(f"Categorías: {dim_products['product_category_name_english'].nunique():,}")
        logger.info(f"Top 5 categorías:\n{dim_products['product_category_name_english'].value_counts().head()}")
        
        self.save_to_database(dim_products, 'dim_products')
        return dim_products
    
    def create_dim_sellers(self):
        """Crear dimensión de vendedores"""
        logger.info("Creando dim_sellers...")
        
        df = self.load_parquet("sellers_clean.parquet")
        
        # Seleccionar columnas
        dim_sellers = df[[
            'seller_id',
            'seller_zip_code_prefix',
            'seller_city',
            'seller_state',
            'seller_region'
        ]].copy()
        
        # Agregar campos de auditoría
        dim_sellers['valid_from'] = pd.Timestamp.now()
        dim_sellers['valid_to'] = pd.Timestamp('2999-12-31')
        dim_sellers['is_current'] = True
        dim_sellers['created_at'] = pd.Timestamp.now()
        dim_sellers['updated_at'] = pd.Timestamp.now()
        
        logger.info(f"Vendedores únicos: {len(dim_sellers):,}")
        logger.info(f"Distribución por región:\n{dim_sellers['seller_region'].value_counts()}")
        
        self.save_to_database(dim_sellers, 'dim_sellers')
        return dim_sellers
    
    def create_dim_geolocation(self):
        """Crear dimensión de geolocalización"""
        logger.info("Creando dim_geolocation...")
        
        df = self.load_parquet("geolocation_clean.parquet")
        
        # Seleccionar columnas
        dim_geolocation = df[[
            'geolocation_zip_code_prefix',
            'geolocation_lat',
            'geolocation_lng',
            'geolocation_city',
            'geolocation_state',
            'geolocation_region'
        ]].copy()
        
        dim_geolocation['created_at'] = pd.Timestamp.now()
        
        logger.info(f"Localizaciones únicas: {len(dim_geolocation):,}")
        
        self.save_to_database(dim_geolocation, 'dim_geolocation')
        return dim_geolocation
    
    def create_dim_date(self):
        """Crear dimensión de fecha"""
        logger.info("Creando dim_date...")
        
        # Cargar órdenes para obtener rango de fechas
        orders_df = self.load_parquet("orders_clean.parquet")
        
        # Obtener fecha mínima y máxima
        min_date = orders_df['order_purchase_timestamp'].min().date()
        max_date = orders_df['order_purchase_timestamp'].max().date()
        
        # Extender el rango un poco
        min_date = pd.Timestamp(min_date) - pd.Timedelta(days=365)
        max_date = pd.Timestamp(max_date) + pd.Timedelta(days=365)
        
        logger.info(f"Rango de fechas: {min_date.date()} a {max_date.date()}")
        
        # Crear rango de fechas
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        # Crear DataFrame de dimensión de fecha
        dim_date = pd.DataFrame({
            'full_date': date_range,
            'year': date_range.year,
            'quarter': date_range.quarter,
            'month': date_range.month,
            'month_name': date_range.strftime('%B'),
            'week': date_range.isocalendar().week,
            'day_of_month': date_range.day,
            'day_of_week': date_range.dayofweek,
            'day_name': date_range.strftime('%A'),
            'is_weekend': date_range.dayofweek.isin([5, 6]),
            'is_holiday': False  # Se puede mejorar agregando días festivos
        })
        
        # Agregar temporada (hemisferio sur - Brasil)
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Verano'
            elif month in [3, 4, 5]:
                return 'Otoño'
            elif month in [6, 7, 8]:
                return 'Invierno'
            else:
                return 'Primavera'
        
        dim_date['season'] = dim_date['month'].apply(get_season)
        
        # Crear date_key (YYYYMMDD)
        dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
        
        dim_date['created_at'] = pd.Timestamp.now()
        
        # Reordenar columnas para que date_key sea la primera
        cols = ['date_key'] + [col for col in dim_date.columns if col != 'date_key']
        dim_date = dim_date[cols]
        
        logger.info(f"Fechas creadas: {len(dim_date):,}")
        logger.info(f"Años cubiertos: {dim_date['year'].min()} - {dim_date['year'].max()}")
        
        self.save_to_database(dim_date, 'dim_date')
        return dim_date
    
    def create_all_dimensions(self):
        """Crear todas las dimensiones"""
        logger.info("="*60)
        logger.info("INICIANDO CREACIÓN DE DIMENSIONES")
        logger.info("="*60)
        
        dimensions = [
            ("Clientes", self.create_dim_customers),
            ("Productos", self.create_dim_products),
            ("Vendedores", self.create_dim_sellers),
            ("Geolocalización", self.create_dim_geolocation),
            ("Fecha", self.create_dim_date)
        ]
        
        for name, func in dimensions:
            try:
                logger.info(f"\n--- {name} ---")
                func()
            except Exception as e:
                logger.error(f"Error creando dimensión {name}: {e}")
                raise
        
        logger.info("="*60)
        logger.success("✓ TODAS LAS DIMENSIONES CREADAS EXITOSAMENTE")
        logger.info("="*60)
        
        # Verificar dimensiones creadas
        self.verify_dimensions()
    
    def verify_dimensions(self):
        """Verificar que las dimensiones se crearon correctamente"""
        logger.info("\nVerificando dimensiones en DWH...")
        
        dimensions = [
            'dim_customers',
            'dim_products',
            'dim_sellers',
            'dim_geolocation',
            'dim_date'
        ]
        
        from sqlalchemy import text
        
        with self.engine.connect() as conn:
            for dim in dimensions:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {dim}"))
                    count = result.scalar()
                    logger.info(f"  ✓ {dim}: {count:,} registros")
                except Exception as e:
                    logger.error(f"  ✗ {dim}: Error - {e}")


def main():
    """Función principal"""
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    builder = DimensionBuilder()
    builder.create_all_dimensions()
    
    logger.success("\n¡Dimensiones creadas exitosamente!")


if __name__ == "__main__":
    main()
