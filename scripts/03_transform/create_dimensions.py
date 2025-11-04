"""
Script para crear dimensiones del modelo estrella
Genera tablas de dimensiones desde datos limpios
"""
import pandas as pd
from pathlib import Path
from loguru import logger
import sys
import os

# Agregar directorio raiz al path
current_dir = Path(__file__).parent
root_dir = current_dir.parent.parent
sys.path.insert(0, str(root_dir))
os.chdir(str(root_dir))

# Importar desde el mismo directorio
from data_cleaning import DataCleaner

logger.add("logs/03_create_dimensions.log", rotation="1 MB", level="INFO")


class DimensionBuilder:
    """Clase para construir dimensiones del modelo estrella"""
    
    def __init__(self, staging_path: str = "data/staging"):
        self.staging_path = Path(staging_path)
        self.cleaner = DataCleaner(staging_path)
        
    def create_dim_customers(self) -> pd.DataFrame:
        """Crea dimension de clientes"""
        logger.info("Creando dimension de clientes...")
        
        df_customers = self.cleaner.clean_customers()
        
        # Crear dimension con campos relevantes
        dim_customers = df_customers[[
            'customer_id',
            'customer_unique_id',
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state'
        ]].copy()
        
        # Agregar clasificacion por region
        regiones = {
            'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
            'PR': 'Sur', 'SC': 'Sur', 'RS': 'Sur',
            'BA': 'Nordeste', 'SE': 'Nordeste', 'AL': 'Nordeste', 'PE': 'Nordeste',
            'PB': 'Nordeste', 'RN': 'Nordeste', 'CE': 'Nordeste', 'PI': 'Nordeste',
            'MA': 'Nordeste',
            'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
            'AM': 'Norte', 'RR': 'Norte', 'AP': 'Norte', 'PA': 'Norte',
            'TO': 'Norte', 'RO': 'Norte', 'AC': 'Norte'
        }
        
        dim_customers['customer_region'] = dim_customers['customer_state'].map(regiones)
        dim_customers['customer_region'] = dim_customers['customer_region'].fillna('Desconocido')
        
        # Agregar surrogate key
        dim_customers.insert(0, 'customer_key', range(1, len(dim_customers) + 1))
        
        logger.success(f"Dimension clientes creada: {len(dim_customers)} registros")
        return dim_customers
    
    def create_dim_products(self) -> pd.DataFrame:
        """Crea dimension de productos"""
        logger.info("Creando dimension de productos...")
        
        df_products = self.cleaner.clean_products()
        
        # Leer traduccion de categorias
        df_translation = pd.read_parquet(self.staging_path / "product_category_translation.parquet")
        
        # Join con traduccion
        dim_products = df_products.merge(
            df_translation[['product_category_name', 'product_category_name_english']],
            on='product_category_name',
            how='left'
        )
        
        # Rellenar categorias sin traduccion
        dim_products['product_category_name_english'] = dim_products['product_category_name_english'].fillna('without_category')
        
        # Seleccionar campos relevantes
        dim_products = dim_products[[
            'product_id',
            'product_category_name',
            'product_category_name_english',
            'product_name_lenght',
            'product_description_lenght',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ]].copy()
        
        # Calcular volumen del producto
        dim_products['product_volume_cm3'] = (
            dim_products['product_length_cm'] * 
            dim_products['product_height_cm'] * 
            dim_products['product_width_cm']
        )
        
        # Clasificar por tamaño
        def classify_size(row):
            if row['product_weight_g'] == 0:
                return 'desconocido'
            elif row['product_weight_g'] < 500:
                return 'pequeno'
            elif row['product_weight_g'] < 2000:
                return 'mediano'
            else:
                return 'grande'
        
        dim_products['product_size'] = dim_products.apply(classify_size, axis=1)
        
        # Agregar surrogate key
        dim_products.insert(0, 'product_key', range(1, len(dim_products) + 1))
        
        logger.success(f"Dimension productos creada: {len(dim_products)} registros")
        return dim_products
    
    def create_dim_sellers(self) -> pd.DataFrame:
        """Crea dimension de vendedores"""
        logger.info("Creando dimension de vendedores...")
        
        df_sellers = self.cleaner.clean_sellers()
        
        # Seleccionar campos relevantes
        dim_sellers = df_sellers[[
            'seller_id',
            'seller_zip_code_prefix',
            'seller_city',
            'seller_state'
        ]].copy()
        
        # Agregar region
        regiones = {
            'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
            'PR': 'Sur', 'SC': 'Sur', 'RS': 'Sur',
            'BA': 'Nordeste', 'SE': 'Nordeste', 'AL': 'Nordeste', 'PE': 'Nordeste',
            'PB': 'Nordeste', 'RN': 'Nordeste', 'CE': 'Nordeste', 'PI': 'Nordeste',
            'MA': 'Nordeste',
            'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
            'AM': 'Norte', 'RR': 'Norte', 'AP': 'Norte', 'PA': 'Norte',
            'TO': 'Norte', 'RO': 'Norte', 'AC': 'Norte'
        }
        
        dim_sellers['seller_region'] = dim_sellers['seller_state'].map(regiones)
        dim_sellers['seller_region'] = dim_sellers['seller_region'].fillna('Desconocido')
        
        # Agregar surrogate key
        dim_sellers.insert(0, 'seller_key', range(1, len(dim_sellers) + 1))
        
        logger.success(f"Dimension vendedores creada: {len(dim_sellers)} registros")
        return dim_sellers
    
    def create_dim_date(self) -> pd.DataFrame:
        """Crea dimension de fecha"""
        logger.info("Creando dimension de fecha...")
        
        df_orders = self.cleaner.clean_orders()
        
        # Extraer todas las fechas de ordenes
        dates = pd.to_datetime(df_orders['order_purchase_timestamp']).dropna()
        
        # Crear rango de fechas desde la minima hasta la maxima
        min_date = dates.min().date()
        max_date = dates.max().date()
        
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        # Crear dimension
        dim_date = pd.DataFrame({
            'date_key': range(1, len(date_range) + 1),
            'full_date': date_range,
            'year': date_range.year,
            'month': date_range.month,
            'day': date_range.day,
            'quarter': date_range.quarter,
            'day_of_week': date_range.dayofweek,
            'day_name': date_range.day_name(),
            'month_name': date_range.month_name(),
            'is_weekend': date_range.dayofweek.isin([5, 6])
        })
        
        # Agregar campos adicionales
        dim_date['week_of_year'] = dim_date['full_date'].dt.isocalendar().week
        dim_date['day_of_year'] = dim_date['full_date'].dt.dayofyear
        
        # Clasificar por trimestre
        dim_date['quarter_name'] = 'Q' + dim_date['quarter'].astype(str)
        
        logger.success(f"Dimension fecha creada: {len(dim_date)} registros")
        return dim_date
    
    def create_all_dimensions(self) -> dict:
        """Crea todas las dimensiones"""
        logger.info("="*60)
        logger.info("INICIANDO CREACION DE DIMENSIONES")
        logger.info("="*60)
        
        dimensions = {
            'dim_customers': self.create_dim_customers(),
            'dim_products': self.create_dim_products(),
            'dim_sellers': self.create_dim_sellers(),
            'dim_date': self.create_dim_date()
        }
        
        total_records = sum(len(df) for df in dimensions.values())
        
        logger.info("="*60)
        logger.success(f"DIMENSIONES CREADAS: {total_records} registros totales")
        logger.info("="*60)
        
        return dimensions


if __name__ == "__main__":
    builder = DimensionBuilder()
    dimensions = builder.create_all_dimensions()
    
    # Mostrar resumen
    for name, df in dimensions.items():
        print(f"{name}: {len(df)} registros")
        print(f"  Columnas: {', '.join(df.columns.tolist())}\n")
