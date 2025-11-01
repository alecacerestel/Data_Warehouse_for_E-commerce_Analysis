"""
Script para limpieza y preparación de datos
Fase 3: Transformación - Limpieza de datos
"""
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from loguru import logger

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import load_config

# Configurar logging
logger.add("logs/03_data_cleaning.log", rotation="1 MB", level="INFO")


class DataCleaner:
    """Clase para limpieza y preparación de datos"""
    
    def __init__(self, staging_path: str = "data/staging", processed_path: str = "data/processed"):
        self.staging_path = Path(staging_path)
        self.processed_path = Path(processed_path)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        self.config = load_config()
        
        logger.info(f"Staging path: {self.staging_path}")
        logger.info(f"Processed path: {self.processed_path}")
    
    def load_parquet(self, filename: str) -> pd.DataFrame:
        """Carga un archivo Parquet"""
        file_path = self.staging_path / filename
        return pd.read_parquet(file_path)
    
    def save_parquet(self, df: pd.DataFrame, filename: str):
        """Guarda un DataFrame como Parquet"""
        output_path = self.processed_path / filename
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        logger.success(f"✓ Guardado: {filename}")
    
    def clean_customers(self):
        """Limpia datos de clientes"""
        logger.info("Limpiando datos de clientes...")
        
        df = self.load_parquet("customers.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar duplicados por customer_id
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        
        # Limpiar valores nulos en campos críticos
        df = df.dropna(subset=['customer_id', 'customer_unique_id'])
        
        # Estandarizar estado (mayúsculas)
        df['customer_state'] = df['customer_state'].str.upper()
        
        # Limpiar ciudad (lowercase y strip)
        df['customer_city'] = df['customer_city'].str.lower().str.strip()
        
        # Agregar región geográfica
        def get_region(state):
            regions = {
                'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
                'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
                'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
                'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
                'Sul': ['PR', 'RS', 'SC']
            }
            for region, states in regions.items():
                if state in states:
                    return region
            return 'Desconocido'
        
        df['customer_region'] = df['customer_state'].apply(get_region)
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        logger.info(f"Registros eliminados: {len(self.load_parquet('customers.parquet')) - len(df):,}")
        
        self.save_parquet(df, "customers_clean.parquet")
        return df
    
    def clean_products(self):
        """Limpia datos de productos"""
        logger.info("Limpiando datos de productos...")
        
        df = self.load_parquet("products.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        # Eliminar productos sin ID
        df = df.dropna(subset=['product_id'])
        
        # Rellenar categorías faltantes
        df['product_category_name'] = df['product_category_name'].fillna('sem_categoria')
        
        # Calcular volumen del producto (cm³)
        df['product_volume_cm3'] = (
            df['product_length_cm'].fillna(0) * 
            df['product_height_cm'].fillna(0) * 
            df['product_width_cm'].fillna(0)
        )
        
        # Categorizar peso
        def categorize_weight(weight):
            if pd.isna(weight) or weight == 0:
                return 'Desconocido'
            elif weight < 500:
                return 'Liviano'
            elif weight < 2000:
                return 'Medio'
            elif weight < 10000:
                return 'Pesado'
            else:
                return 'Muy Pesado'
        
        df['product_weight_category'] = df['product_weight_g'].apply(categorize_weight)
        
        # Cargar traducción de categorías
        translation_df = self.load_parquet("product_category_translation.parquet")
        df = df.merge(
            translation_df[['product_category_name', 'product_category_name_english']],
            on='product_category_name',
            how='left'
        )
        
        # Si no hay traducción, usar la categoría original
        df['product_category_name_english'] = df['product_category_name_english'].fillna(
            df['product_category_name']
        )
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        
        self.save_parquet(df, "products_clean.parquet")
        return df
    
    def clean_sellers(self):
        """Limpia datos de vendedores"""
        logger.info("Limpiando datos de vendedores...")
        
        df = self.load_parquet("sellers.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['seller_id'], keep='first')
        
        # Limpiar estado
        df['seller_state'] = df['seller_state'].str.upper()
        df['seller_city'] = df['seller_city'].str.lower().str.strip()
        
        # Agregar región
        def get_region(state):
            regions = {
                'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
                'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
                'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
                'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
                'Sul': ['PR', 'RS', 'SC']
            }
            for region, states in regions.items():
                if state in states:
                    return region
            return 'Desconocido'
        
        df['seller_region'] = df['seller_state'].apply(get_region)
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        
        self.save_parquet(df, "sellers_clean.parquet")
        return df
    
    def clean_orders(self):
        """Limpia datos de órdenes"""
        logger.info("Limpiando datos de órdenes...")
        
        df = self.load_parquet("orders.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar órdenes sin ID
        df = df.dropna(subset=['order_id', 'customer_id'])
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        
        # Convertir fechas
        date_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Calcular métricas de tiempo (en días)
        df['delivery_time_days'] = (
            df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        ).dt.total_seconds() / 86400
        
        df['estimated_delivery_time_days'] = (
            df['order_estimated_delivery_date'] - df['order_purchase_timestamp']
        ).dt.total_seconds() / 86400
        
        df['delivery_delay_days'] = (
            df['order_delivered_customer_date'] - df['order_estimated_delivery_date']
        ).dt.total_seconds() / 86400
        
        # Flag de retraso
        df['is_delayed'] = df['delivery_delay_days'] > 0
        
        # Flag de cancelación
        df['is_cancelled'] = df['order_status'].isin(['canceled', 'cancelled'])
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        logger.info(f"Órdenes entregadas: {len(df[df['order_status'] == 'delivered']):,}")
        logger.info(f"Órdenes canceladas: {len(df[df['is_cancelled']]):,}")
        logger.info(f"Órdenes con retraso: {len(df[df['is_delayed'] == True]):,}")
        
        self.save_parquet(df, "orders_clean.parquet")
        return df
    
    def clean_order_items(self):
        """Limpia datos de items de órdenes"""
        logger.info("Limpiando datos de items de órdenes...")
        
        df = self.load_parquet("order_items.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar registros sin IDs críticos
        df = df.dropna(subset=['order_id', 'product_id', 'seller_id'])
        
        # Eliminar precios negativos o cero
        df = df[df['price'] > 0]
        
        # Calcular valor total
        df['total_value'] = df['price'] + df['freight_value'].fillna(0)
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        logger.info(f"Valor promedio de item: R$ {df['price'].mean():.2f}")
        logger.info(f"Flete promedio: R$ {df['freight_value'].mean():.2f}")
        
        self.save_parquet(df, "order_items_clean.parquet")
        return df
    
    def clean_order_payments(self):
        """Limpia datos de pagos"""
        logger.info("Limpiando datos de pagos...")
        
        df = self.load_parquet("order_payments.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar pagos sin order_id
        df = df.dropna(subset=['order_id'])
        
        # Eliminar valores negativos
        df = df[df['payment_value'] >= 0]
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        logger.info(f"Valor promedio de pago: R$ {df['payment_value'].mean():.2f}")
        logger.info(f"Tipos de pago: {df['payment_type'].value_counts().to_dict()}")
        
        self.save_parquet(df, "order_payments_clean.parquet")
        return df
    
    def clean_order_reviews(self):
        """Limpia datos de reviews"""
        logger.info("Limpiando datos de reviews...")
        
        df = self.load_parquet("order_reviews.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar reviews sin IDs
        df = df.dropna(subset=['review_id', 'order_id'])
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['review_id'], keep='first')
        
        # Validar scores (1-5)
        df = df[df['review_score'].between(1, 5)]
        
        logger.info(f"Registros después de limpieza: {len(df):,}")
        logger.info(f"Score promedio: {df['review_score'].mean():.2f}")
        logger.info(f"Distribución de scores:\n{df['review_score'].value_counts().sort_index()}")
        
        self.save_parquet(df, "order_reviews_clean.parquet")
        return df
    
    def clean_geolocation(self):
        """Limpia datos de geolocalización"""
        logger.info("Limpiando datos de geolocalización...")
        
        df = self.load_parquet("geolocation.parquet")
        logger.info(f"Registros originales: {len(df):,}")
        
        # Eliminar duplicados (tomar promedio por zip code)
        df_agg = df.groupby('geolocation_zip_code_prefix').agg({
            'geolocation_lat': 'mean',
            'geolocation_lng': 'mean',
            'geolocation_city': 'first',
            'geolocation_state': 'first'
        }).reset_index()
        
        # Agregar región
        def get_region(state):
            regions = {
                'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
                'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
                'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
                'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
                'Sul': ['PR', 'RS', 'SC']
            }
            for region, states in regions.items():
                if state in states:
                    return region
            return 'Desconocido'
        
        df_agg['geolocation_region'] = df_agg['geolocation_state'].apply(get_region)
        
        logger.info(f"Registros después de limpieza: {len(df_agg):,}")
        logger.info(f"Reducción: {len(df) - len(df_agg):,} registros duplicados eliminados")
        
        self.save_parquet(df_agg, "geolocation_clean.parquet")
        return df_agg
    
    def clean_all(self):
        """Ejecuta limpieza de todos los datasets"""
        logger.info("="*60)
        logger.info("INICIANDO LIMPIEZA DE DATOS")
        logger.info("="*60)
        
        cleaning_functions = [
            self.clean_customers,
            self.clean_products,
            self.clean_sellers,
            self.clean_orders,
            self.clean_order_items,
            self.clean_order_payments,
            self.clean_order_reviews,
            self.clean_geolocation
        ]
        
        for func in cleaning_functions:
            try:
                func()
                logger.info("-"*60)
            except Exception as e:
                logger.error(f"Error en {func.__name__}: {e}")
                raise
        
        logger.info("="*60)
        logger.success("✓ LIMPIEZA DE DATOS COMPLETADA")
        logger.info("="*60)


def main():
    """Función principal"""
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    cleaner = DataCleaner()
    cleaner.clean_all()
    
    logger.success("\n¡Limpieza completada exitosamente!")


if __name__ == "__main__":
    main()
