"""
Script para limpieza y calidad de datos
Lee archivos Parquet de staging y aplica transformaciones basicas
"""
import pandas as pd
from pathlib import Path
from loguru import logger

logger.add("logs/03_data_cleaning.log", rotation="1 MB", level="INFO")


class DataCleaner:
    """Clase para limpiar y validar datos de staging"""
    
    def __init__(self, staging_path: str = "data/staging"):
        self.staging_path = Path(staging_path)
        
    def clean_customers(self) -> pd.DataFrame:
        """Limpia datos de clientes"""
        logger.info("Limpiando datos de clientes...")
        
        df = pd.read_parquet(self.staging_path / "customers.parquet")
        
        # Eliminar duplicados por customer_id
        original_count = len(df)
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        
        if original_count != len(df):
            logger.warning(f"Se eliminaron {original_count - len(df)} clientes duplicados")
        
        # Validar campos requeridos
        df = df.dropna(subset=['customer_id', 'customer_zip_code_prefix'])
        
        # Normalizar estado (mayusculas)
        df['customer_state'] = df['customer_state'].str.upper()
        
        # Normalizar ciudad (minusculas)
        df['customer_city'] = df['customer_city'].str.lower().str.strip()
        
        logger.success(f"Clientes limpiados: {len(df)} registros")
        return df
    
    def clean_products(self) -> pd.DataFrame:
        """Limpia datos de productos"""
        logger.info("Limpiando datos de productos...")
        
        df = pd.read_parquet(self.staging_path / "products.parquet")
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        # Rellenar valores nulos en dimensiones con 0
        numeric_cols = ['product_name_lenght', 'product_description_lenght', 
                       'product_photos_qty', 'product_weight_g', 'product_length_cm',
                       'product_height_cm', 'product_width_cm']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # Rellenar categoria nula con 'sin_categoria'
        df['product_category_name'] = df['product_category_name'].fillna('sin_categoria')
        
        logger.success(f"Productos limpiados: {len(df)} registros")
        return df
    
    def clean_sellers(self) -> pd.DataFrame:
        """Limpia datos de vendedores"""
        logger.info("Limpiando datos de vendedores...")
        
        df = pd.read_parquet(self.staging_path / "sellers.parquet")
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['seller_id'], keep='first')
        
        # Normalizar estado
        df['seller_state'] = df['seller_state'].str.upper()
        
        # Normalizar ciudad
        df['seller_city'] = df['seller_city'].str.lower().str.strip()
        
        logger.success(f"Vendedores limpiados: {len(df)} registros")
        return df
    
    def clean_orders(self) -> pd.DataFrame:
        """Limpia datos de ordenes"""
        logger.info("Limpiando datos de ordenes...")
        
        df = pd.read_parquet(self.staging_path / "orders.parquet")
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=['order_id'], keep='first')
        
        # Convertir fechas
        date_columns = ['order_purchase_timestamp', 'order_approved_at', 
                       'order_delivered_carrier_date', 'order_delivered_customer_date',
                       'order_estimated_delivery_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Normalizar estado
        df['order_status'] = df['order_status'].str.lower()
        
        logger.success(f"Ordenes limpiadas: {len(df)} registros")
        return df
    
    def clean_order_items(self) -> pd.DataFrame:
        """Limpia datos de items de ordenes"""
        logger.info("Limpiando datos de items de ordenes...")
        
        df = pd.read_parquet(self.staging_path / "order_items.parquet")
        
        # Convertir fechas
        df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
        
        # Validar valores numericos positivos
        df = df[df['price'] >= 0]
        df = df[df['freight_value'] >= 0]
        
        logger.success(f"Items limpiados: {len(df)} registros")
        return df
    
    def clean_order_payments(self) -> pd.DataFrame:
        """Limpia datos de pagos"""
        logger.info("Limpiando datos de pagos...")
        
        df = pd.read_parquet(self.staging_path / "order_payments.parquet")
        
        # Normalizar tipo de pago
        df['payment_type'] = df['payment_type'].str.lower()
        
        # Validar valores positivos
        df = df[df['payment_value'] >= 0]
        df = df[df['payment_installments'] > 0]
        
        logger.success(f"Pagos limpiados: {len(df)} registros")
        return df
    
    def clean_order_reviews(self) -> pd.DataFrame:
        """Limpia datos de reviews"""
        logger.info("Limpiando datos de reviews...")
        
        df = pd.read_parquet(self.staging_path / "order_reviews.parquet")
        
        # Eliminar duplicados por review_id
        df = df.drop_duplicates(subset=['review_id'], keep='first')
        
        # Convertir fechas
        df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
        df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
        
        # Validar score entre 1 y 5
        df = df[(df['review_score'] >= 1) & (df['review_score'] <= 5)]
        
        logger.success(f"Reviews limpiadas: {len(df)} registros")
        return df
    
    def clean_all(self) -> dict:
        """Limpia todos los datasets"""
        logger.info("="*60)
        logger.info("INICIANDO LIMPIEZA DE DATOS")
        logger.info("="*60)
        
        cleaned_data = {
            'customers': self.clean_customers(),
            'products': self.clean_products(),
            'sellers': self.clean_sellers(),
            'orders': self.clean_orders(),
            'order_items': self.clean_order_items(),
            'order_payments': self.clean_order_payments(),
            'order_reviews': self.clean_order_reviews()
        }
        
        total_records = sum(len(df) for df in cleaned_data.values())
        
        logger.info("="*60)
        logger.success(f"LIMPIEZA COMPLETADA: {total_records} registros procesados")
        logger.info("="*60)
        
        return cleaned_data


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean_all()
    
    # Mostrar resumen
    for name, df in cleaned_data.items():
        print(f"{name}: {len(df)} registros")
