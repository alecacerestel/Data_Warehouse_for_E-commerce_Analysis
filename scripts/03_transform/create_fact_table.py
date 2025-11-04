"""
Script para crear tabla de hechos del modelo estrella
Genera fct_orders con metricas y foreign keys a dimensiones
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
from create_dimensions import DimensionBuilder

logger.add("logs/03_create_fact_table.log", rotation="1 MB", level="INFO")


class FactTableBuilder:
    """Clase para construir tabla de hechos"""
    
    def __init__(self, staging_path: str = "data/staging"):
        self.staging_path = Path(staging_path)
        self.cleaner = DataCleaner(staging_path)
        self.dim_builder = DimensionBuilder(staging_path)
        
    def create_fact_orders(self) -> pd.DataFrame:
        """Crea tabla de hechos de ordenes"""
        logger.info("Creando tabla de hechos de ordenes...")
        
        # Cargar datos limpios
        df_orders = self.cleaner.clean_orders()
        df_order_items = self.cleaner.clean_order_items()
        df_order_payments = self.cleaner.clean_order_payments()
        df_order_reviews = self.cleaner.clean_order_reviews()
        
        # Cargar dimensiones
        logger.info("Cargando dimensiones...")
        dim_customers = self.dim_builder.create_dim_customers()
        dim_products = self.dim_builder.create_dim_products()
        dim_sellers = self.dim_builder.create_dim_sellers()
        dim_date = self.dim_builder.create_dim_date()
        
        # Agregar metricas de items por orden
        logger.info("Agregando metricas de items...")
        order_items_agg = df_order_items.groupby('order_id').agg({
            'order_item_id': 'count',
            'price': 'sum',
            'freight_value': 'sum',
            'product_id': lambda x: x.iloc[0],  # Tomar primer producto
            'seller_id': lambda x: x.iloc[0]    # Tomar primer vendedor
        }).reset_index()
        
        order_items_agg.columns = ['order_id', 'items_count', 'total_items_price', 
                                   'total_freight', 'product_id', 'seller_id']
        
        # Agregar metricas de pagos por orden
        logger.info("Agregando metricas de pagos...")
        order_payments_agg = df_order_payments.groupby('order_id').agg({
            'payment_value': 'sum',
            'payment_installments': 'max',
            'payment_type': lambda x: x.iloc[0]  # Tomar primer tipo de pago
        }).reset_index()
        
        order_payments_agg.columns = ['order_id', 'total_payment', 
                                     'max_installments', 'payment_type']
        
        # Agregar review score
        logger.info("Agregando reviews...")
        order_reviews_agg = df_order_reviews.groupby('order_id').agg({
            'review_score': 'mean'
        }).reset_index()
        
        order_reviews_agg.columns = ['order_id', 'review_score']
        
        # Construir tabla de hechos base
        logger.info("Construyendo tabla de hechos...")
        fct_orders = df_orders.copy()
        
        # Join con agregaciones
        fct_orders = fct_orders.merge(order_items_agg, on='order_id', how='left')
        fct_orders = fct_orders.merge(order_payments_agg, on='order_id', how='left')
        fct_orders = fct_orders.merge(order_reviews_agg, on='order_id', how='left')
        
        # Rellenar valores nulos
        fct_orders['items_count'] = fct_orders['items_count'].fillna(0)
        fct_orders['total_items_price'] = fct_orders['total_items_price'].fillna(0)
        fct_orders['total_freight'] = fct_orders['total_freight'].fillna(0)
        fct_orders['total_payment'] = fct_orders['total_payment'].fillna(0)
        fct_orders['max_installments'] = fct_orders['max_installments'].fillna(1)
        fct_orders['review_score'] = fct_orders['review_score'].fillna(0)
        
        # Calcular valor total de la orden
        fct_orders['order_total_value'] = fct_orders['total_items_price'] + fct_orders['total_freight']
        
        # Calcular tiempos de entrega
        fct_orders['delivery_time_days'] = (
            fct_orders['order_delivered_customer_date'] - 
            fct_orders['order_purchase_timestamp']
        ).dt.days
        
        fct_orders['estimated_delivery_time_days'] = (
            fct_orders['order_estimated_delivery_date'] - 
            fct_orders['order_purchase_timestamp']
        ).dt.days
        
        # Calcular si hubo retraso
        fct_orders['is_delayed'] = (
            fct_orders['order_delivered_customer_date'] > 
            fct_orders['order_estimated_delivery_date']
        ).fillna(False)
        
        fct_orders['delay_days'] = (
            fct_orders['order_delivered_customer_date'] - 
            fct_orders['order_estimated_delivery_date']
        ).dt.days.fillna(0)
        
        fct_orders['delay_days'] = fct_orders['delay_days'].clip(lower=0)
        
        # Agregar foreign keys a dimensiones
        logger.info("Agregando foreign keys...")
        
        # Customer key
        customer_mapping = dim_customers.set_index('customer_id')['customer_key'].to_dict()
        fct_orders['customer_key'] = fct_orders['customer_id'].map(customer_mapping)
        
        # Product key
        product_mapping = dim_products.set_index('product_id')['product_key'].to_dict()
        fct_orders['product_key'] = fct_orders['product_id'].map(product_mapping)
        
        # Seller key
        seller_mapping = dim_sellers.set_index('seller_id')['seller_key'].to_dict()
        fct_orders['seller_key'] = fct_orders['seller_id'].map(seller_mapping)
        
        # Date key
        dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
        date_mapping = dim_date.set_index('full_date')['date_key'].to_dict()
        fct_orders['purchase_date'] = fct_orders['order_purchase_timestamp'].dt.date
        fct_orders['purchase_date'] = pd.to_datetime(fct_orders['purchase_date'])
        fct_orders['purchase_date_key'] = fct_orders['purchase_date'].map(date_mapping)
        
        # Seleccionar columnas finales
        fct_orders = fct_orders[[
            'order_id',
            'customer_key',
            'product_key',
            'seller_key',
            'purchase_date_key',
            'order_status',
            'items_count',
            'total_items_price',
            'total_freight',
            'total_payment',
            'order_total_value',
            'max_installments',
            'payment_type',
            'review_score',
            'delivery_time_days',
            'estimated_delivery_time_days',
            'is_delayed',
            'delay_days'
        ]].copy()
        
        # Eliminar registros sin foreign keys validas
        initial_count = len(fct_orders)
        fct_orders = fct_orders.dropna(subset=['customer_key', 'purchase_date_key'])
        removed = initial_count - len(fct_orders)
        
        if removed > 0:
            logger.warning(f"Se eliminaron {removed} ordenes sin foreign keys validas")
        
        # Agregar surrogate key
        fct_orders.insert(0, 'order_key', range(1, len(fct_orders) + 1))
        
        logger.success(f"Tabla de hechos creada: {len(fct_orders)} registros")
        return fct_orders
    
    def create_fact_table(self) -> pd.DataFrame:
        """Crea la tabla de hechos completa"""
        logger.info("="*60)
        logger.info("INICIANDO CREACION DE TABLA DE HECHOS")
        logger.info("="*60)
        
        fct_orders = self.create_fact_orders()
        
        logger.info("="*60)
        logger.success(f"TABLA DE HECHOS COMPLETADA: {len(fct_orders)} registros")
        logger.info("="*60)
        
        return fct_orders


if __name__ == "__main__":
    builder = FactTableBuilder()
    fact_table = builder.create_fact_table()
    
    print(f"\nTabla de hechos: {len(fact_table)} registros")
    print(f"Columnas: {', '.join(fact_table.columns.tolist())}")
    print(f"\nMuestra:")
    print(fact_table.head())
    
    print(f"\nEstadisticas:")
    print(f"  Valor total promedio: ${fact_table['order_total_value'].mean():.2f}")
    print(f"  Tiempo de entrega promedio: {fact_table['delivery_time_days'].mean():.1f} dias")
    print(f"  Ordenes con retraso: {fact_table['is_delayed'].sum()} ({fact_table['is_delayed'].mean()*100:.1f}%)")
    print(f"  Review score promedio: {fact_table['review_score'].mean():.2f}")
