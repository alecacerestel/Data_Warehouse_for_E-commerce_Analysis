"""
Script para crear la tabla de hechos (Fact Table)
Fase 3: Transformación - Creación de tabla de hechos
"""
import pandas as pd
import sys
from pathlib import Path
from loguru import logger
from sqlalchemy import text

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig

# Configurar logging
logger.add("logs/03_create_fact_table.log", rotation="1 MB", level="INFO")


class FactTableBuilder:
    """Clase para construir la tabla de hechos"""
    
    def __init__(self, processed_path: str = "data/processed"):
        self.processed_path = Path(processed_path)
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_dwh_engine()
        
        logger.info(f"Processed data path: {self.processed_path}")
    
    def load_parquet(self, filename: str) -> pd.DataFrame:
        """Carga un archivo Parquet"""
        file_path = self.processed_path / filename
        return pd.read_parquet(file_path)
    
    def get_dimension_keys(self):
        """Obtener mapeos de claves de dimensiones desde el DWH"""
        logger.info("Obteniendo mapeos de claves de dimensiones...")
        
        # Mapeo de customers
        query_customers = """
            SELECT customer_id, 
                   ROW_NUMBER() OVER (ORDER BY customer_id) as customer_key
            FROM dim_customers
            WHERE is_current = TRUE
        """
        dim_customers = pd.read_sql(query_customers, self.engine)
        logger.info(f"  Clientes: {len(dim_customers):,}")
        
        # Mapeo de products
        query_products = """
            SELECT product_id,
                   ROW_NUMBER() OVER (ORDER BY product_id) as product_key
            FROM dim_products
            WHERE is_current = TRUE
        """
        dim_products = pd.read_sql(query_products, self.engine)
        logger.info(f"  Productos: {len(dim_products):,}")
        
        # Mapeo de sellers
        query_sellers = """
            SELECT seller_id,
                   ROW_NUMBER() OVER (ORDER BY seller_id) as seller_key
            FROM dim_sellers
            WHERE is_current = TRUE
        """
        dim_sellers = pd.read_sql(query_sellers, self.engine)
        logger.info(f"  Vendedores: {len(dim_sellers):,}")
        
        # Mapeo de dates
        query_dates = """
            SELECT date_key, full_date
            FROM dim_date
        """
        dim_dates = pd.read_sql(query_dates, self.engine)
        dim_dates['full_date'] = pd.to_datetime(dim_dates['full_date'])
        logger.info(f"  Fechas: {len(dim_dates):,}")
        
        return dim_customers, dim_products, dim_sellers, dim_dates
    
    def create_fact_orders(self):
        """Crear tabla de hechos fct_orders"""
        logger.info("="*60)
        logger.info("CREANDO TABLA DE HECHOS: fct_orders")
        logger.info("="*60)
        
        # Cargar datos limpios
        logger.info("Cargando datos limpios...")
        orders = self.load_parquet("orders_clean.parquet")
        order_items = self.load_parquet("order_items_clean.parquet")
        order_payments = self.load_parquet("order_payments_clean.parquet")
        order_reviews = self.load_parquet("order_reviews_clean.parquet")
        
        # Obtener mapeos de dimensiones
        dim_customers, dim_products, dim_sellers, dim_dates = self.get_dimension_keys()
        
        # Crear tabla base: unir orders con order_items
        logger.info("Uniendo orders con order_items...")
        fact = order_items.merge(orders, on='order_id', how='inner')
        logger.info(f"  Registros después del join: {len(fact):,}")
        
        # Agregar información de pagos (suma por order_id)
        logger.info("Agregando información de pagos...")
        payments_agg = order_payments.groupby('order_id').agg({
            'payment_value': 'sum'
        }).reset_index()
        fact = fact.merge(payments_agg, on='order_id', how='left')
        fact['payment_value'] = fact['payment_value'].fillna(0)
        
        # Agregar review score (promedio si hay múltiples reviews)
        logger.info("Agregando scores de reviews...")
        reviews_agg = order_reviews.groupby('order_id').agg({
            'review_score': 'mean'
        }).reset_index()
        fact = fact.merge(reviews_agg, on='order_id', how='left')
        
        # Mapear claves de dimensiones
        logger.info("Mapeando claves de dimensiones...")
        
        # Customer keys
        fact = fact.merge(
            dim_customers[['customer_id', 'customer_key']], 
            on='customer_id', 
            how='left'
        )
        
        # Product keys
        fact = fact.merge(
            dim_products[['product_id', 'product_key']], 
            on='product_id', 
            how='left'
        )
        
        # Seller keys
        fact = fact.merge(
            dim_sellers[['seller_id', 'seller_key']], 
            on='seller_id', 
            how='left'
        )
        
        # Date keys
        # Purchase date
        fact['purchase_date'] = pd.to_datetime(fact['order_purchase_timestamp']).dt.date
        fact['purchase_date'] = pd.to_datetime(fact['purchase_date'])
        fact = fact.merge(
            dim_dates[['full_date', 'date_key']],
            left_on='purchase_date',
            right_on='full_date',
            how='left'
        ).rename(columns={'date_key': 'purchase_date_key'})
        
        # Approval date
        fact['approval_date'] = pd.to_datetime(fact['order_approved_at']).dt.date
        fact['approval_date'] = pd.to_datetime(fact['approval_date'])
        fact = fact.merge(
            dim_dates[['full_date', 'date_key']],
            left_on='approval_date',
            right_on='full_date',
            how='left',
            suffixes=('', '_approval')
        ).rename(columns={'date_key': 'approval_date_key'})
        
        # Carrier date
        fact['carrier_date'] = pd.to_datetime(fact['order_delivered_carrier_date']).dt.date
        fact['carrier_date'] = pd.to_datetime(fact['carrier_date'])
        fact = fact.merge(
            dim_dates[['full_date', 'date_key']],
            left_on='carrier_date',
            right_on='full_date',
            how='left',
            suffixes=('', '_carrier')
        ).rename(columns={'date_key': 'carrier_date_key'})
        
        # Delivery date
        fact['delivery_date'] = pd.to_datetime(fact['order_delivered_customer_date']).dt.date
        fact['delivery_date'] = pd.to_datetime(fact['delivery_date'])
        fact = fact.merge(
            dim_dates[['full_date', 'date_key']],
            left_on='delivery_date',
            right_on='full_date',
            how='left',
            suffixes=('', '_delivery')
        ).rename(columns={'date_key': 'delivery_date_key'})
        
        # Seleccionar y renombrar columnas finales
        logger.info("Seleccionando columnas finales...")
        fct_orders = pd.DataFrame({
            # Claves foráneas
            'customer_key': fact['customer_key'],
            'product_key': fact['product_key'],
            'seller_key': fact['seller_key'],
            'purchase_date_key': fact['purchase_date_key'],
            'approval_date_key': fact['approval_date_key'],
            'carrier_date_key': fact['carrier_date_key'],
            'delivery_date_key': fact['delivery_date_key'],
            
            # Claves de negocio
            'order_id': fact['order_id'],
            'order_item_id': fact['order_item_id'],
            
            # Métricas
            'price': fact['price'],
            'freight_value': fact['freight_value'],
            'payment_value': fact['payment_value'],
            'total_order_value': fact['price'] + fact['freight_value'],
            
            # Métricas de tiempo
            'delivery_time_days': fact['delivery_time_days'],
            'estimated_delivery_time_days': fact['estimated_delivery_time_days'],
            'delivery_delay_days': fact['delivery_delay_days'],
            
            # Métricas de calidad
            'review_score': fact['review_score'],
            
            # Flags
            'is_delayed': fact['is_delayed'],
            'is_cancelled': fact['is_cancelled'],
        })
        
        # Eliminar registros con claves nulas (datos que no se pudieron mapear)
        initial_count = len(fct_orders)
        fct_orders = fct_orders.dropna(subset=['customer_key', 'product_key', 'seller_key', 'purchase_date_key'])
        final_count = len(fct_orders)
        
        logger.warning(f"Registros eliminados por claves nulas: {initial_count - final_count:,}")
        logger.info(f"Registros finales en fct_orders: {final_count:,}")
        
        # Agregar timestamp de auditoría
        fct_orders['created_at'] = pd.Timestamp.now()
        fct_orders['updated_at'] = pd.Timestamp.now()
        
        # Guardar en base de datos
        logger.info("Guardando fct_orders en DWH...")
        fct_orders.to_sql(
            'fct_orders',
            self.engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        
        logger.success(f"✓ fct_orders creada: {len(fct_orders):,} registros")
        
        # Estadísticas
        self.print_statistics(fct_orders)
        
        return fct_orders
    
    def print_statistics(self, fct_orders):
        """Imprimir estadísticas de la tabla de hechos"""
        logger.info("\n" + "="*60)
        logger.info("ESTADÍSTICAS DE LA TABLA DE HECHOS")
        logger.info("="*60)
        
        logger.info(f"\nTotal de órdenes: {fct_orders['order_id'].nunique():,}")
        logger.info(f"Total de items: {len(fct_orders):,}")
        logger.info(f"Clientes únicos: {fct_orders['customer_key'].nunique():,}")
        logger.info(f"Productos únicos: {fct_orders['product_key'].nunique():,}")
        logger.info(f"Vendedores únicos: {fct_orders['seller_key'].nunique():,}")
        
        logger.info(f"\n--- Métricas Financieras ---")
        logger.info(f"Ingreso total: R$ {fct_orders['total_order_value'].sum():,.2f}")
        logger.info(f"Ticket promedio: R$ {fct_orders['total_order_value'].mean():,.2f}")
        logger.info(f"Precio promedio: R$ {fct_orders['price'].mean():,.2f}")
        logger.info(f"Flete promedio: R$ {fct_orders['freight_value'].mean():,.2f}")
        
        logger.info(f"\n--- Métricas de Entrega ---")
        logger.info(f"Tiempo promedio de entrega: {fct_orders['delivery_time_days'].mean():.1f} días")
        logger.info(f"Órdenes con retraso: {fct_orders['is_delayed'].sum():,} ({fct_orders['is_delayed'].mean()*100:.1f}%)")
        logger.info(f"Retraso promedio: {fct_orders[fct_orders['is_delayed']==True]['delivery_delay_days'].mean():.1f} días")
        
        logger.info(f"\n--- Métricas de Calidad ---")
        logger.info(f"Review score promedio: {fct_orders['review_score'].mean():.2f}")
        logger.info(f"Órdenes canceladas: {fct_orders['is_cancelled'].sum():,} ({fct_orders['is_cancelled'].mean()*100:.1f}%)")
        
        logger.info("="*60)
    
    def verify_fact_table(self):
        """Verificar integridad de la tabla de hechos"""
        logger.info("\nVerificando integridad de fct_orders...")
        
        with self.engine.connect() as conn:
            # Contar registros
            result = conn.execute(text("SELECT COUNT(*) FROM fct_orders"))
            count = result.scalar()
            logger.info(f"✓ Total de registros: {count:,}")
            
            # Verificar claves nulas
            result = conn.execute(text("""
                SELECT COUNT(*) FROM fct_orders 
                WHERE customer_key IS NULL 
                   OR product_key IS NULL 
                   OR seller_key IS NULL 
                   OR purchase_date_key IS NULL
            """))
            null_keys = result.scalar()
            if null_keys > 0:
                logger.warning(f"⚠ Registros con claves nulas: {null_keys:,}")
            else:
                logger.info("✓ No hay claves nulas")
            
            # Verificar integridad referencial
            logger.info("\nVerificando integridad referencial...")
            
            checks = [
                ("customer_key", "dim_customers", "customer_key"),
                ("product_key", "dim_products", "product_key"),
                ("seller_key", "dim_sellers", "seller_key"),
                ("purchase_date_key", "dim_date", "date_key")
            ]
            
            for fact_col, dim_table, dim_col in checks:
                # Usar la clave primaria generada automáticamente si existe
                if dim_col in ['customer_key', 'product_key', 'seller_key']:
                    # Estos son seriales, no hacer el check aquí
                    continue
                    
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM fct_orders f
                    LEFT JOIN {dim_table} d ON f.{fact_col} = d.{dim_col}
                    WHERE d.{dim_col} IS NULL AND f.{fact_col} IS NOT NULL
                """))
                orphans = result.scalar()
                
                if orphans > 0:
                    logger.warning(f"⚠ {fact_col}: {orphans:,} referencias huérfanas")
                else:
                    logger.info(f"✓ {fact_col}: integridad verificada")


def main():
    """Función principal"""
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    builder = FactTableBuilder()
    builder.create_fact_orders()
    builder.verify_fact_table()
    
    logger.success("\n¡Tabla de hechos creada exitosamente!")


if __name__ == "__main__":
    main()
