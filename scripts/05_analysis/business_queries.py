"""
Script para ejecutar queries de análisis de negocio
Fase 5: Análisis - Responder preguntas de negocio
"""
import pandas as pd
import sys
from pathlib import Path
from loguru import logger
from sqlalchemy import text
import matplotlib.pyplot as plt
import seaborn as sns

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.db_config import DatabaseConfig

# Configurar logging
logger.add("logs/05_business_queries.log", rotation="1 MB", level="INFO")

# Configurar estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class BusinessAnalyzer:
    """Clase para ejecutar análisis de negocio"""
    
    def __init__(self, output_path: str = "data/analysis"):
        self.db_config = DatabaseConfig()
        self.engine = self.db_config.get_dwh_engine()
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Output path: {self.output_path}")
    
    def execute_query(self, query: str, name: str = "query") -> pd.DataFrame:
        """Ejecuta una query y retorna un DataFrame"""
        try:
            df = pd.read_sql(query, self.engine)
            logger.success(f"✓ Query ejecutada: {name} ({len(df)} filas)")
            return df
        except Exception as e:
            logger.error(f"Error ejecutando {name}: {e}")
            raise
    
    def save_results(self, df: pd.DataFrame, filename: str):
        """Guarda resultados en CSV y Excel"""
        # CSV
        csv_path = self.output_path / f"{filename}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"  Guardado: {csv_path}")
        
        # Excel
        excel_path = self.output_path / f"{filename}.xlsx"
        df.to_excel(excel_path, index=False, sheet_name='Datos')
        logger.info(f"  Guardado: {excel_path}")
    
    def query_1_top_products_by_month(self):
        """1. ¿Cuáles son los productos más vendidos por mes?"""
        logger.info("\n" + "="*60)
        logger.info("QUERY 1: Productos más vendidos por mes")
        logger.info("="*60)
        
        query = """
        SELECT 
            dd.year,
            dd.month,
            dd.month_name,
            dp.product_category_name_english as category,
            COUNT(DISTINCT fo.order_id) as num_orders,
            COUNT(*) as items_sold,
            ROUND(SUM(fo.total_order_value), 2) as total_revenue,
            ROUND(AVG(fo.price), 2) as avg_price
        FROM fct_orders fo
        JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
        JOIN dim_products dp ON fo.product_key = dp.product_key
        WHERE dp.is_current = TRUE
        GROUP BY dd.year, dd.month, dd.month_name, dp.product_category_name_english
        ORDER BY dd.year, dd.month, total_revenue DESC
        """
        
        df = self.execute_query(query, "top_products_by_month")
        self.save_results(df, "01_products_by_month")
        
        # Mostrar top 10
        logger.info("\nTop 10 categorías globales:")
        top_categories = df.groupby('category').agg({
            'total_revenue': 'sum'
        }).sort_values('total_revenue', ascending=False).head(10)
        logger.info(f"\n{top_categories}")
        
        return df
    
    def query_2_customers_by_state(self):
        """2. ¿De qué estados provienen los clientes más valiosos?"""
        logger.info("\n" + "="*60)
        logger.info("QUERY 2: Clientes más valiosos por estado")
        logger.info("="*60)
        
        query = """
        SELECT 
            dc.customer_state as state,
            dc.customer_region as region,
            COUNT(DISTINCT dc.customer_unique_id) as num_customers,
            COUNT(DISTINCT fo.order_id) as num_orders,
            ROUND(SUM(fo.total_order_value), 2) as total_revenue,
            ROUND(AVG(fo.total_order_value), 2) as avg_order_value,
            ROUND(SUM(fo.total_order_value) / COUNT(DISTINCT dc.customer_unique_id), 2) as revenue_per_customer
        FROM fct_orders fo
        JOIN dim_customers dc ON fo.customer_key = dc.customer_key
        WHERE dc.is_current = TRUE
        GROUP BY dc.customer_state, dc.customer_region
        ORDER BY total_revenue DESC
        """
        
        df = self.execute_query(query, "customers_by_state")
        self.save_results(df, "02_customers_by_state")
        
        # Mostrar top 10 estados
        logger.info("\nTop 10 estados por ingresos:")
        logger.info(f"\n{df.head(10)}")
        
        return df
    
    def query_3_delivery_time_by_region(self):
        """3. ¿Cuál es el tiempo promedio de entrega por región?"""
        logger.info("\n" + "="*60)
        logger.info("QUERY 3: Tiempo promedio de entrega por región")
        logger.info("="*60)
        
        query = """
        SELECT 
            dc.customer_region as region,
            dc.customer_state as state,
            COUNT(DISTINCT fo.order_id) as num_orders,
            ROUND(AVG(fo.delivery_time_days), 2) as avg_delivery_days,
            ROUND(AVG(fo.estimated_delivery_time_days), 2) as avg_estimated_days,
            ROUND(AVG(fo.delivery_delay_days), 2) as avg_delay_days,
            COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
            ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed
        FROM fct_orders fo
        JOIN dim_customers dc ON fo.customer_key = dc.customer_key
        WHERE dc.is_current = TRUE 
          AND fo.delivery_time_days IS NOT NULL
        GROUP BY dc.customer_region, dc.customer_state
        ORDER BY avg_delivery_days
        """
        
        df = self.execute_query(query, "delivery_time_by_region")
        self.save_results(df, "03_delivery_time_by_region")
        
        # Mostrar estadísticas por región
        logger.info("\nEstadísticas de entrega por región:")
        region_stats = df.groupby('region').agg({
            'num_orders': 'sum',
            'avg_delivery_days': 'mean',
            'pct_delayed': 'mean'
        }).round(2)
        logger.info(f"\n{region_stats}")
        
        return df
    
    def query_4_kpis(self):
        """Obtener KPIs principales del negocio"""
        logger.info("\n" + "="*60)
        logger.info("QUERY 4: KPIs Principales del Negocio")
        logger.info("="*60)
        
        query = """
        SELECT 
            COUNT(DISTINCT fo.order_id) as total_orders,
            COUNT(DISTINCT dc.customer_unique_id) as total_customers,
            COUNT(DISTINCT dp.product_key) as total_products,
            COUNT(DISTINCT ds.seller_key) as total_sellers,
            ROUND(SUM(fo.total_order_value), 2) as total_revenue,
            ROUND(AVG(fo.total_order_value), 2) as avg_order_value,
            ROUND(AVG(fo.price), 2) as avg_product_price,
            ROUND(AVG(fo.freight_value), 2) as avg_freight,
            ROUND(AVG(fo.review_score), 2) as avg_rating,
            ROUND(AVG(fo.delivery_time_days), 2) as avg_delivery_days,
            COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
            ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed
        FROM fct_orders fo
        JOIN dim_customers dc ON fo.customer_key = dc.customer_key
        JOIN dim_products dp ON fo.product_key = dp.product_key
        JOIN dim_sellers ds ON fo.seller_key = ds.seller_key
        WHERE dc.is_current = TRUE 
          AND dp.is_current = TRUE 
          AND ds.is_current = TRUE
        """
        
        df = self.execute_query(query, "kpis")
        self.save_results(df, "04_kpis")
        
        # Mostrar KPIs
        logger.info("\n📊 KPIs del Negocio:")
        for col in df.columns:
            logger.info(f"  {col}: {df[col].iloc[0]}")
        
        return df
    
    def query_5_repeat_customers(self):
        """Análisis de clientes recurrentes"""
        logger.info("\n" + "="*60)
        logger.info("QUERY 5: Análisis de Clientes Recurrentes")
        logger.info("="*60)
        
        query = """
        WITH customer_orders AS (
            SELECT 
                dc.customer_unique_id,
                COUNT(DISTINCT fo.order_id) as num_orders,
                SUM(fo.total_order_value) as total_spent
            FROM fct_orders fo
            JOIN dim_customers dc ON fo.customer_key = dc.customer_key
            WHERE dc.is_current = TRUE
            GROUP BY dc.customer_unique_id
        )
        SELECT 
            CASE 
                WHEN num_orders = 1 THEN '1 compra'
                WHEN num_orders = 2 THEN '2 compras'
                WHEN num_orders BETWEEN 3 AND 5 THEN '3-5 compras'
                ELSE 'Más de 5 compras'
            END as customer_segment,
            COUNT(*) as num_customers,
            ROUND(AVG(total_spent), 2) as avg_lifetime_value,
            ROUND(SUM(total_spent), 2) as total_revenue,
            ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as pct_customers
        FROM customer_orders
        GROUP BY customer_segment
        ORDER BY 
            CASE customer_segment
                WHEN '1 compra' THEN 1
                WHEN '2 compras' THEN 2
                WHEN '3-5 compras' THEN 3
                ELSE 4
            END
        """
        
        df = self.execute_query(query, "repeat_customers")
        self.save_results(df, "05_repeat_customers")
        
        logger.info("\nSegmentación de clientes:")
        logger.info(f"\n{df}")
        
        return df
    
    def generate_summary_report(self):
        """Generar reporte resumen con todos los análisis"""
        logger.info("\n" + "="*60)
        logger.info("GENERANDO REPORTE RESUMEN")
        logger.info("="*60)
        
        # Ejecutar todas las queries
        results = {
            'products_by_month': self.query_1_top_products_by_month(),
            'customers_by_state': self.query_2_customers_by_state(),
            'delivery_by_region': self.query_3_delivery_time_by_region(),
            'kpis': self.query_4_kpis(),
            'repeat_customers': self.query_5_repeat_customers()
        }
        
        # Crear un archivo Excel con múltiples hojas
        excel_path = self.output_path / "00_RESUMEN_COMPLETO.xlsx"
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            for sheet_name, df in results.items():
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        
        logger.success(f"\n✓ Reporte completo guardado: {excel_path}")
        
        return results


def main():
    """Función principal"""
    # Crear directorio de logs si no existe
    Path("logs").mkdir(exist_ok=True)
    
    analyzer = BusinessAnalyzer()
    results = analyzer.generate_summary_report()
    
    logger.success("\n¡Análisis de negocio completado exitosamente!")
    logger.info(f"Resultados guardados en: {analyzer.output_path}")


if __name__ == "__main__":
    main()
