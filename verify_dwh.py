"""
Script para verificar datos cargados en Data Warehouse OLAP
Muestra estadisticas, conteos y metricas de negocio
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent))

from config.db_config import get_olap_connection_string


def verify_dwh():
    """Verifica datos en Data Warehouse OLAP"""
    
    print("\n" + "="*80)
    print("VERIFICACION DE DATA WAREHOUSE OLAP")
    print("="*80)
    
    try:
        # Conectar a base de datos OLAP
        connection_string = get_olap_connection_string()
        engine = create_engine(connection_string)
        
        print("\nConexion establecida con base de datos OLAP")
        
        # Verificar dimensiones
        print("\n" + "-"*80)
        print("DIMENSIONES")
        print("-"*80)
        
        dimensions = {
            'dim_customers': 'Clientes',
            'dim_products': 'Productos',
            'dim_sellers': 'Vendedores',
            'dim_date': 'Fechas'
        }
        
        for table, description in dimensions.items():
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                
                result = conn.execute(text(f"SELECT COUNT(*) as cols FROM information_schema.columns WHERE table_name = '{table}'"))
                cols = result.fetchone()[0]
                
                print(f"\n{description} ({table}):")
                print(f"  Registros: {count:,}")
                print(f"  Columnas: {cols}")
        
        # Verificar tabla de hechos
        print("\n" + "-"*80)
        print("TABLA DE HECHOS")
        print("-"*80)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fct_orders"))
            count = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) as cols FROM information_schema.columns WHERE table_name = 'fct_orders'"))
            cols = result.fetchone()[0]
            
            print(f"\nOrdenes (fct_orders):")
            print(f"  Registros: {count:,}")
            print(f"  Columnas: {cols}")
        
        # Metricas de negocio
        print("\n" + "-"*80)
        print("METRICAS DE NEGOCIO")
        print("-"*80)
        
        query = """
        SELECT 
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_key) as unique_customers,
            SUM(order_total_value) as total_revenue,
            AVG(order_total_value) as avg_order_value,
            AVG(delivery_time_days) as avg_delivery_days,
            AVG(review_score) as avg_review_score,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_orders,
            ROUND(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) as delayed_percentage
        FROM fct_orders
        WHERE order_status NOT IN ('canceled', 'unavailable')
        """
        
        df = pd.read_sql(query, engine)
        
        print(f"\nTotal de ordenes: {df['total_orders'].iloc[0]:,}")
        print(f"Clientes unicos: {df['unique_customers'].iloc[0]:,}")
        print(f"Ingresos totales: R$ {df['total_revenue'].iloc[0]:,.2f}")
        print(f"Valor promedio de orden: R$ {df['avg_order_value'].iloc[0]:.2f}")
        print(f"Tiempo de entrega promedio: {df['avg_delivery_days'].iloc[0]:.1f} dias")
        print(f"Review score promedio: {df['avg_review_score'].iloc[0]:.2f}")
        print(f"Ordenes con retraso: {df['delayed_orders'].iloc[0]:,} ({df['delayed_percentage'].iloc[0]}%)")
        
        # Distribucion por estado de orden
        print("\n" + "-"*80)
        print("DISTRIBUCION POR ESTADO DE ORDEN")
        print("-"*80)
        
        query = """
        SELECT 
            order_status,
            COUNT(*) as count,
            ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as percentage
        FROM fct_orders
        GROUP BY order_status
        ORDER BY count DESC
        """
        
        df = pd.read_sql(query, engine)
        print("\n", df.to_string(index=False))
        
        # Top 5 categorias
        print("\n" + "-"*80)
        print("TOP 5 CATEGORIAS DE PRODUCTOS")
        print("-"*80)
        
        query = """
        SELECT 
            p.product_category_name_english as category,
            COUNT(DISTINCT f.order_id) as orders,
            SUM(f.order_total_value) as revenue
        FROM fct_orders f
        JOIN dim_products p ON f.product_key = p.product_key
        WHERE f.order_status NOT IN ('canceled', 'unavailable')
          AND p.product_category_name_english IS NOT NULL
        GROUP BY p.product_category_name_english
        ORDER BY revenue DESC
        LIMIT 5
        """
        
        df = pd.read_sql(query, engine)
        df['revenue'] = df['revenue'].apply(lambda x: f"R$ {x:,.2f}")
        print("\n", df.to_string(index=False))
        
        # Ventas por region
        print("\n" + "-"*80)
        print("VENTAS POR REGION")
        print("-"*80)
        
        query = """
        SELECT 
            c.customer_region as region,
            COUNT(DISTINCT f.order_id) as orders,
            SUM(f.order_total_value) as revenue,
            AVG(f.review_score) as avg_review_score
        FROM fct_orders f
        JOIN dim_customers c ON f.customer_key = c.customer_key
        WHERE f.order_status NOT IN ('canceled', 'unavailable')
        GROUP BY c.customer_region
        ORDER BY revenue DESC
        """
        
        df = pd.read_sql(query, engine)
        df['revenue'] = df['revenue'].apply(lambda x: f"R$ {x:,.2f}")
        df['avg_review_score'] = df['avg_review_score'].apply(lambda x: f"{x:.2f}")
        print("\n", df.to_string(index=False))
        
        # Verificar integridad referencial
        print("\n" + "-"*80)
        print("INTEGRIDAD REFERENCIAL")
        print("-"*80)
        
        checks = [
            ("Customer FK", "SELECT COUNT(*) FROM fct_orders WHERE customer_key NOT IN (SELECT customer_key FROM dim_customers)"),
            ("Product FK", "SELECT COUNT(*) FROM fct_orders WHERE product_key IS NOT NULL AND product_key NOT IN (SELECT product_key FROM dim_products)"),
            ("Seller FK", "SELECT COUNT(*) FROM fct_orders WHERE seller_key IS NOT NULL AND seller_key NOT IN (SELECT seller_key FROM dim_sellers)"),
            ("Date FK", "SELECT COUNT(*) FROM fct_orders WHERE purchase_date_key NOT IN (SELECT date_key FROM dim_date)")
        ]
        
        all_valid = True
        
        for check_name, query in checks:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                invalid_count = result.fetchone()[0]
                
                if invalid_count > 0:
                    print(f"\n{check_name}: ERROR - {invalid_count} referencias invalidas")
                    all_valid = False
                else:
                    print(f"\n{check_name}: OK")
        
        if all_valid:
            print("\n\nIntegridad referencial: VERIFICADA")
        
        # Verificar vistas materializadas
        print("\n" + "-"*80)
        print("VISTAS MATERIALIZADAS")
        print("-"*80)
        
        query = """
        SELECT 
            matviewname as view_name,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
        FROM pg_matviews
        WHERE schemaname = 'public'
        ORDER BY matviewname
        """
        
        try:
            df = pd.read_sql(query, engine)
            if len(df) > 0:
                print("\n", df.to_string(index=False))
            else:
                print("\nNo hay vistas materializadas creadas")
                print("Ejecutar: psql -U postgres -d olist_olap -f sql/olap_views.sql")
        except Exception as e:
            print(f"\nError al consultar vistas materializadas: {e}")
        
        print("\n" + "="*80)
        print("VERIFICACION COMPLETADA")
        print("="*80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\nError al verificar Data Warehouse: {e}")
        return False


if __name__ == "__main__":
    verify_dwh()
