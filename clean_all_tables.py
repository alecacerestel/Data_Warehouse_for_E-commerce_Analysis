from sqlalchemy import create_engine, text
from config.db_config import DatabaseConfig

config = DatabaseConfig()
engine = config.get_oltp_engine()

with engine.connect() as conn:
    print("Limpiando TODAS las tablas OLTP...")
    conn.execute(text("""
        TRUNCATE TABLE 
            customers, 
            products, 
            sellers, 
            orders, 
            order_items, 
            order_payments, 
            order_reviews, 
            geolocation, 
            product_category_translation 
        CASCADE
    """))
    conn.commit()
    print("✅ Todas las tablas han sido limpiadas")
