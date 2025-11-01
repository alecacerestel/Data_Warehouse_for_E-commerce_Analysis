from sqlalchemy import create_engine, text
engine = create_engine("postgresql://postgres:Aledelflow123@localhost:5432/olist_oltp")
print("Truncando...")
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE customers, products, sellers, orders, order_items, order_payments, order_reviews, geolocation, product_category_translation CASCADE"))
    conn.commit()
print("Listo")
engine.dispose()
