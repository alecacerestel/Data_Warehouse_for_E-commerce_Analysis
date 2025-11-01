# -*- coding: utf-8 -*-
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='oltp_db',
    user='oltp_user',
    password='oltp_password'
)

conn.autocommit = True
cur = conn.cursor()

print("Limpiando TODAS las tablas OLTP...")
cur.execute("""
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
""")

print("✅ Todas las tablas han sido limpiadas")

cur.close()
conn.close()
