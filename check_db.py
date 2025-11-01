import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='oltp_db',
    user='oltp_user',
    password='oltp_password'
)

cur = conn.cursor()

tables = ['customers', 'products', 'sellers', 'orders', 'order_items', 
          'order_payments', 'order_reviews', 'geolocation', 'product_category_translation']

print("\nEstado actual de las tablas OLTP:")
print("-" * 50)
for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f"{table:30}: {count:10,}")

cur.close()
conn.close()
