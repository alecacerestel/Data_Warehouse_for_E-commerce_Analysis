-- ============================================
-- Scripts de Utilidad y Mantenimiento
-- Data Warehouse: Olist E-commerce
-- ============================================

-- ============================================
-- 1. VERIFICACIÓN DE DATOS
-- ============================================

-- Contar registros en todas las tablas
SELECT 'dim_customers' as tabla, COUNT(*) as registros FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL
SELECT 'dim_geolocation', COUNT(*) FROM dim_geolocation
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fct_orders', COUNT(*) FROM fct_orders;

-- Verificar integridad referencial
SELECT 
    'Órdenes sin cliente' as issue,
    COUNT(*) as count
FROM fct_orders f
LEFT JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

UNION ALL

SELECT 
    'Órdenes sin producto',
    COUNT(*)
FROM fct_orders f
LEFT JOIN dim_products p ON f.product_key = p.product_key
WHERE p.product_key IS NULL

UNION ALL

SELECT 
    'Órdenes sin vendedor',
    COUNT(*)
FROM fct_orders f
LEFT JOIN dim_sellers s ON f.seller_key = s.seller_key
WHERE s.seller_key IS NULL;

-- ============================================
-- 2. ESTADÍSTICAS DE CALIDAD DE DATOS
-- ============================================

-- Verificar valores nulos en tabla de hechos
SELECT 
    COUNT(*) as total_registros,
    COUNT(CASE WHEN customer_key IS NULL THEN 1 END) as customer_key_null,
    COUNT(CASE WHEN product_key IS NULL THEN 1 END) as product_key_null,
    COUNT(CASE WHEN seller_key IS NULL THEN 1 END) as seller_key_null,
    COUNT(CASE WHEN price IS NULL THEN 1 END) as price_null,
    COUNT(CASE WHEN review_score IS NULL THEN 1 END) as review_score_null
FROM fct_orders;

-- Distribución de scores
SELECT 
    review_score,
    COUNT(*) as cantidad,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje
FROM fct_orders
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score DESC;

-- ============================================
-- 3. LIMPIEZA Y MANTENIMIENTO
-- ============================================

-- Limpiar tablas (usar con precaución)
-- TRUNCATE TABLE fct_orders CASCADE;
-- TRUNCATE TABLE dim_customers CASCADE;
-- TRUNCATE TABLE dim_products CASCADE;
-- TRUNCATE TABLE dim_sellers CASCADE;
-- TRUNCATE TABLE dim_geolocation CASCADE;
-- TRUNCATE TABLE dim_date CASCADE;

-- Eliminar todas las tablas del DWH (usar con precaución)
-- DROP TABLE IF EXISTS fct_orders CASCADE;
-- DROP TABLE IF EXISTS dim_customers CASCADE;
-- DROP TABLE IF EXISTS dim_products CASCADE;
-- DROP TABLE IF EXISTS dim_sellers CASCADE;
-- DROP TABLE IF EXISTS dim_geolocation CASCADE;
-- DROP TABLE IF EXISTS dim_date CASCADE;

-- ============================================
-- 4. ANÁLISIS DE PERFORMANCE
-- ============================================

-- Tamaño de las tablas
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Índices existentes
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Queries más lentas (requiere pg_stat_statements)
-- SELECT 
--     query,
--     calls,
--     total_time,
--     mean_time,
--     max_time
-- FROM pg_stat_statements
-- ORDER BY mean_time DESC
-- LIMIT 10;

-- ============================================
-- 5. BACKUP Y RESTORE
-- ============================================

-- Crear backup (ejecutar en terminal, no en SQL)
/*
# Backup completo
pg_dump -U postgres -d olist_dwh -F c -b -v -f olist_dwh_backup.dump

# Backup solo schema
pg_dump -U postgres -d olist_dwh -s -f olist_dwh_schema.sql

# Backup solo datos
pg_dump -U postgres -d olist_dwh -a -f olist_dwh_data.sql

# Restore
pg_restore -U postgres -d olist_dwh -v olist_dwh_backup.dump
*/

-- ============================================
-- 6. ACTUALIZACIÓN INCREMENTAL
-- ============================================

-- Tabla de control de cargas
CREATE TABLE IF NOT EXISTS etl_control (
    etl_id SERIAL PRIMARY KEY,
    etl_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_processed INTEGER,
    error_message TEXT
);

-- Registrar inicio de ETL
INSERT INTO etl_control (etl_name, start_time, status)
VALUES ('daily_load', CURRENT_TIMESTAMP, 'RUNNING');

-- Actualizar al finalizar
UPDATE etl_control
SET end_time = CURRENT_TIMESTAMP,
    status = 'SUCCESS',
    records_processed = 1000
WHERE etl_id = (SELECT MAX(etl_id) FROM etl_control);

-- Ver historial de cargas
SELECT 
    etl_name,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) / 60 as duration_minutes,
    status,
    records_processed
FROM etl_control
ORDER BY etl_id DESC
LIMIT 10;

-- ============================================
-- 7. OPTIMIZACIÓN DE QUERIES
-- ============================================

-- Actualizar estadísticas
ANALYZE dim_customers;
ANALYZE dim_products;
ANALYZE dim_sellers;
ANALYZE dim_date;
ANALYZE fct_orders;

-- Vacuum y analyze (mantenimiento)
VACUUM ANALYZE dim_customers;
VACUUM ANALYZE dim_products;
VACUUM ANALYZE dim_sellers;
VACUUM ANALYZE dim_date;
VACUUM ANALYZE fct_orders;

-- Recrear índices
REINDEX TABLE dim_customers;
REINDEX TABLE dim_products;
REINDEX TABLE dim_sellers;
REINDEX TABLE dim_date;
REINDEX TABLE fct_orders;

-- ============================================
-- 8. AUDITORÍA Y MONITOREO
-- ============================================

-- Registros creados/actualizados hoy
SELECT 
    'dim_customers' as tabla,
    COUNT(*) as creados_hoy
FROM dim_customers
WHERE created_at::date = CURRENT_DATE

UNION ALL

SELECT 
    'dim_products',
    COUNT(*)
FROM dim_products
WHERE created_at::date = CURRENT_DATE

UNION ALL

SELECT 
    'fct_orders',
    COUNT(*)
FROM fct_orders
WHERE created_at::date = CURRENT_DATE;

-- Actividad de usuarios
SELECT 
    usename,
    application_name,
    client_addr,
    state,
    query_start,
    query
FROM pg_stat_activity
WHERE datname = 'olist_dwh'
ORDER BY query_start DESC;

-- ============================================
-- 9. VISTAS MATERIALIZADAS (para performance)
-- ============================================

-- Crear vista materializada para ventas mensuales
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_sales AS
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name;

-- Crear índice en vista materializada
CREATE INDEX idx_mv_monthly_sales_year_month 
ON mv_monthly_sales(year, month);

-- Refrescar vista materializada
REFRESH MATERIALIZED VIEW mv_monthly_sales;

-- Consultar vista materializada
SELECT * FROM mv_monthly_sales
ORDER BY year, month;

-- ============================================
-- 10. PERMISOS Y SEGURIDAD
-- ============================================

-- Crear rol de solo lectura para analistas
CREATE ROLE analyst_readonly;
GRANT CONNECT ON DATABASE olist_dwh TO analyst_readonly;
GRANT USAGE ON SCHEMA public TO analyst_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_readonly;

-- Crear usuario analista
-- CREATE USER analyst1 WITH PASSWORD 'password123';
-- GRANT analyst_readonly TO analyst1;

-- Ver permisos
SELECT 
    grantee, 
    table_schema, 
    table_name, 
    privilege_type
FROM information_schema.table_privileges
WHERE table_schema = 'public'
ORDER BY grantee, table_name;

-- ============================================
-- 11. QUERIES DE DIAGNÓSTICO
-- ============================================

-- Duplicados en dimensiones
SELECT 
    customer_id,
    COUNT(*) as duplicados
FROM dim_customers
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Registros huérfanos
SELECT COUNT(*) as huerfanos
FROM fct_orders f
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customers c 
    WHERE c.customer_key = f.customer_key
);

-- Órdenes sin entrega
SELECT 
    order_id,
    purchase_date_key,
    delivery_date_key
FROM fct_orders
WHERE delivery_date_key IS NULL
LIMIT 100;

-- ============================================
-- 12. EXPORTACIÓN DE DATOS
-- ============================================

-- Exportar a CSV (ejecutar en terminal)
/*
# Exportar tabla completa
\copy (SELECT * FROM fct_orders) TO 'fct_orders_export.csv' WITH CSV HEADER;

# Exportar query específica
\copy (SELECT * FROM v_sales_by_state) TO 'sales_by_state.csv' WITH CSV HEADER;
*/
