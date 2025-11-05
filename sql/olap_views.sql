-- ============================================================
-- Vistas Materializadas para Data Warehouse OLAP
-- Metricas de negocio precalculadas y optimizadas
-- ============================================================

-- ============================================================
-- VISTA: Resumen de Ventas por Mes
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_by_month AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.items_count) AS total_items_sold,
    SUM(f.total_items_price) AS total_items_revenue,
    SUM(f.total_freight) AS total_freight_revenue,
    SUM(f.order_total_value) AS total_revenue,
    AVG(f.order_total_value) AS avg_order_value,
    AVG(f.review_score) AS avg_review_score,
    SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS delayed_percentage
FROM fct_orders f
JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

CREATE INDEX idx_mv_sales_by_month_year_month ON mv_sales_by_month(year, month);

COMMENT ON MATERIALIZED VIEW mv_sales_by_month IS 'Metricas mensuales de ventas: ordenes, clientes, ingresos, reviews';


-- ============================================================
-- VISTA: Ventas por Region
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_by_region AS
SELECT 
    c.customer_region,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.order_total_value) AS total_revenue,
    AVG(f.order_total_value) AS avg_order_value,
    AVG(f.delivery_time_days) AS avg_delivery_days,
    AVG(f.review_score) AS avg_review_score,
    SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS delayed_percentage
FROM fct_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_region
ORDER BY total_revenue DESC;

CREATE INDEX idx_mv_sales_by_region ON mv_sales_by_region(customer_region);

COMMENT ON MATERIALIZED VIEW mv_sales_by_region IS 'Metricas por region geografica de Brasil';


-- ============================================================
-- VISTA: Top Productos
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_products AS
SELECT 
    p.product_id,
    p.product_category_name_english AS category,
    p.product_size AS size_category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.items_count) AS total_units_sold,
    SUM(f.total_items_price) AS total_revenue,
    AVG(f.review_score) AS avg_review_score,
    AVG(f.delivery_time_days) AS avg_delivery_days
FROM fct_orders f
JOIN dim_products p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.product_id, p.product_category_name_english, p.product_size
ORDER BY total_revenue DESC
LIMIT 1000;

CREATE INDEX idx_mv_top_products_category ON mv_top_products(category);
CREATE INDEX idx_mv_top_products_revenue ON mv_top_products(total_revenue DESC);

COMMENT ON MATERIALIZED VIEW mv_top_products IS 'Top 1000 productos por ingresos con metricas clave';


-- ============================================================
-- VISTA: Performance de Vendedores
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_seller_performance AS
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.order_total_value) AS total_revenue,
    AVG(f.order_total_value) AS avg_order_value,
    AVG(f.delivery_time_days) AS avg_delivery_days,
    AVG(f.review_score) AS avg_review_score,
    SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS delayed_percentage
FROM fct_orders f
JOIN dim_sellers s ON f.seller_key = s.seller_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY s.seller_id, s.seller_city, s.seller_state, s.seller_region
ORDER BY total_revenue DESC;

CREATE INDEX idx_mv_seller_performance_id ON mv_seller_performance(seller_id);
CREATE INDEX idx_mv_seller_performance_region ON mv_seller_performance(seller_region);
CREATE INDEX idx_mv_seller_performance_revenue ON mv_seller_performance(total_revenue DESC);

COMMENT ON MATERIALIZED VIEW mv_seller_performance IS 'Metricas de desempeno por vendedor';


-- ============================================================
-- VISTA: Analisis de Entregas
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_delivery_analysis AS
SELECT 
    d.year,
    d.quarter,
    d.quarter_name,
    COUNT(*) AS total_orders,
    AVG(f.delivery_time_days) AS avg_actual_delivery_days,
    AVG(f.estimated_delivery_time_days) AS avg_estimated_delivery_days,
    AVG(f.delay_days) AS avg_delay_days,
    SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(SUM(CASE WHEN f.is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS delayed_percentage,
    SUM(CASE WHEN f.delivery_time_days <= 7 THEN 1 ELSE 0 END) AS delivered_within_week,
    SUM(CASE WHEN f.delivery_time_days > 30 THEN 1 ELSE 0 END) AS delivered_after_month
FROM fct_orders f
JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.year, d.quarter, d.quarter_name
ORDER BY d.year, d.quarter;

CREATE INDEX idx_mv_delivery_analysis_year_quarter ON mv_delivery_analysis(year, quarter);

COMMENT ON MATERIALIZED VIEW mv_delivery_analysis IS 'Analisis de tiempos y retrasos en entregas por trimestre';


-- ============================================================
-- VISTA: Analisis de Pagos
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_payment_analysis AS
SELECT 
    f.payment_type,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.order_total_value) AS total_revenue,
    AVG(f.order_total_value) AS avg_order_value,
    AVG(f.max_installments) AS avg_installments,
    AVG(f.review_score) AS avg_review_score,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) AS percentage_of_orders
FROM fct_orders f
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY f.payment_type
ORDER BY total_orders DESC;

CREATE INDEX idx_mv_payment_analysis_type ON mv_payment_analysis(payment_type);

COMMENT ON MATERIALIZED VIEW mv_payment_analysis IS 'Analisis de metodos de pago y cuotas';


-- ============================================================
-- VISTA: Categorias de Productos
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_categories AS
SELECT 
    p.product_category_name_english AS category,
    COUNT(DISTINCT p.product_id) AS unique_products,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.items_count) AS total_units_sold,
    SUM(f.total_items_price) AS total_revenue,
    AVG(f.total_items_price) AS avg_revenue_per_order,
    AVG(f.review_score) AS avg_review_score,
    AVG(p.product_weight_g) AS avg_product_weight,
    AVG(p.product_volume_cm3) AS avg_product_volume
FROM fct_orders f
JOIN dim_products p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
  AND p.product_category_name_english IS NOT NULL
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC;

CREATE INDEX idx_mv_product_categories_name ON mv_product_categories(category);
CREATE INDEX idx_mv_product_categories_revenue ON mv_product_categories(total_revenue DESC);

COMMENT ON MATERIALIZED VIEW mv_product_categories IS 'Metricas agregadas por categoria de producto';


-- ============================================================
-- VISTA: Clientes Recurrentes
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_recurrence AS
SELECT 
    CASE 
        WHEN order_count = 1 THEN '1 orden'
        WHEN order_count = 2 THEN '2 ordenes'
        WHEN order_count BETWEEN 3 AND 5 THEN '3-5 ordenes'
        ELSE '6+ ordenes'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(total_spent) AS avg_customer_value,
    AVG(avg_order_value) AS avg_order_value,
    AVG(avg_review_score) AS avg_review_score
FROM (
    SELECT 
        c.customer_key,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.order_total_value) AS total_spent,
        AVG(f.order_total_value) AS avg_order_value,
        AVG(f.review_score) AS avg_review_score
    FROM dim_customers c
    JOIN fct_orders f ON c.customer_key = f.customer_key
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_key
) customer_stats
GROUP BY customer_segment
ORDER BY 
    CASE customer_segment
        WHEN '1 orden' THEN 1
        WHEN '2 ordenes' THEN 2
        WHEN '3-5 ordenes' THEN 3
        ELSE 4
    END;

COMMENT ON MATERIALIZED VIEW mv_customer_recurrence IS 'Segmentacion de clientes por numero de ordenes';


-- ============================================================
-- VISTA: Dashboard Ejecutivo
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_executive_dashboard AS
SELECT 
    'Total Orders' AS metric_name,
    COUNT(DISTINCT order_id)::TEXT AS metric_value,
    'count' AS metric_type
FROM fct_orders
WHERE order_status NOT IN ('canceled', 'unavailable')

UNION ALL

SELECT 
    'Total Revenue' AS metric_name,
    'R$ ' || TO_CHAR(SUM(order_total_value), 'FM999,999,999.00') AS metric_value,
    'currency' AS metric_type
FROM fct_orders
WHERE order_status NOT IN ('canceled', 'unavailable')

UNION ALL

SELECT 
    'Average Order Value' AS metric_name,
    'R$ ' || TO_CHAR(AVG(order_total_value), 'FM999.00') AS metric_value,
    'currency' AS metric_type
FROM fct_orders
WHERE order_status NOT IN ('canceled', 'unavailable')

UNION ALL

SELECT 
    'Unique Customers' AS metric_name,
    COUNT(DISTINCT customer_key)::TEXT AS metric_value,
    'count' AS metric_type
FROM fct_orders
WHERE order_status NOT IN ('canceled', 'unavailable')

UNION ALL

SELECT 
    'Average Review Score' AS metric_name,
    TO_CHAR(AVG(review_score), 'FM9.00') AS metric_value,
    'rating' AS metric_type
FROM fct_orders
WHERE order_status NOT IN ('canceled', 'unavailable')

UNION ALL

SELECT 
    'On-Time Delivery Rate' AS metric_name,
    TO_CHAR(100 - (SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100), 'FM90.0') || '%' AS metric_value,
    'percentage' AS metric_type
FROM fct_orders
WHERE order_status = 'delivered';

COMMENT ON MATERIALIZED VIEW mv_executive_dashboard IS 'KPIs principales para dashboard ejecutivo';


-- ============================================================
-- FUNCIONES DE REFRESCO
-- ============================================================

-- Funcion para refrescar todas las vistas materializadas
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS TEXT AS $$
DECLARE
    view_name TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    result_text TEXT := 'Vistas materializadas refrescadas:' || E'\n';
BEGIN
    start_time := clock_timestamp();
    
    FOR view_name IN 
        SELECT matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'public'
        ORDER BY matviewname
    LOOP
        EXECUTE 'REFRESH MATERIALIZED VIEW ' || view_name;
        result_text := result_text || '  - ' || view_name || E'\n';
    END LOOP;
    
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    result_text := result_text || 'Tiempo total: ' || duration;
    
    RETURN result_text;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_materialized_views() IS 'Refresca todas las vistas materializadas del esquema public';


-- ============================================================
-- INSTRUCCIONES DE USO
-- ============================================================

-- Refrescar una vista especifica:
-- REFRESH MATERIALIZED VIEW mv_sales_by_month;

-- Refrescar todas las vistas:
-- SELECT refresh_all_materialized_views();

-- Ver lista de vistas materializadas:
-- SELECT * FROM pg_matviews WHERE schemaname = 'public';
