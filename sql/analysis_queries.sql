-- ============================================
-- Queries de Análisis de Negocio
-- Data Warehouse: Olist E-commerce
-- ============================================

-- ============================================
-- 1. PRODUCTOS MÁS VENDIDOS POR MES
-- ============================================

SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) as num_orders,
    SUM(fo.price) as total_revenue,
    AVG(fo.price) as avg_price,
    AVG(fo.review_score) as avg_rating
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
JOIN dim_products dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dd.year, dd.month, dd.month_name, dp.product_category_name_english
ORDER BY dd.year, dd.month, total_revenue DESC;

-- Top 10 productos más vendidos globalmente
SELECT 
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) as num_orders,
    COUNT(*) as num_items,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.price) as avg_price,
    AVG(fo.review_score) as avg_rating
FROM fct_orders fo
JOIN dim_products dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================
-- 2. CLIENTES MÁS VALIOSOS POR ESTADO
-- ============================================

SELECT 
    dc.customer_state,
    dc.customer_region,
    COUNT(DISTINCT dc.customer_unique_id) as num_unique_customers,
    COUNT(DISTINCT fo.order_id) as num_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value,
    SUM(fo.total_order_value) / COUNT(DISTINCT dc.customer_unique_id) as revenue_per_customer
FROM fct_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_state, dc.customer_region
ORDER BY total_revenue DESC;

-- Top 20 clientes con mayor gasto
WITH customer_spend AS (
    SELECT 
        dc.customer_unique_id,
        dc.customer_city,
        dc.customer_state,
        dc.customer_region,
        COUNT(DISTINCT fo.order_id) as num_orders,
        SUM(fo.total_order_value) as total_spent,
        AVG(fo.total_order_value) as avg_order_value,
        AVG(fo.review_score) as avg_rating,
        MIN(dd.full_date) as first_purchase,
        MAX(dd.full_date) as last_purchase
    FROM fct_orders fo
    JOIN dim_customers dc ON fo.customer_key = dc.customer_key
    JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
    WHERE dc.is_current = TRUE
    GROUP BY dc.customer_unique_id, dc.customer_city, dc.customer_state, dc.customer_region
)
SELECT 
    customer_unique_id,
    customer_city,
    customer_state,
    customer_region,
    num_orders,
    ROUND(total_spent, 2) as total_spent,
    ROUND(avg_order_value, 2) as avg_order_value,
    ROUND(avg_rating, 2) as avg_rating,
    first_purchase,
    last_purchase,
    (last_purchase - first_purchase) as customer_lifetime_days
FROM customer_spend
ORDER BY total_spent DESC
LIMIT 20;

-- ============================================
-- 3. TIEMPO PROMEDIO DE ENTREGA POR REGIÓN
-- ============================================

SELECT 
    dc.customer_region,
    dc.customer_state,
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
ORDER BY avg_delivery_days;

-- Análisis de entrega por vendedor
SELECT 
    ds.seller_state,
    ds.seller_region,
    COUNT(DISTINCT fo.order_id) as num_orders,
    ROUND(AVG(fo.delivery_time_days), 2) as avg_delivery_days,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
    ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed,
    ROUND(AVG(fo.review_score), 2) as avg_rating
FROM fct_orders fo
JOIN dim_sellers ds ON fo.seller_key = ds.seller_key
WHERE ds.is_current = TRUE
  AND fo.delivery_time_days IS NOT NULL
GROUP BY ds.seller_state, ds.seller_region
ORDER BY num_orders DESC;

-- ============================================
-- 4. ANÁLISIS TEMPORAL DE VENTAS
-- ============================================

-- Ventas por año y mes
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id) as num_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value,
    SUM(fo.price) as product_revenue,
    SUM(fo.freight_value) as freight_revenue
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Ventas por día de la semana
SELECT 
    dd.day_name,
    dd.day_of_week,
    COUNT(DISTINCT fo.order_id) as num_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
GROUP BY dd.day_name, dd.day_of_week
ORDER BY dd.day_of_week;

-- Ventas por temporada
SELECT 
    dd.season,
    COUNT(DISTINCT fo.order_id) as num_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
GROUP BY dd.season
ORDER BY total_revenue DESC;

-- ============================================
-- 5. ANÁLISIS DE CATEGORÍAS DE PRODUCTOS
-- ============================================

SELECT 
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) as num_orders,
    COUNT(*) as num_items_sold,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.price) as avg_price,
    AVG(fo.freight_value) as avg_freight,
    AVG(fo.review_score) as avg_rating,
    AVG(fo.delivery_time_days) as avg_delivery_days
FROM fct_orders fo
JOIN dim_products dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;

-- ============================================
-- 6. ANÁLISIS DE CLIENTES RECURRENTES
-- ============================================

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
        WHEN num_orders = 1 THEN 'Una compra'
        WHEN num_orders = 2 THEN 'Dos compras'
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
        WHEN 'Una compra' THEN 1
        WHEN 'Dos compras' THEN 2
        WHEN '3-5 compras' THEN 3
        ELSE 4
    END;

-- ============================================
-- 7. ANÁLISIS DE REVIEWS Y SATISFACCIÓN
-- ============================================

SELECT 
    ROUND(fo.review_score) as rating,
    COUNT(DISTINCT fo.order_id) as num_orders,
    ROUND(AVG(fo.total_order_value), 2) as avg_order_value,
    ROUND(AVG(fo.delivery_time_days), 2) as avg_delivery_days,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
    ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed
FROM fct_orders fo
WHERE fo.review_score IS NOT NULL
GROUP BY ROUND(fo.review_score)
ORDER BY rating DESC;

-- Correlación entre retraso y rating
SELECT 
    CASE 
        WHEN fo.is_delayed THEN 'Con Retraso'
        ELSE 'Sin Retraso'
    END as delivery_status,
    COUNT(DISTINCT fo.order_id) as num_orders,
    ROUND(AVG(fo.review_score), 2) as avg_rating,
    ROUND(AVG(fo.delivery_delay_days), 2) as avg_delay_days
FROM fct_orders fo
WHERE fo.review_score IS NOT NULL
  AND fo.delivery_time_days IS NOT NULL
GROUP BY fo.is_delayed;

-- ============================================
-- 8. ANÁLISIS DE PERFORMANCE DE VENDEDORES
-- ============================================

SELECT 
    ds.seller_id,
    ds.seller_state,
    ds.seller_region,
    COUNT(DISTINCT fo.order_id) as num_orders,
    COUNT(DISTINCT fo.product_key) as num_products,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value,
    AVG(fo.review_score) as avg_rating,
    AVG(fo.delivery_time_days) as avg_delivery_days,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
    ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed
FROM fct_orders fo
JOIN dim_sellers ds ON fo.seller_key = ds.seller_key
WHERE ds.is_current = TRUE
GROUP BY ds.seller_id, ds.seller_state, ds.seller_region
HAVING COUNT(DISTINCT fo.order_id) >= 10
ORDER BY total_revenue DESC
LIMIT 50;

-- ============================================
-- 9. ANÁLISIS COHORT (Mes de primera compra)
-- ============================================

WITH first_purchase AS (
    SELECT 
        dc.customer_unique_id,
        MIN(dd.year) as cohort_year,
        MIN(dd.month) as cohort_month
    FROM fct_orders fo
    JOIN dim_customers dc ON fo.customer_key = dc.customer_key
    JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
    WHERE dc.is_current = TRUE
    GROUP BY dc.customer_unique_id
),
cohort_data AS (
    SELECT 
        fp.cohort_year,
        fp.cohort_month,
        COUNT(DISTINCT fp.customer_unique_id) as cohort_size,
        SUM(fo.total_order_value) as cohort_revenue
    FROM first_purchase fp
    JOIN dim_customers dc ON fp.customer_unique_id = dc.customer_unique_id
    JOIN fct_orders fo ON dc.customer_key = fo.customer_key
    WHERE dc.is_current = TRUE
    GROUP BY fp.cohort_year, fp.cohort_month
)
SELECT 
    cohort_year,
    cohort_month,
    cohort_size,
    ROUND(cohort_revenue, 2) as total_revenue,
    ROUND(cohort_revenue / cohort_size, 2) as revenue_per_customer
FROM cohort_data
ORDER BY cohort_year, cohort_month;

-- ============================================
-- 10. KPIs PRINCIPALES DEL NEGOCIO
-- ============================================

SELECT 
    COUNT(DISTINCT fo.order_id) as total_orders,
    COUNT(DISTINCT dc.customer_unique_id) as total_unique_customers,
    COUNT(DISTINCT dp.product_key) as total_products_sold,
    COUNT(DISTINCT ds.seller_key) as total_sellers,
    ROUND(SUM(fo.total_order_value), 2) as total_revenue,
    ROUND(AVG(fo.total_order_value), 2) as avg_order_value,
    ROUND(AVG(fo.price), 2) as avg_product_price,
    ROUND(AVG(fo.freight_value), 2) as avg_freight,
    ROUND(AVG(fo.review_score), 2) as avg_rating,
    ROUND(AVG(fo.delivery_time_days), 2) as avg_delivery_days,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders,
    ROUND(COUNT(CASE WHEN fo.is_delayed THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_delayed,
    COUNT(CASE WHEN fo.is_cancelled THEN 1 END) as cancelled_orders,
    ROUND(COUNT(CASE WHEN fo.is_cancelled THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as pct_cancelled
FROM fct_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
JOIN dim_products dp ON fo.product_key = dp.product_key
JOIN dim_sellers ds ON fo.seller_key = ds.seller_key
WHERE dc.is_current = TRUE 
  AND dp.is_current = TRUE 
  AND ds.is_current = TRUE;
