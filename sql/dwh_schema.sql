-- ============================================
-- Schema para Data Warehouse (OLAP)
-- Modelo en Estrella (Star Schema)
-- ============================================

-- ============================================
-- TABLAS DE DIMENSIONES
-- ============================================

-- Dimensión de Clientes
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2),
    customer_region VARCHAR(20),
    
    -- Campos de auditoría (SCD Type 2)
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_customers_unique_id ON dim_customers(customer_unique_id);
CREATE INDEX idx_dim_customers_state ON dim_customers(customer_state);

-- Dimensión de Productos
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    
    -- Campos calculados
    product_volume_cm3 INTEGER,
    product_weight_category VARCHAR(20),
    
    -- Auditoría
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_category ON dim_products(product_category_name);

-- Dimensión de Vendedores
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state CHAR(2),
    seller_region VARCHAR(20),
    
    -- Auditoría
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_sellers_id ON dim_sellers(seller_id);
CREATE INDEX idx_dim_sellers_state ON dim_sellers(seller_state);

-- Dimensión de Geolocalización
CREATE TABLE IF NOT EXISTS dim_geolocation (
    geolocation_key SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat DECIMAL(11, 8),
    geolocation_lng DECIMAL(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2),
    geolocation_region VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_geo_zip ON dim_geolocation(geolocation_zip_code_prefix);
CREATE INDEX idx_dim_geo_state ON dim_geolocation(geolocation_state);

-- Dimensión de Fecha (Date Dimension)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    
    -- Para análisis de temporada
    season VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- ============================================
-- TABLA DE HECHOS
-- ============================================

-- Tabla de Hechos: Órdenes
CREATE TABLE IF NOT EXISTS fct_orders (
    order_fact_key BIGSERIAL PRIMARY KEY,
    
    -- Claves foráneas a dimensiones
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    seller_key INTEGER NOT NULL,
    purchase_date_key INTEGER NOT NULL,
    approval_date_key INTEGER,
    carrier_date_key INTEGER,
    delivery_date_key INTEGER,
    
    -- Claves de negocio
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    
    -- Métricas aditivas
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    payment_value DECIMAL(10, 2),
    
    -- Métricas calculadas
    total_order_value DECIMAL(10, 2),
    
    -- Métricas de tiempo
    delivery_time_days INTEGER,
    estimated_delivery_time_days INTEGER,
    delivery_delay_days INTEGER,
    
    -- Métricas de calidad
    review_score INTEGER,
    
    -- Flags
    is_delayed BOOLEAN,
    is_cancelled BOOLEAN,
    
    -- Auditoría
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    FOREIGN KEY (seller_key) REFERENCES dim_sellers(seller_key),
    FOREIGN KEY (purchase_date_key) REFERENCES dim_date(date_key)
);

-- Índices para optimizar queries
CREATE INDEX idx_fct_orders_customer ON fct_orders(customer_key);
CREATE INDEX idx_fct_orders_product ON fct_orders(product_key);
CREATE INDEX idx_fct_orders_seller ON fct_orders(seller_key);
CREATE INDEX idx_fct_orders_purchase_date ON fct_orders(purchase_date_key);
CREATE INDEX idx_fct_orders_order_id ON fct_orders(order_id);
CREATE INDEX idx_fct_orders_delivery_date ON fct_orders(delivery_date_key);

-- ============================================
-- TABLAS AGREGADAS (Para mejorar performance)
-- ============================================

-- Tabla agregada: Ventas por mes y producto
CREATE TABLE IF NOT EXISTS fct_sales_monthly (
    sales_monthly_key SERIAL PRIMARY KEY,
    product_key INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_quantity INTEGER,
    total_revenue DECIMAL(12, 2),
    total_freight DECIMAL(12, 2),
    avg_price DECIMAL(10, 2),
    num_orders INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key)
);

CREATE INDEX idx_fct_sales_monthly_product ON fct_sales_monthly(product_key);
CREATE INDEX idx_fct_sales_monthly_date ON fct_sales_monthly(year, month);

-- Tabla agregada: Análisis de clientes
CREATE TABLE IF NOT EXISTS fct_customer_summary (
    customer_summary_key SERIAL PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    total_orders INTEGER,
    total_spent DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    customer_lifetime_days INTEGER,
    avg_review_score DECIMAL(3, 2),
    is_repeat_customer BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key)
);

CREATE INDEX idx_fct_customer_summary_customer ON fct_customer_summary(customer_key);

-- ============================================
-- VISTAS PARA ANÁLISIS DE NEGOCIO
-- ============================================

-- Vista: Ventas totales por estado
CREATE OR REPLACE VIEW v_sales_by_state AS
SELECT 
    dc.customer_state,
    dc.customer_region,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value,
    AVG(fo.delivery_time_days) as avg_delivery_time
FROM fct_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_state, dc.customer_region
ORDER BY total_revenue DESC;

-- Vista: Top productos por categoría
CREATE OR REPLACE VIEW v_top_products_by_category AS
SELECT 
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) as orders_count,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.review_score) as avg_rating
FROM fct_orders fo
JOIN dim_products dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;

-- Vista: Análisis temporal de ventas
CREATE OR REPLACE VIEW v_sales_trend AS
SELECT 
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.total_order_value) as avg_order_value,
    SUM(fo.price) as total_product_value,
    SUM(fo.freight_value) as total_freight_value
FROM fct_orders fo
JOIN dim_date dd ON fo.purchase_date_key = dd.date_key
GROUP BY dd.year, dd.quarter, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- Vista: Performance de vendedores por región
CREATE OR REPLACE VIEW v_seller_performance AS
SELECT 
    ds.seller_id,
    ds.seller_state,
    ds.seller_region,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_order_value) as total_revenue,
    AVG(fo.review_score) as avg_rating,
    AVG(fo.delivery_time_days) as avg_delivery_time,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) as delayed_orders
FROM fct_orders fo
JOIN dim_sellers ds ON fo.seller_key = ds.seller_key
WHERE ds.is_current = TRUE
GROUP BY ds.seller_id, ds.seller_state, ds.seller_region
ORDER BY total_revenue DESC;

-- ============================================
-- Funciones de utilidad
-- ============================================

-- Función para poblar dimensión de fechas
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
DECLARE
    date_iter DATE;
BEGIN
    date_iter := start_date;
    WHILE date_iter <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            week,
            day_of_month,
            day_of_week,
            day_name,
            is_weekend,
            season
        ) VALUES (
            TO_CHAR(date_iter, 'YYYYMMDD')::INTEGER,
            date_iter,
            EXTRACT(YEAR FROM date_iter)::INTEGER,
            EXTRACT(QUARTER FROM date_iter)::INTEGER,
            EXTRACT(MONTH FROM date_iter)::INTEGER,
            TO_CHAR(date_iter, 'Month'),
            EXTRACT(WEEK FROM date_iter)::INTEGER,
            EXTRACT(DAY FROM date_iter)::INTEGER,
            EXTRACT(DOW FROM date_iter)::INTEGER,
            TO_CHAR(date_iter, 'Day'),
            CASE WHEN EXTRACT(DOW FROM date_iter) IN (0, 6) THEN TRUE ELSE FALSE END,
            CASE 
                WHEN EXTRACT(MONTH FROM date_iter) IN (12, 1, 2) THEN 'Verano'
                WHEN EXTRACT(MONTH FROM date_iter) IN (3, 4, 5) THEN 'Otoño'
                WHEN EXTRACT(MONTH FROM date_iter) IN (6, 7, 8) THEN 'Invierno'
                ELSE 'Primavera'
            END
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        date_iter := date_iter + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Poblar la dimensión de fechas desde 2016 hasta 2020
SELECT populate_dim_date('2016-01-01'::DATE, '2020-12-31'::DATE);
