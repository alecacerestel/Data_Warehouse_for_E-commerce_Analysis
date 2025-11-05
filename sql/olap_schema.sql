-- ============================================================
-- Schema para Data Warehouse OLAP - Modelo Estrella
-- Base de datos: olist_olap
-- Modelo: 4 Dimensiones + 1 Tabla de Hechos
-- ============================================================

-- Crear base de datos OLAP (ejecutar como superusuario)
-- CREATE DATABASE olist_olap;

-- ============================================================
-- TABLAS DE DIMENSIONES
-- ============================================================

-- Dimension: Clientes
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    customer_region VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices para dim_customers
CREATE INDEX idx_dim_customers_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_customers_state ON dim_customers(customer_state);
CREATE INDEX idx_dim_customers_region ON dim_customers(customer_region);
CREATE INDEX idx_dim_customers_city ON dim_customers(customer_city);

-- Comentarios
COMMENT ON TABLE dim_customers IS 'Dimension de clientes con clasificacion regional';
COMMENT ON COLUMN dim_customers.customer_key IS 'Clave surrogada (PK)';
COMMENT ON COLUMN dim_customers.customer_id IS 'Clave natural de cliente';
COMMENT ON COLUMN dim_customers.customer_region IS 'Region de Brasil: Norte, Nordeste, Centro-Oeste, Sudeste, Sur';


-- Dimension: Productos
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    product_volume_cm3 DECIMAL(12,2),
    product_size VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices para dim_products
CREATE INDEX idx_dim_products_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_category_pt ON dim_products(product_category_name);
CREATE INDEX idx_dim_products_category_en ON dim_products(product_category_name_english);
CREATE INDEX idx_dim_products_size ON dim_products(product_size);

-- Comentarios
COMMENT ON TABLE dim_products IS 'Dimension de productos con categorias traducidas y clasificacion de tamano';
COMMENT ON COLUMN dim_products.product_key IS 'Clave surrogada (PK)';
COMMENT ON COLUMN dim_products.product_id IS 'Clave natural de producto';
COMMENT ON COLUMN dim_products.product_volume_cm3 IS 'Volumen calculado: length x height x width';
COMMENT ON COLUMN dim_products.product_size IS 'Clasificacion: pequeno, mediano, grande';


-- Dimension: Vendedores
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL UNIQUE,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    seller_region VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices para dim_sellers
CREATE INDEX idx_dim_sellers_id ON dim_sellers(seller_id);
CREATE INDEX idx_dim_sellers_state ON dim_sellers(seller_state);
CREATE INDEX idx_dim_sellers_region ON dim_sellers(seller_region);
CREATE INDEX idx_dim_sellers_city ON dim_sellers(seller_city);

-- Comentarios
COMMENT ON TABLE dim_sellers IS 'Dimension de vendedores con clasificacion regional';
COMMENT ON COLUMN dim_sellers.seller_key IS 'Clave surrogada (PK)';
COMMENT ON COLUMN dim_sellers.seller_id IS 'Clave natural de vendedor';
COMMENT ON COLUMN dim_sellers.seller_region IS 'Region de Brasil: Norte, Nordeste, Centro-Oeste, Sudeste, Sur';


-- Dimension: Fecha
CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    quarter_name VARCHAR(10),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indices para dim_date
CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_quarter ON dim_date(year, quarter);
CREATE INDEX idx_dim_date_month ON dim_date(year, month);
CREATE INDEX idx_dim_date_weekend ON dim_date(is_weekend);

-- Comentarios
COMMENT ON TABLE dim_date IS 'Dimension de fecha con atributos de calendario completo';
COMMENT ON COLUMN dim_date.date_key IS 'Clave surrogada (PK)';
COMMENT ON COLUMN dim_date.full_date IS 'Fecha completa (clave natural)';
COMMENT ON COLUMN dim_date.quarter_name IS 'Nombre del trimestre: Q1, Q2, Q3, Q4';


-- ============================================================
-- TABLA DE HECHOS
-- ============================================================

CREATE TABLE IF NOT EXISTS fct_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_key INTEGER NOT NULL,
    product_key INTEGER,
    seller_key INTEGER,
    purchase_date_key INTEGER NOT NULL,
    order_status VARCHAR(20),
    items_count INTEGER DEFAULT 0,
    total_items_price DECIMAL(10,2) DEFAULT 0,
    total_freight DECIMAL(10,2) DEFAULT 0,
    total_payment DECIMAL(10,2) DEFAULT 0,
    order_total_value DECIMAL(10,2) DEFAULT 0,
    max_installments INTEGER DEFAULT 1,
    payment_type VARCHAR(50),
    review_score DECIMAL(3,2) DEFAULT 0,
    delivery_time_days INTEGER,
    estimated_delivery_time_days INTEGER,
    is_delayed BOOLEAN DEFAULT FALSE,
    delay_days INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Claves foraneas
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    CONSTRAINT fk_seller FOREIGN KEY (seller_key) REFERENCES dim_sellers(seller_key),
    CONSTRAINT fk_purchase_date FOREIGN KEY (purchase_date_key) REFERENCES dim_date(date_key)
);

-- Indices para fct_orders
CREATE INDEX idx_fct_orders_customer ON fct_orders(customer_key);
CREATE INDEX idx_fct_orders_product ON fct_orders(product_key);
CREATE INDEX idx_fct_orders_seller ON fct_orders(seller_key);
CREATE INDEX idx_fct_orders_date ON fct_orders(purchase_date_key);
CREATE INDEX idx_fct_orders_status ON fct_orders(order_status);
CREATE INDEX idx_fct_orders_delayed ON fct_orders(is_delayed);
CREATE INDEX idx_fct_orders_payment_type ON fct_orders(payment_type);

-- Indices compuestos para queries comunes
CREATE INDEX idx_fct_orders_date_status ON fct_orders(purchase_date_key, order_status);
CREATE INDEX idx_fct_orders_customer_date ON fct_orders(customer_key, purchase_date_key);
CREATE INDEX idx_fct_orders_product_date ON fct_orders(product_key, purchase_date_key);
CREATE INDEX idx_fct_orders_seller_date ON fct_orders(seller_key, purchase_date_key);

-- Comentarios
COMMENT ON TABLE fct_orders IS 'Tabla de hechos de ordenes con metricas de negocio';
COMMENT ON COLUMN fct_orders.order_key IS 'Clave surrogada (PK)';
COMMENT ON COLUMN fct_orders.order_id IS 'Clave natural de orden';
COMMENT ON COLUMN fct_orders.order_total_value IS 'Valor total = items + flete';
COMMENT ON COLUMN fct_orders.delivery_time_days IS 'Tiempo real de entrega en dias';
COMMENT ON COLUMN fct_orders.is_delayed IS 'TRUE si entrega supero fecha estimada';
COMMENT ON COLUMN fct_orders.delay_days IS 'Dias de retraso (0 si no hay retraso)';


-- ============================================================
-- FUNCIONES DE AUDITORIA
-- ============================================================

-- Funcion para actualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para actualizar updated_at
CREATE TRIGGER update_dim_customers_updated_at BEFORE UPDATE ON dim_customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dim_products_updated_at BEFORE UPDATE ON dim_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dim_sellers_updated_at BEFORE UPDATE ON dim_sellers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dim_date_updated_at BEFORE UPDATE ON dim_date
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fct_orders_updated_at BEFORE UPDATE ON fct_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================================================
-- ESTADISTICAS Y VACUUM
-- ============================================================

-- Analizar tablas para optimizar queries
ANALYZE dim_customers;
ANALYZE dim_products;
ANALYZE dim_sellers;
ANALYZE dim_date;
ANALYZE fct_orders;

-- Vacuum para recuperar espacio
VACUUM ANALYZE dim_customers;
VACUUM ANALYZE dim_products;
VACUUM ANALYZE dim_sellers;
VACUUM ANALYZE dim_date;
VACUUM ANALYZE fct_orders;
