# Modelo de Datos - Data Warehouse Olist E-commerce

## Esquema en Estrella (Star Schema)

Este documento describe el modelo de datos del Data Warehouse implementado para el análisis del e-commerce de Olist.

## Arquitectura del Modelo

```
                                    dim_date
                                        |
                                        |
    dim_customers ----+                 |
                      |                 |
    dim_products  ----+--- fct_orders --+
                      |                 |
    dim_sellers   ----+                 |
                      |                 |
    dim_geolocation --+
```

## Tablas de Dimensiones

### dim_customers (Dimensión de Clientes)

Contiene información sobre los clientes.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| customer_key | SERIAL | Clave primaria (surrogate key) |
| customer_id | VARCHAR(50) | ID del cliente en la orden |
| customer_unique_id | VARCHAR(50) | ID único del cliente real |
| customer_zip_code_prefix | VARCHAR(10) | Código postal |
| customer_city | VARCHAR(100) | Ciudad |
| customer_state | CHAR(2) | Estado (SP, RJ, MG, etc.) |
| customer_region | VARCHAR(20) | Región (Norte, Nordeste, Sul, etc.) |
| valid_from | TIMESTAMP | Fecha de inicio de validez (SCD Type 2) |
| valid_to | TIMESTAMP | Fecha de fin de validez |
| is_current | BOOLEAN | Registro actual |

**Granularidad:** Un registro por customer_id (puede haber múltiples customer_id para el mismo customer_unique_id)

---

### dim_products (Dimensión de Productos)

Contiene información sobre los productos.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| product_key | SERIAL | Clave primaria |
| product_id | VARCHAR(50) | ID único del producto |
| product_category_name | VARCHAR(100) | Categoría en portugués |
| product_category_name_english | VARCHAR(100) | Categoría en inglés |
| product_name_lenght | INTEGER | Longitud del nombre |
| product_description_lenght | INTEGER | Longitud de la descripción |
| product_photos_qty | INTEGER | Cantidad de fotos |
| product_weight_g | INTEGER | Peso en gramos |
| product_length_cm | INTEGER | Largo en cm |
| product_height_cm | INTEGER | Alto en cm |
| product_width_cm | INTEGER | Ancho en cm |
| product_volume_cm3 | INTEGER | Volumen calculado |
| product_weight_category | VARCHAR(20) | Categoría de peso (Liviano, Medio, Pesado) |

**Granularidad:** Un registro por producto único

---

### dim_sellers (Dimensión de Vendedores)

Contiene información sobre los vendedores.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| seller_key | SERIAL | Clave primaria |
| seller_id | VARCHAR(50) | ID único del vendedor |
| seller_zip_code_prefix | VARCHAR(10) | Código postal |
| seller_city | VARCHAR(100) | Ciudad |
| seller_state | CHAR(2) | Estado |
| seller_region | VARCHAR(20) | Región |

**Granularidad:** Un registro por vendedor único

---

### dim_geolocation (Dimensión de Geolocalización)

Contiene información geográfica.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| geolocation_key | SERIAL | Clave primaria |
| geolocation_zip_code_prefix | VARCHAR(10) | Código postal |
| geolocation_lat | DECIMAL(10,8) | Latitud |
| geolocation_lng | DECIMAL(10,8) | Longitud |
| geolocation_city | VARCHAR(100) | Ciudad |
| geolocation_state | CHAR(2) | Estado |
| geolocation_region | VARCHAR(20) | Región |

**Granularidad:** Un registro por código postal (promedio de coordenadas)

---

### dim_date (Dimensión de Tiempo)

Contiene información temporal para análisis.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| date_key | INTEGER | Clave primaria (formato YYYYMMDD) |
| full_date | DATE | Fecha completa |
| year | INTEGER | Año |
| quarter | INTEGER | Trimestre (1-4) |
| month | INTEGER | Mes (1-12) |
| month_name | VARCHAR(20) | Nombre del mes |
| week | INTEGER | Semana del año |
| day_of_month | INTEGER | Día del mes |
| day_of_week | INTEGER | Día de la semana (0=Domingo) |
| day_name | VARCHAR(20) | Nombre del día |
| is_weekend | BOOLEAN | Es fin de semana |
| is_holiday | BOOLEAN | Es día festivo |
| season | VARCHAR(20) | Temporada (Verano, Otoño, etc.) |

**Granularidad:** Un registro por día

---

## Tabla de Hechos

### fct_orders (Hechos de Órdenes)

Tabla central con las métricas de negocio.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| **Claves Foráneas** |
| customer_key | INTEGER | FK a dim_customers |
| product_key | INTEGER | FK a dim_products |
| seller_key | INTEGER | FK a dim_sellers |
| purchase_date_key | INTEGER | FK a dim_date (fecha de compra) |
| approval_date_key | INTEGER | FK a dim_date (fecha de aprobación) |
| carrier_date_key | INTEGER | FK a dim_date (fecha de envío) |
| delivery_date_key | INTEGER | FK a dim_date (fecha de entrega) |
| **Claves de Negocio** |
| order_id | VARCHAR(50) | ID de la orden |
| order_item_id | INTEGER | ID del item en la orden |
| **Métricas Aditivas** |
| price | DECIMAL(10,2) | Precio del producto (R$) |
| freight_value | DECIMAL(10,2) | Valor del flete (R$) |
| payment_value | DECIMAL(10,2) | Valor pagado total (R$) |
| total_order_value | DECIMAL(10,2) | Valor total (precio + flete) |
| **Métricas de Tiempo** |
| delivery_time_days | INTEGER | Días de entrega real |
| estimated_delivery_time_days | INTEGER | Días estimados de entrega |
| delivery_delay_days | INTEGER | Días de retraso |
| **Métricas de Calidad** |
| review_score | INTEGER | Puntuación del review (1-5) |
| **Flags** |
| is_delayed | BOOLEAN | Indica si hubo retraso |
| is_cancelled | BOOLEAN | Indica si fue cancelada |

**Granularidad:** Un registro por cada item de cada orden (order_id + order_item_id)

---

## Relaciones

```
fct_orders.customer_key → dim_customers.customer_key
fct_orders.product_key → dim_products.product_key
fct_orders.seller_key → dim_sellers.seller_key
fct_orders.purchase_date_key → dim_date.date_key
fct_orders.approval_date_key → dim_date.date_key
fct_orders.carrier_date_key → dim_date.date_key
fct_orders.delivery_date_key → dim_date.date_key
```

---

## Métricas de Negocio Calculadas

### Ingresos Totales
```sql
SUM(fct_orders.total_order_value)
```

### Ticket Promedio
```sql
AVG(fct_orders.total_order_value)
```

### Tiempo Promedio de Entrega
```sql
AVG(fct_orders.delivery_time_days)
```

### Tasa de Retraso
```sql
COUNT(CASE WHEN is_delayed THEN 1 END) / COUNT(*) * 100
```

### Customer Lifetime Value
```sql
SUM(total_order_value) GROUP BY customer_unique_id
```

### Tasa de Retención
```sql
-- Clientes con más de una orden
COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_unique_id END) / 
COUNT(DISTINCT customer_unique_id)
```

---

## Ventajas del Modelo en Estrella

1. **Simplicidad:** Fácil de entender y consultar
2. **Performance:** Queries rápidas con joins simples
3. **Flexibilidad:** Fácil agregar nuevas dimensiones
4. **Navegabilidad:** Drill-down y roll-up intuitivos
5. **Optimización:** Ideal para herramientas de BI

---

## Vistas Pre-calculadas

### v_sales_by_state
Ventas agregadas por estado

### v_top_products_by_category
Productos más vendidos por categoría

### v_sales_trend
Tendencia temporal de ventas

### v_seller_performance
Performance de vendedores

---

## Ejemplos de Queries de Análisis

### Top 10 Productos más Vendidos
```sql
SELECT 
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) as orders,
    SUM(fo.total_order_value) as revenue
FROM fct_orders fo
JOIN dim_products dp ON fo.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_category_name_english
ORDER BY revenue DESC
LIMIT 10;
```

### Clientes Más Valiosos por Estado
```sql
SELECT 
    dc.customer_state,
    COUNT(DISTINCT dc.customer_unique_id) as customers,
    SUM(fo.total_order_value) as total_revenue
FROM fct_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_state
ORDER BY total_revenue DESC;
```

### Tiempo de Entrega por Región
```sql
SELECT 
    dc.customer_region,
    AVG(fo.delivery_time_days) as avg_delivery_days,
    COUNT(CASE WHEN fo.is_delayed THEN 1 END) * 100.0 / COUNT(*) as pct_delayed
FROM fct_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_region
ORDER BY avg_delivery_days;
```

---

## Referencias

- Kimball, Ralph. "The Data Warehouse Toolkit"
- Inmon, Bill. "Building the Data Warehouse"
- Dataset original: [Olist E-commerce Dataset en Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
