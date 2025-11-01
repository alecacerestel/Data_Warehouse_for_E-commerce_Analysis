# Guía Rápida de Ejecución
## Data Warehouse Olist E-commerce

## Estado Actual del Proyecto

### ¿Qué está LISTO?
- Código completo del pipeline ETL/ELT
- Scripts de transformación y análisis
- Documentación completa
- Archivos CSV organizados en `data/raw/` (9 archivos, 126 MB)
- Python 3.12.6 instalado

### ¿Qué FALTA para ejecutar?

## Pasos Necesarios para Ejecutar el Programa

### **1. Instalar PostgreSQL** PENDIENTE
```powershell
# Descargar e instalar desde:
# https://www.postgresql.org/download/windows/

# Durante la instalación, anotar:
# - Usuario: postgres
# - Contraseña: (tu contraseña)
# - Puerto: 5432
```

**Tiempo estimado:** 10-15 minutos

---

### **2. Crear las Bases de Datos** PENDIENTE
```powershell
# Abrir PowerShell y ejecutar:
psql -U postgres

# Dentro de PostgreSQL:
CREATE DATABASE olist_oltp;
CREATE DATABASE olist_dwh;

# Verificar:
\l

# Salir:
\q
```

**Tiempo estimado:** 2 minutos

---

### **3. Crear Entorno Virtual de Python** PENDIENTE
```powershell
# En el directorio del proyecto:
python -m venv venv

# Activar el entorno virtual:
.\venv\Scripts\Activate.ps1

# Si hay error de permisos:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Tiempo estimado:** 2 minutos

---

### **4. Instalar Dependencias de Python** PENDIENTE
```powershell
# Con el entorno virtual activado:
pip install --upgrade pip
pip install -r requirements.txt
```

**Dependencias principales:**
- pandas==2.1.4
- sqlalchemy==2.0.23
- psycopg2-binary==2.9.9
- pyarrow==14.0.1
- python-dotenv==1.0.0
- loguru==0.7.2
- openpyxl==3.1.2
- apache-airflow==2.8.0

**Tiempo estimado:** 3-5 minutos

---

### **5. Configurar Variables de Entorno** PENDIENTE
```powershell
# Crear archivo .env desde la plantilla:
Copy-Item .env.example .env

# Editar .env con tus credenciales:
notepad .env
```

**Configuración mínima requerida:**
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=TU_CONTRASEÑA_AQUI  # Cambiar esto

OLTP_DB=olist_oltp
DWH_DB=olist_dwh

LOG_LEVEL=INFO
LOG_FILE=./logs/pipeline.log
```

**Tiempo estimado:** 1 minuto

---

### **6. Crear Schemas de Base de Datos** PENDIENTE
```powershell
# Crear schema OLTP:
psql -U postgres -d olist_oltp -f sql\oltp_schema.sql

# Crear schema DWH:
psql -U postgres -d olist_dwh -f sql\dwh_schema.sql
```

**Tiempo estimado:** 1 minuto

---

## Ejecutar el Pipeline

### **Opción 1: Script Completo (Recomendado)**
```powershell
# Con el entorno virtual activado:
python run_pipeline.py
```

**Esto ejecutará:**
1. Carga de CSV → OLTP (2-3 min)
2. Extracción a Data Lake en Parquet (1-2 min)
3. Limpieza y transformación (1-2 min)
4. Creación de dimensiones (1 min)
5. Creación de tabla de hechos (2 min)
6. Análisis de negocio (1 min)
7. Generación de reportes Excel

**Tiempo total estimado:** 8-12 minutos

---

### **Opción 2: Paso a Paso (Para debugging)**
```powershell
# 1. Cargar CSVs a OLTP
python scripts\01_extract\load_csv_to_oltp.py

# 2. Extraer a Data Lake
python scripts\02_staging\load_to_datalake.py

# 3. Limpiar datos
python scripts\03_transform\data_cleaning.py

# 4. Crear dimensiones
python scripts\03_transform\create_dimensions.py

# 5. Crear tabla de hechos
python scripts\03_transform\create_fact_table.py

# 6. Ejecutar análisis
python scripts\05_analysis\business_queries.py
```

---

### **Opción 3: Script Interactivo**
```powershell
# Script guiado con menú:
.\quick_start.ps1
```

---

## Verificar Resultados

### Verificar datos en OLTP:
```powershell
psql -U postgres -d olist_oltp

# Contar registros:
SELECT 'orders' as table, COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products;

\q
```

**Esperado:**
- orders: ~100,000
- order_items: ~112,000
- customers: ~96,000
- products: ~32,000

---

### Verificar DWH:
```powershell
psql -U postgres -d olist_dwh

# Verificar dimensiones:
SELECT 
    'dim_customers' as dimension, COUNT(*) as records FROM dim_customers
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

\q
```

**Esperado:**
- dim_customers: ~96,000
- dim_products: ~32,000
- dim_sellers: ~3,000
- dim_geolocation: ~19,000 (ciudades únicas)
- dim_date: 1,827 (2016-2020)
- fct_orders: ~112,650

---

### Verificar archivos generados:
```powershell
# Data Lake (Parquet):
ls data\staging\*.parquet

# Datos limpios:
ls data\processed\*.parquet

# Reportes de análisis:
ls data\analysis\*.xlsx
```

---

## 🎓 Análisis de Negocio Disponibles

Después de ejecutar el pipeline, encontrarás en `data/analysis/`:

### **business_summary_YYYYMMDD_HHMMSS.xlsx**
Con las siguientes hojas:

1. **Top_Products_By_Month**
   - Productos más vendidos cada mes
   - Volumen de ventas y ingresos

2. **Valuable_Customers_By_State**
   - Estados con clientes que más gastan
   - Total de órdenes y monto promedio

3. **Delivery_Time_By_Region**
   - Tiempo promedio de entrega por región
   - Porcentaje de entregas a tiempo

4. **Repeat_Customers**
   - Comportamiento de clientes recurrentes
   - Tasa de retención

5. **Category_Profitability**
   - Categorías más rentables
   - Análisis de márgenes

---

## Troubleshooting

### Error: "No se puede conectar a PostgreSQL"
```powershell
# Verificar que PostgreSQL está corriendo:
Get-Service -Name postgresql*

# Si no está corriendo, iniciarlo:
Start-Service postgresql-x64-XX  # XX = versión
```

### Error: "Módulo no encontrado"
```powershell
# Verificar que el entorno virtual está activado:
# El prompt debe mostrar (venv)

# Si no está activado:
.\venv\Scripts\Activate.ps1

# Reinstalar dependencias:
pip install -r requirements.txt
```

### Error: "Archivo .env no encontrado"
```powershell
# Crear desde plantilla:
Copy-Item .env.example .env

# Editar:
notepad .env
```

### Error: "Permiso denegado al ejecutar scripts"
```powershell
# Permitir ejecución de scripts:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## Soporte

Para más información, consulta:
- `README.md` - Documentación completa
- `SETUP.md` - Guía de instalación detallada
- `MODELO_DATOS.md` - Especificación del modelo de datos
- `CHECKLIST.md` - Lista de verificación completa

---

## ¡Éxito!

Si ves este mensaje al final de la ejecución:
```
Pipeline completado exitosamente
Reportes generados en: data/analysis/
Tiempo total: XX minutos
```

**¡El Data Warehouse está listo para usar!** 

Puedes conectar herramientas de visualización como:
- Power BI
- Tableau
- Looker Studio
- Jupyter Notebooks
- Qlik

---