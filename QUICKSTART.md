# Inicio Rápido - 5 Minutos

## Opción 1: Script Automatizado (Recomendado)

```powershell
# Ejecutar el script de inicio rápido
.\quick_start.ps1
```

Este script te guiará paso a paso por la configuración e instalación.

---

## Opción 2: Manual Paso a Paso

### Instalar PostgreSQL

Descargar e instalar desde: https://www.postgresql.org/download/windows/

Durante la instalación, anotar:
- Usuario: `postgres`
- Contraseña: (tu contraseña)
- Puerto: `5432`

### Crear Bases de Datos

```powershell
# Abrir PowerShell y ejecutar
psql -U postgres

# En la consola de PostgreSQL:
CREATE DATABASE olist_oltp;
CREATE DATABASE olist_dwh;
\q
```

### Configurar Python

```powershell
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
.\venv\Scripts\Activate.ps1

# Si hay error de permisos:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Instalar dependencias
pip install -r requirements.txt
```

### Configurar Variables de Entorno

```powershell
# Copiar archivo de ejemplo
Copy-Item .env.example .env

# Editar .env con tu editor favorito
notepad .env
```

**Cambiar estas líneas:**
```
POSTGRES_PASSWORD=TU_CONTRASEÑA_AQUI
```

### Ejecutar Pipeline

```powershell
# Ejecutar pipeline completo
python run_pipeline.py
```

**Tiempo estimado:** 20-35 minutos

---

## Verificar Resultados

### Opción A: SQL

```powershell
# Conectar a la base de datos DWH
psql -U postgres -d olist_dwh

# Ver tablas creadas
\dt

# Ver conteos
SELECT COUNT(*) FROM fct_orders;

# Salir
\q
```

### Opción B: Python

```powershell
python config/db_config.py
```

### Opción C: Archivos

```powershell
# Ver archivos generados
ls data/raw/          # CSVs originales
ls data/staging/      # Archivos Parquet
ls data/analysis/     # Reportes Excel
ls logs/              # Logs de ejecución
```

---

## Próximos Pasos

### 1. Explorar los Datos

```powershell
# Abrir Jupyter Notebook
jupyter notebook notebooks/exploratory_analysis.ipynb
```

### 2. Ver Reportes

Los reportes se generan automáticamente en `data/analysis/`:
- `00_RESUMEN_COMPLETO.xlsx` - Reporte completo con todas las hojas
- `01_products_by_month.xlsx` - Productos por mes
- `02_customers_by_state.xlsx` - Clientes por estado
- `03_delivery_time_by_region.xlsx` - Tiempos de entrega
- `04_kpis.xlsx` - KPIs principales
- `05_repeat_customers.xlsx` - Análisis de clientes recurrentes

### 3. Conectar Herramienta de BI

#### Power BI:
1. Abrir Power BI Desktop
2. Obtener Datos → PostgreSQL
3. Servidor: `localhost`
4. Base de datos: `olist_dwh`
5. Seleccionar tablas: `fct_orders`, `dim_*`

#### Tableau:
1. Conectar → PostgreSQL
2. Servidor: `localhost`
3. Base de datos: `olist_dwh`

---

## Solución de Problemas Rápida

### Error: "Connection refused"
```powershell
# Verificar que PostgreSQL esté corriendo
Get-Service postgresql*

# Si está detenido, iniciarlo
Start-Service postgresql-x64-14
```

### Error: "Permission denied" al activar venv
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Error: "psycopg2 not found"
```powershell
pip install psycopg2-binary
```

### Más ayuda
Ver `SETUP.md` para guía completa de solución de problemas.

---

## Documentación

- `README.md` - Descripción general del proyecto
- `SETUP.md` - Guía completa de instalación
- `MODELO_DATOS.md` - Especificación del modelo de datos
- `RESUMEN_EJECUTIVO.md` - Resumen ejecutivo del proyecto
- `CHECKLIST.md` - Checklist de verificación

---

## Comandos Útiles

```powershell
# Ejecutar solo extracción
python scripts/01_extract/load_csv_to_oltp.py

# Ejecutar solo transformación
python scripts/03_transform/data_cleaning.py
python scripts/03_transform/create_dimensions.py
python scripts/03_transform/create_fact_table.py

# Ejecutar solo análisis
python scripts/05_analysis/business_queries.py

# Verificar conexión
python config/db_config.py
```

---

## Checklist Rápido

- [ ] PostgreSQL instalado y corriendo
- [ ] Bases de datos creadas (`olist_oltp`, `olist_dwh`)
- [ ] Python 3.9+ instalado
- [ ] Entorno virtual creado y activado
- [ ] Dependencias instaladas
- [ ] Archivo `.env` configurado
- [ ] Pipeline ejecutado exitosamente
- [ ] Reportes generados en `data/analysis/`

---
