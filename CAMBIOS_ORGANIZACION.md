# ✅ Cambios de Organización de Datos

## Fecha: 31 de Octubre de 2025

## 📋 Resumen

Se reorganizó la estructura de archivos CSV del proyecto para seguir las mejores prácticas de ingeniería de datos.

## 🔄 Cambios Realizados

### 1. Estructura de Directorios
**ANTES:**
```
Análisis de E-commerce/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
├── product_category_name_translation.csv
├── scripts/
└── ...
```

**DESPUÉS:**
```
Análisis de E-commerce/
├── data/
│   └── raw/
│       ├── olist_customers_dataset.csv
│       ├── olist_orders_dataset.csv
│       ├── olist_order_items_dataset.csv
│       ├── olist_order_payments_dataset.csv
│       ├── olist_order_reviews_dataset.csv
│       ├── olist_products_dataset.csv
│       ├── olist_sellers_dataset.csv
│       ├── olist_geolocation_dataset.csv
│       └── product_category_name_translation.csv
├── scripts/
└── ...
```

### 2. Archivos Actualizados

#### ✅ scripts/01_extract/load_csv_to_oltp.py
```python
# ANTES
def __init__(self, data_path: str = "."):

# DESPUÉS
def __init__(self, data_path: str = "data/raw"):
```

#### ✅ config/config.yaml
```yaml
# ANTES
paths:
  raw_data: "./data/raw"

# DESPUÉS
paths:
  raw_data: "data/raw"
```

#### ✅ .gitignore
Agregado:
```gitignore
# Archivos de datos pero mantén el directorio
data/raw/*
!data/raw/.gitkeep
```

#### ✅ Documentación
- README.md → Agregada sección `data/raw/` en estructura
- QUICKSTART.md → Agregada verificación de CSV en data/raw/
- SETUP.md → Agregado paso de verificación de CSV
- CHECKLIST.md → Actualizado requisito de ubicación de archivos

### 3. Archivos Movidos

Se movieron **9 archivos CSV** (126.18 MB total):

| Archivo | Tamaño | Registros Aprox. |
|---------|--------|------------------|
| olist_customers_dataset.csv | 9 MB | ~100,000 |
| olist_geolocation_dataset.csv | 61 MB | ~1,000,000 |
| olist_orders_dataset.csv | 17 MB | ~100,000 |
| olist_order_items_dataset.csv | 15 MB | ~112,000 |
| olist_order_payments_dataset.csv | 5.7 MB | ~103,000 |
| olist_order_reviews_dataset.csv | 14 MB | ~100,000 |
| olist_products_dataset.csv | 2.3 MB | ~32,000 |
| olist_sellers_dataset.csv | 170 KB | ~3,000 |
| product_category_name_translation.csv | 2.6 KB | 71 |

## ✅ Verificación

Para confirmar que todo está correcto:

```powershell
# Ver archivos en data/raw/
ls data\raw\*.csv

# Debería mostrar 9 archivos CSV
```

**Resultado esperado:** 9 archivos listados

## 📝 Beneficios

1. **Organización clara:** Datos separados del código
2. **Escalabilidad:** Fácil agregar más datasets
3. **Versionamiento:** Git puede ignorar archivos grandes pero mantener estructura
4. **Convención estándar:** Sigue estructura típica de proyectos de Data Science/Engineering:
   ```
   data/
   ├── raw/       # Datos originales (sin modificar)
   ├── staging/   # Datos intermedios (Parquet)
   ├── processed/ # Datos limpios
   └── analysis/  # Reportes generados
   ```

## 🚀 Próximos Pasos

1. Verificar que los CSV están en `data/raw/`
2. Configurar PostgreSQL
3. Ejecutar el pipeline: `python run_pipeline.py`
4. Los datos fluirán automáticamente desde `data/raw/` → OLTP → Staging → DWH

---

**Nota:** No se requieren acciones adicionales. El pipeline ahora usará automáticamente `data/raw/` como ubicación predeterminada para los archivos CSV.
