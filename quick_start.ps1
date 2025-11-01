# Script de Inicio Rápido - Data Warehouse Olist
# Quick Start Script

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Data Warehouse - Olist E-commerce" -ForegroundColor Cyan
Write-Host "Script de Inicio Rápido" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Verificar si Python está instalado
Write-Host "[1/6] Verificando Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  $pythonVersion encontrado" -ForegroundColor Green
} catch {
    Write-Host "  Python no encontrado. Por favor instala Python 3.9+" -ForegroundColor Red
    exit 1
}

# Verificar si PostgreSQL está instalado
Write-Host "[2/6] Verificando PostgreSQL..." -ForegroundColor Yellow
try {
    $psqlVersion = psql --version 2>&1
    Write-Host "  PostgreSQL encontrado" -ForegroundColor Green
} catch {
    Write-Host "  PostgreSQL no encontrado. Por favor instala PostgreSQL" -ForegroundColor Red
    exit 1
}

# Verificar/crear entorno virtual
Write-Host "[3/6] Configurando entorno virtual..." -ForegroundColor Yellow
if (Test-Path "venv") {
    Write-Host "  Entorno virtual ya existe" -ForegroundColor Green
} else {
    Write-Host "  Creando entorno virtual..." -ForegroundColor Yellow
    python -m venv venv
    Write-Host "  Entorno virtual creado" -ForegroundColor Green
}

# Activar entorno virtual
Write-Host "  Activando entorno virtual..." -ForegroundColor Yellow
& .\venv\Scripts\Activate.ps1

# Instalar/actualizar dependencias
Write-Host "[4/6] Instalando dependencias..." -ForegroundColor Yellow
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
Write-Host "  Dependencias instaladas" -ForegroundColor Green

# Verificar archivo .env
Write-Host "[5/6] Verificando configuración..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Write-Host "  Archivo .env encontrado" -ForegroundColor Green
} else {
    Write-Host "  Archivo .env no encontrado" -ForegroundColor Yellow
    Write-Host "  Copiando desde .env.example..." -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host "  Archivo .env creado" -ForegroundColor Green
    Write-Host ""
    Write-Host "  IMPORTANTE: Edita el archivo .env con tus credenciales de PostgreSQL" -ForegroundColor Red
    Write-Host "  Archivo ubicado en: .\.env" -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "  ¿Has configurado el archivo .env? (s/n)"
    if ($continue -ne "s") {
        Write-Host "  Por favor configura .env y vuelve a ejecutar este script" -ForegroundColor Yellow
        exit 0
    }
}

# Crear directorios necesarios
Write-Host "[6/6] Creando directorios..." -ForegroundColor Yellow
$directories = @("logs", "data\staging", "data\processed", "data\analysis")
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}
Write-Host "  Directorios creados" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "Configuración completada exitosamente!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""

# Menú de opciones
Write-Host "¿Qué deseas hacer?" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Ejecutar pipeline completo (recomendado)" -ForegroundColor White
Write-Host "2. Ejecutar solo extracción (CSVs → PostgreSQL)" -ForegroundColor White
Write-Host "3. Ejecutar solo transformación (crear DWH)" -ForegroundColor White
Write-Host "4. Ejecutar solo análisis" -ForegroundColor White
Write-Host "5. Verificar conexión a base de datos" -ForegroundColor White
Write-Host "6. Ver estructura del proyecto" -ForegroundColor White
Write-Host "7. Abrir documentación" -ForegroundColor White
Write-Host "0. Salir" -ForegroundColor White
Write-Host ""

$opcion = Read-Host "Selecciona una opción"

switch ($opcion) {
    "1" {
        Write-Host ""
        Write-Host "Ejecutando pipeline completo..." -ForegroundColor Cyan
        Write-Host "Esto puede tomar 20-35 minutos..." -ForegroundColor Yellow
        Write-Host ""
        python run_pipeline.py
    }
    "2" {
        Write-Host ""
        Write-Host "Ejecutando extracción..." -ForegroundColor Cyan
        python scripts\01_extract\load_csv_to_oltp.py
    }
    "3" {
        Write-Host ""
        Write-Host "Ejecutando transformación..." -ForegroundColor Cyan
        python scripts\03_transform\data_cleaning.py
        python scripts\03_transform\create_dimensions.py
        python scripts\03_transform\create_fact_table.py
    }
    "4" {
        Write-Host ""
        Write-Host "Ejecutando análisis..." -ForegroundColor Cyan
        python scripts\05_analysis\business_queries.py
    }
    "5" {
        Write-Host ""
        Write-Host "Verificando conexión..." -ForegroundColor Cyan
        python config\db_config.py
    }
    "6" {
        Write-Host ""
        Write-Host "Estructura del proyecto:" -ForegroundColor Cyan
        tree /F
    }
    "7" {
        Write-Host ""
        Write-Host "Abriendo documentación..." -ForegroundColor Cyan
        Start-Process "README.md"
        Start-Process "SETUP.md"
        Start-Process "MODELO_DATOS.md"
    }
    "0" {
        Write-Host ""
        Write-Host "¡Hasta luego!" -ForegroundColor Cyan
        exit 0
    }
    default {
        Write-Host ""
        Write-Host "Opción no válida" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Presiona cualquier tecla para continuar..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
