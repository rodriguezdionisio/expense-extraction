# 📊 Expense Extraction & Processing System

Sistema automatizado de extracción y procesamiento de gastos desde la API de Fudo con transformación a Parquet estructurado, orquestación completa y sincronización en Google Cloud Storage.

## Descripción

Este proyecto extrae datos de gastos (expenses) desde la API REST de Fudo, los procesa y los convierte en archivos Parquet particionados por fecha para análisis de datos. El sistema incluye un orquestador automatizado (`main.py`) que controla todo el pipeline, sistema de logging para prevenir duplicados, y estructura de datos tipo data warehouse con formato Parquet de alto rendimiento.

## Características Principales

- **📊 Extracción Inteligente**: Sistema con logging para prevenir duplicados automáticamente
- **🏗️ Arquitectura Simplificada**: Código optimizado y limpio
- **📁 Estructura Data Warehouse**: Tablas fact separadas (`fact_expenses`, `fact_expense_orders`)
- **🗂️ Particionado Hive-Style**: Estructura `clean/fact_*/date=YYYY-MM-DD/`
- **🚀 Formato Parquet Optimizado**: Alto rendimiento con compresión y tipado
- **🔐 Autenticación Segura**: Integración con Google Cloud Secret Manager
- **🤖 Orquestador Automatizado**: `main.py` controla todo el pipeline
- **🌩️ Sincronización GCS**: Almacenamiento automático en la nube
- **🏗️ Arquitectura Simplificada**: Código optimizado y limpio
- **📁 Estructura Data Warehouse**: Tablas fact separadas (`fact_expenses`, `fact_expense_orders`)
- **🗂️ Particionado Hive-Style**: Estructura `clean/fact_*/date=YYYY-MM-DD/`
- **🚀 Formato Parquet Optimizado**: Alto rendimiento con compresión y tipado
- **🔐 Autenticación Segura**: Integración con Google Cloud Secret Manager
- **🤖 Orquestador Automatizado**: `main.py` controla todo el pipeline
- **🌩️ Sincronización GCS**: Almacenamiento automático en la nubeión Inteligente**: Sistema con logging para prevenir duplicados automáticamente
- **🏗️ Arquitectura Simplificada**: Código optimizado reducido en 28% manteniendo funcionalidad completa
- **📁 Estructura Data Warehouse**: Tablas fact separadas (`fact_expenses`, `fact_expense_orders`)
- **🗂️ Particionado Hive-Style**: Estructura `clean/fact_*/date=YYYY-MM-DD/`
- **🚀 Formato Parquet**: Archivos de alto rendimiento con compresión y tipado optimizado
- **🔐 Autenticación Segura**: Integración con Google Cloud Secret Manager
- **📝 Sistema de Logging**: Prevención automática de extracciones duplicadas
- **🛠️ Scripts CLI Simplificados**: Interfaces de línea de comandos fáciles de usarion & Processing System

Sistema optimizado de extracción y procesamiento de gastos desde la API de Fudo con transformación a Parquet estructurado y prevención de duplicados.

## Descripción

Este proyecto extrae datos de gastos (expenses) desde la API REST de Fudo, los procesa y los convierte en archivos Parquet particionados por fecha para análisis de datos. El sistema ha sido completamente optimizado con arquitectura simplificada, sistema de logging para prevenir duplicados, y estructura de datos tipo data warehouse con formato Parquet de alto rendimiento.

## Características Principales

- **� Extracción Inteligente**: Sistema con logging para prevenir duplicados automáticamente
- **🏗️ Arquitectura Simplificada**: Código optimizado reducido en 28% manteniendo funcionalidad completa
- **📁 Estructura Data Warehouse**: Tablas fact separadas (`fact_expenses`, `fact_expense_orders`)
- **�️ Particionado Hive-Style**: Estructura `clean/fact_*/date=YYYY-MM-DD/`
- **🔐 Autenticación Segura**: Integración con Google Cloud Secret Manager
- **� Sistema de Logging**: Prevención automática de extracciones duplicadas
- **🚀 Scripts CLI Simplificados**: Interfaces de línea de comandos fáciles de usar

## Estructura del Proyecto

```
expense-extraction/
├── config/
│   └── credentials.json          # Credenciales de Google Cloud
├── utils/                        # Módulos de utilidades optimizados
│   ├── __init__.py
│   ├── env_config.py            # Configuración de variables de entorno
│   ├── gcp.py                   # Utilidades de Google Cloud
│   └── logger.py                # Sistema de logging
├── raw/                         # Archivos JSON individuales extraídos
│   ├── expense_1.json
│   ├── expense_2.json
│   └── ...
├── clean/                       # Datos procesados en estructura data warehouse
│   ├── fact_expenses/           # Tabla principal de gastos
│   │   ├── date=2019-10-27/
│   │   │   └── fact_expenses.parquet
│   │   ├── date=2020-01-04/
│   │   │   └── fact_expenses.parquet
│   │   └── ...
│   └── fact_expense_orders/     # Tabla de órdenes/items de gastos
│       ├── date=2019-10-27/
│       │   └── fact_expense_orders.parquet
│       ├── date=2020-01-04/
│       │   └── fact_expense_orders.parquet
│       └── ...
├── logs/                        # Sistema de logging para duplicados
│   └── extracted_expenses_log.txt
├── main.py                     # 🎯 ORQUESTADOR PRINCIPAL
├── expense_extractor.py         # Sistema de extracción optimizado
├── expense_processor.py         # Sistema de procesamiento optimizado
├── system_summary.py           # Resumen del sistema
├── verify_expense_gcs.py       # Verificación de GCS
├── requirements.txt            # Dependencias
└── README.md                   # Este archivo
```

## Arquitectura del Sistema

```
📁 expense-extraction/
├── main.py                     # 🎯 Orquestador principal (USAR ESTE)
├── expense_extractor.py        # Extracción de datos desde API
├── expense_processor.py        # Procesamiento y conversión Parquet
├── system_summary.py           # Resumen del estado del sistema
├── config/
│   └── credentials.json        # Credenciales GCP
├── utils/
│   ├── env_config.py          # Configuración de environment
│   ├── gcp.py                 # Utilidades Google Cloud Storage
│   └── logger.py              # Sistema de logging unificado
└── requirements.txt           # Dependencias Python
```

**Archivos principales optimizados**:
- **main.py** (311 líneas): Interfaz CLI unificada
- **expense_extractor.py** (278 líneas): Extracción desde Fudo API  
- **expense_processor.py** (296 líneas): Conversión a formato Parquet
- **utils/gcp.py** (121 líneas): Operaciones Google Cloud Storage

## Instalación

1. **Clonar el repositorio**:
```bash
git clone <repository-url>
cd expense-extraction
```

2. **Crear entorno virtual**:
```bash
python -m venv .venv
source .venv/bin/activate  # En Linux/Mac
# .venv\Scripts\activate  # En Windows
```

3. **Instalar dependencias**:
```bash
pip install -r requirements.txt
```

4. **Configurar variables de entorno**:
```bash
cp .env.example .env
# Editar .env con tus valores
```

5. **Configurar credenciales de Google Cloud**:
   - Crear proyecto en Google Cloud
   - Habilitar Secret Manager API
   - Crear service account y descargar credenciales
   - Colocar credenciales en `config/credentials.json`

## Configuración

### Variables de Entorno (.env)

```env
# Google Cloud
GCP_PROJECT_ID=tu-proyecto-gcp-id
GCP_PROJECT_NAME=tu-proyecto-gcp
GCS_BUCKET_NAME=tu-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=config/credentials.json

# Configuración para ambiente
ENV=local
```

### Secretos en Google Cloud Secret Manager

El sistema requiere los siguientes secretos configurados en Secret Manager:
- **`fudo-api-key`**: Tu API key de Fudo
- **`fudo-api-secret`**: Tu API secret de Fudo

## Uso del Sistema

### 🎯 Orquestador Principal (Recomendado)

El nuevo archivo `main.py` automatiza todo el flujo de extracción, procesamiento y almacenamiento:

#### Procesamiento Automático del Próximo Lote
```bash
# Procesa automáticamente los próximos 10 IDs desde donde se quedó
python main.py auto

# Procesa automáticamente lote de tamaño personalizado
python main.py auto --batch-size 20
```

#### Procesamiento de Rango Específico
```bash
# Pipeline completo: extracción + procesamiento + almacenamiento
python main.py range 1 20

# Un solo expense
python main.py range 25 25
```

#### Procesamiento Continuo Automatizado
```bash
# Procesamiento continuo con lotes de 10, pausa de 60 segundos
python main.py continuous

# Procesamiento continuo personalizado
python main.py continuous --batch-size 20 --delay 30 --max-batches 5
```

#### Operaciones Individuales
```bash
# Solo extracción (sin procesamiento)
python main.py extract 1 20

# Solo procesamiento (sin extracción)
python main.py process 1 20
```

## Estructura de Archivos de Salida

### Estructura Data Warehouse

El sistema genera dos tablas fact separadas organizadas por fechas:

```
clean/
├── fact_expenses/              # Tabla principal de gastos
│   ├── date=2019-10-27/
│   │   └── fact_expenses.csv
│   ├── date=2020-01-04/
│   │   └── fact_expenses.csv
│   └── ...
└── fact_expense_orders/        # Tabla de órdenes/items
    ├── date=2019-10-27/
    │   └── fact_expense_orders.parquet
    ├── date=2020-01-04/
    │   └── fact_expense_orders.parquet
    └── ...
```

### Archivos Parquet Generados

#### `fact_expenses.parquet` - Datos principales de gastos
Columnas optimizadas con tipos de datos eficientes:
- `expense_key` (int64): Clave primaria del gasto
- `expense_amount` (float64): Monto del gasto
- `cancelled` (bool): Estado de cancelación
- `expense_date_key` (int64): Fecha del gasto (YYYYMMDD)
- `created_date_key`, `created_time_key` (int64): Fechas y hora de creación
- Claves foráneas: `cashregister_key`, `paymentmethod_key`, `provider_key`, etc.

#### `fact_expense_orders.parquet` - Líneas de detalle (expense items)
Columnas optimizadas para análisis de items:
- `expense_order_key` (int64): Clave primaria del item
- `expense_key` (int64): Clave foránea al expense principal
- `item_detail` (string): Descripción del item
- `item_price`, `item_quantity` (float64): Precio y cantidad
- Datos de productos e ingredientes con prefijos optimizados

## Transformaciones Aplicadas

### Renombrado de Columnas
- `expense_id` → `expense_key` (int64)
- `amount` → `expense_amount` (float64)
- `canceled` → `cancelled` (bool)
- `date` → `expense_date_key` (int64, formato YYYYMMDD)
- `created_at` → `created_date_key`, `created_time_key` (int64)
- `description` → `expense_note` (string)
- IDs de relaciones → `*_key` (int64)

### Transformación de Expense Items
- `expense_item_id` → `expense_order_key` (int64)
- `expense_id` → `expense_key` (int64, clave foránea)
- `detail` → `item_detail` (string)
- `price` → `item_price` (float64)
- `quantity` → `item_quantity` (float64)
- Datos de productos e ingredientes incluidos con prefijos

### Sistema de Logging
- **Prevención de Duplicados**: `logs/extracted_expenses_log.txt`
- **Verificación Automática**: El sistema verifica IDs ya extraídos
- **Inicialización Inteligente**: Detecta archivos existentes al inicio

### Particionado por Fecha

Los archivos se organizan en estructura Hive-style por tablas fact:
```
clean/
├── fact_expenses/
│   ├── date=2019-10-27/
│   │   └── fact_expenses.parquet      # Expenses de esa fecha
│   ├── date=2020-01-04/
│   │   └── fact_expenses.parquet
│   └── ...
└── fact_expense_orders/
    ├── date=2019-10-27/
    │   └── fact_expense_orders.parquet # Items de esa fecha
    ├── date=2020-01-04/
    │   └── fact_expense_orders.parquet
    └── ...
```

### Archivos Parquet Generados

#### `fact_expenses.parquet` - Datos principales de gastos
Columnas optimizadas con tipos de datos eficientes:
- `expense_key` (int64): Clave primaria del gasto
- `expense_amount` (float64): Monto del gasto
- `cancelled` (bool): Estado de cancelación
- `expense_date_key` (int64): Fecha del gasto (YYYYMMDD)
- `payment_date_key`, `due_date_key`, `created_date_key` (int64): Fechas relacionadas
- `created_time_key` (int64): Hora de creación (HHMM)
- Claves foráneas: `cashregister_key`, `paymentmethod_key`, `provider_key`, etc.

#### `fact_expense_orders.parquet` - Líneas de detalle (expense items)
Columnas optimizadas para análisis de items:
- `expense_order_key` (int64): Clave primaria del item
- `expense_key` (int64): Clave foránea al expense principal
- `item_detail` (string): Descripción del item
- `item_price`, `item_quantity` (float64): Precio y cantidad
- Datos de productos e ingredientes con prefijos optimizados

## API Reference

### ExpenseExtractor (Optimizado)
```python
from expense_extractor import ExpenseExtractor

extractor = ExpenseExtractor()

# Inicializar sistema de logging
extractor.initialize_log_from_existing_files()

# Extraer rango con prevención de duplicados
expenses, count = extractor.extract_range(1, 20)
```

### ExpenseProcessor (Optimizado)
```python
from expense_processor import ExpenseProcessor

processor = ExpenseProcessor()

# Procesar rango específico
processor.process_range(1, 20)
```

## Optimizaciones del Sistema

- **🏗️ Arquitectura Simplificada**: Eliminación de scripts redundantes y código duplicado
- **🎯 Orquestador Centralizado**: `main.py` controla todo el pipeline automatizado
- **📋 Sistema de Logging**: Prevención automática de duplicados con seguimiento de IDs
- **🔧 Validación Unificada**: Funciones reutilizables para validación de rangos
- **📚 Documentación Actualizada**: README coherente con estado actual
- **🚀 Formato Parquet Optimizado**: 
  - Compresión automática para menor uso de espacio
  - Tipado de datos optimizado para consultas rápidas
  - Compatible con herramientas de análisis modernas (Pandas, Spark, etc.)
  - Metadatos incluidos para mejor rendimiento
  - Sincronización automática con Google Cloud Storage

## 🎯 Orquestador Principal (main.py)

El orquestador `main.py` es la **interfaz principal** del sistema. Automatiza completamente el flujo de extracción, procesamiento y almacenamiento, reemplazando los scripts manuales anteriores.

### Características del Orquestador:

- **🤖 Automatización Completa**: Un solo comando ejecuta todo el pipeline
- **📊 Detección Inteligente**: Identifica automáticamente el próximo lote a procesar
- **🔄 Procesamiento Continuo**: Soporte para ejecución tipo daemon
- **⚠️ Manejo de Errores**: Control robusto de errores y logging detallado
- **🎛️ Flexibilidad**: Múltiples modos de operación según necesidades

### Comandos Principales:

```bash
# 🚀 RECOMENDADO: Procesamiento automático
python main.py auto                    # Próximos 10 IDs automáticamente
python main.py auto --batch-size 20    # Lote personalizado

# 📊 Rango específico  
python main.py range 1 50              # Pipeline completo para IDs 1-50

# 🔄 Procesamiento continuo (producción)
python main.py continuous              # Lotes de 10, pausa 60 segundos
python main.py continuous --batch-size 15 --delay 30 --max-batches 5

# ⚙️ Operaciones individuales
python main.py extract 1 100           # Solo extracción
python main.py process 1 100           # Solo procesamiento
```

### Casos de Uso del Orquestador:

1. **Procesamiento Inicial Masivo**:
   ```bash
   python main.py range 1 1000
   ```

2. **Mantenimiento Diario Automatizado**:
   ```bash
   python main.py auto --batch-size 50
   ```

3. **Procesamiento Continuo (Servidor)**:
   ```bash
   python main.py continuous --batch-size 20 --delay 300
   ```

4. **Testing y Desarrollo**:
   ```bash
   python main.py continuous --batch-size 3 --delay 10 --max-batches 2
   ```

## Ventajas del Formato Parquet

- **🚀 Rendimiento**: Consultas hasta 10x más rápidas que CSV
- **💾 Compresión**: Archivos 50-80% más pequeños
- **🎯 Tipado**: Preservación de tipos de datos (int64, float64, bool, string)
- **📊 Compatibilidad**: Soporte nativo en Pandas, Spark, PowerBI, Tableau
- **🔍 Filtrado**: Pushdown predicates para consultas eficientes
- **📈 Columnar**: Almacenamiento optimizado para análisis

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.
