# Expense - **📊 Extracción Inteligente**: Sistema con logging para prevenir duplicados automáticamente
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
├── expense_extractor.py         # Sistema de extracción optimizado
├── expense_processor.py         # Sistema de procesamiento optimizado
├── run_extraction.py           # Script CLI para extracción
├── run_processing.py           # Script CLI para procesamiento
├── requirements.txt            # Dependencias
└── README.md                   # Este archivo
```

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
GOOGLE_APPLICATION_CREDENTIALS=config/credentials.json

# Fudo API (almacenados en Google Cloud Secret Manager)
# fudo-api-key: Tu API key de Fudo
# fudo-api-secret: Tu API secret de Fudo

# Configuración de extracción
EXPENSE_EXTRACTION_MODE=maintenance
EXPENSE_START_ID=500
```

## Uso del Sistema

### 1. Extracción de Datos

El sistema optimizado incluye prevención automática de duplicados mediante sistema de logging.

#### Extracción por Rango
```bash
# Extraer IDs específicos (ej: primeros 20)
python run_extraction.py 1 20

# Extraer un solo ID
python run_extraction.py 25 25

# Extraer siguiente lote
python run_extraction.py 21 40
```

### 2. Procesamiento a CSV

#### Procesamiento por Rango (Recomendado)
```bash
# Procesar los mismos IDs extraídos (ej: 1-20)
python run_processing.py 1 20

# Procesar un solo expense
python run_processing.py 25 25

# Procesar lote completo
python run_processing.py 21 40
```

### 3. Flujo Completo Típico

```bash
# 1. Extraer datos desde API
python run_extraction.py 1 20

# 2. Procesar a tablas fact
python run_processing.py 1 20

# 3. Continuar con siguiente lote
python run_extraction.py 21 40
python run_processing.py 21 40
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
    │   └── fact_expense_orders.csv
    ├── date=2020-01-04/
    │   └── fact_expense_orders.csv
    └── ...
```

### Archivos CSV Generados

#### `fact_expenses.csv` - Datos principales de gastos
```csv
expense_key,expense_amount,cancelled,expense_date_key,payment_date_key,due_date_key,created_date_key,created_time_key,expense_note,receipt_number,use_in_cash_count,cashregister_key,paymentmethod_key,provider_key,receipttype_key,user_key
1,1500.0,False,20200104,20200110,20200115,20200104,1430,Compra materiales,001-123,True,1,2,45,1,7
```

#### `fact_expense_orders.csv` - Líneas de detalle (expense items)
```csv
expense_order_key,expense_key,cancelled,item_detail,item_price,item_quantity,product_key,product_name,product_cost,product_unit,ingredient_key,ingredient_name,ingredient_cost,ingredient_unit
456,1,False,Harina 000,850.0,2.0,789,Harina 000 x 1kg,800.0,kg,101,Harina 000,800.0,kg
```

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

- **28% Reducción de Código**: De 973 a 698 líneas manteniendo funcionalidad completa
- **Arquitectura Simplificada**: Eliminación de código duplicado y no utilizado
- **Sistema de Logging**: Prevención automática de duplicados
- **Scripts CLI Mejorados**: Interfaces más simples y directas
- **Estructura Data Warehouse**: Separación clara de tablas fact
- **Formato Parquet Optimizado**: 
  - Compresión automática para menor uso de espacio
  - Tipado de datos optimizado para consultas rápidas
  - Compatible con herramientas de análisis modernas (Pandas, Spark, etc.)
  - Metadatos incluidos para mejor rendimiento

## Ventajas del Formato Parquet

- **🚀 Rendimiento**: Consultas hasta 10x más rápidas que CSV
- **💾 Compresión**: Archivos 50-80% más pequeños
- **🎯 Tipado**: Preservación de tipos de datos (int64, float64, bool, string)
- **📊 Compatibilidad**: Soporte nativo en Pandas, Spark, PowerBI, Tableau
- **🔍 Filtrado**: Pushdown predicates para consultas eficientes
- **📈 Columnar**: Almacenamiento optimizado para análisis

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.
