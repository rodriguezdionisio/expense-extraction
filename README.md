# Expense Extraction & Processing System

Sistema completo de extracción y procesamiento de gastos desde la API de Fudo con transformación a CSV estructurado.

## Descripción

Este proyecto extrae datos de gastos (expenses) desde la API REST de Fudo, los procesa y los convierte en archivos CSV particionados por fecha para análisis de datos. El sistema incluye extracción dual-mode (inicial vs mantenimiento), transformaciones de datos avanzadas, y organización Hive-style de archivos.

## Características Principales

- **🔄 Extracción Dual-Mode**: Modo inicial (desde ID 1) y mantenimiento (incremental)
- **📊 Procesamiento CSV**: Conversión automática de JSON a CSV con particionado por fecha
- **🏗️ Transformación de Datos**: Renombrado de columnas, conversión de tipos, y manejo de zonas horarias
- **📁 Organización Hive-Style**: Estructura `processed_data/date=YYYY-MM-DD/`
- **🔐 Autenticación Segura**: Integración con Google Cloud Secret Manager
- **📈 Update Logic**: Append a archivos existentes en lugar de reemplazar
- **🌍 Timezone Support**: Conversión automática a zona horaria Argentina

## Estructura del Proyecto

```
expense-extraction/
├── config/
│   └── credentials.json          # Credenciales de Google Cloud
├── utils/
│   ├── __init__.py
│   ├── env_config.py            # Configuración de variables de entorno
│   ├── fudo.py                  # Cliente API de Fudo
│   ├── gcp.py                   # Utilidades de Google Cloud
│   └── logger.py                # Sistema de logging
├── extraction_data/             # Archivos JSON individuales por expense
│   ├── expense_1.json
│   ├── expense_2.json
│   └── ...
├── processed_data/              # Archivos CSV particionados por fecha
│   ├── date=2020-01-04/
│   │   ├── fact_expenses.csv
│   │   └── fact_expense_orders.csv
│   ├── date=2020-01-06/
│   │   ├── fact_expenses.csv
│   │   └── fact_expense_orders.csv
│   └── ...
├── expense_extractor.py         # Sistema de extracción de API
├── expense_processor.py         # Sistema de procesamiento CSV
├── run_extraction.py           # Utilidad CLI para extracción
├── run_processing.py           # Utilidad CLI para procesamiento
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
GOOGLE_CLOUD_PROJECT=tu-proyecto-gcp
GOOGLE_APPLICATION_CREDENTIALS=config/credentials.json

# Fudo API
FUDO_API_SECRET_NAME=fudo-api-key

# Configuración de extracción
EXPENSE_EXTRACTION_MODE=initial    # o 'maintenance'
EXPENSE_START_ID=1
```

## Uso del Sistema

### 1. Extracción de Datos

#### Modo Inicial (Carga completa)
```bash
# Configurar modo inicial en .env
EXPENSE_EXTRACTION_MODE=initial

# Ejecutar extracción desde ID 1
python run_extraction.py
```

#### Modo Mantenimiento (Incremental)
```bash
# Configurar modo mantenimiento en .env
EXPENSE_EXTRACTION_MODE=maintenance
EXPENSE_START_ID=500

# Ejecutar extracción incremental
python run_extraction.py
```

#### Extracción por Rango
```bash
# Extraer IDs específicos (ej: 1-20)
python run_extraction.py range 1 20
```

### 2. Procesamiento a CSV

#### Procesamiento Inicial
```bash
# Procesar todos los archivos JSON a CSV particionado
python run_processing.py
```

#### Procesamiento por Rango
```bash
# Procesar rango específico (ej: IDs 1-20)
python run_processing.py range 1 20
```

## Estructura de Archivos de Salida

### Archivos CSV Generados

#### `fact_expenses.csv` - Datos principales de gastos
```csv
expense_key,expense_amount,cancelled,expense_date_key,payment_date_key,due_date_key,created_date_key,created_time_key,expense_note,receipt_number,use_in_cash_count,cash_register_key,payment_method_key,provider_key,receipt_type_key,employee_key
1,1500.0,False,20200104,20200110,20200115,20200104,1430,Compra materiales,001-123,True,1,2,45,1,7
```

#### `fact_expense_orders.csv` - Líneas de detalle (expense items)
```csv
expense_order_key,expense_key,cancelled,item_detail,item_price,item_quantity,product_key,product_name,product_cost,product_unit,ingredient_key,ingredient_name,ingredient_cost,ingredient_unit
456,1,False,Harina 000,850.0,2.0,789,Harina 000 x 1kg,800.0,kg,101,Harina 000,800.0,kg
```

## Transformaciones Aplicadas

### Columnas Expenses
- `expense_id` → `expense_key` (int64)
- `amount` → `expense_amount` (float64)
- `canceled` → `cancelled` (bool)
- `date` → `expense_date_key` (int64, formato YYYYMMDD)
- `created_at` → `created_date_key`, `created_time_key` (int64, zona horaria Argentina)
- `description` → `expense_note` (string)
- IDs de relaciones → `*_key` (int64)

### Columnas Expense Items
- `expense_item_id` → `expense_order_key` (int64)
- `expense_id` → `expense_key` (int64, FK)
- `detail` → `item_detail` (string)
- `price` → `item_price` (float64)
- `quantity` → `item_quantity` (float64)
- Datos de productos e ingredientes incluidos

### Particionado por Fecha

Los archivos se organizan en estructura Hive-style:
```
processed_data/
├── date=2020-01-04/
│   ├── fact_expenses.csv      # Expenses de esa fecha
│   └── fact_expense_orders.csv # Items de esa fecha
├── date=2020-01-06/
│   ├── fact_expenses.csv
│   └── fact_expense_orders.csv
└── ...
```

## API Reference

### ExpenseExtractor
```python
from expense_extractor import ExpenseExtractor

extractor = ExpenseExtractor()

# Extraer expense individual con datos completos
expense_data = extractor.get_expense_by_id(123)

# Extraer rango y guardar archivos individuales
extractor.extract_expenses_range(1, 100)
```

### ExpenseProcessor
```python
from expense_processor import ExpenseProcessor

processor = ExpenseProcessor()

# Procesar todos los archivos JSON
expenses_df, expense_items_df, summary = processor.run_initial_processing()

# Procesar rango específico
expenses_df, expense_items_df, summary = processor.run_range_processing(1, 20)
```

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.
