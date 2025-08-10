# Nueva Estructura de Datos - Data Lake

## 📁 Estructura de Carpetas

### **Raw Data (Datos en Bruto)**
```
raw/
├── expense_1.json
├── expense_2.json
├── expense_3.json
└── ...
```
- **Propósito**: Almacenar datos extraídos directamente de la API de Fudo
- **Formato**: JSON individual por expense
- **Fuente**: `expense_extractor.py` → `run_extraction.py`

### **Clean Data (Datos Procesados)**
```
clean/
├── fact_expenses/
│   ├── date=2019-10-27/
│   │   └── fact_expenses.csv
│   ├── date=2019-11-06/
│   │   └── fact_expenses.csv
│   └── date=YYYY-MM-DD/
│       └── fact_expenses.csv
└── fact_expense_orders/
    ├── date=2019-10-27/
    │   └── fact_expense_orders.csv
    ├── date=2019-11-06/
    │   └── fact_expense_orders.csv
    └── date=YYYY-MM-DD/
        └── fact_expense_orders.csv
```

## 📊 Tablas de Datos

### **fact_expenses.csv**
- **Ubicación**: `clean/fact_expenses/date=YYYY-MM-DD/`
- **Contenido**: Datos principales de expenses
- **Particionado**: Por fecha de creación (created_at)
- **Formato**: CSV con headers

### **fact_expense_orders.csv**
- **Ubicación**: `clean/fact_expense_orders/date=YYYY-MM-DD/`
- **Contenido**: Items/productos dentro de cada expense
- **Particionado**: Por fecha de creación del expense padre
- **Formato**: CSV con headers

## 🔄 Flujo de Datos

1. **Extracción**: API Fudo → `raw/expense_X.json`
2. **Procesamiento**: `raw/*.json` → `clean/fact_*/date=YYYY-MM-DD/*.csv`

## 🚀 Comandos de Uso

### Extraer datos
```bash
python run_extraction.py <start_id> <end_id>
# Ejemplo: python run_extraction.py 1 100
```

### Procesar datos
```bash
python run_processing.py <start_id> <end_id>
# Ejemplo: python run_processing.py 1 100
```

## 📈 Ventajas de la Nueva Estructura

1. **Separación Clara**: Raw vs Clean data
2. **Escalabilidad**: Particionado por fecha para consultas eficientes
3. **Tablas Fact**: Separación lógica entre expenses y expense orders
4. **Estándar Data Lake**: Estructura tipo Hive con `date=YYYY-MM-DD`
5. **Flexibilidad**: Fácil integración con herramientas de Big Data

## ⚠️ Migración Completada

- ✅ `extraction_data/` → `raw/`
- ✅ `processed_data/` → `clean/`
- ✅ Archivos CSV separados en `fact_expenses/` y `fact_expense_orders/`
- ✅ Mantenimiento de estructura de particionado por fecha
