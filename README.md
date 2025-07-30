# Fudo Data Extraction Pipeline

Pipeline para extraer y procesar datos de la API de Fudo, diseñado para almacenamiento en data lake con **particionado por fecha**.

## 🏗️ Arquitectura

```
Raw Data por Fecha (JSON) → Processing → Clean CSV Files
        ↓                     ↓              ↓
   expenses_by_date/    src/process_    processed_data/
                        expenses.py
```

## 📁 Estructura de Archivos

```
expense-extraction/
├── main.py                  # Orquestador principal
├── src/                     # Módulos principales
│   ├── extract_expenses.py  # Extractor de datos raw en JSON
│   └── process_expenses.py  # Procesador JSON → CSV
├── raw_data/                # Datos raw en JSON
│   ├── expenses/
│   ├── payment-methods/
│   └── cash-registers/
├── processed_data/          # Datos procesados en CSV
├── utils/                   # Utilidades
└── config/                  # Configuración
```

## 🚀 Uso Principal: Extracción por Fecha (recomendado)

### Extracción Completa
```bash
# Extraer todos los datos particionados por DÍA (máxima granularidad)
python main.py --mode extract-by-date --partition-by day

# Extraer todos los datos particionados por MES (balance óptimo)
python main.py --mode extract-by-date --partition-by month

# Extraer todos los datos particionados por AÑO (agregado)
python main.py --mode extract-by-date --partition-by year
```

### Extracción de Períodos Específicos
```bash
# Extraer datos de un período específico por DÍA
python main.py --mode extract-by-date --start-date 2025-01-01 --end-date 2025-12-31 --partition-by day

# Extraer solo los últimos 30 días
python main.py --mode extract-by-date --start-date 2025-07-01 --end-date 2025-07-30 --partition-by day

# Extraer datos anuales
python main.py --mode extract-by-date --start-date 2024-01-01 --end-date 2024-12-31 --partition-by year
```

### Procesamiento de Datos
```bash
# Procesar datos JSON extraídos a CSV
python main.py --mode process
```

### Ejecución Individual de Módulos
```bash
# Solo extracción (usar los módulos directamente)
python src/extract_expenses.py

# Solo procesamiento
python src/process_expenses.py
```

## 📊 Outputs

### Raw Data (JSON) - Particionado por Fecha
- `raw_data/expenses_by_date/expenses_day_YYYY-MM-DD_TIMESTAMP.json` - Archivos por día
- `raw_data/expenses_by_date/expenses_month_YYYY-MM_TIMESTAMP.json` - Archivos por mes  
- `raw_data/expenses_by_date/expenses_year_YYYY_TIMESTAMP.json` - Archivos por año
- `raw_data/expenses_by_date/extraction_metadata_PARTITION_TIMESTAMP.json` - Metadatos de extracción

### Processed Data (CSV)
- `processed_data/expenses_flattened_TIMESTAMP.csv` - Datos completos aplanados (archivo principal)
- `processed_data/expense_items_relationships_TIMESTAMP.csv` - Relaciones de items
- `processed_data/processing_report_expenses_TIMESTAMP.json` - Reporte de calidad

## 🎯 Ventajas de Extracción por Fecha

### ✅ Para Data Lake
- **Particionado natural**: Archivos organizados por fecha (año/mes/día)
- **Consultas eficientes**: Acceso directo a períodos específicos
- **Paralelización**: Procesamiento independiente por partición
- **Escalabilidad**: Ideal para volúmenes grandes de datos

### ✅ Para Análisis
- **Actualizaciones incrementales**: Solo extraer nuevos datos
- **Análisis temporal**: Comparación entre períodos
- **Menor transferencia**: Descargar solo el rango necesario
- **Recuperación granular**: Reextraer solo fechas específicas con errores

### 📈 Recomendaciones de Uso
- **Data Lake/S3**: Usar `--partition-by month` para balance óptimo
- **Análisis diario**: Usar `--partition-by day` para granularidad máxima  
- **Reportes anuales**: Usar `--partition-by year` para agregación
- **Actualizaciones**: Especificar `--start-date` y `--end-date` para períodos específicos

## 🔧 Configuración

### Variables de Entorno
```bash
GCP_PROJECT_ID=your_project_id
GOOGLE_APPLICATION_CREDENTIALS=config/credentials.json
```

### Secrets en GCP
- `fudo-api-key`: API Key de Fudo
- `fudo-api-secret`: API Secret de Fudo

## 📋 Campos Extraídos

### Gastos (Expenses)
- **Básicos**: ID, monto, fecha, descripción, estado cancelado
- **Timestamps**: fecha creación, fecha vencimiento
- **Relaciones**: 
  - Caja registradora (ID/tipo)
  - Método de pago (ID/tipo)
  - Categoría de gasto (ID/tipo)
  - Items de gasto (IDs/tipos/cantidad)

### Estructura de Datos
```json
{
  "id": "123",
  "type": "Expense",
  "attributes": {
    "amount": 1500.0,
    "date": "2025-01-15",
    "description": "Compra de materiales"
  },
  "relationships": {
    "paymentMethod": {"data": {"id": "1", "type": "PaymentMethod"}},
    "expenseItems": {"data": [{"id": "456", "type": "ExpenseItem"}]}
  }
}
```

## 🔄 Procesamiento

### Aplanado de Datos
- `attributes` → campos con prefijo `attr_`
- `relationships` → campos con prefijo `rel_`
- Listas → campos `_count`, `_ids`, `_types`

### Ejemplo de Transformación
```
JSON: {"attributes": {"amount": 1500}, "relationships": {"paymentMethod": {"data": {"id": "1"}}}}
CSV:  attr_amount=1500, rel_paymentMethod_id=1
```

## 📈 Análisis Incluidos

### Datos Generados
- **Flattened CSV**: Datos completos aplanados con todos los campos y relaciones
- **Relationships CSV**: Mapeo detallado de gastos → items de gastos
- **Calidad de datos**: Reporte JSON con estadísticas y validaciones

### Archivos Especializados
- **Flattened CSV**: Archivo principal con todos los datos normalizados
- **Relationships CSV**: Mapeo de gastos → items para análisis relacionales
- **Processing Report**: Métricas de calidad y estadísticas de procesamiento

## 🛠️ Mantenimiento

### Logs
Todos los procesos generan logs detallados usando el sistema de logging configurado.

### Recuperación de Errores
- Extracción por páginas permite reanudar desde cualquier punto
- Metadatos de extracción incluyen información de recuperación
- Procesamiento valida archivos JSON antes de procesar

### Monitoreo
- Reportes de calidad de datos en JSON
- Conteo de registros en cada fase
- Timestamps para auditoría

## 🔍 Troubleshooting

### Error de Autenticación
```bash
# Verificar secrets
python -c "from utils.gcp import get_secret; print(get_secret('fudo-api-key'))"
```

### Error de Datos Faltantes
```bash
# Verificar archivos raw
ls -la raw_data/expenses/
```

### Error de Procesamiento
```bash
# Ejecutar solo procesamiento con logs
python src/process_expenses.py
```

## 📚 Ejemplos de Uso Prácticos

### Extracción Inicial Completa
```bash
# Extraer TODOS los datos disponibles particionados por día
python main.py --mode extract-by-date --partition-by day
```

### Actualización Incremental Diaria
```bash
# Extraer solo los datos de hoy
python main.py --mode extract-by-date --start-date 2025-07-30 --end-date 2025-07-30 --partition-by day

# Extraer últimos 7 días
python main.py --mode extract-by-date --start-date 2025-07-23 --end-date 2025-07-30 --partition-by day
```

### Data Lake con Python
```python
from src.extract_expenses import FudoRawExtractor

extractor = FudoRawExtractor(output_dir="s3://my-data-lake/raw/fudo/")
extractor.get_token()

# Extracción particionada por mes para data lake
result = extractor.extract_expenses_by_date(
    start_date="2025-01-01",
    end_date="2025-12-31", 
    partition_by="month"
)
```

### Procesamiento de Datos
```python
from src.process_expenses import FudoDataProcessor

processor = FudoDataProcessor(
    raw_data_dir="raw_data/expenses_by_date/",
    output_dir="processed_data/"
)
processor.process_expenses()
```

---

**Nota**: Este pipeline está diseñado para ser modular y escalable, permitiendo procesamiento tanto local como en cloud.
