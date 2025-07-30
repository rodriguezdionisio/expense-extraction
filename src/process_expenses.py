import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)

class FudoDataProcessor:
    """Procesador que convierte datos JSON raw a CSV estructurado."""
    
    def __init__(self, raw_data_dir="raw_data", output_dir="processed_data"):
        self.raw_data_dir = Path(raw_data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def load_json_files(self, pattern):
        """Carga y combina múltiples archivos JSON que coincidan con el patrón."""
        json_files = list(self.raw_data_dir.glob(pattern))
        
        if not json_files:
            logger.warning(f"No se encontraron archivos con patrón: {pattern}")
            return []
        
        all_data = []
        logger.info(f"Cargando {len(json_files)} archivos JSON...")
        
        for json_file in sorted(json_files):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    page_data = json.load(f)
                    
                    # Extraer los datos de la página
                    data = page_data.get("data", [])
                    all_data.extend(data)
                    
                    logger.info(f"Cargado {json_file.name}: {len(data)} registros")
                    
            except Exception as e:
                logger.error(f"Error cargando {json_file}: {e}")
                continue
        
        logger.info(f"Total de registros cargados: {len(all_data)}")
        return all_data
    
    def flatten_expense_record(self, expense):
        """Aplana un registro de gasto individual."""
        flattened = {}
        
        # Campos básicos
        flattened['fudo_id'] = expense.get('id')
        flattened['type'] = expense.get('type')
        
        # Attributes
        attributes = expense.get('attributes', {})
        for key, value in attributes.items():
            flattened[f'attr_{key}'] = value
        
        # Relationships - extraer IDs y tipos
        relationships = expense.get('relationships', {})
        
        for rel_name, rel_data in relationships.items():
            if rel_data and rel_data.get('data'):
                rel_info = rel_data['data']
                
                # Si es una lista (como expenseItems)
                if isinstance(rel_info, list):
                    flattened[f'rel_{rel_name}_count'] = len(rel_info)
                    flattened[f'rel_{rel_name}_ids'] = [item.get('id') for item in rel_info if item.get('id')]
                    flattened[f'rel_{rel_name}_types'] = [item.get('type') for item in rel_info if item.get('type')]
                
                # Si es un objeto único
                elif isinstance(rel_info, dict):
                    flattened[f'rel_{rel_name}_id'] = rel_info.get('id')
                    flattened[f'rel_{rel_name}_type'] = rel_info.get('type')
            else:
                # Relación vacía
                if rel_name == 'expenseItems':
                    flattened[f'rel_{rel_name}_count'] = 0
                    flattened[f'rel_{rel_name}_ids'] = []
                    flattened[f'rel_{rel_name}_types'] = []
                else:
                    flattened[f'rel_{rel_name}_id'] = None
                    flattened[f'rel_{rel_name}_type'] = None
        
        return flattened
    
    def process_expenses(self):
        """Procesa los archivos JSON de gastos y genera CSVs."""
        logger.info("=== PROCESANDO GASTOS ===")
        
        # Cargar datos raw
        expenses_data = self.load_json_files("expenses/expenses_page_*.json")
        
        if not expenses_data:
            logger.error("No se encontraron datos de gastos para procesar")
            return None
        
        # Aplanar todos los registros
        logger.info("Aplanando registros de gastos...")
        flattened_expenses = []
        
        for expense in expenses_data:
            try:
                flattened = self.flatten_expense_record(expense)
                flattened_expenses.append(flattened)
            except Exception as e:
                logger.error(f"Error aplanando gasto {expense.get('id', 'unknown')}: {e}")
                continue
        
        if not flattened_expenses:
            logger.error("No se pudieron aplanar los datos de gastos")
            return None
        
        # Crear DataFrame
        df_expenses = pd.DataFrame(flattened_expenses)
        
        # Limpiar y ordenar columnas
        df_expenses = self.clean_expenses_dataframe(df_expenses)
        
        # Guardar CSV principal
        main_csv = self.output_dir / f"expenses_flattened_{self.timestamp}.csv"
        df_expenses.to_csv(main_csv, index=False, encoding='utf-8')
        logger.info(f"✅ CSV principal guardado: {main_csv}")
        
        # Generar CSVs especializados
        self.generate_specialized_csvs(df_expenses)
        
        # Generar reporte
        self.generate_processing_report(df_expenses, 'expenses')
        
        return df_expenses
    
    def clean_expenses_dataframe(self, df):
        """Limpia y ordena el DataFrame de gastos."""
        logger.info("Limpiando y organizando datos...")
        
        # Renombrar columnas para que sean más legibles
        column_mapping = {
            'attr_amount': 'amount',
            'attr_canceled': 'canceled',
            'attr_createdAt': 'created_at',
            'attr_date': 'expense_date',
            'attr_description': 'description',
            'attr_dueDate': 'due_date',
            'rel_cashRegister_id': 'cash_register_id',
            'rel_cashRegister_type': 'cash_register_type',
            'rel_paymentMethod_id': 'payment_method_id',
            'rel_paymentMethod_type': 'payment_method_type',
            'rel_expenseCategory_id': 'expense_category_id',
            'rel_expenseCategory_type': 'expense_category_type',
            'rel_expenseItems_count': 'expense_items_count',
            'rel_expenseItems_ids': 'expense_items_ids',
            'rel_expenseItems_types': 'expense_items_types'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convertir tipos de datos
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        if 'expense_date' in df.columns:
            df['expense_date'] = pd.to_datetime(df['expense_date'], errors='coerce')
        
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        
        # Ordenar por fecha
        if 'expense_date' in df.columns:
            df = df.sort_values('expense_date')
        
        return df
    
    def generate_specialized_csvs(self, df_expenses):
        """Genera CSVs especializados para diferentes análisis."""
        logger.info("Generando CSVs especializados...")
        
        # CSV de relaciones de items de gastos
        if 'expense_items_ids' in df_expenses.columns:
            items_data = []
            for _, row in df_expenses.iterrows():
                expense_id = row['fudo_id']
                item_ids = row.get('expense_items_ids', [])
                
                if item_ids and isinstance(item_ids, list):
                    for item_id in item_ids:
                        items_data.append({
                            'expense_id': expense_id,
                            'expense_item_id': item_id,
                            'expense_date': row.get('expense_date'),
                            'expense_amount': row.get('amount')
                        })
            
            if items_data:
                df_items = pd.DataFrame(items_data)
                items_csv = self.output_dir / f"expense_items_relationships_{self.timestamp}.csv"
                df_items.to_csv(items_csv, index=False, encoding='utf-8')
                logger.info(f"✅ CSV de relaciones de items guardado: {items_csv}")
    
    def generate_processing_report(self, df, data_type):
        """Genera un reporte de procesamiento."""
        report = {
            "processing_timestamp": self.timestamp,
            "data_type": data_type,
            "total_records": len(df),
            "columns": list(df.columns),
            "data_quality": {
                "null_counts": df.isnull().sum().to_dict(),
                "data_types": df.dtypes.astype(str).to_dict()
            }
        }
        
        if data_type == 'expenses' and 'amount' in df.columns:
            report["summary_stats"] = {
                "total_amount": float(df['amount'].sum()),
                "avg_amount": float(df['amount'].mean()),
                "min_amount": float(df['amount'].min()),
                "max_amount": float(df['amount'].max()),
                "date_range": {
                    "min_date": str(df['expense_date'].min()) if 'expense_date' in df.columns else None,
                    "max_date": str(df['expense_date'].max()) if 'expense_date' in df.columns else None
                }
            }
        
        report_file = self.output_dir / f"processing_report_{data_type}_{self.timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"✅ Reporte de procesamiento guardado: {report_file}")
    
    def process_all(self):
        """Procesa todos los tipos de datos disponibles."""
        logger.info("=== INICIANDO PROCESAMIENTO COMPLETO ===")
        
        results = {}
        
        # Procesar gastos
        try:
            df_expenses = self.process_expenses()
            results['expenses'] = df_expenses
        except Exception as e:
            logger.error(f"Error procesando gastos: {e}")
            results['expenses'] = None
        
        # Procesar métodos de pago (si existen)
        try:
            payment_data = self.load_json_files("payment-methods/payment-methods_page_*.json")
            if payment_data:
                df_payments = pd.json_normalize(payment_data)
                payment_csv = self.output_dir / f"payment_methods_{self.timestamp}.csv"
                df_payments.to_csv(payment_csv, index=False, encoding='utf-8')
                logger.info(f"✅ CSV de métodos de pago guardado: {payment_csv}")
                results['payment_methods'] = df_payments
        except Exception as e:
            logger.warning(f"No se pudieron procesar métodos de pago: {e}")
            results['payment_methods'] = None
        
        # Procesar cajas registradoras (si existen)
        try:
            cash_data = self.load_json_files("cash-registers/cash-registers_page_*.json")
            if cash_data:
                df_cash = pd.json_normalize(cash_data)
                cash_csv = self.output_dir / f"cash_registers_{self.timestamp}.csv"
                df_cash.to_csv(cash_csv, index=False, encoding='utf-8')
                logger.info(f"✅ CSV de cajas registradoras guardado: {cash_csv}")
                results['cash_registers'] = df_cash
        except Exception as e:
            logger.warning(f"No se pudieron procesar cajas registradoras: {e}")
            results['cash_registers'] = None
        
        # Generar resumen final
        self.print_processing_summary(results)
        
        return results
    
    def print_processing_summary(self, results):
        """Imprime un resumen del procesamiento."""
        print("\n" + "="*60)
        print("RESUMEN DE PROCESAMIENTO COMPLETADO")
        print("="*60)
        
        for data_type, df in results.items():
            if df is not None:
                print(f"✅ {data_type.upper()}: {len(df)} registros procesados")
            else:
                print(f"❌ {data_type.upper()}: No procesado")
        
        print(f"\n📁 Archivos CSV guardados en: {self.output_dir}")
        print(f"📅 Timestamp de procesamiento: {self.timestamp}")
        print("="*60)

def main():
    """Función principal para ejecutar el procesamiento."""
    try:
        processor = FudoDataProcessor()
        results = processor.process_all()
        return results
        
    except Exception as e:
        logger.critical(f"Error crítico en el procesamiento: {e}")
        raise

if __name__ == "__main__":
    main()
