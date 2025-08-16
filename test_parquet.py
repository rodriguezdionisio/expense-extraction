#!/usr/bin/env python3
"""
Script para probar el procesamiento completo y verificar archivos Parquet locales.
"""

import os
import pandas as pd
from expense_processor import ExpenseProcessor

def test_parquet_generation():
    """Prueba que se generen archivos Parquet con los names incluidos."""
    
    print("🔄 Iniciando prueba de generación de archivos Parquet...")
    print("="*60)
    
    # Crear una versión modificada del procesador que no suba a GCS
    class LocalExpenseProcessor(ExpenseProcessor):
        def _save_to_parquet(self, expenses_df, items_df):
            """Versión local que no sube a GCS."""
            unique_dates = sorted(expenses_df['date'].unique())
            files_created = 0
            files_updated = 0
            
            for date_str in unique_dates:
                try:
                    # Crear directorios locales
                    expenses_dir = os.path.join(self.clean_dir, "fact_expenses", f"date={date_str}")
                    items_dir = os.path.join(self.clean_dir, "fact_expense_orders", f"date={date_str}")
                    os.makedirs(expenses_dir, exist_ok=True)
                    os.makedirs(items_dir, exist_ok=True)
                    
                    expenses_file = os.path.join(expenses_dir, "fact_expenses.parquet")
                    items_file = os.path.join(items_dir, "fact_expense_orders.parquet")
                    
                    # Filtrar datos por fecha
                    date_expenses = expenses_df[expenses_df['date'] == date_str].copy()
                    expense_ids = date_expenses['expense_key'].tolist()
                    date_items = items_df[items_df['expense_key'].isin(expense_ids)].copy() if not items_df.empty else pd.DataFrame()
                    
                    # Limpiar columna 'date' antes de guardar
                    date_expenses = date_expenses.drop(columns=['date'])
                    
                    # Manejar archivos existentes
                    is_update = False
                    
                    # Expenses
                    if os.path.exists(expenses_file):
                        existing = pd.read_parquet(expenses_file)
                        existing = existing[~existing['expense_key'].isin(date_expenses['expense_key'])]
                        combined = pd.concat([existing, date_expenses], ignore_index=True).sort_values('expense_key').reset_index(drop=True)
                        is_update = True
                    else:
                        combined = date_expenses
                    
                    # Guardar localmente
                    combined.to_parquet(expenses_file, index=False, engine='pyarrow')
                    print(f"✅ Guardado expenses: {expenses_file}")
                    
                    # Items
                    if not date_items.empty:
                        if os.path.exists(items_file):
                            existing_items = pd.read_parquet(items_file)
                            existing_items = existing_items[~existing_items['expense_key'].isin(date_items['expense_key'])]
                            combined_items = pd.concat([existing_items, date_items], ignore_index=True).sort_values('expense_order_key').reset_index(drop=True)
                        else:
                            combined_items = date_items
                    else:
                        # Crear archivo vacío con headers
                        combined_items = pd.DataFrame(columns=[
                            'expense_order_key', 'expense_key', 'cancelled', 'item_detail', 'item_price',
                            'item_quantity', 'product_key', 'product_name', 'product_cost', 'product_unit',
                            'ingredient_key', 'ingredient_name', 'ingredient_cost', 'ingredient_unit'
                        ])
                    
                    # Guardar items localmente
                    combined_items.to_parquet(items_file, index=False, engine='pyarrow')
                    print(f"✅ Guardado expense_orders: {items_file}")
                    
                    action = "📔 ACTUALIZADO" if is_update else "📁 CREADO"
                    print(f"{action} {date_str}: +{len(date_expenses)} expenses, +{len(date_items)} items | Total: {len(combined)} expenses, {len(combined_items)} items")
                    
                    if is_update:
                        files_updated += 1
                    else:
                        files_created += 1
                        
                except Exception as e:
                    print(f"Error procesando fecha {date_str}: {e}")
                    continue
            
            print(f"✅ Procesamiento completado: {files_created} fechas nuevas, {files_updated} actualizadas")
            return {"dates_processed": list(unique_dates), "files_created": files_created, "files_updated": files_updated}
    
    # Crear instancia del procesador local
    processor = LocalExpenseProcessor()
    
    # Procesar un rango pequeño para prueba
    result = processor.process_range(7, 8)  # Solo expense_7 y expense_8
    
    print(f"\n📊 Resultado del procesamiento: {result}")
    
    # Verificar archivos generados
    print(f"\n📁 Verificando archivos generados...")
    
    for root, dirs, files in os.walk(processor.clean_dir):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                print(f"\n📄 Archivo: {file_path}")
                
                # Leer y analizar el archivo
                df = pd.read_parquet(file_path)
                print(f"   Filas: {len(df)}")
                print(f"   Columnas: {len(df.columns)}")
                
                # Buscar columnas de names
                name_columns = [col for col in df.columns if '_name' in col]
                if name_columns:
                    print(f"   ✅ Columnas de names: {name_columns}")
                    
                    # Mostrar valores únicos de los names
                    for col in name_columns:
                        unique_values = df[col].unique()
                        non_empty_values = [v for v in unique_values if v and str(v) != 'nan' and str(v) != '']
                        if non_empty_values:
                            print(f"      {col}: {non_empty_values}")
                else:
                    print(f"   ❌ No se encontraron columnas de names")

if __name__ == "__main__":
    test_parquet_generation()
