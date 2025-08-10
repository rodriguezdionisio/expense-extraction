#!/usr/bin/env python3
"""
Procesador de expenses extraídos - Convierte archivos JSON a CSV aplanado.
"""

import json
import os
import pandas as pd
from datetime import datetime
import pytz
from typing import Dict, Optional, Tuple
from utils.logger import get_logger

logger = get_logger(__name__)

EXTRACTION_DATA_DIR = "extraction_data"
PROCESSED_DATA_DIR = "processed_data"

class ExpenseProcessor:
    """Clase para procesar expenses de JSON a CSV aplanado."""
    
    def __init__(self):
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    def read_expense_json(self, file_path: str) -> Optional[Dict]:
        """Lee un archivo JSON de expense."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error leyendo archivo {file_path}: {e}")
            return None
    
    def _extract_relationship_data(self, relationships: Dict, included: Dict, rel_name: str, rel_type: str) -> Dict:
        """Extrae datos de una relación desde included."""
        rel_data = relationships.get(rel_name, {}).get('data')
        if not rel_data:
            return {}
            
        rel_id = rel_data.get('id')
        key = f"{rel_type}_{rel_id}"
        attrs = included.get(key, {})
        
        return {
            f'{rel_name.lower()}_id': rel_id,
            **{f'{rel_name.lower()}_{k}': v for k, v in attrs.items()}
        }

    def process_expense_data(self, expense_data: Dict) -> Tuple[Dict, list]:
        """Procesa los datos de un expense y separa en expense principal y expense items."""
        try:
            main_data = expense_data.get('data', {})
            expense_id = main_data.get('id')
            attributes = main_data.get('attributes', {})
            relationships = main_data.get('relationships', {})
            
            # Crear índice de datos incluidos
            included = {}
            for item in expense_data.get('included', []):
                item_type = item.get('type')
                item_id = item.get('id')
                if item_type and item_id:
                    included[f"{item_type}_{item_id}"] = item.get('attributes', {})
            
            # Datos principales del expense
            expense_row = {
                'expense_id': expense_id,
                'amount': attributes.get('amount'),
                'canceled': attributes.get('canceled'),
                'created_at': attributes.get('createdAt'),
                'date': attributes.get('date'),
                'description': attributes.get('description'),
                'due_date': attributes.get('dueDate'),
                'payment_date': attributes.get('paymentDate'),
                'receipt_number': attributes.get('receiptNumber'),
                'use_in_cash_count': attributes.get('useInCashCount'),
            }
            
            # Agregar relaciones principales
            for rel_name, rel_type in [
                ('cashRegister', 'CashRegister'),
                ('paymentMethod', 'PaymentMethod'),
                ('provider', 'Provider'),
                ('receiptType', 'ReceiptType'),
                ('user', 'User'),
                ('expenseCategory', 'ExpenseCategory')
            ]:
                expense_row.update(self._extract_relationship_data(relationships, included, rel_name, rel_type))
            
            # Procesar expense items
            expense_items_rows = []
            expense_items = relationships.get('expenseItems', {}).get('data', [])
            
            for item_ref in expense_items:
                item_id = item_ref.get('id')
                
                # Buscar datos del item en included
                item_data = None
                for inc_item in expense_data.get('included', []):
                    if inc_item.get('type') == 'ExpenseItem' and inc_item.get('id') == item_id:
                        item_data = inc_item
                        break
                
                if not item_data:
                    continue
                
                item_attributes = item_data.get('attributes', {})
                item_relationships = item_data.get('relationships', {})
                
                item_row = {
                    'expense_item_id': item_id,
                    'expense_id': expense_id,
                    'canceled': item_attributes.get('canceled'),
                    'detail': item_attributes.get('detail'),
                    'price': item_attributes.get('price'),
                    'quantity': item_attributes.get('quantity'),
                }
                
                # Agregar datos del producto si existe
                product = item_relationships.get('product', {}).get('data')
                if product:
                    prod_id = product.get('id')
                    prod_key = f"Product_{prod_id}"
                    prod_data = included.get(prod_key, {})
                    
                    item_row.update({
                        'product_id': prod_id,
                        'product_name': prod_data.get('name'),
                        'product_cost': prod_data.get('cost'),
                        'product_unit': None,  # Simplificado - unidad no es crítica
                    })
                
                # Agregar datos del ingrediente si existe
                ingredient = item_relationships.get('ingredient', {}).get('data')
                if ingredient:
                    ing_id = ingredient.get('id')
                    ing_key = f"Ingredient_{ing_id}"
                    ing_data = included.get(ing_key, {})
                    
                    item_row.update({
                        'ingredient_id': ing_id,
                        'ingredient_name': ing_data.get('name'),
                        'ingredient_cost': ing_data.get('cost'),
                        'ingredient_unit': None,  # Simplificado - unidad no es crítica
                    })
                
                expense_items_rows.append(item_row)
            
            return expense_row, expense_items_rows
            
        except Exception as e:
            logger.error(f"Error procesando expense {expense_data.get('data', {}).get('id', 'unknown')}: {e}")
            return None, []
    
    def process_range(self, start_id: int, end_id: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Procesa expenses en un rango específico de IDs."""
        logger.info(f"Procesando rango de IDs: {start_id} - {end_id}")
        
        all_expenses = []
        all_expense_items = []
        processed_count = 0
        
        for expense_id in range(start_id, end_id + 1):
            file_path = os.path.join(EXTRACTION_DATA_DIR, f"expense_{expense_id}.json")
            
            if os.path.exists(file_path):
                expense_data = self.read_expense_json(file_path)
                if expense_data:
                    expense_row, expense_items_rows = self.process_expense_data(expense_data)
                    if expense_row:
                        all_expenses.append(expense_row)
                        all_expense_items.extend(expense_items_rows)
                        processed_count += 1
        
        logger.info(f"Rango procesado: {processed_count} archivos")
        logger.info(f"Total expenses: {len(all_expenses)}")
        logger.info(f"Total expense items: {len(all_expense_items)}")
        
        expenses_df = pd.DataFrame(all_expenses) if all_expenses else pd.DataFrame()
        expense_items_df = pd.DataFrame(all_expense_items) if all_expense_items else pd.DataFrame()
        
        return expenses_df, expense_items_df
    
    def transform_expenses_dataframe(self, expenses_df: pd.DataFrame) -> pd.DataFrame:
        """Transforma el DataFrame de expenses aplicando renombrado de columnas y conversión de tipos."""
        if expenses_df.empty:
            return expenses_df
        
        try:
            df = expenses_df.copy()
            argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
            
            # Transformaciones principales
            df['expense_key'] = pd.to_numeric(df['expense_id'], errors='coerce').astype('Int64')
            df['expense_amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
            df['cancelled'] = df['canceled'].astype('boolean')
            
            # Conversión de fechas a formato int YYYYMMDD
            def date_to_int(date_str):
                if pd.isna(date_str) or date_str is None:
                    return None
                try:
                    return int(pd.to_datetime(date_str).strftime('%Y%m%d'))
                except:
                    return None
            
            df['expense_date_key'] = df['date'].apply(date_to_int).astype('Int64')
            df['payment_date_key'] = df['payment_date'].apply(date_to_int).astype('Int64')
            df['due_date_key'] = df['due_date'].apply(date_to_int).astype('Int64')
            
            # Procesar created_at
            def process_created_at(created_at_str):
                if pd.isna(created_at_str) or created_at_str is None:
                    return None, None
                try:
                    dt_utc = pd.to_datetime(created_at_str, utc=True)
                    dt_argentina = dt_utc.astimezone(argentina_tz)
                    date_key = int(dt_argentina.strftime('%Y%m%d'))
                    time_key = int(dt_argentina.strftime('%H%M'))
                    return date_key, time_key
                except:
                    return None, None
            
            df[['created_date_key', 'created_time_key']] = df['created_at'].apply(
                lambda x: pd.Series(process_created_at(x))
            )
            df['created_date_key'] = df['created_date_key'].astype('Int64')
            df['created_time_key'] = df['created_time_key'].astype('Int64')
            
            # Otros campos
            df['expense_note'] = df['description'].astype('string')
            df['receipt_number'] = df['receipt_number'].astype('string')
            df['use_in_cash_count'] = df['use_in_cash_count'].astype('boolean')
            
            # IDs de relaciones
            for orig_col, new_col in [
                ('cashregister_id', 'cash_register_key'),
                ('paymentmethod_id', 'payment_method_key'), 
                ('provider_id', 'provider_key'),
                ('user_id', 'employee_key')
            ]:
                if orig_col in df.columns:
                    df[new_col] = pd.to_numeric(df[orig_col], errors='coerce').astype('Int64')
                else:
                    df[new_col] = pd.Series(dtype='Int64')
            
            # receipt_type_key no puede ser nulo, usar 0 por defecto
            if 'receipttype_id' in df.columns:
                df['receipt_type_key'] = pd.to_numeric(df['receipttype_id'], errors='coerce').fillna(0).astype('int64')
            else:
                df['receipt_type_key'] = 0
            
            # Seleccionar columnas finales
            final_columns = [
                'expense_key', 'expense_amount', 'cancelled', 'expense_date_key',
                'payment_date_key', 'due_date_key', 'created_date_key', 'created_time_key',
                'expense_note', 'receipt_number', 'use_in_cash_count',
                'cash_register_key', 'payment_method_key', 'provider_key',
                'receipt_type_key', 'employee_key'
            ]
            
            available_columns = [col for col in final_columns if col in df.columns]
            return df[available_columns].copy()
            
        except Exception as e:
            logger.error(f"Error transformando DataFrame de expenses: {e}")
            return expenses_df
    
    def transform_expense_items_dataframe(self, expense_items_df: pd.DataFrame) -> pd.DataFrame:
        """Transforma el DataFrame de expense_items aplicando conversión de tipos."""
        if expense_items_df.empty:
            return expense_items_df
        
        try:
            df = expense_items_df.copy()
            
            # Transformaciones de tipos
            df['expense_order_key'] = pd.to_numeric(df['expense_item_id'], errors='coerce').fillna(0).astype('int64')
            df['expense_key'] = pd.to_numeric(df['expense_id'], errors='coerce').fillna(0).astype('int64')
            df['cancelled'] = df['canceled'].astype('bool')
            df['item_detail'] = df['detail'].astype('string')
            df['item_price'] = pd.to_numeric(df['price'], errors='coerce').astype('float64')
            df['item_quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('float64')
            
            # IDs y campos opcionales
            for col, default_val in [
                ('product_id', 0), ('ingredient_id', 0), ('product_cost', 0.0), ('ingredient_cost', 0.0)
            ]:
                if col in df.columns:
                    if 'cost' in col:
                        df[col.replace('_id', '_key') if '_id' in col else col] = pd.to_numeric(df[col], errors='coerce').fillna(default_val).astype('float64')
                    else:
                        df[col.replace('_id', '_key')] = pd.to_numeric(df[col], errors='coerce').fillna(default_val).astype('int64')
                else:
                    if 'cost' in col:
                        df[col] = default_val
                    else:
                        df[col.replace('_id', '_key')] = default_val
            
            # Campos de texto opcionales
            for col in ['product_name', 'ingredient_name', 'product_unit', 'ingredient_unit']:
                if col in df.columns:
                    df[col] = df[col].astype('string')
                else:
                    df[col] = pd.Series(dtype='string')
            
            # Seleccionar columnas finales
            final_columns = [
                'expense_order_key', 'expense_key', 'cancelled', 'item_detail', 'item_price',
                'item_quantity', 'product_key', 'product_name', 'product_cost', 'product_unit',
                'ingredient_key', 'ingredient_name', 'ingredient_cost', 'ingredient_unit'
            ]
            
            available_columns = [col for col in final_columns if col in df.columns]
            return df[available_columns].copy()
            
        except Exception as e:
            logger.error(f"Error transformando DataFrame de expense_items: {e}")
            return expense_items_df
    
    def save_to_csv_by_date(self, expenses_df: pd.DataFrame, expense_items_df: pd.DataFrame) -> dict:
        """Guarda los DataFrames procesados particionados por fecha."""
        if expenses_df.empty:
            logger.warning("DataFrame de expenses vacío")
            return {"dates_processed": [], "files_created": 0, "files_updated": 0}
        
        unique_dates = expenses_df['date'].dropna().unique()
        logger.info(f"Procesando {len(unique_dates)} fechas únicas")
        
        files_created = 0
        files_updated = 0
        
        for date_str in unique_dates:
            try:
                # Crear directorio para la fecha
                date_dir = os.path.join(PROCESSED_DATA_DIR, f"date={date_str}")
                os.makedirs(date_dir, exist_ok=True)
                
                expenses_file = os.path.join(date_dir, "fact_expenses.csv")
                expense_items_file = os.path.join(date_dir, "fact_expense_orders.csv")
                
                # Filtrar datos por fecha
                expenses_for_date = expenses_df[expenses_df['date'] == date_str].copy()
                expense_ids_for_date = expenses_for_date['expense_id'].tolist()
                expense_items_for_date = expense_items_df[
                    expense_items_df['expense_id'].isin(expense_ids_for_date)
                ].copy() if not expense_items_df.empty else pd.DataFrame()
                
                # Aplicar transformaciones
                expenses_for_date = self.transform_expenses_dataframe(expenses_for_date)
                expense_items_for_date = self.transform_expense_items_dataframe(expense_items_for_date)
                
                # Manejar archivos existentes
                is_update = False
                
                # Procesar expenses
                if os.path.exists(expenses_file):
                    existing_expenses = pd.read_csv(expenses_file)
                    # Eliminar duplicados por expense_key
                    if 'expense_key' in existing_expenses.columns and 'expense_key' in expenses_for_date.columns:
                        existing_expenses = existing_expenses[
                            ~existing_expenses['expense_key'].isin(expenses_for_date['expense_key'])
                        ]
                        combined_expenses = pd.concat([existing_expenses, expenses_for_date], ignore_index=True)
                        combined_expenses = combined_expenses.sort_values('expense_key').reset_index(drop=True)
                        is_update = True
                    else:
                        combined_expenses = expenses_for_date
                else:
                    combined_expenses = expenses_for_date
                
                combined_expenses.to_csv(expenses_file, index=False, encoding='utf-8')
                
                # Procesar expense items
                if os.path.exists(expense_items_file) and not expense_items_for_date.empty:
                    existing_items = pd.read_csv(expense_items_file)
                    # Eliminar items existentes de los mismos expense_keys
                    if 'expense_key' in existing_items.columns and 'expense_key' in expense_items_for_date.columns:
                        existing_items = existing_items[
                            ~existing_items['expense_key'].isin(expense_items_for_date['expense_key'])
                        ]
                        combined_items = pd.concat([existing_items, expense_items_for_date], ignore_index=True)
                        combined_items = combined_items.sort_values('expense_order_key').reset_index(drop=True)
                    else:
                        combined_items = expense_items_for_date
                elif not expense_items_for_date.empty:
                    combined_items = expense_items_for_date
                else:
                    # Crear archivo vacío con headers
                    combined_items = pd.DataFrame(columns=[
                        'expense_order_key', 'expense_key', 'cancelled', 'item_detail', 'item_price',
                        'item_quantity', 'product_key', 'product_name', 'product_cost', 'product_unit',
                        'ingredient_key', 'ingredient_name', 'ingredient_cost', 'ingredient_unit'
                    ])
                
                combined_items.to_csv(expense_items_file, index=False, encoding='utf-8')
                
                # Logging
                action = "📔 ACTUALIZADO" if is_update else "📁 CREADO"
                logger.info(f"{action} {date_str}: +{len(expenses_for_date)} expenses, +{len(expense_items_for_date)} items | Total: {len(combined_expenses)} expenses, {len(combined_items)} items")
                
                if is_update:
                    files_updated += 1
                else:
                    files_created += 1
                    
            except Exception as date_error:
                logger.error(f"Error procesando fecha {date_str}: {date_error}")
                continue
        
        logger.info(f"✅ Procesamiento completado: {files_created} fechas nuevas, {files_updated} fechas actualizadas")
        
        return {
            "dates_processed": list(unique_dates),
            "files_created": files_created,
            "files_updated": files_updated
        }
    
    def run_range_processing(self, start_id: int, end_id: int):
        """Ejecuta el procesamiento por rango de IDs."""
        logger.info(f"🔄 INICIANDO PROCESAMIENTO DE RANGO {start_id}-{end_id}")
        logger.info("="*60)
        
        # Procesar rango específico
        expenses_df, expense_items_df = self.process_range(start_id, end_id)
        
        if expenses_df.empty and expense_items_df.empty:
            logger.warning(f"No se encontraron datos en el rango {start_id}-{end_id}")
            return None, None, None
        
        # Guardar CSVs particionados por fecha
        partitioned_files = self.save_to_csv_by_date(expenses_df, expense_items_df)
        
        # Generar resumen
        summary = {
            "processing_type": "range",
            "start_id": start_id,
            "end_id": end_id,
            "total_expenses": len(expenses_df),
            "total_expense_items": len(expense_items_df),
            "processing_date": datetime.now().isoformat(),
            "unique_dates": len(partitioned_files.get("dates_processed", [])),
            "files_created": partitioned_files.get("files_created", 0),
            "files_updated": partitioned_files.get("files_updated", 0),
        }
        
        logger.info("✅ PROCESAMIENTO DE RANGO COMPLETADO")
        logger.info(f"Total expenses: {summary['total_expenses']}")
        logger.info(f"Total expense items: {summary['total_expense_items']}")
        logger.info(f"Fechas únicas: {summary['unique_dates']}")
        logger.info(f"Archivos nuevos: {summary['files_created']}")
        logger.info(f"Archivos actualizados: {summary['files_updated']}")
        
        return expenses_df, expense_items_df, summary


def main():
    """Función principal."""
    try:
        processor = ExpenseProcessor()
        # Por defecto procesamos todos los archivos disponibles
        files = [f for f in os.listdir(EXTRACTION_DATA_DIR) if f.startswith('expense_') and f.endswith('.json')]
        if not files:
            print("❌ No se encontraron archivos JSON para procesar")
            return
            
        # Extraer rango de IDs de los archivos disponibles
        ids = []
        for f in files:
            try:
                id_num = int(f.replace('expense_', '').replace('.json', ''))
                ids.append(id_num)
            except ValueError:
                continue
        
        if not ids:
            print("❌ No se pudieron extraer IDs válidos")
            return
            
        start_id, end_id = min(ids), max(ids)
        expenses_df, expense_items_df, summary = processor.run_range_processing(start_id, end_id)
        
        if summary:
            print("="*60)
            print("✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE")
            print("="*60)
            print(f"📊 Tipo: {summary['processing_type']}")
            print(f"📋 Rango: {summary['start_id']} - {summary['end_id']}")
            print(f"🏢 Total expenses: {summary['total_expenses']}")
            print(f"📦 Total expense items: {summary['total_expense_items']}")
            print(f"📅 Fechas únicas: {summary['unique_dates']}")
            print(f"📁 Archivos creados: {summary['files_created']}")
            print(f"📔 Archivos actualizados: {summary['files_updated']}")
            print(f"� Fecha de procesamiento: {summary['processing_date']}")
            print("="*60)
        else:
            print("❌ No se procesaron datos")
        
    except Exception as e:
        logger.error(f"Error en procesamiento: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
