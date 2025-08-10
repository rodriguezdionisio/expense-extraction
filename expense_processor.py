#!/usr/bin/env python3
"""
Procesador simplificado de expenses - JSON a CSV particionado.
"""

import json
import os
import pandas as pd
from datetime import datetime
import pytz
from utils.logger import get_logger

logger = get_logger(__name__)

class ExpenseProcessor:
    """Procesador simplificado de expenses."""
    
    def __init__(self):
        self.raw_dir = "raw"
        self.clean_dir = "clean"
        self.timezone = pytz.timezone('America/Bogota')
        os.makedirs(self.clean_dir, exist_ok=True)
    
    def _read_json(self, file_path):
        """Lee archivo JSON."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error leyendo {file_path}: {e}")
            return None
    
    def _extract_relationships(self, relationships, included, rel_name, rel_type):
        """Extrae datos de relaciones."""
        rel_data = relationships.get(rel_name, {}).get('data', {})
        if not rel_data:
            return {}
        
        rel_id = rel_data.get('id')
        if not rel_id:
            return {}
        
        key = f"{rel_type}_{rel_id}"
        attrs = included.get(key, {})
        
        return {f'{rel_name.lower()}_id': rel_id, **{f'{rel_name.lower()}_{k}': v for k, v in attrs.items()}}
    
    def _process_expense(self, expense_data):
        """Procesa un expense y retorna datos principales e items."""
        try:
            data = expense_data.get('data', {})
            included_raw = expense_data.get('included', [])
            
            # Organizar included por tipo_id
            included = {}
            for item in included_raw:
                key = f"{item.get('type', '')}_{item.get('id', '')}"
                included[key] = item.get('attributes', {})
            
            attrs = data.get('attributes', {})
            relationships = data.get('relationships', {})
            
            # Convertir fecha a timezone local
            created_at = attrs.get('createdAt', '')
            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    local_dt = dt.astimezone(self.timezone)
                    date_key = local_dt.strftime('%Y%m%d')
                    time_key = local_dt.strftime('%H%M')
                    date_str = local_dt.strftime('%Y-%m-%d')
                except:
                    date_key = time_key = date_str = ''
            else:
                date_key = time_key = date_str = ''
            
            # Expense principal
            expense = {
                'expense_key': int(data.get('id', 0)),
                'expense_amount': float(attrs.get('amount', 0.0)),
                'cancelled': bool(attrs.get('canceled', False)),
                'expense_date_key': date_key,
                'payment_date_key': date_key,
                'due_date_key': '',
                'created_date_key': date_key,
                'created_time_key': time_key,
                'expense_note': str(attrs.get('description', '')),
                'receipt_number': str(attrs.get('receiptNumber', '')),
                'use_in_cash_count': bool(attrs.get('useInCashCount', False)),
                'date': date_str
            }
            
            # Agregar relaciones
            for rel_name, rel_type in [
                ('cashRegister', 'cashRegister'),
                ('paymentMethod', 'paymentMethod'),
                ('provider', 'provider'),
                ('receiptType', 'receiptType'),
                ('user', 'user')
            ]:
                rel_data = self._extract_relationships(relationships, included, rel_name, rel_type)
                expense[f'{rel_name.lower()}_key'] = int(rel_data.get(f'{rel_name.lower()}_id', 0))
            
            # Expense items
            items = []
            expense_items = relationships.get('expenseItems', {}).get('data', [])
            
            for item_ref in expense_items:
                item_id = item_ref.get('id')
                if not item_id:
                    continue
                    
                item_attrs = included.get(f"expenseItem_{item_id}", {})
                
                item = {
                    'expense_order_key': int(item_id),
                    'expense_key': int(expense['expense_key']),
                    'cancelled': bool(item_attrs.get('canceled', False)),
                    'item_detail': str(item_attrs.get('detail', '')),
                    'item_price': float(item_attrs.get('price', 0.0)),
                    'item_quantity': float(item_attrs.get('quantity', 0.0)),
                    'product_key': 0,
                    'product_name': '',
                    'product_cost': 0.0,
                    'product_unit': '',
                    'ingredient_key': 0,
                    'ingredient_name': '',
                    'ingredient_cost': 0.0,
                    'ingredient_unit': ''
                }
                
                # Buscar producto o ingrediente
                for rel_name, rel_type in [('product', 'product'), ('ingredient', 'ingredient')]:
                    if rel_name in item_attrs:
                        rel_id = item_attrs[rel_name]
                        rel_attrs = included.get(f"{rel_type}_{rel_id}", {})
                        if rel_attrs:
                            item[f'{rel_name}_key'] = int(rel_id)
                            item[f'{rel_name}_name'] = str(rel_attrs.get('name', ''))
                            item[f'{rel_name}_cost'] = float(rel_attrs.get('cost', 0.0))
                            item[f'{rel_name}_unit'] = str(rel_attrs.get('unit', ''))
                
                items.append(item)
            
            return expense, items
            
        except Exception as e:
            logger.error(f"Error procesando expense: {e}")
            return None, []
    
    def _save_to_parquet(self, expenses_df, items_df):
        """Guarda DataFrames a Parquet particionados por fecha."""
        if expenses_df.empty:
            return {"dates_processed": [], "files_created": 0, "files_updated": 0}
        
        unique_dates = expenses_df['date'].dropna().unique()
        logger.info(f"Procesando {len(unique_dates)} fechas únicas")
        
        files_created = files_updated = 0
        
        for date_str in unique_dates:
            try:
                # Crear directorios
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
                
                combined.to_parquet(expenses_file, index=False, engine='pyarrow')
                
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
                
                combined_items.to_parquet(items_file, index=False, engine='pyarrow')
                
                action = "📔 ACTUALIZADO" if is_update else "📁 CREADO"
                logger.info(f"{action} {date_str}: +{len(date_expenses)} expenses, +{len(date_items)} items | Total: {len(combined)} expenses, {len(combined_items)} items")
                
                if is_update:
                    files_updated += 1
                else:
                    files_created += 1
                    
            except Exception as e:
                logger.error(f"Error procesando fecha {date_str}: {e}")
                continue
        
        logger.info(f"✅ Procesamiento completado: {files_created} fechas nuevas, {files_updated} actualizadas")
        return {"dates_processed": list(unique_dates), "files_created": files_created, "files_updated": files_updated}
    
    def process_range(self, start_id, end_id):
        """Procesa un rango de IDs."""
        logger.info(f"🔄 INICIANDO PROCESAMIENTO DE RANGO {start_id}-{end_id}")
        logger.info("="*50)
        
        expenses_list = []
        items_list = []
        
        for expense_id in range(start_id, end_id + 1):
            file_path = os.path.join(self.raw_dir, f"expense_{expense_id}.json")
            
            if not os.path.exists(file_path):
                continue
                
            expense_data = self._read_json(file_path)
            if not expense_data:
                continue
            
            expense, items = self._process_expense(expense_data)
            if expense:
                expenses_list.append(expense)
                items_list.extend(items)
        
        if not expenses_list:
            logger.warning("No se encontraron expenses para procesar")
            return {"files_created": 0, "files_updated": 0}
        
        # Crear DataFrames
        expenses_df = pd.DataFrame(expenses_list)
        items_df = pd.DataFrame(items_list) if items_list else pd.DataFrame()
        
        logger.info(f"Procesando rango de IDs: {start_id} - {end_id}")
        logger.info(f"Rango procesado: {len(expenses_list)} archivos")
        logger.info(f"Total expenses: {len(expenses_list)}")
        logger.info(f"Total expense items: {len(items_list)}")
        
        # Guardar Parquet particionados
        result = self._save_to_parquet(expenses_df, items_df)
        
        logger.info("✅ PROCESAMIENTO DE RANGO COMPLETADO")
        logger.info(f"Total expenses: {len(expenses_list)}")
        logger.info(f"Total expense items: {len(items_list)}")
        logger.info(f"Fechas únicas: {len(result['dates_processed'])}")
        logger.info(f"Archivos nuevos: {result['files_created']}")
        logger.info(f"Archivos actualizados: {result['files_updated']}")
        
        return result
