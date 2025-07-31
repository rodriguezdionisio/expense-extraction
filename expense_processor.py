#!/usr/bin/env python3
"""
Procesador de expenses extraídos - Convierte archivos JSON a CSV aplanado.
"""

import json
import os
import pandas as pd
import glob
from datetime import datetime
import pytz
from typing import List, Dict, Optional, Any
from utils.logger import get_logger
from utils.env_config import config

logger = get_logger(__name__)

# Directorios
EXTRACTION_DATA_DIR = "extraction_data"
PROCESSED_DATA_DIR = "processed_data"

class ExpenseProcessor:
    """Clase para procesar expenses de JSON a CSV aplanado."""
    
    def __init__(self):
        self.mode = config.EXPENSE_EXTRACTION_MODE
        self.start_id = config.EXPENSE_START_ID
        self.ensure_processed_directory()
        
    def ensure_processed_directory(self):
        """Crea el directorio para guardar los datos procesados si no existe."""
        if not os.path.exists(PROCESSED_DATA_DIR):
            os.makedirs(PROCESSED_DATA_DIR)
            logger.info(f"Directorio creado: {PROCESSED_DATA_DIR}")
    
    def read_expense_json(self, file_path: str) -> Optional[Dict]:
        """
        Lee un archivo JSON de expense.
        
        Args:
            file_path: Ruta del archivo JSON
            
        Returns:
            Dict con los datos del expense o None si hay error
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error leyendo archivo {file_path}: {e}")
            return None
    
    def process_expense_data(self, expense_data: Dict) -> tuple:
        """
        Procesa los datos de un expense y separa en expense principal y expense items.
        
        Args:
            expense_data: Datos JSON del expense
            
        Returns:
            Tupla (expense_row, expense_items_rows)
        """
        try:
            # Extraer datos principales del expense
            main_data = expense_data.get('data', {})
            expense_id = main_data.get('id')
            attributes = main_data.get('attributes', {})
            relationships = main_data.get('relationships', {})
            included = expense_data.get('included', [])
            
            # Crear índices para los datos incluidos
            included_by_type_id = {}
            for item in included:
                item_type = item.get('type')
                item_id = item.get('id')
                if item_type and item_id:
                    key = f"{item_type}_{item_id}"
                    included_by_type_id[key] = item.get('attributes', {})
            
            # DATOS PRINCIPALES DEL EXPENSE
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
            
            # Agregar datos de relaciones principales al expense
            cash_register = relationships.get('cashRegister', {}).get('data')
            if cash_register:
                cr_key = f"CashRegister_{cash_register.get('id')}"
                cr_data = included_by_type_id.get(cr_key, {})
                expense_row['cash_register_id'] = cash_register.get('id')
                expense_row['cash_register_name'] = cr_data.get('name')
            
            payment_method = relationships.get('paymentMethod', {}).get('data')
            if payment_method:
                pm_key = f"PaymentMethod_{payment_method.get('id')}"
                pm_data = included_by_type_id.get(pm_key, {})
                expense_row['payment_method_id'] = payment_method.get('id')
                expense_row['payment_method_code'] = pm_data.get('code')
                expense_row['payment_method_name'] = pm_data.get('name')
            
            provider = relationships.get('provider', {}).get('data')
            if provider:
                prov_key = f"Provider_{provider.get('id')}"
                prov_data = included_by_type_id.get(prov_key, {})
                expense_row['provider_id'] = provider.get('id')
                expense_row['provider_name'] = prov_data.get('name')
            
            receipt_type = relationships.get('receiptType', {}).get('data')
            if receipt_type:
                rt_key = f"ReceiptType_{receipt_type.get('id')}"
                rt_data = included_by_type_id.get(rt_key, {})
                expense_row['receipt_type_id'] = receipt_type.get('id')
                expense_row['receipt_type_name'] = rt_data.get('name')
            
            user = relationships.get('user', {}).get('data')
            if user:
                user_key = f"User_{user.get('id')}"
                user_data = included_by_type_id.get(user_key, {})
                expense_row['user_id'] = user.get('id')
                expense_row['user_name'] = user_data.get('name')
            
            expense_category = relationships.get('expenseCategory', {}).get('data')
            if expense_category:
                ec_key = f"ExpenseCategory_{expense_category.get('id')}"
                ec_data = included_by_type_id.get(ec_key, {})
                expense_row['expense_category_id'] = expense_category.get('id')
                expense_row['expense_category_name'] = ec_data.get('name')
            
            # PROCESAR EXPENSE ITEMS
            expense_items = relationships.get('expenseItems', {}).get('data', [])
            expense_items_rows = []
            
            for item_ref in expense_items:
                item_id = item_ref.get('id')
                
                # Buscar los datos del expenseItem en included
                item_data = None
                for included_item in included:
                    if included_item.get('type') == 'ExpenseItem' and included_item.get('id') == item_id:
                        item_data = included_item
                        break
                
                if not item_data:
                    logger.warning(f"ExpenseItem {item_id} no encontrado en included")
                    continue
                
                # Crear fila del expense item
                item_attributes = item_data.get('attributes', {})
                item_relationships = item_data.get('relationships', {})
                
                item_row = {
                    'expense_item_id': item_id,
                    'expense_id': expense_id,  # FK al expense padre
                    'canceled': item_attributes.get('canceled'),
                    'detail': item_attributes.get('detail'),
                    'price': item_attributes.get('price'),
                    'quantity': item_attributes.get('quantity'),
                }
                
                # Datos del producto (si existe)
                product = item_relationships.get('product', {}).get('data')
                if product:
                    prod_id = product.get('id')
                    prod_key = f"Product_{prod_id}"
                    prod_data = included_by_type_id.get(prod_key, {})
                    
                    # Buscar unidad del producto
                    product_unit = None
                    for included_item in included:
                        if included_item.get('type') == 'Product' and included_item.get('id') == prod_id:
                            prod_relationships = included_item.get('relationships', {})
                            unit_data = prod_relationships.get('unit', {}).get('data')
                            if unit_data:
                                unit_key = f"Unit_{unit_data.get('id')}"
                                unit_info = included_by_type_id.get(unit_key, {})
                                product_unit = unit_info.get('name')
                            break
                    
                    item_row.update({
                        'product_id': prod_id,
                        'product_name': prod_data.get('name'),
                        'product_cost': prod_data.get('cost'),
                        'product_unit': product_unit,
                    })
                
                # Datos del ingrediente (si existe)
                ingredient = item_relationships.get('ingredient', {}).get('data')
                if ingredient:
                    ing_id = ingredient.get('id')
                    ing_key = f"Ingredient_{ing_id}"
                    ing_data = included_by_type_id.get(ing_key, {})
                    
                    # Buscar unidad del ingrediente
                    ingredient_unit = None
                    for included_item in included:
                        if included_item.get('type') == 'Ingredient' and included_item.get('id') == ing_id:
                            ing_relationships = included_item.get('relationships', {})
                            unit_data = ing_relationships.get('unit', {}).get('data')
                            if unit_data:
                                unit_key = f"Unit_{unit_data.get('id')}"
                                unit_info = included_by_type_id.get(unit_key, {})
                                ingredient_unit = unit_info.get('name')
                            break
                    
                    item_row.update({
                        'ingredient_id': ing_id,
                        'ingredient_name': ing_data.get('name'),
                        'ingredient_cost': ing_data.get('cost'),
                        'ingredient_unit': ingredient_unit,
                    })
                
                expense_items_rows.append(item_row)
            
            return expense_row, expense_items_rows
            
        except Exception as e:
            logger.error(f"Error procesando expense {expense_data.get('data', {}).get('id', 'unknown')}: {e}")
            return None, []
    
    def process_expense_files(self, file_pattern: str) -> tuple:
        """
        Procesa múltiples archivos de expense según un patrón.
        
        Args:
            file_pattern: Patrón de archivos a procesar (ej: "expense_*.json")
            
        Returns:
            Tupla (expenses_df, expense_items_df)
        """
        file_paths = glob.glob(os.path.join(EXTRACTION_DATA_DIR, file_pattern))
        file_paths.sort()  # Ordenar por nombre de archivo
        
        if not file_paths:
            logger.warning(f"No se encontraron archivos con patrón: {file_pattern}")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"Procesando {len(file_paths)} archivos...")
        
        all_expenses = []
        all_expense_items = []
        processed_count = 0
        error_count = 0
        
        for file_path in file_paths:
            expense_data = self.read_expense_json(file_path)
            
            if expense_data:
                expense_row, expense_items_rows = self.process_expense_data(expense_data)
                
                if expense_row:
                    all_expenses.append(expense_row)
                    all_expense_items.extend(expense_items_rows)
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        logger.info(f"Procesados {processed_count} archivos...")
                else:
                    error_count += 1
            else:
                error_count += 1
        
        logger.info(f"Procesamiento completado: {processed_count} exitosos, {error_count} errores")
        logger.info(f"Total expenses: {len(all_expenses)}")
        logger.info(f"Total expense items: {len(all_expense_items)}")
        
        expenses_df = pd.DataFrame(all_expenses) if all_expenses else pd.DataFrame()
        expense_items_df = pd.DataFrame(all_expense_items) if all_expense_items else pd.DataFrame()
        
        return expenses_df, expense_items_df
    
    def transform_expenses_dataframe(self, expenses_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma el DataFrame de expenses aplicando renombrado de columnas y conversión de tipos.
        
        Args:
            expenses_df: DataFrame original de expenses
            
        Returns:
            DataFrame transformado con nuevas columnas y tipos
        """
        if expenses_df.empty:
            return expenses_df
        
        try:
            df = expenses_df.copy()
            
            # Configurar zona horaria de Argentina
            argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
            
            # TRANSFORMACIONES DE COLUMNAS Y TIPOS
            
            # 1. expense_id -> expense_key (int)
            df['expense_key'] = pd.to_numeric(df['expense_id'], errors='coerce').astype('Int64')
            
            # 2. amount -> expense_amount (float)
            df['expense_amount'] = pd.to_numeric(df['amount'], errors='coerce').astype('float64')
            
            # 3. canceled -> cancelled (bool)
            df['cancelled'] = df['canceled'].astype('boolean')
            
            # 4. date -> expense_date_key (int YYYYMMDD)
            def date_to_int(date_str):
                if pd.isna(date_str) or date_str is None:
                    return None
                try:
                    if isinstance(date_str, str):
                        date_obj = pd.to_datetime(date_str)
                        return int(date_obj.strftime('%Y%m%d'))
                    return None
                except:
                    return None
            
            df['expense_date_key'] = df['date'].apply(date_to_int).astype('Int64')
            
            # 5. payment_date -> payment_date_key (int YYYYMMDD)
            df['payment_date_key'] = df['payment_date'].apply(date_to_int).astype('Int64')
            
            # 6. due_date -> due_date_key (int YYYYMMDD)
            df['due_date_key'] = df['due_date'].apply(date_to_int).astype('Int64')
            
            # 7. created_at -> created_date_key (int YYYYMMDD) y created_time_key (int HHMM)
            def process_created_at(created_at_str):
                if pd.isna(created_at_str) or created_at_str is None:
                    return None, None
                try:
                    # Parsear datetime UTC
                    dt_utc = pd.to_datetime(created_at_str, utc=True)
                    # Convertir a zona horaria de Argentina
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
            
            # 8. description -> expense_note (string)
            df['expense_note'] = df['description'].astype('string')
            
            # 9. receipt_number -> receipt_number (string)
            df['receipt_number'] = df['receipt_number'].astype('string')
            
            # 10. use_in_cash_count -> use_in_cash_count (bool)
            df['use_in_cash_count'] = df['use_in_cash_count'].astype('boolean')
            
            # 11. IDs de relaciones -> keys (int) - manejar valores nulos
            relation_mappings = [
                ('cash_register_id', 'cash_register_key'),
                ('payment_method_id', 'payment_method_key'), 
                ('provider_id', 'provider_key'),
                ('user_id', 'employee_key')
            ]
            
            for orig_col, new_col in relation_mappings:
                if orig_col in df.columns:
                    df[new_col] = pd.to_numeric(df[orig_col], errors='coerce').astype('Int64')
                else:
                    # Crear columna vacía si no existe
                    df[new_col] = pd.Series(dtype='Int64')
            
            # 12. receipt_type_key especial - no puede ser nulo, usar 0 por defecto
            if 'receipt_type_id' in df.columns:
                df['receipt_type_key'] = pd.to_numeric(df['receipt_type_id'], errors='coerce').fillna(0).astype('int64')
            else:
                df['receipt_type_key'] = 0
            
            # SELECCIONAR SOLO LAS COLUMNAS TRANSFORMADAS (eliminar las originales y las de nombres)
            final_columns = [
                'expense_key',
                'expense_amount',
                'cancelled',
                'expense_date_key',
                'payment_date_key',
                'due_date_key',
                'created_date_key',
                'created_time_key',
                'expense_note',
                'receipt_number',
                'use_in_cash_count',
                'cash_register_key',
                'payment_method_key',
                'provider_key',
                'receipt_type_key',
                'employee_key'
            ]
            
            # Filtrar solo las columnas transformadas que existen en el DataFrame
            available_columns = [col for col in final_columns if col in df.columns]
            df_transformed = df[available_columns].copy()
            
            logger.debug(f"Expenses transformado: {len(df_transformed)} filas, {len(df_transformed.columns)} columnas")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transformando DataFrame de expenses: {e}")
            return expenses_df
    
    def transform_expense_items_dataframe(self, expense_items_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma el DataFrame de expense_items aplicando conversión de tipos para consistencia.
        
        Args:
            expense_items_df: DataFrame original de expense_items
            
        Returns:
            DataFrame transformado con tipos correctos
        """
        if expense_items_df.empty:
            return expense_items_df
        
        try:
            df = expense_items_df.copy()
            
            # TRANSFORMACIONES DE TIPOS PARA EXPENSE ITEMS
            
            # 1. expense_item_id -> expense_order_key (int)
            df['expense_order_key'] = pd.to_numeric(df['expense_item_id'], errors='coerce').fillna(0).astype('int64')
            
            # 2. expense_id -> expense_key (int) - FK
            df['expense_key'] = pd.to_numeric(df['expense_id'], errors='coerce').fillna(0).astype('int64')
            
            # 3. canceled -> cancelled (bool)
            df['cancelled'] = df['canceled'].astype('bool')
            
            # 4. detail -> item_detail (string)
            df['item_detail'] = df['detail'].astype('string')
            
            # 5. price -> item_price (float)
            df['item_price'] = pd.to_numeric(df['price'], errors='coerce').astype('float64')
            
            # 6. quantity -> item_quantity (float)
            df['item_quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('float64')
            
            # 7. product_id -> product_key (int)
            if 'product_id' in df.columns:
                df['product_key'] = pd.to_numeric(df['product_id'], errors='coerce').fillna(0).astype('int64')
            else:
                df['product_key'] = 0
            
            # 8. ingredient_id -> ingredient_key (int) - opcional
            if 'ingredient_id' in df.columns:
                df['ingredient_key'] = pd.to_numeric(df['ingredient_id'], errors='coerce').fillna(0).astype('int64')
            else:
                df['ingredient_key'] = 0
            
            # 9. Mantener nombres como string para referencia - opcional
            for col_map in [
                ('product_name', 'product_name'),
                ('ingredient_name', 'ingredient_name'), 
                ('product_unit', 'product_unit'),
                ('ingredient_unit', 'ingredient_unit')
            ]:
                orig_col, new_col = col_map
                if orig_col in df.columns:
                    df[new_col] = df[orig_col].astype('string')
                else:
                    df[new_col] = pd.Series(dtype='string')
            
            # 10. Costos como float - opcional
            for col_map in [
                ('product_cost', 'product_cost'),
                ('ingredient_cost', 'ingredient_cost')
            ]:
                orig_col, new_col = col_map
                if orig_col in df.columns:
                    df[new_col] = pd.to_numeric(df[orig_col], errors='coerce').astype('float64')
                else:
                    df[new_col] = 0.0
            
            # SELECCIONAR COLUMNAS FINALES PARA EXPENSE ITEMS
            final_columns = [
                'expense_order_key',
                'expense_key',  # FK
                'cancelled',
                'item_detail',
                'item_price',
                'item_quantity',
                'product_key',
                'product_name',
                'product_cost',
                'product_unit',
                'ingredient_key',
                'ingredient_name',
                'ingredient_cost',
                'ingredient_unit'
            ]
            
            # Filtrar solo las columnas que existen en el DataFrame
            available_columns = [col for col in final_columns if col in df.columns]
            df_transformed = df[available_columns].copy()
            
            logger.debug(f"Expense items transformado: {len(df_transformed)} filas, {len(df_transformed.columns)} columnas")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transformando DataFrame de expense_items: {e}")
            return expense_items_df

    def process_range(self, start_id: int, end_id: int) -> tuple:
        """
        Procesa expenses en un rango específico de IDs.
        
        Args:
            start_id: ID inicial
            end_id: ID final
            
        Returns:
            Tupla (expenses_df, expense_items_df)
        """
        logger.info(f"Procesando rango de IDs: {start_id} - {end_id}")
        
        all_expenses = []
        all_expense_items = []
        processed_count = 0
        missing_count = 0
        
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
                else:
                    logger.warning(f"Error procesando expense_{expense_id}.json")
            else:
                missing_count += 1
                logger.debug(f"Archivo no encontrado: expense_{expense_id}.json")
        
        logger.info(f"Rango procesado: {processed_count} archivos, {missing_count} faltantes")
        logger.info(f"Total expenses: {len(all_expenses)}")
        logger.info(f"Total expense items: {len(all_expense_items)}")
        
        expenses_df = pd.DataFrame(all_expenses) if all_expenses else pd.DataFrame()
        expense_items_df = pd.DataFrame(all_expense_items) if all_expense_items else pd.DataFrame()
        
        return expenses_df, expense_items_df
    
    def save_to_csv_by_date(self, expenses_df: pd.DataFrame, expense_items_df: pd.DataFrame, filename_prefix: str) -> dict:
        """
        Guarda los DataFrames procesados particionados por fecha en estructura de directorios.
        Si ya existen archivos para una fecha, los nuevos datos se agregan (append) a los existentes.
        
        Args:
            expenses_df: DataFrame con datos de expenses
            expense_items_df: DataFrame con datos de expense items
            filename_prefix: Prefijo del nombre del archivo
            
        Returns:
            Dict con información de archivos guardados por fecha
        """
        try:
            saved_files = {
                "processing_type": filename_prefix,
                "dates_processed": [],
                "files_created": [],
                "files_updated": [],
                "total_files": 0
            }
            
            if expenses_df.empty:
                logger.warning("DataFrame de expenses vacío")
                return saved_files
            
            # Obtener fechas únicas de los expenses
            unique_dates = expenses_df['date'].dropna().unique()
            logger.info(f"Procesando {len(unique_dates)} fechas únicas")
            
            for date_str in unique_dates:
                try:
                    # Crear directorio para la fecha
                    date_dir = os.path.join(PROCESSED_DATA_DIR, f"date={date_str}")
                    os.makedirs(date_dir, exist_ok=True)
                    
                    # Rutas de archivos
                    expenses_file = os.path.join(date_dir, "fact_expenses.csv")
                    expense_items_file = os.path.join(date_dir, "fact_expense_orders.csv")
                    
                    # Filtrar expenses por fecha
                    expenses_for_date = expenses_df[expenses_df['date'] == date_str].copy()
                    
                    # Obtener IDs de expenses para esta fecha
                    expense_ids_for_date = expenses_for_date['expense_id'].tolist()
                    
                    # Filtrar expense items por los expense_ids de esta fecha
                    expense_items_for_date = expense_items_df[
                        expense_items_df['expense_id'].isin(expense_ids_for_date)
                    ].copy() if not expense_items_df.empty else pd.DataFrame()
                    
                    # APLICAR TRANSFORMACIONES A LOS DATOS
                    logger.debug(f"Aplicando transformaciones para fecha {date_str}")
                    
                    # Transformar expenses
                    expenses_for_date = self.transform_expenses_dataframe(expenses_for_date)
                    
                    # Transformar expense_items
                    expense_items_for_date = self.transform_expense_items_dataframe(expense_items_for_date)
                    
                    logger.debug(f"Transformaciones completadas para fecha {date_str}")
                    
                    # PROCESAR FACT_EXPENSES.CSV
                    existing_expenses_df = pd.DataFrame()
                    is_expenses_update = False
                    
                    if os.path.exists(expenses_file):
                        # Leer archivo existente
                        try:
                            existing_expenses_df = pd.read_csv(expenses_file)
                            logger.debug(f"Archivo existente encontrado: {expenses_file} ({len(existing_expenses_df)} filas)")
                            is_expenses_update = True
                        except Exception as e:
                            logger.warning(f"Error leyendo archivo existente {expenses_file}: {e}")
                            existing_expenses_df = pd.DataFrame()
                    
                    # Combinar datos existentes con nuevos
                    if not existing_expenses_df.empty:
                        # Eliminar duplicados por expense_key (mantener los nuevos)
                        # Nota: Los archivos existentes pueden tener expense_id, necesitamos compatibilidad
                        if 'expense_key' in existing_expenses_df.columns:
                            # Asegurar tipos compatibles para la comparación
                            existing_expenses_df['expense_key'] = pd.to_numeric(existing_expenses_df['expense_key'], errors='coerce').astype('Int64')
                            new_expense_keys = pd.to_numeric(expenses_for_date['expense_key'], errors='coerce').astype('Int64')
                            existing_expenses_df = existing_expenses_df[
                                ~existing_expenses_df['expense_key'].isin(new_expense_keys)
                            ]
                        elif 'expense_id' in existing_expenses_df.columns:
                            # Archivo anterior con expense_id - convertir para compatibilidad
                            existing_expenses_df = existing_expenses_df[
                                ~existing_expenses_df['expense_id'].isin(expense_ids_for_date)
                            ]
                        combined_expenses_df = pd.concat([existing_expenses_df, expenses_for_date], ignore_index=True)
                    else:
                        combined_expenses_df = expenses_for_date
                    
                    # Ordenar por expense_key para consistencia  
                    if 'expense_key' in combined_expenses_df.columns:
                        combined_expenses_df = combined_expenses_df.sort_values('expense_key').reset_index(drop=True)
                    else:
                        combined_expenses_df = combined_expenses_df.sort_values('expense_id').reset_index(drop=True)
                    
                    # Guardar archivo actualizado
                    combined_expenses_df.to_csv(expenses_file, index=False, encoding='utf-8')
                    
                    # PROCESAR FACT_EXPENSE_ORDERS.CSV
                    existing_expense_items_df = pd.DataFrame()
                    is_expense_items_update = False
                    
                    if os.path.exists(expense_items_file):
                        # Leer archivo existente
                        try:
                            existing_expense_items_df = pd.read_csv(expense_items_file)
                            logger.debug(f"Archivo existente encontrado: {expense_items_file} ({len(existing_expense_items_df)} filas)")
                            is_expense_items_update = True
                        except Exception as e:
                            logger.warning(f"Error leyendo archivo existente {expense_items_file}: {e}")
                            existing_expense_items_df = pd.DataFrame()
                    
                    # Combinar datos existentes con nuevos
                    if not existing_expense_items_df.empty and not expense_items_for_date.empty:
                        # Eliminar expense items existentes de los mismos expense_ids
                        # Manejar compatibilidad entre expense_key y expense_id
                        if 'expense_key' in existing_expense_items_df.columns:
                            # Usar expense_key (nuevo formato) - asegurar tipos compatibles
                            existing_expense_items_df['expense_key'] = pd.to_numeric(existing_expense_items_df['expense_key'], errors='coerce').astype('Int64')
                            expense_keys_for_date = pd.to_numeric(expenses_for_date['expense_key'], errors='coerce').astype('Int64').tolist()
                            existing_expense_items_df = existing_expense_items_df[
                                ~existing_expense_items_df['expense_key'].isin(expense_keys_for_date)
                            ]
                        elif 'expense_id' in existing_expense_items_df.columns:
                            # Usar expense_id (formato anterior)
                            existing_expense_items_df = existing_expense_items_df[
                                ~existing_expense_items_df['expense_id'].isin(expense_ids_for_date)
                            ]
                        combined_expense_items_df = pd.concat([existing_expense_items_df, expense_items_for_date], ignore_index=True)
                    elif not expense_items_for_date.empty:
                        combined_expense_items_df = expense_items_for_date
                    elif not existing_expense_items_df.empty:
                        combined_expense_items_df = existing_expense_items_df
                    else:
                        # Crear archivo vacío con headers transformados
                        combined_expense_items_df = pd.DataFrame(columns=[
                            'expense_order_key', 'expense_key', 'cancelled', 'item_detail', 'item_price', 
                            'item_quantity', 'product_key', 'product_name', 'product_cost', 'product_unit',
                            'ingredient_key', 'ingredient_name', 'ingredient_cost', 'ingredient_unit'
                        ])
                    
                    # Ordenar por expense_order_key para consistencia
                    if not combined_expense_items_df.empty:
                        if 'expense_order_key' in combined_expense_items_df.columns:
                            combined_expense_items_df = combined_expense_items_df.sort_values('expense_order_key').reset_index(drop=True)
                        elif 'expense_item_key' in combined_expense_items_df.columns:
                            combined_expense_items_df = combined_expense_items_df.sort_values('expense_item_key').reset_index(drop=True)
                        elif 'expense_item_id' in combined_expense_items_df.columns:
                            combined_expense_items_df = combined_expense_items_df.sort_values('expense_item_id').reset_index(drop=True)
                    
                    # Guardar archivo actualizado
                    combined_expense_items_df.to_csv(expense_items_file, index=False, encoding='utf-8')
                    
                    # Registrar información del procesamiento
                    date_info = {
                        "date": date_str,
                        "expenses_file": expenses_file,
                        "expense_items_file": expense_items_file,
                        "new_expenses_count": len(expenses_for_date),
                        "new_expense_items_count": len(expense_items_for_date),
                        "total_expenses_count": len(combined_expenses_df),
                        "total_expense_items_count": len(combined_expense_items_df),
                        "expenses_updated": is_expenses_update,
                        "expense_items_updated": is_expense_items_update
                    }
                    
                    saved_files["dates_processed"].append(date_str)
                    
                    if is_expenses_update or is_expense_items_update:
                        saved_files["files_updated"].append(date_info)
                        action = "📔 ACTUALIZADO"
                    else:
                        saved_files["files_created"].append(date_info)
                        saved_files["total_files"] += 2
                        action = "📁 CREADO"
                    
                    logger.info(f"{action} {date_str}: +{len(expenses_for_date)} expenses, +{len(expense_items_for_date)} items | Total: {len(combined_expenses_df)} expenses, {len(combined_expense_items_df)} items")
                    
                except Exception as date_error:
                    logger.error(f"Error procesando fecha {date_str}: {date_error}")
                    continue
            
            created_count = len(saved_files["files_created"])
            updated_count = len(saved_files["files_updated"])
            total_files = saved_files["total_files"] + (updated_count * 2)  # Archivos actualizados también cuentan
            
            logger.info(f"✅ Procesamiento completado: {created_count} fechas nuevas, {updated_count} fechas actualizadas, {total_files} archivos totales")
            
            return saved_files
            
        except Exception as e:
            logger.error(f"Error guardando CSVs particionados: {e}")
            return {
                "processing_type": filename_prefix,
                "dates_processed": [],
                "files_created": [],
                "files_updated": [],
                "total_files": 0,
                "error": str(e)
            }
    
    def run_initial_processing(self):
        """Ejecuta el procesamiento inicial completo."""
        logger.info("🔄 INICIANDO PROCESAMIENTO INICIAL")
        logger.info(f"Modo: {self.mode}")
        logger.info("="*60)
        
        try:
            # Procesar todos los archivos de expense
            expenses_df, expense_items_df = self.process_expense_files("expense_*.json")
            
            if expenses_df.empty and expense_items_df.empty:
                logger.warning("No se encontraron datos para procesar")
                return None, None, None
            
            # Guardar CSVs particionados por fecha
            partitioned_files = self.save_to_csv_by_date(expenses_df, expense_items_df, "initial")
            
            # Generar resumen
            summary = {
                "processing_type": "initial",
                "total_expenses": len(expenses_df) if not expenses_df.empty else 0,
                "total_expense_items": len(expense_items_df) if not expense_items_df.empty else 0,
                "processing_date": datetime.now().isoformat(),
                "partitioned_files": partitioned_files,
                "unique_dates": len(partitioned_files.get("dates_processed", [])),
                "files_created": len(partitioned_files.get("files_created", [])),
                "files_updated": len(partitioned_files.get("files_updated", [])),
                "total_files_created": partitioned_files.get("total_files", 0),
                "expenses_columns": list(expenses_df.columns) if not expenses_df.empty else [],
                "expense_items_columns": list(expense_items_df.columns) if not expense_items_df.empty else []
            }
            
            logger.info("✅ PROCESAMIENTO INICIAL COMPLETADO")
            logger.info(f"Total expenses: {summary['total_expenses']}")
            logger.info(f"Total expense items: {summary['total_expense_items']}")
            logger.info(f"Fechas únicas: {summary['unique_dates']}")
            logger.info(f"Archivos nuevos: {summary['files_created']}")
            logger.info(f"Archivos actualizados: {summary['files_updated']}")
            logger.info(f"Archivos totales: {summary['total_files_created']}")
            
            return expenses_df, expense_items_df, summary
            
        except Exception as e:
            logger.error(f"Error en procesamiento inicial: {e}")
            raise
    
    def run_maintenance_processing(self, days_back: int = 7):
        """
        Ejecuta el procesamiento de mantenimiento.
        Nota: Para mantenimiento, procesamos todos los archivos disponibles
        ya que el filtro por fecha se hizo en la extracción.
        """
        logger.info("🔄 INICIANDO PROCESAMIENTO DE MANTENIMIENTO")
        logger.info(f"Modo: {self.mode}")
        logger.info("="*60)
        
        try:
            # Procesar todos los archivos disponibles
            expenses_df, expense_items_df = self.process_expense_files("expense_*.json")
            
            if expenses_df.empty and expense_items_df.empty:
                logger.warning("No se encontraron datos para procesar")
                return None, None, None
            
            # Guardar CSVs particionados por fecha
            partitioned_files = self.save_to_csv_by_date(expenses_df, expense_items_df, "maintenance")
            
            # Generar resumen
            summary = {
                "processing_type": "maintenance",
                "total_expenses": len(expenses_df) if not expenses_df.empty else 0,
                "total_expense_items": len(expense_items_df) if not expense_items_df.empty else 0,
                "processing_date": datetime.now().isoformat(),
                "partitioned_files": partitioned_files,
                "unique_dates": len(partitioned_files.get("dates_processed", [])),
                "files_created": len(partitioned_files.get("files_created", [])),
                "files_updated": len(partitioned_files.get("files_updated", [])),
                "total_files_created": partitioned_files.get("total_files", 0),
                "expenses_columns": list(expenses_df.columns) if not expenses_df.empty else [],
                "expense_items_columns": list(expense_items_df.columns) if not expense_items_df.empty else []
            }
            
            logger.info("✅ PROCESAMIENTO DE MANTENIMIENTO COMPLETADO")
            logger.info(f"Total expenses: {summary['total_expenses']}")
            logger.info(f"Total expense items: {summary['total_expense_items']}")
            logger.info(f"Fechas únicas: {summary['unique_dates']}")
            logger.info(f"Archivos nuevos: {summary['files_created']}")
            logger.info(f"Archivos actualizados: {summary['files_updated']}")
            logger.info(f"Archivos totales: {summary['total_files_created']}")
            
            return expenses_df, expense_items_df, summary
            
        except Exception as e:
            logger.error(f"Error en procesamiento de mantenimiento: {e}")
            raise
    
    def run_range_processing(self, start_id: int, end_id: int):
        """Ejecuta el procesamiento por rango de IDs."""
        logger.info(f"🔄 INICIANDO PROCESAMIENTO DE RANGO {start_id}-{end_id}")
        logger.info("="*60)
        
        try:
            # Procesar rango específico
            expenses_df, expense_items_df = self.process_range(start_id, end_id)
            
            if expenses_df.empty and expense_items_df.empty:
                logger.warning(f"No se encontraron datos en el rango {start_id}-{end_id}")
                return None, None, None
            
            # Guardar CSVs particionados por fecha
            partitioned_files = self.save_to_csv_by_date(expenses_df, expense_items_df, f"range_{start_id}_{end_id}")
            
            # Generar resumen
            summary = {
                "processing_type": "range",
                "start_id": start_id,
                "end_id": end_id,
                "total_expenses": len(expenses_df) if not expenses_df.empty else 0,
                "total_expense_items": len(expense_items_df) if not expense_items_df.empty else 0,
                "processing_date": datetime.now().isoformat(),
                "partitioned_files": partitioned_files,
                "unique_dates": len(partitioned_files.get("dates_processed", [])),
                "files_created": len(partitioned_files.get("files_created", [])),
                "files_updated": len(partitioned_files.get("files_updated", [])),
                "total_files_created": partitioned_files.get("total_files", 0),
                "expenses_columns": list(expenses_df.columns) if not expenses_df.empty else [],
                "expense_items_columns": list(expense_items_df.columns) if not expense_items_df.empty else []
            }
            
            logger.info("✅ PROCESAMIENTO DE RANGO COMPLETADO")
            logger.info(f"Total expenses: {summary['total_expenses']}")
            logger.info(f"Total expense items: {summary['total_expense_items']}")
            logger.info(f"Fechas únicas: {summary['unique_dates']}")
            logger.info(f"Archivos nuevos: {summary['files_created']}")
            logger.info(f"Archivos actualizados: {summary['files_updated']}")
            logger.info(f"Archivos totales: {summary['total_files_created']}")
            
            return expenses_df, expense_items_df, summary
            
        except Exception as e:
            logger.error(f"Error en procesamiento de rango: {e}")
            raise
    
    def run(self):
        """Ejecuta el procesamiento según el modo configurado."""
        if self.mode == "initial":
            return self.run_initial_processing()
        elif self.mode == "maintenance":
            return self.run_maintenance_processing()
        else:
            raise ValueError(f"Modo no válido: {self.mode}. Use 'initial' o 'maintenance'")

def main():
    """Función principal."""
    try:
        processor = ExpenseProcessor()
        expenses_df, expense_items_df, summary = processor.run()
        
        if summary:
            print("="*60)
            print("✅ PROCESAMIENTO COMPLETADO")
            print(f"Tipo: {summary['processing_type']}")
            print(f"Total expenses: {summary['total_expenses']}")
            print(f"Total expense items: {summary['total_expense_items']}")
            print(f"Fechas únicas: {summary.get('unique_dates', 0)}")
            print(f"Archivos nuevos: {summary.get('files_created', 0)}")
            print(f"Archivos actualizados: {summary.get('files_updated', 0)}")
            
            # Mostrar estructura de archivos creados/actualizados
            partitioned_files = summary.get('partitioned_files', {})
            
            if partitioned_files.get('files_created'):
                print("\n📁 Archivos CREADOS por fecha:")
                for date_info in partitioned_files['files_created'][:3]:  # Mostrar primeros 3
                    print(f"  📅 {date_info['date']}: {date_info['new_expenses_count']} expenses, {date_info['new_expense_items_count']} items")
                if len(partitioned_files['files_created']) > 3:
                    print(f"  ... y {len(partitioned_files['files_created']) - 3} fechas más")
            
            if partitioned_files.get('files_updated'):
                print("\n📔 Archivos ACTUALIZADOS por fecha:")
                for date_info in partitioned_files['files_updated'][:3]:  # Mostrar primeros 3
                    print(f"  📅 {date_info['date']}: +{date_info['new_expenses_count']} expenses, +{date_info['new_expense_items_count']} items | Total: {date_info['total_expenses_count']} expenses, {date_info['total_expense_items_count']} items")
                if len(partitioned_files['files_updated']) > 3:
                    print(f"  ... y {len(partitioned_files['files_updated']) - 3} fechas más")
            
            print("="*60)
        else:
            print("❌ No se procesaron datos")
        
    except Exception as e:
        logger.error(f"Error en procesamiento: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
