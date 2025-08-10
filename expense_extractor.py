#!/usr/bin/env python3
"""
Extractor simplificado de expenses de la API de Fudo con logging automático.
"""

import json
import os
import requests
import time
from utils.gcp import get_secret
from utils.logger import get_logger

logger = get_logger(__name__)

class ExpenseExtractor:
    """Extractor simplificado de expenses."""
    
    def __init__(self):
        self.token = None
        self.data_dir = "raw"
        self.log_file = "logs/extracted_expenses_log.txt"
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def _get_extracted_ids(self):
        """Lee IDs ya extraídos del log."""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    return {int(line.strip()) for line in f if line.strip().isdigit()}
        except Exception as e:
            logger.warning(f"Error leyendo log: {e}")
        return set()
    
    def _log_extracted_id(self, expense_id):
        """Registra ID extraído en el log."""
        try:
            with open(self.log_file, 'a') as f:
                f.write(f"{expense_id}\n")
        except Exception as e:
            logger.error(f"Error escribiendo log: {e}")
    
    def _get_token(self):
        """Obtiene token de autenticación."""
        if self.token:
            return self.token
            
        try:
            payload = {
                "apiKey": get_secret("fudo-api-key"),
                "apiSecret": get_secret("fudo-api-secret")
            }
            response = requests.post("https://auth.fu.do/api", json=payload)
            response.raise_for_status()
            
            self.token = response.json().get("token")
            if not self.token:
                raise Exception("Token no encontrado")
                
            logger.info("Token obtenido correctamente")
            return self.token
        except Exception as e:
            logger.error(f"Error obteniendo token: {e}")
            raise
    
    def _fetch_expense(self, expense_id):
        """Obtiene un expense por ID."""
        try:
            headers = {"Authorization": f"Bearer {self._get_token()}"}
            url = f"https://api.fu.do/v1alpha1/expenses/{expense_id}"
            
            params = {
                "fields[expense]": "amount,canceled,cashRegister,createdAt,date,description,dueDate,expenseCategory,expenseItems,paymentDate,paymentMethod,provider,receiptNumber,receiptType,useInCashCount,user",
                "fields[cashRegister]": "name",
                "fields[expenseCategory]": "name", 
                "fields[paymentMethod]": "code,name",
                "fields[provider]": "name",
                "fields[receiptType]": "name",
                "fields[product]": "cost,unit,name",
                "fields[ingredient]": "cost,unit,name",
                "fields[expenseItem]": "canceled,detail,price,product,ingredient,quantity",
                "fields[user]": "name",
                "include": "expenseItems,expenseItems.product,expenseItems.ingredient,cashRegister,expenseCategory,paymentMethod,provider,receiptType,user"
            }
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 404:
                logger.info(f"Expense {expense_id} no encontrado")
                return None
            elif response.status_code == 401:
                logger.warning("Token expirado, renovando...")
                self.token = None
                return self._fetch_expense(expense_id)  # Retry con nuevo token
            
            response.raise_for_status()
            logger.info(f"Expense {expense_id} extraído correctamente")
            return response.json()
            
        except Exception as e:
            logger.error(f"Error obteniendo expense {expense_id}: {e}")
            return None
    
    def _save_expense(self, expense_data, expense_id):
        """Guarda expense en archivo JSON."""
        try:
            filepath = os.path.join(self.data_dir, f"expense_{expense_id}.json")
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(expense_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Guardado: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error guardando expense {expense_id}: {e}")
            raise
    
    def initialize_log_from_existing_files(self):
        """Inicializa log con archivos existentes si no existe."""
        if os.path.exists(self.log_file):
            logger.info("📋 Log ya existe")
            return
        
        existing_files = [f for f in os.listdir(self.data_dir) 
                         if f.startswith('expense_') and f.endswith('.json')] if os.path.exists(self.data_dir) else []
        
        if not existing_files:
            logger.info("📋 Log inicializado vacío")
            return
        
        existing_ids = []
        for filename in existing_files:
            try:
                expense_id = int(filename.replace('expense_', '').replace('.json', ''))
                existing_ids.append(expense_id)
            except ValueError:
                continue
        
        existing_ids.sort()
        with open(self.log_file, 'w') as f:
            for expense_id in existing_ids:
                f.write(f"{expense_id}\n")
        
        logger.info(f"📋 Log inicializado con {len(existing_ids)} IDs: {min(existing_ids)}-{max(existing_ids)}")
    
    def extract_range(self, start_id, end_id):
        """Extrae expenses en un rango, omitiendo los ya extraídos."""
        logger.info(f"Iniciando extracción desde ID {start_id} hasta {end_id}")
        
        # Filtrar IDs ya extraídos
        extracted_ids = self._get_extracted_ids()
        all_ids = list(range(start_id, end_id + 1))
        unextracted_ids = [id for id in all_ids if id not in extracted_ids]
        
        if len(unextracted_ids) != len(all_ids):
            skipped = len(all_ids) - len(unextracted_ids)
            logger.info(f"📋 Omitiendo {skipped} IDs ya extraídos, procesando {len(unextracted_ids)} nuevos")
        
        if not unextracted_ids:
            logger.info("🎯 Todos los IDs ya fueron extraídos")
            return [], 0
        
        expenses = []
        successful_count = 0
        
        for expense_id in unextracted_ids:
            try:
                expense_data = self._fetch_expense(expense_id)
                if expense_data:
                    expenses.append(expense_data)
                    self._save_expense(expense_data, expense_id)
                    self._log_extracted_id(expense_id)
                    successful_count += 1
                
                time.sleep(0.1)  # Pausa para no sobrecargar API
                
            except Exception as e:
                logger.error(f"Error procesando ID {expense_id}: {e}")
                continue
        
        logger.info(f"Extracción completada. Total extraídos: {successful_count}")
        return expenses, successful_count
