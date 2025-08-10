#!/usr/bin/env python3
"""
Extractor de expenses de la API de Fudo con soporte para logging de IDs extraídos.
"""

import json
import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from utils.gcp import get_secret
from utils.logger import get_logger

logger = get_logger(__name__)

FUDO_AUTH_URL = "https://auth.fu.do/api"
FUDO_API_URL = "https://api.fu.do/v1alpha1"
EXTRACTION_DATA_DIR = "raw"
EXTRACTED_LOG_FILE = "logs/extracted_expenses_log.txt"

class ExpenseExtractor:
    """Clase para extraer expenses de la API de Fudo."""
    
    def __init__(self):
        self.token = None
        os.makedirs(EXTRACTION_DATA_DIR, exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def get_extracted_ids(self):
        """Obtiene la lista de IDs ya extraídos desde el archivo de log."""
        try:
            if os.path.exists(EXTRACTED_LOG_FILE):
                with open(EXTRACTED_LOG_FILE, 'r') as f:
                    return {int(line.strip()) for line in f if line.strip().isdigit()}
            return set()
        except Exception as e:
            logger.warning(f"Error leyendo archivo de log: {e}")
            return set()
    
    def log_extracted_id(self, expense_id):
        """Registra un ID como extraído en el archivo de log."""
        try:
            with open(EXTRACTED_LOG_FILE, 'a') as f:
                f.write(f"{expense_id}\n")
        except Exception as e:
            logger.error(f"Error escribiendo en el log: {e}")
    
    def filter_unextracted_ids(self, start_id, end_id):
        """Filtra los IDs que aún no han sido extraídos."""
        extracted_ids = self.get_extracted_ids()
        all_ids = list(range(start_id, end_id + 1))
        unextracted_ids = [id for id in all_ids if id not in extracted_ids]
        
        if len(unextracted_ids) != len(all_ids):
            skipped_count = len(all_ids) - len(unextracted_ids)
            logger.info(f"📋 Omitiendo {skipped_count} IDs ya extraídos, procesando {len(unextracted_ids)} IDs nuevos")
        
        return unextracted_ids

    def initialize_log_from_existing_files(self):
        """Inicializa el log con los IDs de archivos ya existentes en raw/."""
        if os.path.exists(EXTRACTED_LOG_FILE):
            logger.info("📋 Archivo de log ya existe, no es necesario inicializar")
            return
        
        existing_files = []
        if os.path.exists(EXTRACTION_DATA_DIR):
            existing_files = [f for f in os.listdir(EXTRACTION_DATA_DIR) if f.startswith('expense_') and f.endswith('.json')]
        
        if not existing_files:
            logger.info("📋 No hay archivos existentes, log inicializado vacío")
            return
        
        existing_ids = []
        for filename in existing_files:
            try:
                expense_id = int(filename.replace('expense_', '').replace('.json', ''))
                existing_ids.append(expense_id)
            except ValueError:
                continue
        
        existing_ids.sort()
        
        with open(EXTRACTED_LOG_FILE, 'w') as f:
            for expense_id in existing_ids:
                f.write(f"{expense_id}\n")
        
        logger.info(f"📋 Log inicializado con {len(existing_ids)} IDs existentes: {min(existing_ids)}-{max(existing_ids)}")
    
    def get_token(self):
        """Obtiene el token de autenticación desde la API de Fudo."""
        try:
            api_key = get_secret("fudo-api-key")
            api_secret = get_secret("fudo-api-secret")

            payload = {"apiKey": api_key, "apiSecret": api_secret}
            headers = {"Content-Type": "application/json"}

            response = requests.post(FUDO_AUTH_URL, json=payload, headers=headers)
            response.raise_for_status()

            token = response.json().get("token")
            if not token:
                raise Exception("Token no encontrado en la respuesta.")

            logger.info("Token obtenido correctamente desde Fudo.")
            self.token = token
            return token

        except Exception as e:
            logger.error(f"Error al obtener token: {e}")
            raise
    
    def get_expense_by_id(self, expense_id):
        """Obtiene un expense específico por ID con todos los campos disponibles."""
        try:
            if not self.token:
                self.get_token()
                
            headers = {"Authorization": f"Bearer {self.token}"}
            url = f"{FUDO_API_URL}/expenses/{expense_id}"
            
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
                logger.info(f"Expense {expense_id} no encontrado (404)")
                return None
            elif response.status_code == 401:
                logger.warning("Token expirado, renovando...")
                self.token = None
                self.get_token()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            
            logger.info(f"Expense {expense_id} extraído correctamente")
            return response.json()
            
        except Exception as e:
            logger.error(f"Error al obtener expense {expense_id}: {e}")
            return None

    def save_individual_expense(self, expense_data, expense_id):
        """Guarda un expense individual en un archivo JSON."""
        try:
            filename = f"expense_{expense_id}.json"
            filepath = os.path.join(EXTRACTION_DATA_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(expense_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Expense individual guardado: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando expense {expense_id}: {e}")
            raise

    def extract_range(self, start_id, end_id):
        """Extrae expenses en un rango específico de IDs, omitiendo los ya extraídos."""
        logger.info(f"Iniciando extracción desde ID {start_id} hasta {end_id}")
        
        # Filtrar IDs ya extraídos
        unextracted_ids = self.filter_unextracted_ids(start_id, end_id)
        
        if not unextracted_ids:
            logger.info("🎯 Todos los IDs en el rango ya fueron extraídos")
            return [], 0
        
        expenses = []
        successful_count = 0
        
        for expense_id in unextracted_ids:
            try:
                expense_data = self.get_expense_by_id(expense_id)
                
                if expense_data:
                    expenses.append(expense_data)
                    self.save_individual_expense(expense_data, expense_id)
                    # Registrar en el log después de extracción exitosa
                    self.log_extracted_id(expense_id)
                    successful_count += 1
                
                # Pequeña pausa para no sobrecargar la API
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error procesando ID {expense_id}: {e}")
                continue
        
        logger.info(f"Extracción completada. Total expenses extraídos: {successful_count}")
        return expenses, successful_count
