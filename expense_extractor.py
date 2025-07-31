#!/usr/bin/env python3
"""
Extractor de expenses de la API de Fudo con soporte para carga inicial y mantenimiento diario.
"""

import json
import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from utils.gcp import get_secret
from utils.logger import get_logger
from utils.env_config import config

logger = get_logger(__name__)

FUDO_AUTH_URL = "https://auth.fu.do/api"
FUDO_API_URL = "https://api.fu.do/v1alpha1"

# Directorio para guardar las respuestas
EXTRACTION_DATA_DIR = "extraction_data"

class ExpenseExtractor:
    """Clase para extraer expenses de la API de Fudo."""
    
    def __init__(self):
        self.token = None
        self.mode = config.EXPENSE_EXTRACTION_MODE
        self.start_id = config.EXPENSE_START_ID
        self.page_size = config.EXPENSE_PAGE_SIZE
        self.max_pages = config.EXPENSE_MAX_PAGES
        self.ensure_data_directory()
        
    def ensure_data_directory(self):
        """Crea el directorio para guardar los datos extraídos si no existe."""
        if not os.path.exists(EXTRACTION_DATA_DIR):
            os.makedirs(EXTRACTION_DATA_DIR)
            logger.info(f"Directorio creado: {EXTRACTION_DATA_DIR}")
    
    def get_token(self) -> str:
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
    
    def get_expense_by_id(self, expense_id: int) -> Optional[Dict]:
        """
        Obtiene un expense específico por ID con todos los campos disponibles.
        
        Args:
            expense_id: ID del expense a obtener
            
        Returns:
            Dict con los datos del expense o None si no se encuentra
        """
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
            
            # Si es 404, el expense no existe
            if response.status_code == 404:
                logger.info(f"Expense {expense_id} no encontrado (404)")
                return None
                
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"Expense {expense_id} extraído correctamente")
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Expense {expense_id} no encontrado")
                return None
            else:
                logger.error(f"Error HTTP al obtener expense {expense_id}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error al obtener expense {expense_id}: {e}")
            raise
    
    def extract_expenses_range(self, start_id: int, end_id: Optional[int] = None, batch_size: int = 50, save_individual: bool = True) -> Tuple[List[Dict], int]:
        """
        Extrae expenses en un rango de IDs.
        
        Args:
            start_id: ID inicial
            end_id: ID final (opcional, si no se especifica extrae hasta encontrar 404s consecutivos)
            batch_size: Tamaño del lote para guardar datos
            save_individual: Si True, guarda cada expense en archivo individual
            
        Returns:
            Tuple con (lista de expenses extraídos, último ID procesado)
        """
        expenses = []
        current_id = start_id
        consecutive_404s = 0
        max_consecutive_404s = 50  # Parar después de 50 404s consecutivos
        individual_files = []  # Lista de archivos individuales creados
        
        logger.info(f"Iniciando extracción desde ID {start_id}")
        if save_individual:
            logger.info("Modo: Guardado individual de archivos activado")
        
        while True:
            # Verificar condiciones de parada
            if end_id and current_id > end_id:
                logger.info(f"Alcanzado ID final: {end_id}")
                break
                
            if consecutive_404s >= max_consecutive_404s:
                logger.info(f"Encontrados {consecutive_404s} 404s consecutivos. Finalizando extracción.")
                break
            
            try:
                expense_data = self.get_expense_by_id(current_id)
                
                if expense_data:
                    expenses.append(expense_data)
                    consecutive_404s = 0  # Reset contador
                    
                    # Guardar archivo individual si está habilitado
                    if save_individual:
                        individual_file = self.save_individual_expense(expense_data, current_id)
                        if individual_file:
                            individual_files.append(individual_file)
                    
                    # Guardar lotes periódicamente (solo si no se están guardando archivos individuales)
                    if not save_individual and len(expenses) % batch_size == 0:
                        self.save_batch(expenses[-batch_size:], f"batch_{current_id - batch_size + 1}_{current_id}")
                        logger.info(f"Guardado lote hasta ID {current_id}. Total extraídos: {len(expenses)}")
                else:
                    consecutive_404s += 1
                    
                current_id += 1
                
                # Pequeña pausa para no sobrecargar la API
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error procesando ID {current_id}: {e}")
                current_id += 1
                consecutive_404s += 1
        
        # Guardar último lote si hay datos pendientes y no se están guardando archivos individuales
        if not save_individual:
            remaining = len(expenses) % batch_size
            if remaining > 0:
                self.save_batch(expenses[-remaining:], f"final_batch_{current_id - remaining}_{current_id - 1}")
        
        # Log del resumen
        logger.info(f"Extracción completada. Total expenses: {len(expenses)}, Último ID: {current_id - 1}")
        if save_individual:
            logger.info(f"Archivos individuales creados: {len(individual_files)}")
            for file in individual_files:
                logger.info(f"  - {file}")
        
        return expenses, current_id - 1
    
    def extract_recent_expenses(self, days_back: int = 7) -> List[Dict]:
        """
        Extrae expenses recientes usando filtro de fecha (para mantenimiento diario).
        
        Args:
            days_back: Número de días hacia atrás para extraer
            
        Returns:
            Lista de expenses recientes
        """
        try:
            if not self.token:
                self.get_token()
                
            headers = {"Authorization": f"Bearer {self.token}"}
            url = f"{FUDO_API_URL}/expenses"
            
            # Calcular fecha de inicio
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            params = {
                "page[size]": self.page_size,
                "page[number]": 1,
                "sort": "-date",  # Más recientes primero
                "filter[date][gte]": start_date.strftime("%Y-%m-%d"),
                "filter[date][lte]": end_date.strftime("%Y-%m-%d"),
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
            
            all_expenses = []
            page = 1
            
            logger.info(f"Extrayendo expenses desde {start_date.strftime('%Y-%m-%d')} hasta {end_date.strftime('%Y-%m-%d')}")
            
            while True:
                params["page[number]"] = page
                
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                expenses = data.get('data', [])
                
                if not expenses:
                    break
                    
                all_expenses.extend(expenses)
                logger.info(f"Página {page}: {len(expenses)} expenses extraídos")
                
                # Verificar si hay más páginas
                links = data.get('links', {})
                if not links.get('next'):
                    break
                    
                page += 1
                
                # Límite de páginas si está configurado
                if self.max_pages > 0 and page > self.max_pages:
                    logger.warning(f"Alcanzado límite de páginas: {self.max_pages}")
                    break
                    
                time.sleep(0.1)  # Pausa entre requests
            
            logger.info(f"Extracción por fecha completada. Total: {len(all_expenses)} expenses")
            
            # Guardar resultado completo
            self.save_extraction_result({"data": all_expenses, "included": data.get('included', [])}, 
                                      f"recent_expenses_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")
            
            return all_expenses
            
        except Exception as e:
            logger.error(f"Error en extracción por fecha: {e}")
            raise
    
    def save_individual_expense(self, expense_data: Dict, expense_id: int) -> str:
        """
        Guarda un expense individual en un archivo JSON con nombre específico.
        
        Args:
            expense_data: Datos completos del expense
            expense_id: ID del expense
            
        Returns:
            str: Ruta del archivo guardado
        """
        try:
            filename = f"expense_{expense_id}.json"
            filepath = os.path.join(EXTRACTION_DATA_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(expense_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Expense individual guardado: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando expense {expense_id}: {e}")
            return None

    def save_batch(self, expenses: List[Dict], batch_name: str):
        """Guarda un lote de expenses en archivo JSON."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{batch_name}_{timestamp}.json"
            filepath = os.path.join(EXTRACTION_DATA_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(expenses, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Lote guardado: {filepath}")
            
        except Exception as e:
            logger.error(f"Error guardando lote {batch_name}: {e}")
    
    def save_extraction_result(self, data: Dict, filename_prefix: str):
        """Guarda el resultado completo de una extracción."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}.json"
            filepath = os.path.join(EXTRACTION_DATA_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Resultado de extracción guardado: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando resultado: {e}")
            return None
    
    def run_initial_extraction(self, save_individual: bool = True):
        """Ejecuta la carga inicial completa desde el ID configurado."""
        logger.info("🚀 INICIANDO CARGA INICIAL")
        logger.info(f"Modo: {self.mode}")
        logger.info(f"ID inicial: {self.start_id}")
        logger.info(f"Archivos individuales: {'Sí' if save_individual else 'No'}")
        logger.info("="*60)
        
        try:
            expenses, last_id = self.extract_expenses_range(self.start_id, save_individual=save_individual)
            
            # Guardar resumen final
            summary = {
                "extraction_type": "initial",
                "start_id": self.start_id,
                "last_id": last_id,
                "total_expenses": len(expenses),
                "extraction_date": datetime.now().isoformat(),
                "mode": self.mode,
                "individual_files": save_individual
            }
            
            self.save_extraction_result(summary, "initial_extraction_summary")
            
            logger.info("✅ CARGA INICIAL COMPLETADA")
            logger.info(f"Total expenses extraídos: {len(expenses)}")
            logger.info(f"Rango procesado: {self.start_id} - {last_id}")
            
            return expenses, summary
            
        except Exception as e:
            logger.error(f"Error en carga inicial: {e}")
            raise
    
    def run_maintenance_extraction(self, days_back: int = 7):
        """Ejecuta la extracción de mantenimiento diario."""
        logger.info("🔄 INICIANDO EXTRACCIÓN DE MANTENIMIENTO")
        logger.info(f"Modo: {self.mode}")
        logger.info(f"Días hacia atrás: {days_back}")
        logger.info("="*60)
        
        try:
            expenses = self.extract_recent_expenses(days_back)
            
            # Guardar resumen
            summary = {
                "extraction_type": "maintenance",
                "days_back": days_back,
                "total_expenses": len(expenses),
                "extraction_date": datetime.now().isoformat(),
                "mode": self.mode
            }
            
            self.save_extraction_result(summary, "maintenance_extraction_summary")
            
            logger.info("✅ EXTRACCIÓN DE MANTENIMIENTO COMPLETADA")
            logger.info(f"Total expenses extraídos: {len(expenses)}")
            
            return expenses, summary
            
        except Exception as e:
            logger.error(f"Error en extracción de mantenimiento: {e}")
            raise
    
    def run(self):
        """Ejecuta la extracción según el modo configurado."""
        if self.mode == "initial":
            return self.run_initial_extraction()
        elif self.mode == "maintenance":
            return self.run_maintenance_extraction()
        else:
            raise ValueError(f"Modo no válido: {self.mode}. Use 'initial' o 'maintenance'")

def main():
    """Función principal."""
    try:
        extractor = ExpenseExtractor()
        expenses, summary = extractor.run()
        
        print("="*60)
        print("✅ EXTRACCIÓN COMPLETADA")
        print(f"Modo: {summary['extraction_type']}")
        print(f"Total expenses: {summary['total_expenses']}")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Error en extracción: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
