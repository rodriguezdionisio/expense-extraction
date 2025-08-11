#!/usr/bin/env python3
"""
Script de prueba para verificar la descarga del log desde GCS.
"""

import os
import sys
from expense_extractor import ExpenseExtractor
from utils.logger import get_logger

logger = get_logger(__name__)

def test_log_download():
    """Prueba la funcionalidad de descarga del log desde GCS."""
    
    # Eliminar el archivo de log local si existe
    log_file = "logs/extracted_expenses_log.txt"
    if os.path.exists(log_file):
        os.remove(log_file)
        logger.info(f"Archivo de log local eliminado: {log_file}")
    
    # Crear una instancia del extractor (esto debería intentar descargar el log)
    logger.info("Creando instancia de ExpenseExtractor...")
    extractor = ExpenseExtractor()
    
    # Verificar si se descargó el log y obtener los IDs extraídos
    extracted_ids = extractor.get_extracted_ids()
    
    if extracted_ids:
        logger.info(f"✅ Log descargado correctamente. IDs extraídos encontrados: {len(extracted_ids)}")
        logger.info(f"Rango de IDs: {min(extracted_ids)} - {max(extracted_ids)}")
    else:
        logger.info("📄 No se encontraron IDs extraídos (archivo de log vacío o no existe en GCS)")
    
    # Verificar si el archivo local ahora existe
    if os.path.exists(log_file):
        logger.info(f"✅ Archivo de log local ahora existe: {log_file}")
        with open(log_file, 'r') as f:
            lines = f.readlines()
            logger.info(f"Número de líneas en el archivo: {len(lines)}")
    else:
        logger.info(f"❌ Archivo de log local no existe: {log_file}")

if __name__ == "__main__":
    try:
        test_log_download()
    except Exception as e:
        logger.error(f"Error en la prueba: {e}")
        sys.exit(1)
