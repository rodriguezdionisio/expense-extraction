#!/usr/bin/env python3
"""Script simplificado de procesamiento."""

import sys
from expense_processor import ExpenseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)

def main():
    if len(sys.argv) != 3:
        print("Uso: python run_processing_simple.py <start_id> <end_id>")
        print("Ejemplo: python run_processing_simple.py 1 20")
        sys.exit(1)
    
    try:
        start_id, end_id = int(sys.argv[1]), int(sys.argv[2])
        
        if start_id <= 0 or end_id <= 0 or start_id > end_id:
            print("❌ Error: IDs deben ser positivos y start_id <= end_id")
            sys.exit(1)
        
        print(f"🔄 PROCESANDO EXPENSES {start_id}-{end_id}")
        print("="*60)
        
        processor = ExpenseProcessor()
        result = processor.process_range(start_id, end_id)
        
        print("✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        print("="*60)
        print(f"📊 Tipo: range")
        print(f"📋 Rango: {start_id} - {end_id}")
        print(f"📁 Archivos creados: {result['files_created']}")
        print(f"📔 Archivos actualizados: {result['files_updated']}")
        print("="*60)
        
    except ValueError:
        print("❌ Error: Los argumentos deben ser números enteros")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error en procesamiento: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
