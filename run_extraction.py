#!/usr/bin/env python3
"""Script simplificado de extracción."""

import sys
from expense_extractor import ExpenseExtractor
from utils.logger import get_logger

logger = get_logger(__name__)

def main():
    if len(sys.argv) != 3:
        print("Uso: python run_extraction_simple.py <start_id> <end_id>")
        print("Ejemplo: python run_extraction_simple.py 1 20")
        sys.exit(1)
    
    try:
        start_id, end_id = int(sys.argv[1]), int(sys.argv[2])
        
        if start_id <= 0 or end_id <= 0 or start_id > end_id:
            print("❌ Error: IDs deben ser positivos y start_id <= end_id")
            sys.exit(1)
        
        print(f"�� EXTRAYENDO EXPENSES {start_id}-{end_id}")
        print("="*60)
        
        extractor = ExpenseExtractor()
        extractor.initialize_log_from_existing_files()
        expenses, count = extractor.extract_range(start_id, end_id)
        
        print(f"\n✅ EXTRACCIÓN COMPLETADA")
        print(f"📊 Expenses extraídos: {count}")
        print(f"📁 Archivos guardados en raw/")
        print("="*60)
        
    except ValueError:
        print("❌ Error: Los argumentos deben ser números enteros")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error en extracción: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
