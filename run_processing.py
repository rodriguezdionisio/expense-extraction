#!/usr/bin/env python3
"""
Script simplificado para el procesamiento de expenses.
Convierte archivos JSON extraídos a CSV particionado por fecha.
"""

import sys
import os
from expense_processor import ExpenseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)

def process_range(start_id: int, end_id: int):
    """Procesa expenses en un rango específico de IDs."""
    print(f"🔄 PROCESANDO EXPENSES {start_id}-{end_id}")
    print("="*60)
    
    processor = ExpenseProcessor()
    expenses_df, expense_items_df, summary = processor.run_range_processing(start_id, end_id)
    
    if summary:
        print("\n✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        print("="*60)
        print(f"📊 Tipo: {summary['processing_type']}")
        print(f"📋 Rango: {summary['start_id']} - {summary['end_id']}")
        print(f"🏢 Total expenses: {summary['total_expenses']}")
        print(f"📦 Total expense items: {summary['total_expense_items']}")
        print(f"📅 Fechas únicas: {summary['unique_dates']}")
        print(f"📁 Archivos creados: {summary['files_created']}")
        print(f"📔 Archivos actualizados: {summary['files_updated']}")
        print("="*60)
    else:
        print("❌ No se procesaron datos")
    
    return summary

def process_all_available():
    """Procesa todos los archivos JSON disponibles."""
    extraction_dir = "extraction_data"
    
    if not os.path.exists(extraction_dir):
        print("❌ Error: No existe el directorio extraction_data/")
        return None
    
    # Encontrar archivos JSON de expenses
    files = [f for f in os.listdir(extraction_dir) if f.startswith('expense_') and f.endswith('.json')]
    
    if not files:
        print("❌ Error: No se encontraron archivos expense_*.json para procesar")
        return None
    
    # Extraer rango de IDs
    ids = []
    for f in files:
        try:
            id_num = int(f.replace('expense_', '').replace('.json', ''))
            ids.append(id_num)
        except ValueError:
            continue
    
    if not ids:
        print("❌ Error: No se pudieron extraer IDs válidos")
        return None
    
    start_id, end_id = min(ids), max(ids)
    print(f"📁 Encontrados {len(files)} archivos (IDs {start_id}-{end_id})")
    
    return process_range(start_id, end_id)

def main():
    """Función principal."""
    if len(sys.argv) == 1:
        # Sin argumentos: procesar todos los archivos disponibles
        process_all_available()
    elif len(sys.argv) == 3:
        # Con argumentos: procesar rango específico
        try:
            start_id = int(sys.argv[1])
            end_id = int(sys.argv[2])
            
            if start_id <= 0 or end_id <= 0 or start_id > end_id:
                print("❌ Error: Los IDs deben ser números positivos y start_id <= end_id")
                sys.exit(1)
            
            process_range(start_id, end_id)
            
        except ValueError:
            print("❌ Error: Los argumentos deben ser números enteros")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error en procesamiento: {e}")
            print(f"❌ Error: {e}")
            sys.exit(1)
    else:
        print("Uso:")
        print("  python run_processing_simple.py                    # Procesa todos los archivos disponibles")
        print("  python run_processing_simple.py <start_id> <end_id> # Procesa rango específico")
        print("\nEjemplos:")
        print("  python run_processing_simple.py")
        print("  python run_processing_simple.py 1 20")
        sys.exit(1)

if __name__ == "__main__":
    main()
