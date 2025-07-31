#!/usr/bin/env python3
"""
Script de utilidad para ejecutar la extracción de expenses con diferentes configuraciones.
"""

import os
import sys
from expense_extractor import ExpenseExtractor
from utils.logger import get_logger

logger = get_logger(__name__)

def set_env_var(key: str, value: str):
    """Establece una variable de entorno."""
    os.environ[key] = value
    print(f"✅ {key} = {value}")

def run_initial_extraction(start_id: int = 1):
    """Ejecuta carga inicial desde el ID especificado."""
    print("🚀 CONFIGURANDO CARGA INICIAL")
    print("="*60)
    
    # Configurar variables de entorno para carga inicial
    set_env_var("EXPENSE_EXTRACTION_MODE", "initial")
    set_env_var("EXPENSE_START_ID", str(start_id))
    set_env_var("EXPENSE_PAGE_SIZE", "500")
    set_env_var("EXPENSE_MAX_PAGES", "0")
    
    print()
    
    # Ejecutar extracción
    extractor = ExpenseExtractor()
    return extractor.run()

def run_maintenance_extraction(days_back: int = 7):
    """Ejecuta extracción de mantenimiento."""
    print("🔄 CONFIGURANDO EXTRACCIÓN DE MANTENIMIENTO")
    print("="*60)
    
    # Configurar variables de entorno para mantenimiento
    set_env_var("EXPENSE_EXTRACTION_MODE", "maintenance")
    set_env_var("EXPENSE_START_ID", "500")  # No se usa en modo maintenance
    set_env_var("EXPENSE_PAGE_SIZE", "500")
    set_env_var("EXPENSE_MAX_PAGES", "0")
    
    print()
    
    # Ejecutar extracción
    extractor = ExpenseExtractor()
    return extractor.run_maintenance_extraction(days_back)

def run_range_extraction(start_id: int, end_id: int, individual_files: bool = True):
    """Ejecuta extracción en un rango específico de IDs."""
    print(f"🎯 CONFIGURANDO EXTRACCIÓN DE RANGO {start_id}-{end_id}")
    if individual_files:
        print("📄 Modo: Archivos individuales por expense")
    else:
        print("📦 Modo: Archivo de lote")
    print("="*60)
    
    # Configurar variables de entorno
    set_env_var("EXPENSE_EXTRACTION_MODE", "initial")
    set_env_var("EXPENSE_START_ID", str(start_id))
    
    print()
    
    # Ejecutar extracción con rango específico
    extractor = ExpenseExtractor()
    return extractor.extract_expenses_range(start_id, end_id, save_individual=individual_files)

def show_help():
    """Muestra la ayuda del script."""
    print("🔍 EXTRACTOR DE EXPENSES - UTILIDAD")
    print("="*60)
    print("Uso:")
    print("  python run_extraction.py initial [start_id]     # Carga inicial desde ID")
    print("  python run_extraction.py maintenance [days]     # Mantenimiento (últimos N días)")
    print("  python run_extraction.py range start_id end_id  # Rango específico de IDs")
    print("  python run_extraction.py help                   # Mostrar esta ayuda")
    print()
    print("Ejemplos:")
    print("  python run_extraction.py initial 1              # Carga completa desde ID 1")
    print("  python run_extraction.py initial 500            # Carga desde ID 500")
    print("  python run_extraction.py maintenance 7          # Últimos 7 días")
    print("  python run_extraction.py range 510 512          # IDs 510-512 (archivos individuales)")
    print()
    print("📄 ARCHIVOS GENERADOS:")
    print("  - Comando 'range': Genera archivos individuales expense_XXX.json")
    print("  - Comando 'initial': Genera archivos por lotes y resumen")
    print("  - Comando 'maintenance': Genera archivo con expenses recientes")
    print()
    print("Variables de entorno importantes:")
    print("  EXPENSE_EXTRACTION_MODE: 'initial' o 'maintenance'")
    print("  EXPENSE_START_ID: ID inicial para extracción")
    print("  EXPENSE_PAGE_SIZE: Tamaño de página (default: 500)")
    print("="*60)

def main():
    """Función principal."""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    summary = {}  # Inicializar summary
    
    try:
        if command == "help" or command == "-h" or command == "--help":
            show_help()
            return  # Salir después de mostrar ayuda
            
        elif command == "initial":
            start_id = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            expenses, summary = run_initial_extraction(start_id)
            
        elif command == "maintenance":
            days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            expenses, summary = run_maintenance_extraction(days_back)
            
        elif command == "range":
            if len(sys.argv) < 4:
                print("❌ Error: Se requieren start_id y end_id para extracción por rango")
                print("Uso: python run_extraction.py range <start_id> <end_id>")
                return
            
            start_id = int(sys.argv[2])
            end_id = int(sys.argv[3])
            expenses, last_id = run_range_extraction(start_id, end_id)
            
            summary = {
                "extraction_type": "range",
                "start_id": start_id,
                "end_id": end_id,
                "last_id": last_id,
                "total_expenses": len(expenses)
            }
            
        else:
            print(f"❌ Comando no reconocido: {command}")
            show_help()
            return
        
        print("="*60)
        print("✅ EXTRACCIÓN COMPLETADA")
        print(f"Tipo: {summary.get('extraction_type', 'unknown')}")
        print(f"Total expenses: {summary.get('total_expenses', 0)}")
        if 'start_id' in summary:
            print(f"ID inicial: {summary['start_id']}")
        if 'last_id' in summary:
            print(f"Último ID: {summary['last_id']}")
        print("="*60)
        
    except ValueError as e:
        print(f"❌ Error de valor: {e}")
        show_help()
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
