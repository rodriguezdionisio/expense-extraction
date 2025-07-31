#!/usr/bin/env python3
"""
Script de ejecución para el procesamiento de expenses.
Convierte archivos JSON extraídos a CSV aplanado.

Modos de uso:
- python run_processing.py i            print(f"📅 Fechas únicas: {summary.get('unique_dates', 0)}")
            print(f"📁 Archivos nuevos: {summary.get('files_created', 0)}")
            print(f"📔 Archivos actualizados: {summary.get('files_updated', 0)}")tial
- python run_processing.py maintenance  
- python run_processing.py range <start_id> <end_id>
"""

import sys
import argparse
from expense_processor import ExpenseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)

def show_help():
    """Muestra información de ayuda detallada."""
    help_text = """
╔══════════════════════════════════════════════════════════════╗
║                    PROCESADOR DE EXPENSES                    ║
║              Convierte JSON extraídos a CSV                  ║
╚══════════════════════════════════════════════════════════════╝

COMANDOS DISPONIBLES:

📋 PROCESAMIENTO INICIAL:
   python run_processing.py initial
   
   • Procesa TODOS los archivos expense_*.json encontrados
   • Ideal para procesar la carga histórica completa
   • Genera: expenses_initial_YYYYMMDD_HHMMSS.csv

🔄 PROCESAMIENTO DE MANTENIMIENTO:
   python run_processing.py maintenance
   
   • Procesa todos los archivos JSON disponibles
   • Ideal para procesar actualizaciones recientes
   • Genera: expenses_maintenance_YYYYMMDD_HHMMSS.csv

📊 PROCESAMIENTO POR RANGO:
   python run_processing.py range <start_id> <end_id>
   
   Ejemplos:
   python run_processing.py range 1 100      # Procesa IDs 1-100
   python run_processing.py range 500 510    # Procesa IDs 500-510
   
   • Procesa solo los IDs especificados en el rango
   • Genera: expenses_range_<start>_<end>_YYYYMMDD_HHMMSS.csv

📁 ESTRUCTURA DE SALIDA:
   
   processed_data/
   ├── date=2019-10-27/
   │   ├── fact_expenses.csv
   │   └── fact_expense_orders.csv
   ├── date=2019-11-06/
   │   ├── fact_expenses.csv
   │   └── fact_expense_orders.csv
   └── date=2020-01-15/
       ├── fact_expenses.csv
       └── fact_expense_orders.csv

📋 FORMATO CSV:
   
   FACT_EXPENSES.CSV (por fecha):
   • expense_id, amount, canceled, date, created_at
   • cash_register_id, cash_register_name
   • payment_method_id, payment_method_code, payment_method_name
   • provider_id, provider_name
   • user_id, user_name
   • receipt_type_id, receipt_type_name
   
   FACT_EXPENSE_ORDERS.CSV (por fecha):
   • expense_item_id, expense_id (FK), canceled, detail, price, quantity
   • product_id, product_name, product_cost, product_unit
   • ingredient_id, ingredient_name, ingredient_cost, ingredient_unit
   
   Particionado por fecha: Cada directorio date=YYYY-MM-DD contiene 
   solo los expenses y expense_items de esa fecha específica

❓ AYUDA:
   python run_processing.py --help
   python run_processing.py -h

════════════════════════════════════════════════════════════════
"""
    print(help_text)

def main():
    """Función principal del script."""
    
    # Si no hay argumentos, mostrar ayuda
    if len(sys.argv) == 1:
        show_help()
        return
    
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(
        description="Procesador de expenses - Convierte JSON a CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
    
    # Comando inicial
    initial_parser = subparsers.add_parser(
        'initial', 
        help='Procesamiento inicial completo'
    )
    
    # Comando mantenimiento
    maintenance_parser = subparsers.add_parser(
        'maintenance', 
        help='Procesamiento de mantenimiento'
    )
    
    # Comando rango
    range_parser = subparsers.add_parser(
        'range', 
        help='Procesamiento por rango de IDs'
    )
    range_parser.add_argument(
        'start_id', 
        type=int, 
        help='ID inicial del rango'
    )
    range_parser.add_argument(
        'end_id', 
        type=int, 
        help='ID final del rango'
    )
    
    # Parsear argumentos
    args = parser.parse_args()
    
    # Validar comando
    if not args.command:
        show_help()
        return
    
    try:
        processor = ExpenseProcessor()
        
        if args.command == 'initial':
            logger.info("Ejecutando procesamiento inicial...")
            expenses_df, expense_items_df, summary = processor.run_initial_processing()
            
        elif args.command == 'maintenance':
            logger.info("Ejecutando procesamiento de mantenimiento...")
            expenses_df, expense_items_df, summary = processor.run_maintenance_processing()
            
        elif args.command == 'range':
            start_id = args.start_id
            end_id = args.end_id
            
            if start_id > end_id:
                print("❌ Error: start_id debe ser menor o igual que end_id")
                return
            
            logger.info(f"Ejecutando procesamiento por rango: {start_id}-{end_id}")
            expenses_df, expense_items_df, summary = processor.run_range_processing(start_id, end_id)
        
        # Mostrar resultado
        if summary:
            print("\n" + "="*70)
            print("✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE")
            print("="*70)
            print(f"📊 Tipo: {summary['processing_type']}")
            if 'start_id' in summary:
                print(f"📋 Rango: {summary['start_id']} - {summary['end_id']}")
            print(f"🏢 Total expenses: {summary['total_expenses']}")
            print(f"📦 Total expense items: {summary['total_expense_items']}")
            print(f"� Fechas únicas: {summary.get('unique_dates', 0)}")
            print(f"📁 Archivos creados: {summary.get('total_files_created', 0)}")
            
            # Mostrar estructura de archivos por fecha
            partitioned_files = summary.get('partitioned_files', {})
            
            if partitioned_files.get('files_created'):
                print(f"\n📂 Archivos CREADOS:")
                for date_info in partitioned_files['files_created'][:2]:  # Mostrar primeros 2
                    print(f"   📅 date={date_info['date']}/")
                    print(f"      ├── fact_expenses.csv ({date_info['new_expenses_count']} expenses)")
                    print(f"      └── fact_expense_orders.csv ({date_info['new_expense_items_count']} items)")
                if len(partitioned_files['files_created']) > 2:
                    remaining = len(partitioned_files['files_created']) - 2
                    print(f"   └── ... y {remaining} fechas más creadas")
            
            if partitioned_files.get('files_updated'):
                print(f"\n📔 Archivos ACTUALIZADOS:")
                for date_info in partitioned_files['files_updated'][:2]:  # Mostrar primeros 2
                    print(f"   📅 date={date_info['date']}/")
                    print(f"      ├── fact_expenses.csv (+{date_info['new_expenses_count']} → {date_info['total_expenses_count']} total)")
                    print(f"      └── fact_expense_orders.csv (+{date_info['new_expense_items_count']} → {date_info['total_expense_items_count']} total)")
                if len(partitioned_files['files_updated']) > 2:
                    remaining = len(partitioned_files['files_updated']) - 2
                    print(f"   └── ... y {remaining} fechas más actualizadas")
            
            print(f"\n📅 Fecha de procesamiento: {summary['processing_date']}")
            print("="*70)
        else:
            print("\n❌ No se procesaron datos")
            
    except KeyboardInterrupt:
        print("\n🛑 Procesamiento interrumpido por el usuario")
        
    except Exception as e:
        logger.error(f"Error durante el procesamiento: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
