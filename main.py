#!/usr/bin/env python3
"""
Script principal para orquestar la extracción y procesamiento de datos de Fudo.

Este script ejecuta:
1. Extracción de datos raw en formato JSON (por página o por fecha)
2. Procesamiento y aplanado de JSON a CSV
"""

import sys
from pathlib import Path
from utils.logger import get_logger

# Importar nuestros módulos
from src.extract_expenses import FudoRawExtractor
from src.process_expenses import FudoDataProcessor

logger = get_logger(__name__)

def run_extraction_by_date(start_date=None, end_date=None, partition_by="month"):
    """Ejecuta solo la extracción por fecha."""
    try:
        print(f"🔄 EJECUTANDO EXTRACCIÓN POR FECHA ({partition_by.upper()})")
        print("="*60)
        print(f"📅 Período: {start_date or 'inicio'} a {end_date or 'fin'}")
        print(f"📂 Partición: {partition_by}")
        print()
        
        # EXTRACCIÓN
        extractor = FudoRawExtractor()
        extractor.get_token()
        
        result = extractor.extract_expenses_by_date(
            start_date=start_date,
            end_date=end_date,
            partition_by=partition_by
        )
        
        # RESUMEN
        print()
        print("="*60)
        print("RESUMEN DE EXTRACCIÓN POR FECHA COMPLETADA")
        print("="*60)
        print(f"📊 Total registros: {result['summary']['total_records']}")
        print(f"📁 Particiones creadas: {result['summary']['partitions_created']}")
        print(f"🗂️ Archivos generados: {result['summary']['files_created']}")
        print(f"📂 Directorio: raw_data/expenses_by_date/")
        print("="*60)
        
        return result
        
    except Exception as e:
        logger.critical(f"Error en la extracción por fecha: {e}")
        raise

def run_processing_only():
    """Ejecuta solo el procesamiento de datos JSON existentes."""
    try:
        print("🔄 EJECUTANDO SOLO PROCESAMIENTO")
        print("="*60)
        
        processor = FudoDataProcessor()
        results = processor.process_all()
        
        print()
        print("="*60)
        print("RESUMEN DE PROCESAMIENTO COMPLETADO")
        print("="*60)
        
        if results['expenses'] is not None:
            print(f"✅ EXPENSES: {len(results['expenses'])} registros procesados")
        
        print(f"📁 Archivos CSV guardados en: processed_data")
        print("="*60)
        print("✅ Procesamiento completado")
        
        return results
        
    except Exception as e:
        logger.critical(f"Error en el procesamiento: {e}")
        raise

def main():
    """Función principal con opciones de línea de comandos."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline de extracción de datos Fudo por fecha')
    parser.add_argument('--mode', choices=['extract-by-date', 'process'], 
                       default='extract-by-date', help='Modo de ejecución')
    parser.add_argument('--start-date', type=str,
                       help='Fecha de inicio (YYYY-MM-DD). Si no se especifica, extrae todos los datos')
    parser.add_argument('--end-date', type=str,
                       help='Fecha de fin (YYYY-MM-DD). Si no se especifica, extrae hasta el final')
    parser.add_argument('--partition-by', 
                       choices=['day', 'month', 'year'],
                       default='day',
                       help='Criterio de partición. Recomendado: day para máxima granularidad, month para data lake')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'extract-by-date':
            return run_extraction_by_date(
                start_date=args.start_date,
                end_date=args.end_date,
                partition_by=args.partition_by
            )
        
        elif args.mode == 'process':
            return run_processing_only()
            
    except KeyboardInterrupt:
        print("⚠️  Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
