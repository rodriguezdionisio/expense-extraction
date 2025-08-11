#!/usr/bin/env python3
"""
Orquestador principal para automatizar extracción, procesamiento y almacenamiento
de expenses desde la API de Fudo hacia Google Cloud Storage.
"""

import sys
import argparse
import time
from datetime import datetime
from typing import Optional, Tuple
from expense_extractor import ExpenseExtractor
from expense_processor import ExpenseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)

class ExpenseOrchestrator:
    """Orquestador principal para automatizar el flujo completo de expenses."""
    
    def __init__(self):
        self.extractor = ExpenseExtractor()
        self.processor = ExpenseProcessor()
    
    def get_next_ids_to_process(self, batch_size: int = 10) -> Tuple[int, int]:
        """
        Determina el próximo rango de IDs a procesar basado en el log de extraídos.
        
        Args:
            batch_size: Tamaño del lote a procesar
            
        Returns:
            Tuple con (start_id, end_id) del próximo rango
        """
        extracted_ids = self.extractor.get_extracted_ids()
        
        if not extracted_ids:
            # Si no hay IDs extraídos, empezar desde 1
            start_id = 1
        else:
            # Continuar desde el último ID extraído + 1
            start_id = max(extracted_ids) + 1
        
        end_id = start_id + batch_size - 1
        
        logger.info(f"Próximo rango a procesar: {start_id} - {end_id}")
        return start_id, end_id
    
    def extract_batch(self, start_id: int, end_id: int) -> Tuple[bool, int]:
        """
        Extrae un lote de expenses desde la API.
        
        Args:
            start_id: ID inicial del rango
            end_id: ID final del rango
            
        Returns:
            Tuple con (éxito, cantidad_extraída)
        """
        try:
            logger.info(f"🔄 INICIANDO EXTRACCIÓN DE LOTE {start_id}-{end_id}")
            print(f"🔄 EXTRAYENDO EXPENSES {start_id}-{end_id}")
            print("="*60)
            
            # Inicializar log si es necesario
            self.extractor.initialize_log_from_existing_files()
            
            # Extraer rango
            expenses, count = self.extractor.extract_range(start_id, end_id)
            
            print(f"✅ EXTRACCIÓN COMPLETADA")
            print(f"📊 Expenses extraídos: {count}")
            print(f"📁 Archivos guardados en raw/")
            
            logger.info(f"✅ Extracción completada: {count} expenses extraídos")
            return True, count
            
        except Exception as e:
            logger.error(f"❌ Error en extracción del lote {start_id}-{end_id}: {e}")
            print(f"❌ Error en extracción: {e}")
            return False, 0
    
    def process_batch(self, start_id: int, end_id: int) -> bool:
        """
        Procesa un lote de expenses a formato Parquet.
        
        Args:
            start_id: ID inicial del rango
            end_id: ID final del rango
            
        Returns:
            True si el procesamiento fue exitoso
        """
        try:
            logger.info(f"🔄 INICIANDO PROCESAMIENTO DE LOTE {start_id}-{end_id}")
            print(f"🔄 PROCESANDO EXPENSES {start_id}-{end_id}")
            print("="*60)
            
            # Procesar rango
            result = self.processor.process_range(start_id, end_id)
            
            print(f"✅ PROCESAMIENTO COMPLETADO")
            print(f"📊 Archivos creados: {result['files_created']}")
            print(f"📔 Archivos actualizados: {result['files_updated']}")
            
            logger.info(f"✅ Procesamiento completado: {result['files_created']} nuevos, {result['files_updated']} actualizados")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en procesamiento del lote {start_id}-{end_id}: {e}")
            print(f"❌ Error en procesamiento: {e}")
            return False
    
    def run_full_pipeline(self, start_id: int, end_id: int) -> bool:
        """
        Ejecuta el pipeline completo: extracción + procesamiento.
        
        Args:
            start_id: ID inicial del rango
            end_id: ID final del rango
            
        Returns:
            True si todo el pipeline fue exitoso
        """
        logger.info(f"🚀 INICIANDO PIPELINE COMPLETO {start_id}-{end_id}")
        print(f"🚀 INICIANDO PIPELINE COMPLETO EXPENSES {start_id}-{end_id}")
        print("="*80)
        
        # Paso 1: Extracción
        success, count = self.extract_batch(start_id, end_id)
        if not success:
            return False
        
        if count == 0:
            print("⏭️  No hay nuevos expenses para procesar")
            logger.info("No hay nuevos expenses para procesar")
            return True
        
        # Pequeña pausa entre procesos
        time.sleep(1)
        
        # Paso 2: Procesamiento
        success = self.process_batch(start_id, end_id)
        if not success:
            return False
        
        print("="*80)
        print(f"🎉 PIPELINE COMPLETADO EXITOSAMENTE PARA {start_id}-{end_id}")
        print(f"📊 Total expenses procesados: {count}")
        print(f"🌩️  Datos sincronizados en Google Cloud Storage")
        print("="*80)
        
        logger.info(f"🎉 Pipeline completado exitosamente para rango {start_id}-{end_id}")
        return True
    
    def run_auto_batch(self, batch_size: int = 10) -> bool:
        """
        Ejecuta automáticamente el próximo lote basado en el log de extraídos.
        
        Args:
            batch_size: Tamaño del lote a procesar
            
        Returns:
            True si el lote fue procesado exitosamente
        """
        start_id, end_id = self.get_next_ids_to_process(batch_size)
        return self.run_full_pipeline(start_id, end_id)
    
    def run_continuous(self, batch_size: int = 10, max_batches: int = 0, delay_seconds: int = 60) -> None:
        """
        Ejecuta el procesamiento de forma continua en lotes.
        
        Args:
            batch_size: Tamaño de cada lote
            max_batches: Máximo número de lotes (0 = ilimitado)
            delay_seconds: Segundos de espera entre lotes
        """
        logger.info(f"🔄 INICIANDO PROCESAMIENTO CONTINUO")
        print(f"🔄 INICIANDO PROCESAMIENTO CONTINUO")
        print(f"📊 Lotes de {batch_size} expenses")
        print(f"⏱️  Pausa entre lotes: {delay_seconds} segundos")
        if max_batches > 0:
            print(f"🎯 Máximo {max_batches} lotes")
        print("="*80)
        
        batch_count = 0
        while True:
            batch_count += 1
            
            print(f"\n🔢 LOTE #{batch_count}")
            success = self.run_auto_batch(batch_size)
            
            if not success:
                print(f"❌ Error en lote #{batch_count}, deteniendo procesamiento continuo")
                logger.error(f"Error en lote #{batch_count}, deteniendo procesamiento continuo")
                break
            
            # Verificar límite de lotes
            if max_batches > 0 and batch_count >= max_batches:
                print(f"🎯 Alcanzado límite de {max_batches} lotes")
                logger.info(f"Alcanzado límite de {max_batches} lotes")
                break
            
            # Pausa entre lotes
            if delay_seconds > 0:
                print(f"⏸️  Pausando {delay_seconds} segundos antes del próximo lote...")
                time.sleep(delay_seconds)
        
        print(f"\n🏁 PROCESAMIENTO CONTINUO FINALIZADO")
        print(f"📊 Total de lotes procesados: {batch_count}")


def main():
    """Función principal con interfaz de línea de comandos."""
    parser = argparse.ArgumentParser(description="Orquestador de expenses - Extracción, Procesamiento y Almacenamiento")
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
    
    # Comando: range - Procesar rango específico
    range_parser = subparsers.add_parser('range', help='Procesar rango específico de IDs')
    range_parser.add_argument('start_id', type=int, help='ID inicial del rango')
    range_parser.add_argument('end_id', type=int, help='ID final del rango')
    
    # Comando: auto - Procesar próximo lote automáticamente
    auto_parser = subparsers.add_parser('auto', help='Procesar próximo lote automáticamente')
    auto_parser.add_argument('--batch-size', type=int, default=10, help='Tamaño del lote (default: 10)')
    
    # Comando: continuous - Procesamiento continuo
    continuous_parser = subparsers.add_parser('continuous', help='Procesamiento continuo en lotes')
    continuous_parser.add_argument('--batch-size', type=int, default=10, help='Tamaño de cada lote (default: 10)')
    continuous_parser.add_argument('--max-batches', type=int, default=0, help='Máximo número de lotes (0 = ilimitado)')
    continuous_parser.add_argument('--delay', type=int, default=60, help='Segundos entre lotes (default: 60)')
    
    # Comando: extract - Solo extracción
    extract_parser = subparsers.add_parser('extract', help='Solo extracción (sin procesamiento)')
    extract_parser.add_argument('start_id', type=int, help='ID inicial del rango')
    extract_parser.add_argument('end_id', type=int, help='ID final del rango')
    
    # Comando: process - Solo procesamiento
    process_parser = subparsers.add_parser('process', help='Solo procesamiento (sin extracción)')
    process_parser.add_argument('start_id', type=int, help='ID inicial del rango')
    process_parser.add_argument('end_id', type=int, help='ID final del rango')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Inicializar orquestador
    orchestrator = ExpenseOrchestrator()
    
    try:
        if args.command == 'range':
            # Validar rango
            if args.start_id <= 0 or args.end_id <= 0 or args.start_id > args.end_id:
                print("❌ Error: IDs deben ser positivos y start_id <= end_id")
                sys.exit(1)
            
            success = orchestrator.run_full_pipeline(args.start_id, args.end_id)
            sys.exit(0 if success else 1)
        
        elif args.command == 'auto':
            success = orchestrator.run_auto_batch(args.batch_size)
            sys.exit(0 if success else 1)
        
        elif args.command == 'continuous':
            orchestrator.run_continuous(args.batch_size, args.max_batches, args.delay)
        
        elif args.command == 'extract':
            if args.start_id <= 0 or args.end_id <= 0 or args.start_id > args.end_id:
                print("❌ Error: IDs deben ser positivos y start_id <= end_id")
                sys.exit(1)
            
            success, count = orchestrator.extract_batch(args.start_id, args.end_id)
            sys.exit(0 if success else 1)
        
        elif args.command == 'process':
            if args.start_id <= 0 or args.end_id <= 0 or args.start_id > args.end_id:
                print("❌ Error: IDs deben ser positivos y start_id <= end_id")
                sys.exit(1)
            
            success = orchestrator.process_batch(args.start_id, args.end_id)
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print("\n⏹️  Operación cancelada por el usuario")
        logger.info("Operación cancelada por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error inesperado en main: {e}")
        print(f"❌ Error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
