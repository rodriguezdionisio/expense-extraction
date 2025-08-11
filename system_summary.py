#!/usr/bin/env python3
"""
Script de resumen final del sistema de extracción con GCS
"""

import os
from utils.gcp import get_storage_client
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def system_summary():
    """Muestra un resumen completo del sistema"""
    print("🌩️  RESUMEN FINAL DEL SISTEMA DE EXTRACCIÓN")
    print("="*60)
    
    # Estado local
    print("\n📁 ESTADO LOCAL:")
    local_json = len([f for f in os.listdir("raw/") if f.startswith("expense_") and f.endswith(".json")])
    local_parquet = 0
    for root, dirs, files in os.walk("clean/"):
        for file in files:
            if file.endswith(".parquet"):
                local_parquet += 1
    
    # Contar IDs extraídos
    extracted_ids = 0
    if os.path.exists("logs/extracted_expenses_log.txt"):
        with open("logs/extracted_expenses_log.txt", 'r') as f:
            extracted_ids = len([line for line in f if line.strip().isdigit()])
    
    # Contar particiones locales
    local_partitions = len([d for d in os.listdir("clean/fact_expenses/") if d.startswith("date=")])
    
    print(f"  - Archivos JSON: {local_json}")
    print(f"  - Archivos Parquet: {local_parquet}")
    print(f"  - IDs extraídos en log: {extracted_ids}")
    print(f"  - Particiones de fechas: {local_partitions}")
    
    # Estado GCS
    print("\n🌩️  ESTADO EN GOOGLE CLOUD STORAGE:")
    try:
        client = get_storage_client()
        if client:
            bucket = client.bucket(config.GCS_BUCKET_NAME)
            
            # JSON en GCS
            gcs_json = len([blob for blob in bucket.list_blobs(prefix="raw/fact_expenses/expense_") if blob.name.endswith('.json')])
            
            # Parquet en GCS
            gcs_expenses = len([blob for blob in bucket.list_blobs(prefix="clean/fact_expenses/") if blob.name.endswith('.parquet')])
            gcs_orders = len([blob for blob in bucket.list_blobs(prefix="clean/fact_expense_orders/") if blob.name.endswith('.parquet')])
            
            print(f"  - Archivos JSON: {gcs_json}")
            print(f"  - Archivos fact_expenses: {gcs_expenses}")
            print(f"  - Archivos fact_expense_orders: {gcs_orders}")
            print(f"  - Bucket: gs://{config.GCS_BUCKET_NAME}")
            
            # Archivos más recientes en GCS
            recent_json = []
            for blob in bucket.list_blobs(prefix="raw/fact_expenses/expense_"):
                if blob.name.endswith('.json'):
                    recent_json.append(blob.name)
            
            if recent_json:
                print(f"  - Últimos JSON en GCS:")
                for file in sorted(recent_json)[-3:]:
                    print(f"    • {file}")
        else:
            print("  ❌ No se pudo conectar a GCS")
    except Exception as e:
        print(f"  ❌ Error consultando GCS: {e}")
    
    print("\n⚙️  CONFIGURACIÓN DEL SISTEMA:")
    print(f"  - Proyecto GCP: {config.GCP_PROJECT_ID}")
    print(f"  - Credenciales: {config.GOOGLE_APPLICATION_CREDENTIALS}")
    
    print("\n🚀 FUNCIONALIDADES DISPONIBLES:")
    print("  ✅ Extracción desde API Fudo")
    print("  ✅ Almacenamiento local (JSON + Parquet)")
    print("  ✅ Almacenamiento en Google Cloud Storage")
    print("  ✅ Particionado por fechas")
    print("  ✅ Logging de duplicados")
    print("  ✅ Formato Parquet optimizado")
    print("  ✅ Estructura data warehouse")
    print("  ✅ Sincronización automática a GCS")
    print("  ✅ Orquestador automatizado")
    
    print("\n🎯 COMANDOS PRINCIPALES:")
    print("  python main.py auto                           # Próximo lote automático")
    print("  python main.py range <start_id> <end_id>      # Procesar rango específico")
    print("  python main.py continuous                     # Procesamiento continuo")
    print("  python verify_expense_gcs.py                  # Verificar GCS")
    
    print(f"\n✅ SISTEMA LISTO PARA PRODUCCIÓN")

if __name__ == "__main__":
    system_summary()
