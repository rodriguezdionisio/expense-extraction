#!/usr/bin/env python3
"""
Script para verificar archivos en Google Cloud Storage
"""

from utils.gcp import get_storage_client, list_gcs_files
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def verify_gcs_files():
    """Verifica los archivos en GCS"""
    print("🌩️  VERIFICANDO ARCHIVOS EN GOOGLE CLOUD STORAGE")
    print("="*60)
    
    try:
        client = get_storage_client()
        if not client:
            print("❌ No se pudo conectar a GCS")
            return
            
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        
        # Verificar archivos JSON
        print("\n📁 ARCHIVOS JSON EN raw/:")
        raw_blobs = list(bucket.list_blobs(prefix="raw/"))
        json_files = [blob.name for blob in raw_blobs if blob.name.endswith('.json')]
        print(f"Total archivos JSON: {len(json_files)}")
        if json_files:
            print("Últimos 5 archivos:")
            for file in sorted(json_files)[-5:]:
                print(f"  - {file}")
        
        # Verificar archivos Parquet
        print("\n📊 ARCHIVOS PARQUET EN clean/:")
        clean_blobs = list(bucket.list_blobs(prefix="clean/"))
        parquet_files = [blob.name for blob in clean_blobs if blob.name.endswith('.parquet')]
        print(f"Total archivos Parquet: {len(parquet_files)}")
        
        # Agrupar por tipo
        expenses_files = [f for f in parquet_files if "fact_expenses" in f]
        orders_files = [f for f in parquet_files if "fact_expense_orders" in f]
        
        print(f"  - fact_expenses: {len(expenses_files)} archivos")
        print(f"  - fact_expense_orders: {len(orders_files)} archivos")
        
        if parquet_files:
            print("Últimos 3 archivos Parquet:")
            for file in sorted(parquet_files)[-3:]:
                print(f"  - {file}")
        
        # Verificar fechas únicas
        dates = set()
        for file in parquet_files:
            if "date=" in file:
                date_part = file.split("date=")[1].split("/")[0]
                dates.add(date_part)
        
        print(f"\n📅 PARTICIONES DE FECHAS: {len(dates)}")
        if dates:
            print("Fechas disponibles:")
            for date in sorted(dates):
                print(f"  - {date}")
        
        print(f"\n✅ VERIFICACIÓN COMPLETADA")
        print(f"Bucket: gs://{config.GCS_BUCKET_NAME}")
        
    except Exception as e:
        logger.error(f"Error verificando GCS: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_gcs_files()
