#!/usr/bin/env python3
"""
Script para verificar la sincronización entre archivos locales y GCS
"""

import os
from utils.gcp import get_storage_client
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def verify_local_vs_gcs():
    """Compara archivos locales con los de GCS"""
    print("🔄 VERIFICANDO SINCRONIZACIÓN LOCAL vs GCS")
    print("="*60)
    
    try:
        client = get_storage_client()
        if not client:
            print("❌ No se pudo conectar a GCS")
            return
            
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        
        # Verificar archivos JSON
        print("\n📁 ARCHIVOS JSON:")
        local_json = set()
        for file in os.listdir("raw/"):
            if file.startswith("expense_") and file.endswith(".json"):
                local_json.add(file)
        
        gcs_json = set()
        for blob in bucket.list_blobs(prefix="raw/fact_expenses/expense_"):
            if blob.name.endswith(".json"):
                filename = os.path.basename(blob.name)
                gcs_json.add(filename)
        
        print(f"Local: {len(local_json)} archivos")
        print(f"GCS:   {len(gcs_json)} archivos")
        
        missing_in_gcs = local_json - gcs_json
        missing_locally = gcs_json - local_json
        
        if missing_in_gcs:
            print(f"❌ Faltan en GCS: {missing_in_gcs}")
        if missing_locally:
            print(f"❌ Faltan localmente: {missing_locally}")
        if not missing_in_gcs and not missing_locally:
            print("✅ Archivos JSON sincronizados correctamente")
        
        # Verificar archivos Parquet
        print("\n📊 ARCHIVOS PARQUET:")
        local_parquet = set()
        for root, dirs, files in os.walk("clean/"):
            for file in files:
                if file.endswith(".parquet"):
                    rel_path = os.path.relpath(os.path.join(root, file), "clean/")
                    local_parquet.add(rel_path)
        
        gcs_parquet = set()
        for blob in bucket.list_blobs(prefix="clean/"):
            if blob.name.endswith(".parquet") and ("fact_expenses" in blob.name or "fact_expense_orders" in blob.name):
                rel_path = blob.name.replace("clean/", "")
                gcs_parquet.add(rel_path)
        
        print(f"Local: {len(local_parquet)} archivos Parquet de expenses")
        print(f"GCS:   {len(gcs_parquet)} archivos Parquet de expenses")
        
        missing_parquet_gcs = local_parquet - gcs_parquet
        missing_parquet_local = gcs_parquet - local_parquet
        
        if missing_parquet_gcs:
            print(f"❌ Faltan en GCS: {missing_parquet_gcs}")
        if missing_parquet_local:
            print(f"❌ Faltan localmente: {missing_parquet_local}")
        if not missing_parquet_gcs and not missing_parquet_local:
            print("✅ Archivos Parquet sincronizados correctamente")
        
        print(f"\n✅ VERIFICACIÓN COMPLETADA")
        
    except Exception as e:
        logger.error(f"Error verificando sincronización: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_local_vs_gcs()
