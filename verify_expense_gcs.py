#!/usr/bin/env python3
"""
Script para verificar específicamente los archivos de expenses en GCS
"""

from utils.gcp import get_storage_client
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def verify_expense_files():
    """Verifica específicamente los archivos de expenses en GCS"""
    print("🌩️  VERIFICANDO ARCHIVOS DE EXPENSES EN GCS")
    print("="*60)
    
    try:
        client = get_storage_client()
        if not client:
            print("❌ No se pudo conectar a GCS")
            return
            
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        
        # Verificar archivos JSON de expenses
        print("\n📁 ARCHIVOS JSON DE EXPENSES:")
        raw_blobs = list(bucket.list_blobs(prefix="raw/fact_expenses/expense_"))
        expense_json_files = sorted([blob.name for blob in raw_blobs if blob.name.endswith('.json')])
        print(f"Total archivos expense JSON: {len(expense_json_files)}")
        for file in expense_json_files:
            print(f"  - {file}")
        
        # Verificar logs
        print("\n📋 ARCHIVOS DE LOG:")
        log_blobs = list(bucket.list_blobs(prefix="logs/"))
        log_files = [blob.name for blob in log_blobs]
        print(f"Total archivos de log: {len(log_files)}")
        for file in log_files:
            print(f"  - {file}")
        
        # Verificar archivos Parquet de expenses específicamente
        print("\n📊 ARCHIVOS PARQUET DE EXPENSES:")
        
        # fact_expenses
        expenses_blobs = list(bucket.list_blobs(prefix="clean/fact_expenses/"))
        expenses_parquet = [blob.name for blob in expenses_blobs if blob.name.endswith('.parquet')]
        print(f"\nfact_expenses files: {len(expenses_parquet)}")
        for file in sorted(expenses_parquet):
            print(f"  - {file}")
        
        # fact_expense_orders  
        orders_blobs = list(bucket.list_blobs(prefix="clean/fact_expense_orders/"))
        orders_parquet = [blob.name for blob in orders_blobs if blob.name.endswith('.parquet')]
        print(f"\nfact_expense_orders files: {len(orders_parquet)}")
        for file in sorted(orders_parquet):
            print(f"  - {file}")
        
        print(f"\n✅ VERIFICACIÓN DE EXPENSES COMPLETADA")
        
    except Exception as e:
        logger.error(f"Error verificando GCS: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_expense_files()
