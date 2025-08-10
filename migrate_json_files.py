#!/usr/bin/env python3
"""
Script para migrar archivos JSON existentes a la nueva estructura en GCS
"""

from utils.gcp import get_storage_client
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def migrate_existing_json_files():
    """Migra archivos JSON existentes de raw/ a raw/fact_expenses/ en GCS"""
    print("🔄 MIGRANDO ARCHIVOS JSON A NUEVA ESTRUCTURA")
    print("="*60)
    
    try:
        client = get_storage_client()
        if not client:
            print("❌ No se pudo conectar a GCS")
            return
            
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        
        # Buscar archivos en la ubicación anterior (raw/expense_*.json)
        old_blobs = list(bucket.list_blobs(prefix="raw/expense_"))
        json_files_to_move = [blob for blob in old_blobs if blob.name.endswith('.json') and '/fact_expenses/' not in blob.name]
        
        print(f"📁 Archivos encontrados en ubicación anterior: {len(json_files_to_move)}")
        
        moved_count = 0
        for blob in json_files_to_move:
            try:
                # Obtener el nombre del archivo
                filename = blob.name.split('/')[-1]  # expense_X.json
                
                # Nueva ubicación
                new_path = f"raw/fact_expenses/{filename}"
                
                # Copiar a nueva ubicación
                new_blob = bucket.copy_blob(blob, bucket, new_path)
                
                # Eliminar archivo anterior
                blob.delete()
                
                print(f"✅ Movido: {blob.name} → {new_path}")
                moved_count += 1
                
            except Exception as e:
                print(f"❌ Error moviendo {blob.name}: {e}")
        
        print(f"\n✅ MIGRACIÓN COMPLETADA")
        print(f"📊 Archivos migrados: {moved_count}")
        
    except Exception as e:
        logger.error(f"Error en migración: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    migrate_existing_json_files()
