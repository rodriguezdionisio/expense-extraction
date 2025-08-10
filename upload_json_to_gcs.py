#!/usr/bin/env python3
"""
Script para subir archivos JSON locales existentes a GCS
"""

import os
from utils.gcp import get_storage_client
from utils.env_config import config
from utils.logger import get_logger

logger = get_logger(__name__)

def upload_existing_json_files():
    """Sube todos los archivos JSON locales a GCS"""
    print("🔄 SUBIENDO ARCHIVOS JSON LOCALES A GCS")
    print("="*60)
    
    try:
        client = get_storage_client()
        if not client:
            print("❌ No se pudo conectar a GCS")
            return
            
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        
        # Buscar archivos JSON locales
        raw_dir = "raw"
        if not os.path.exists(raw_dir):
            print("❌ Directorio raw/ no existe")
            return
            
        json_files = [f for f in os.listdir(raw_dir) if f.startswith('expense_') and f.endswith('.json')]
        
        print(f"📁 Archivos JSON encontrados localmente: {len(json_files)}")
        
        uploaded_count = 0
        for filename in sorted(json_files):
            try:
                local_path = os.path.join(raw_dir, filename)
                gcs_path = f"raw/fact_expenses/{filename}"
                
                # Verificar si ya existe en GCS
                blob = bucket.blob(gcs_path)
                if blob.exists():
                    print(f"⏭️  Ya existe en GCS: {filename}")
                    continue
                
                # Subir archivo
                with open(local_path, 'rb') as f:
                    blob.upload_from_file(f, content_type='application/json')
                
                print(f"✅ Subido: {filename} → {gcs_path}")
                uploaded_count += 1
                
            except Exception as e:
                print(f"❌ Error subiendo {filename}: {e}")
        
        print(f"\n✅ SUBIDA COMPLETADA")
        print(f"📊 Archivos subidos: {uploaded_count}")
        print(f"📊 Archivos ya existían: {len(json_files) - uploaded_count}")
        
    except Exception as e:
        logger.error(f"Error subiendo JSON a GCS: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    upload_existing_json_files()
