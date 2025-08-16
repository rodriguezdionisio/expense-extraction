#!/usr/bin/env python3
"""
Script para demostrar la diferencia en los datos procesados.
"""

import pandas as pd
import os

def demonstrate_changes():
    """Demuestra los cambios en los archivos Parquet generados."""
    
    print("📊 DEMOSTRACIÓN DE MEJORAS EN EL PROCESAMIENTO")
    print("="*60)
    print("🎯 PROBLEMA ORIGINAL:")
    print("   - Los JSON extraídos tenían tanto 'id' como 'name' para las entidades")
    print("   - Los archivos Parquet solo conservaban el 'id' (como '_key')")
    print("   - Se perdía la información de nombres legibles")
    print()
    
    print("✅ SOLUCIÓN IMPLEMENTADA:")
    print("   - Modificado expense_processor.py para extraer también los 'names'")
    print("   - Agregadas columnas *_name junto a las columnas *_key existentes")
    print()
    
    # Verificar archivos existentes
    clean_dir = "clean"
    if os.path.exists(clean_dir):
        print("📁 ARCHIVOS PARQUET GENERADOS CON LAS MEJORAS:")
        print("-" * 60)
        
        for root, dirs, files in os.walk(clean_dir):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    df = pd.read_parquet(file_path)
                    
                    print(f"\n📄 {file_path}")
                    print(f"   Filas: {len(df)}")
                    
                    # Mostrar columnas de keys y names
                    key_columns = [col for col in df.columns if '_key' in col and col != 'expense_key']
                    name_columns = [col for col in df.columns if '_name' in col]
                    
                    if key_columns or name_columns:
                        print("   Entidades relacionadas:")
                        
                        # Crear mapeo de keys a names
                        entity_types = set()
                        for col in key_columns:
                            entity_type = col.replace('_key', '')
                            entity_types.add(entity_type)
                        
                        for entity_type in sorted(entity_types):
                            key_col = f"{entity_type}_key"
                            name_col = f"{entity_type}_name"
                            
                            if key_col in df.columns and name_col in df.columns:
                                # Obtener valores únicos no vacíos
                                unique_pairs = df[[key_col, name_col]].drop_duplicates()
                                unique_pairs = unique_pairs[
                                    (unique_pairs[key_col] != 0) & 
                                    (unique_pairs[name_col] != '') & 
                                    (unique_pairs[name_col].notna())
                                ]
                                
                                if not unique_pairs.empty:
                                    print(f"      {entity_type}:")
                                    for _, row in unique_pairs.iterrows():
                                        print(f"        ID {row[key_col]} → '{row[name_col]}'")
        
        print("\n" + "="*60)
        print("✅ BENEFICIOS DE LA MEJORA:")
        print("   ✓ Datos más legibles para análisis")
        print("   ✓ No necesidad de joins adicionales para obtener nombres")
        print("   ✓ Mantiene compatibilidad con las columnas '_key' existentes")
        print("   ✓ Facilita la creación de reportes y dashboards")
        
    else:
        print("❗ No se encontraron archivos procesados.")
        print("   Ejecuta el procesador primero: python expense_processor.py")

if __name__ == "__main__":
    demonstrate_changes()
